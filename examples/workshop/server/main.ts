/**
 * The workshop server, see ../README.md.
 *
 * Configuration comes from the environment:
 *
 * - `PORT`, 3000 by default. Render sets it.
 * - `POSTGRES_URL` stores events in PostgreSQL. On Supabase use the session
 *   pooler with `?sslmode=require`. Without it events go to the SQLite file
 *   `SQLITE_FILE`, `workshop.sqlite` by default, which is enough on a laptop.
 * - `POSTGRES_CA_CERT`, a PEM certificate to verify the database with, when its
 *   certificate isn't signed by a public authority.
 * - `AUTH_SECRET` signs the tokens, `ADMIN_CODE` lets the presenter join as admin.
 * - `PERSONAL_DATA_MASTER_KEY_ID` and `PERSONAL_DATA_MASTER_KEY`, 32 bytes in
 *   base64, wrap the keys of personal data.
 * - `CORS_ORIGIN`, the origins the UI is served from, separated by commas. Without
 *   it any origin may call the API.
 * - `RENDER_GIT_COMMIT`, set by Render, is reported by `/health`.
 *
 * Without `AUTH_SECRET` or the master key the server uses development values,
 * unless `NODE_ENV` is `production`.
 */
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Config, Effect, Layer, Option, Redacted } from "effect"
import { PersonalData, PostgresEventStore, SqliteEventStore } from "effect-cqrs"
import { HttpRouter } from "effect/http"
import { HttpApiBuilder, HttpApiSwagger } from "effect/http-api"
import { createServer } from "node:http"
import { Api } from "../api.ts"
import { BankLive } from "../domain/bank.ts"
import * as Auth from "./auth.ts"
import { Handlers, StoreKind } from "./http.ts"

const isProduction = Config.String("NODE_ENV").pipe(
  Config.withDefault("development"),
  Config.map((env) => env === "production")
)

/** The event store, the SQL client it uses and what kind it is. */
const Storage = Layer.unwrap(Effect.gen(function*() {
  const url = yield* Config.option(Config.Redacted("POSTGRES_URL"))
  if (Option.isSome(url)) {
    const ca = yield* Config.option(Config.String("POSTGRES_CA_CERT"))
    yield* Effect.logInfo("Storing events in PostgreSQL")
    const client = PgClient.layer({
      url: url.value,
      applicationName: "effect-cqrs-workshop",
      ...(Option.isSome(ca) ? { ssl: { ca: ca.value } } : {})
    })
    return Layer.mergeAll(PostgresEventStore.layer(), Layer.succeed(StoreKind, "postgres")).pipe(
      Layer.provideMerge(client)
    )
  }
  const filename = yield* Config.String("SQLITE_FILE").pipe(Config.withDefault("workshop.sqlite"))
  yield* Effect.logInfo(`Storing events in ${filename}`)
  return Layer.mergeAll(SqliteEventStore.layer(), Layer.succeed(StoreKind, "sqlite")).pipe(
    Layer.provideMerge(SqliteClient.layer({ filename }))
  )
}))

/** 32 bytes anyone can read in this file: fine on a laptop, refused in production. */
const developmentMasterKey = "d29ya3Nob3AtZGV2ZWxvcG1lbnQtbWFzdGVyLWtleSE="

const MasterKey = Layer.unwrap(Effect.gen(function*() {
  const configured = yield* Config.option(Config.Redacted("PERSONAL_DATA_MASTER_KEY"))
  if (Option.isSome(configured)) return PersonalData.layerMasterKeyConfig()
  if (yield* isProduction) return yield* Effect.die("PERSONAL_DATA_MASTER_KEY is required when NODE_ENV is production")
  yield* Effect.logWarning("PERSONAL_DATA_MASTER_KEY is not set: personal data is wrapped with the development key")
  return PersonalData.layerMasterKeyConfig({
    keyId: Config.succeed("development"),
    key: Config.succeed(Redacted.make(developmentMasterKey))
  })
}))

/** The keys of personal data, in the same database as the events. */
const Keys = PersonalData.layerSql().pipe(Layer.provide(MasterKey))

const Bank = BankLive.pipe(Layer.provideMerge(Keys), Layer.provideMerge(Storage))

const Cors = Layer.unwrap(Effect.gen(function*() {
  const origins = yield* Config.option(Config.String("CORS_ORIGIN"))
  const allowedOrigins = Option.match(origins, {
    onNone: () => [],
    onSome: (value) => value.split(",").map((origin) => origin.trim()).filter((origin) => origin !== "")
  })
  // Without a list of headers the answer allows those the browser asks for, which
  // include `traceparent`: Effect's HTTP client sends it to carry the trace.
  return HttpRouter.cors({ allowedOrigins, maxAge: 600 })
}))

const Routes = Layer.mergeAll(
  HttpApiBuilder.layer(Api, { openapiPath: "/openapi.json" }).pipe(Layer.provide(Handlers)),
  HttpApiSwagger.layer(Api, { path: "/docs" }),
  Cors
)

const HttpLive = Layer.unwrap(Effect.gen(function*() {
  const port = yield* Config.Port("PORT").pipe(Config.withDefault(3000))
  return HttpRouter.serve(Routes).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port })))
}))

const Main = HttpLive.pipe(Layer.provide(Auth.layerConfig), Layer.provide(Bank))

Layer.launch(Main).pipe(NodeRuntime.runMain)
