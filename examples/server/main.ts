/**
 * The todo aggregate behind an HTTP server, see README.md.
 *
 * Configuration comes from the environment:
 *
 * - `PORT`, 3000 by default.
 * - `POSTGRES_URL` stores events in PostgreSQL. Without it they go to the SQLite
 *   file `SQLITE_FILE`, `todo-server.sqlite` by default.
 * - `OTEL_EXPORTER_OTLP_ENDPOINT` sends traces, metrics and logs over OTLP/HTTP,
 *   for example to `http://localhost:4318`. Without it nothing is exported.
 */
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Config, Effect, Layer, Option } from "effect"
import { FetchHttpClient, HttpRouter } from "effect/http"
import { HttpApiBuilder, HttpApiScalar } from "effect/http-api"
import { Otlp } from "effect/observability"
import { AggregateService, Checkpoints, CommandApi, PostgresEventStore, SqliteEventStore, ViewModel } from "effect-cqrs"
import { createServer } from "node:http"
import { Todo } from "../todo/aggregate.ts"
import { listTodos, TodoList, TodoListStore } from "../todo/view-model.ts"
import { Api } from "./api.ts"

const CommandHandlers = CommandApi.layer(Api, "todo", AggregateService.make(Todo))

const QueryHandlers = HttpApiBuilder.group(Api, "todoList", (handlers) =>
  handlers.handle("list", ({ query }) =>
    Effect.gen(function*() {
      if (query.position !== undefined) {
        const viewModels = yield* ViewModel.ViewModels
        // Reading your own writes: wait a little for the list to see the command.
        yield* viewModels.caughtUp(query.position).pipe(Effect.timeout("5 seconds"), Effect.ignore)
      }
      return yield* listTodos
    })))

const Routes = Layer.mergeAll(
  HttpApiBuilder.layer(Api, { openapiPath: "/openapi.json" }).pipe(
    Layer.provide([CommandHandlers, QueryHandlers])
  ),
  HttpApiScalar.layer(Api, { path: "/docs" })
)

const HttpLive = Layer.unwrap(Effect.gen(function*() {
  const port = yield* Config.Port("PORT").pipe(Config.withDefault(3000))
  return HttpRouter.serve(Routes).pipe(Layer.provide(NodeHttpServer.layer(createServer, { port })))
}))

const Storage = Layer.unwrap(Effect.gen(function*() {
  const url = yield* Config.option(Config.Redacted("POSTGRES_URL"))
  if (Option.isSome(url)) {
    yield* Effect.logInfo("Storing events in PostgreSQL")
    return PostgresEventStore.layer().pipe(Layer.provide(PgClient.layer({ url: url.value })))
  }
  const filename = yield* Config.String("SQLITE_FILE").pipe(Config.withDefault("todo-server.sqlite"))
  yield* Effect.logInfo(`Storing events in ${filename}`)
  return SqliteEventStore.layer().pipe(Layer.provide(SqliteClient.layer({ filename })))
}))

const Observability = Layer.unwrap(Effect.gen(function*() {
  const endpoint = yield* Config.option(Config.String("OTEL_EXPORTER_OTLP_ENDPOINT"))
  if (Option.isNone(endpoint)) return Layer.empty
  yield* Effect.logInfo(`Exporting telemetry to ${endpoint.value}`)
  return Otlp.layerJson({
    baseUrl: endpoint.value,
    resource: { serviceName: "effect-cqrs-todo-server", serviceVersion: "1.0.0" }
  }).pipe(Layer.provide(FetchHttpClient.layer))
}))

// The list lives in memory, so its checkpoint does too: it is rebuilt from the
// store on every start.
const Services = ViewModel.layer([TodoList]).pipe(
  Layer.provideMerge(Layer.mergeAll(Storage, Checkpoints.layerMemory, TodoListStore.layer))
)

const Main = HttpLive.pipe(Layer.provide(Services), Layer.provide(Observability))

Layer.launch(Main).pipe(NodeRuntime.runMain)
