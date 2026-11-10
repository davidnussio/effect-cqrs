/**
 * Empties the workshop's database, for a clean start after a rehearsal: it drops
 * the framework's tables, events, checkpoints and keys of personal data alike,
 * and the server creates them again when it starts. Stop the server first.
 *
 *   RESET=yes POSTGRES_URL=... pnpm workshop:reset
 *   RESET=yes pnpm workshop:reset              # the SQLite file, workshop.sqlite by default
 *
 * There is no undo: the events are the only copy of what happened.
 */
import { NodeRuntime } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Config, Effect, Layer, Option } from "effect"
import { SqlClient } from "effect/sql"

const Database = Layer.unwrap(Effect.gen(function*() {
  const url = yield* Config.option(Config.Redacted("POSTGRES_URL"))
  if (Option.isSome(url)) return PgClient.layer({ url: url.value })
  const filename = yield* Config.String("SQLITE_FILE").pipe(Config.withDefault("workshop.sqlite"))
  return SqliteClient.layer({ filename })
}))

const program = Effect.gen(function*() {
  const confirmed = yield* Config.String("RESET").pipe(Config.withDefault(""))
  if (confirmed !== "yes") {
    return yield* Effect.fail("This drops every event of the workshop. Run it again with RESET=yes to go ahead.")
  }
  const sql = yield* SqlClient.SqlClient
  const postgres = Option.isSome(yield* Config.option(Config.String("POSTGRES_URL")))
  const tables = postgres
    ? yield* sql<{ readonly name: string }>`
        SELECT table_name AS name FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name LIKE 'cqrs\\_%'
      `
    : yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'cqrs\\_%' ESCAPE '\\'
      `
  if (tables.length === 0) return yield* Effect.logInfo("Nothing to drop: the database is already empty")
  for (const { name } of tables) {
    yield* postgres ? sql`DROP TABLE IF EXISTS ${sql(name)} CASCADE` : sql`DROP TABLE IF EXISTS ${sql(name)}`
  }
  yield* Effect.logInfo(`Dropped ${tables.map(({ name }) => name).join(", ")}. Start the server again.`)
}).pipe(Effect.provide(Database))

NodeRuntime.runMain(program)
