import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { ParkedEvents, PersonalData } from "effect-cqrs"
import { SqlClient } from "effect/sql"
import { postgresClient, postgresUrl } from "../bench/common.ts"

/** As many replicas as the pool has connections, all creating one table at once. */
const replicas = 8

const startTogether = (table: string, create: Effect.Effect<unknown, unknown, SqlClient.SqlClient>) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    const results = yield* Effect.all(Array.from({ length: replicas }, () => Effect.exit(create)), {
      concurrency: "unbounded"
    })
    const [row] = yield* sql<{ readonly exists: boolean }>`SELECT to_regclass(${table}) IS NOT NULL AS "exists"`
    yield* sql`DROP TABLE IF EXISTS ${sql(table)}`
    return { failures: results.filter((exit) => exit._tag === "Failure").length, exists: row?.exists === true }
  }).pipe(Effect.provide(Layer.fresh(postgresClient())))

// Needs the database of `docker-compose.yml` and POSTGRES_URL.
describe.skipIf(postgresUrl === undefined)("tables created by replicas that start together", () => {
  it.effect("the keys of personal data", () =>
    Effect.gen(function*() {
      const table = `test_keys_${crypto.randomUUID().replaceAll("-", "")}`
      const outcome = yield* startTogether(table, PersonalData.makeSqlBackend({ table }))
      assert.deepStrictEqual(outcome, { failures: 0, exists: true })
    }))

  it.effect("parked events", () =>
    Effect.gen(function*() {
      const table = `test_parked_${crypto.randomUUID().replaceAll("-", "")}`
      const outcome = yield* startTogether(table, ParkedEvents.makeSql({ table }))
      assert.deepStrictEqual(outcome, { failures: 0, exists: true })
    }))
})
