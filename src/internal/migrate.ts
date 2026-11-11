import { Effect } from "effect"
import type { SqlClient } from "effect/sql"

/**
 * Runs the statements that create or update one table. On PostgreSQL they run in a
 * transaction that first takes an advisory lock named after the table: replicas
 * that start together would otherwise run `CREATE TABLE IF NOT EXISTS` at once, and
 * all but one can fail with a unique violation on `pg_type`.
 */
export const migrateTable = (sql: SqlClient.SqlClient, table: string, statements: ReadonlyArray<string>) => {
  const run = Effect.forEach(statements, (statement) => sql.unsafe(statement), { discard: true })
  return sql.onDialectOrElse({
    pg: () =>
      sql`SELECT pg_advisory_xact_lock(hashtext(${`effect-cqrs ${table}`}))`.pipe(
        Effect.andThen(run),
        sql.withTransaction
      ),
    orElse: () => run
  })
}
