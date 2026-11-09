/**
 * Durable storage on PostgreSQL, through any `effect/sql` PostgreSQL driver.
 *
 * A sequence can't number events here. Transactions commit in a different order from
 * the one they drew their numbers in, so a view model that has read up to 6 would
 * never come back for a 5 that commits later. Positions come from a counter row
 * instead, which an append holds until it commits: writers wait for each other at
 * that one step, and positions follow commit order with no gaps.
 *
 * The tables are created on startup.
 */
import { Array as Arr, Clock, DateTime, Effect, Layer, Schema } from "effect"
import { SqlClient, type SqlError } from "effect/sql"
import { Checkpoints } from "./Checkpoints.ts"
import * as EventStore from "./EventStore.ts"

export interface Options {
  readonly eventsTable?: string
  readonly checkpointsTable?: string
}

const decodeRows = Schema.decodeUnknownEffect(Schema.Array(EventStore.StoredEvent))

const storeError = (operation: string) => (cause: unknown) => new EventStore.EventStoreError({ operation, cause })

const isUniqueViolation = (error: SqlError.SqlError) => error.reason._tag === "UniqueViolation"

// The client binds at most 32,767 parameters to a statement, and each row uses 11.
const rowsPerInsert = 2_900

export const make = Effect.fnUntraced(function*(options?: Options) {
  const sql = yield* SqlClient.SqlClient
  const prefix = options?.eventsTable ?? "cqrs_events"
  const events = sql(prefix)
  const counter = sql(`${prefix}_head`)

  const initialize = Effect.gen(function*() {
    yield* sql`
      CREATE TABLE IF NOT EXISTS ${events} (
        position BIGINT PRIMARY KEY,
        aggregate_name TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        type TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1,
        payload JSONB NOT NULL,
        "timestamp" BIGINT NOT NULL,
        correlation_id TEXT,
        causation_id TEXT,
        metadata JSONB NOT NULL DEFAULT '{}',
        UNIQUE (aggregate_name, aggregate_id, version)
      )
    `
    // Tables created before these columns existed get them here.
    yield* sql`ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1`
    yield* sql`ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS correlation_id TEXT`
    yield* sql`ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS causation_id TEXT`
    yield* sql`ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'`
    yield* sql`CREATE INDEX IF NOT EXISTS ${sql(`${prefix}_type_idx`)} ON ${events} (type, position)`
    yield* sql`
      CREATE TABLE IF NOT EXISTS ${counter} (
        only_row BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (only_row),
        position BIGINT NOT NULL
      )
    `
    yield* sql`INSERT INTO ${counter} (position) VALUES (0) ON CONFLICT DO NOTHING`
  })
  yield* initialize.pipe(Effect.mapError(storeError("initialize")))

  const columns = sql.literal(`
    position::float8 AS position,
    aggregate_name AS "aggregateName",
    aggregate_id AS "aggregateId",
    version,
    type,
    schema_version AS "schemaVersion",
    payload,
    "timestamp"::float8 AS "timestamp",
    correlation_id AS "correlationId",
    causation_id AS "causationId",
    metadata
  `)

  const append = (input: EventStore.AppendInput) => {
    const conflict = new EventStore.ConcurrencyConflict({
      aggregateName: input.aggregateName,
      aggregateId: input.aggregateId,
      expectedVersion: input.expectedVersion
    })
    return Effect.gen(function*() {
      const [current] = yield* sql<{ readonly version: number }>`
        SELECT COALESCE(MAX(version), 0) AS version FROM ${events}
        WHERE aggregate_name = ${input.aggregateName} AND aggregate_id = ${input.aggregateId}
      `
      if (current?.version !== input.expectedVersion) {
        return yield* conflict
      }
      const timestamp = yield* Clock.currentTimeMillis
      const [head] = yield* sql<{ readonly position: number }>`
        UPDATE ${counter} SET position = position + ${input.events.length} RETURNING position::float8 AS position
      `
      const first = head!.position - input.events.length + 1
      const stored = input.events.map((event, index) =>
        EventStore.toStored(input, event, index, first + index, DateTime.makeUnsafe(timestamp))
      )
      const rows = stored.map((event) => ({
        position: event.position,
        aggregate_name: event.aggregateName,
        aggregate_id: event.aggregateId,
        version: event.version,
        type: event.type,
        schema_version: event.schemaVersion,
        payload: JSON.stringify(event.payload),
        timestamp,
        correlation_id: event.correlationId,
        causation_id: event.causationId,
        metadata: JSON.stringify(event.metadata)
      }))
      yield* Effect.forEach(
        Arr.chunksOf(rows, rowsPerInsert),
        (chunk) => sql`INSERT INTO ${events} ${sql.insert(chunk)}`,
        { discard: true }
      )
      return stored
    }).pipe(
      sql.withTransaction,
      // The version check can't see a writer that hasn't committed, but the UNIQUE
      // constraint can. Rolling back gives the position back, so that leaves no gap.
      Effect.catchTag("SqlError", (error) => Effect.fail(isUniqueViolation(error) ? conflict : storeError("append")(error)))
    )
  }

  const readBatch = (filter: EventStore.EventFilter, afterPosition: number, limit: number) => {
    const oneStream = EventStore.isOneStream(filter)
    const clauses = [
      sql`${events}.position > ${afterPosition}`,
      ...(oneStream ? [sql`version > ${filter.afterVersion ?? 0}`] : []),
      ...(filter.aggregateNames === undefined ? [] : [sql.in("aggregate_name", filter.aggregateNames)]),
      ...(filter.aggregateIds === undefined ? [] : [sql.in("aggregate_id", filter.aggregateIds)]),
      ...(filter.eventTypes === undefined ? [] : [sql.in("type", filter.eventTypes)]),
      ...(filter.startTime === undefined ? [] : [sql`"timestamp" >= ${DateTime.toEpochMillis(filter.startTime)}`]),
      ...(filter.finishTime === undefined ? [] : [sql`"timestamp" < ${DateTime.toEpochMillis(filter.finishTime)}`])
    ]
    // Inside a stream versions follow positions, and ordering by them keeps to the
    // stream's index. With `LIMIT` the planner would otherwise walk the primary key,
    // through every later event of the log, looking for a stream with nothing new.
    // `position` alone would mean the converted column above, which has no index.
    const order = oneStream ? sql`version` : sql`${events}.position`
    return sql`SELECT ${columns} FROM ${events} WHERE ${sql.and(clauses)} ORDER BY ${order} LIMIT ${limit}`.pipe(
      Effect.flatMap(decodeRows),
      Effect.mapError(storeError("read"))
    )
  }

  const head = sql<{ readonly position: number }>`SELECT position::float8 AS position FROM ${counter}`.pipe(
    Effect.map(([row]) => row?.position ?? 0),
    Effect.mapError(storeError("head"))
  )

  return yield* EventStore.make({ append, readBatch, head })
})

export const layer = (options?: Options): Layer.Layer<EventStore.EventStore, EventStore.EventStoreError, SqlClient.SqlClient> =>
  Layer.effect(EventStore.EventStore, make(options))

/** Keeps checkpoints in the same database, for view models that are durable too. */
export const layerCheckpoints = (options?: Options): Layer.Layer<Checkpoints, EventStore.EventStoreError, SqlClient.SqlClient> =>
  Layer.effect(
    Checkpoints,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const table = sql(options?.checkpointsTable ?? "cqrs_checkpoints")

      yield* sql`
        CREATE TABLE IF NOT EXISTS ${table} (
          view_model TEXT PRIMARY KEY,
          position BIGINT NOT NULL
        )
      `.pipe(Effect.mapError(storeError("initialize")))

      return Checkpoints.of({
        get: (viewModel) =>
          sql<{ readonly position: number }>`
            SELECT position::float8 AS position FROM ${table} WHERE view_model = ${viewModel}
          `.pipe(
            Effect.map(([row]) => row?.position ?? 0),
            Effect.mapError(storeError("checkpoint"))
          ),
        set: (viewModel, position) =>
          sql`
            INSERT INTO ${table} (view_model, position) VALUES (${viewModel}, ${position})
            ON CONFLICT (view_model) DO UPDATE SET position = excluded.position
          `.pipe(
            Effect.asVoid,
            Effect.mapError(storeError("checkpoint"))
          )
      })
    })
  )
