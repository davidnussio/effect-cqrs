/**
 * Durable storage on SQLite, through any `effect/sql` SQLite driver.
 *
 * The tables are created on startup. The schema relies on SQLite's single writer
 * for positions to follow commit order, so it is not portable to other databases.
 */
import { Clock, DateTime, Effect, Layer, Schema } from "effect"
import { SqlClient, type SqlError } from "effect/sql"
import { Checkpoints } from "./Checkpoints.ts"
import * as EventStore from "./EventStore.ts"

export interface Options {
  readonly eventsTable?: string
  readonly checkpointsTable?: string
}

const EventRow = Schema.Struct({
  ...EventStore.StoredEvent.fields,
  payload: Schema.fromJsonString(Schema.Json),
  metadata: Schema.fromJsonString(Schema.JsonObject)
})

const decodeRows = Schema.decodeUnknownEffect(Schema.Array(EventRow))

const storeError = (operation: string) => (cause: unknown) => new EventStore.EventStoreError({ operation, cause })

const isUniqueViolation = (error: SqlError.SqlError) => error.reason._tag === "UniqueViolation"

const addedColumns: ReadonlyArray<readonly [string, string]> = [
  ["schema_version", "INTEGER NOT NULL DEFAULT 1"],
  ["correlation_id", "TEXT"],
  ["causation_id", "TEXT"],
  ["metadata", "TEXT NOT NULL DEFAULT '{}'"]
]

export const make = Effect.fnUntraced(function*(options?: Options) {
  const sql = yield* SqlClient.SqlClient
  const events = sql(options?.eventsTable ?? "cqrs_events")
  const indexPrefix = options?.eventsTable ?? "cqrs_events"

  yield* sql`
    CREATE TABLE IF NOT EXISTS ${events} (
      position INTEGER PRIMARY KEY AUTOINCREMENT,
      aggregate_name TEXT NOT NULL,
      aggregate_id TEXT NOT NULL,
      version INTEGER NOT NULL,
      type TEXT NOT NULL,
      schema_version INTEGER NOT NULL DEFAULT 1,
      payload TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      correlation_id TEXT,
      causation_id TEXT,
      metadata TEXT NOT NULL DEFAULT '{}',
      UNIQUE (aggregate_name, aggregate_id, version)
    )
  `.pipe(Effect.mapError(storeError("initialize")))
  // Tables created before these columns existed get them here. SQLite has no
  // `ADD COLUMN IF NOT EXISTS`, so the existing columns are listed first.
  const existing = yield* sql<{ readonly name: string }>`PRAGMA table_info(${events})`.pipe(
    Effect.map((rows) => new Set(rows.map(({ name }) => name))),
    Effect.mapError(storeError("initialize"))
  )
  yield* Effect.forEach(
    addedColumns.filter(([name]) => !existing.has(name)),
    ([name, definition]) =>
      sql`ALTER TABLE ${events} ADD COLUMN ${sql(name)} ${sql.literal(definition)}`.pipe(
        Effect.mapError(storeError("initialize"))
      ),
    { discard: true }
  )
  yield* sql`
    CREATE INDEX IF NOT EXISTS ${sql(`${indexPrefix}_type_idx`)} ON ${events} (type, position)
  `.pipe(Effect.mapError(storeError("initialize")))

  const columns = sql.literal(`
    position,
    aggregate_name AS "aggregateName",
    aggregate_id AS "aggregateId",
    version,
    type,
    schema_version AS "schemaVersion",
    payload,
    timestamp,
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
      const metadata = JSON.stringify(input.metadata ?? {})
      return yield* Effect.forEach(input.events, (event, index) =>
        sql<{ readonly position: number }>`
          INSERT INTO ${events} (
            aggregate_name, aggregate_id, version, type, schema_version, payload, timestamp, correlation_id, causation_id,
            metadata
          )
          VALUES (
            ${input.aggregateName},
            ${input.aggregateId},
            ${input.expectedVersion + index + 1},
            ${event.type},
            ${event.schemaVersion ?? 1},
            ${JSON.stringify(event.payload)},
            ${timestamp},
            ${input.correlationId ?? null},
            ${input.causationId ?? null},
            ${metadata}
          )
          RETURNING position
        `.pipe(
          Effect.map(([row]) => EventStore.toStored(input, event, index, row!.position, DateTime.makeUnsafe(timestamp)))
        ))
    }).pipe(
      sql.withTransaction,
      // The version check can't see a writer racing it on a driver that opens
      // deferred transactions, but the UNIQUE constraint can.
      Effect.catchTag("SqlError", (error) => Effect.fail(isUniqueViolation(error) ? conflict : storeError("append")(error)))
    )
  }

  const readBatch = (filter: EventStore.EventFilter, afterPosition: number, limit: number) => {
    const oneStream = EventStore.isOneStream(filter)
    const clauses = [
      sql`position > ${afterPosition}`,
      ...(oneStream ? [sql`version > ${filter.afterVersion ?? 0}`] : []),
      ...(filter.aggregateNames === undefined ? [] : [sql.in("aggregate_name", filter.aggregateNames)]),
      ...(filter.aggregateIds === undefined ? [] : [sql.in("aggregate_id", filter.aggregateIds)]),
      ...(filter.eventTypes === undefined ? [] : [sql.in("type", filter.eventTypes)]),
      ...(filter.startTime === undefined ? [] : [sql`timestamp >= ${DateTime.toEpochMillis(filter.startTime)}`]),
      ...(filter.finishTime === undefined ? [] : [sql`timestamp < ${DateTime.toEpochMillis(filter.finishTime)}`])
    ]
    // Inside a stream versions follow positions, and only versions are indexed.
    const order = oneStream ? sql`version` : sql`position`
    return sql`SELECT ${columns} FROM ${events} WHERE ${sql.and(clauses)} ORDER BY ${order} LIMIT ${limit}`.pipe(
      Effect.flatMap(decodeRows),
      Effect.mapError(storeError("read"))
    )
  }

  const head = sql<{ readonly position: number }>`SELECT COALESCE(MAX(position), 0) AS position FROM ${events}`.pipe(
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
          position INTEGER NOT NULL
        )
      `.pipe(Effect.mapError(storeError("initialize")))

      return Checkpoints.of({
        get: (viewModel) =>
          sql<{ readonly position: number }>`SELECT position FROM ${table} WHERE view_model = ${viewModel}`.pipe(
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
