/**
 * Durable storage on SQLite, through any `effect/sql` SQLite driver.
 *
 * The schema relies on SQLite's single writer for positions to follow commit order,
 * so it is not portable to other databases.
 *
 * An append is one statement, which checks the stream's version and inserts the
 * events only if it still matches. Appends that arrive while others are being
 * written go together into one transaction, so they share the cost of a commit.
 *
 * @module
 */
import { Clock, DateTime, type Duration, Effect, Exit, Layer, Schema } from "effect"
import { SqlClient, type SqlError } from "effect/sql"
import { Checkpoints } from "./Checkpoints.ts"
import * as EventStore from "./EventStore.ts"

export interface Options {
  readonly eventsTable?: string | undefined
  readonly checkpointsTable?: string | undefined
  /**
   * Creates the tables, or brings tables of an earlier version up to date, on
   * startup. Defaults to `true`. Set it to `false` where the application may not
   * change the schema, and apply {@link migrations} with your own tooling.
   */
  readonly migrate?: boolean | undefined
  /**
   * Reads the head this often, so subscribers notice appends that other processes
   * make to the same file. Off by default. See `EventStore.MakeOptions`.
   */
  readonly pollInterval?: Duration.Input | undefined
}

/** A schema change, see `EventStore.Migration`. Migrations are listed in the order to apply them. */
export type Migration = EventStore.Migration

const quote = (name: string) => `"${name.replaceAll(`"`, `""`)}"`

const tableNames = (options?: Options) => {
  const events = options?.eventsTable ?? "cqrs_events"
  return {
    events: quote(events),
    typeIndex: quote(`${events}_type_idx`),
    checkpoints: quote(options?.checkpointsTable ?? "cqrs_checkpoints")
  }
}

type Tables = ReturnType<typeof tableNames>

/** A column added to a table. Applied on startup only when the table lacks it. */
interface AddColumn {
  readonly table: string
  readonly column: string
  readonly definition: string
}

const addColumn = (table: string, column: string, definition: string): AddColumn => ({ table, column, definition })

const render = (statement: string | AddColumn) =>
  typeof statement === "string"
    ? statement
    : `ALTER TABLE ${statement.table} ADD COLUMN ${statement.column} ${statement.definition}`

const steps: ReadonlyArray<{
  readonly id: number
  readonly name: string
  readonly table: "events" | "checkpoints"
  readonly statements: (tables: Tables) => ReadonlyArray<string | AddColumn>
}> = [
  {
    id: 1,
    name: "create_events",
    table: "events",
    statements: ({ events, typeIndex }) => [
      `CREATE TABLE IF NOT EXISTS ${events} (
  position INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_name TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  UNIQUE (aggregate_name, aggregate_id, version)
)`,
      `CREATE INDEX IF NOT EXISTS ${typeIndex} ON ${events} (type, position)`
    ]
  },
  {
    id: 2,
    name: "add_causation",
    table: "events",
    statements: ({ events }) => [
      addColumn(events, "schema_version", "INTEGER NOT NULL DEFAULT 1"),
      addColumn(events, "correlation_id", "TEXT"),
      addColumn(events, "causation_id", "TEXT")
    ]
  },
  {
    id: 3,
    name: "add_metadata",
    table: "events",
    statements: ({ events }) => [addColumn(events, "metadata", "TEXT NOT NULL DEFAULT '{}'")]
  },
  {
    id: 4,
    name: "create_checkpoints",
    table: "checkpoints",
    statements: ({ checkpoints }) => [
      `CREATE TABLE IF NOT EXISTS ${checkpoints} (
  view_model TEXT PRIMARY KEY,
  position INTEGER NOT NULL
)`
    ]
  }
]

/**
 * The schema of the events and checkpoints tables, for tools that apply migrations
 * themselves. `layer` and `layerCheckpoints` run the same statements on startup
 * unless `migrate` is `false`. SQLite can't add a column only if it is missing, so
 * the `ALTER TABLE` statements must run exactly once.
 */
export const migrations = (options?: Options): ReadonlyArray<Migration> => {
  const tables = tableNames(options)
  return steps.map(({ id, name, statements }) => ({ id, name, statements: statements(tables).map(render) }))
}

const storeError = (operation: string) => (cause: unknown) => new EventStore.EventStoreError({ operation, cause })

/**
 * Applies the migrations of one table in a transaction, which keeps processes that
 * start together apart. A column is only added when the table lacks it, so tables
 * created by any earlier version are brought up to date.
 */
const migrate = (sql: SqlClient.SqlClient, options: Options | undefined, table: "events" | "checkpoints") => {
  const tables = tableNames(options)
  return Effect.gen(function*() {
    const columns = new Map<string, Set<string>>()
    const hasColumn = Effect.fnUntraced(function*({ column, table }: AddColumn) {
      let existing = columns.get(table)
      if (existing === undefined) {
        const rows = yield* sql.unsafe<{ readonly name: string }>(`PRAGMA table_info(${table})`)
        existing = new Set(rows.map(({ name }) => name))
        columns.set(table, existing)
      }
      return existing.has(column)
    })
    for (const step of steps) {
      if (step.table !== table) continue
      for (const statement of step.statements(tables)) {
        if (typeof statement !== "string" && (yield* hasColumn(statement))) continue
        yield* sql.unsafe(render(statement))
      }
    }
  }).pipe(sql.withTransaction, Effect.mapError(storeError("migrate")))
}

const EventRow = Schema.Struct({
  ...EventStore.StoredEvent.fields,
  payload: Schema.fromJsonString(Schema.Json),
  metadata: Schema.fromJsonString(Schema.JsonObject)
})

const decodeRows = Schema.decodeUnknownEffect(Schema.Array(EventRow))

const isUniqueViolation = (error: SqlError.SqlError) => error.reason._tag === "UniqueViolation"

type AppendExit = Exit.Exit<
  ReadonlyArray<EventStore.StoredEvent>,
  EventStore.ConcurrencyConflict | EventStore.EventStoreError
>

export const make = Effect.fnUntraced(function*(options?: Options) {
  const sql = yield* SqlClient.SqlClient
  const events = sql(options?.eventsTable ?? "cqrs_events")

  if (options?.migrate !== false) yield* migrate(sql, options, "events")

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

  /**
   * One statement, so it needs no transaction of its own. Inserting nothing means
   * the stream moved on. A constraint that fails undoes the statement alone, and
   * the transaction around it, if any, goes on.
   */
  const writeOne = (input: EventStore.AppendInput) => {
    const conflict = new EventStore.ConcurrencyConflict({
      aggregateName: input.aggregateName,
      aggregateId: input.aggregateId,
      expectedVersion: input.expectedVersion
    })
    const version = sql`
      SELECT COALESCE(MAX(version), 0) FROM ${events}
      WHERE aggregate_name = ${input.aggregateName} AND aggregate_id = ${input.aggregateId}
    `
    return Effect.gen(function*() {
      if (input.events.length === 0) {
        const [row] = yield* sql<{ readonly version: number }>`SELECT (${version}) AS version`
        return row?.version === input.expectedVersion ? [] : yield* conflict
      }
      const timestamp = yield* Clock.currentTimeMillis
      // Type, schema version and payload as JSON text, which `json_extract` returns as is.
      const newEvents = JSON.stringify(
        input.events.map((event) => [event.type, event.schemaVersion ?? 1, JSON.stringify(event.payload)])
      )
      const rows = yield* sql<{ readonly position: number }>`
        INSERT INTO ${events} (
          aggregate_name, aggregate_id, version, type, schema_version, payload, timestamp, correlation_id, causation_id,
          metadata
        )
        SELECT
          ${input.aggregateName},
          ${input.aggregateId},
          ${input.expectedVersion} + event.key + 1,
          json_extract(event.value, '$[0]'),
          json_extract(event.value, '$[1]'),
          json_extract(event.value, '$[2]'),
          ${timestamp},
          ${input.correlationId ?? null},
          ${input.causationId ?? null},
          ${JSON.stringify(input.metadata ?? {})}
        FROM json_each(${newEvents}) AS event
        WHERE (${version}) = ${input.expectedVersion}
        ORDER BY event.key
        RETURNING position
      `
      if (rows.length === 0) return yield* conflict
      // Positions follow the order of insertion, and RETURNING may not.
      const positions = rows.map(({ position }) => position).sort((a, b) => a - b)
      return input.events.map((event, index) =>
        EventStore.toStored(input, event, index, positions[index]!, DateTime.makeUnsafe(timestamp))
      )
    }).pipe(
      // The version check can't see a writer racing it on a driver that opens
      // deferred transactions, but the UNIQUE constraint can.
      Effect.catchTag(
        "SqlError",
        (error) => Effect.fail(isUniqueViolation(error) ? conflict : storeError("append")(error))
      )
    )
  }

  const appendGroup = (inputs: ReadonlyArray<EventStore.AppendInput>) =>
    inputs.length === 1
      ? Effect.map(Effect.exit(writeOne(inputs[0]!)), (exit): ReadonlyArray<AppendExit> => [exit])
      : Effect.forEach(inputs, (input) => Effect.exit(writeOne(input))).pipe(
        sql.withTransaction,
        // When the commit itself fails, so does every append of the group.
        Effect.catchTag("SqlError", (error) => Effect.succeed(inputs.map(() => Exit.fail(storeError("append")(error)))))
      )

  // Grouping pays where a commit costs an fsync, so not for a database in memory.
  // `node:sqlite` is synchronous, so writers only wait for each other if each lets
  // the others run first.
  const inMemory = yield* sql<{ readonly file: string }>`SELECT file FROM pragma_database_list WHERE name = 'main'`
    .pipe(
      Effect.map(([row]) => row?.file === ""),
      Effect.orElseSucceed(() => false)
    )
  const grouped = inMemory ? writeOne : yield* EventStore.groupCommit(appendGroup, { gather: true })

  // An append in the caller's transaction is written there and then.
  const append = (input: EventStore.AppendInput) =>
    Effect.flatMap(
      Effect.serviceOption(sql.transactionService),
      (transaction) => transaction._tag === "Some" ? writeOne(input) : grouped(input)
    )

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

  return yield* EventStore.make({ append, readBatch, head }, { pollInterval: options?.pollInterval })
})

export const layer = (
  options?: Options
): Layer.Layer<EventStore.EventStore, EventStore.EventStoreError, SqlClient.SqlClient> =>
  Layer.effect(EventStore.EventStore, make(options))

/** Keeps checkpoints in the same database, for view models that are durable too. */
export const layerCheckpoints = (
  options?: Options
): Layer.Layer<Checkpoints, EventStore.EventStoreError, SqlClient.SqlClient> =>
  Layer.effect(
    Checkpoints,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const table = sql(options?.checkpointsTable ?? "cqrs_checkpoints")

      if (options?.migrate !== false) yield* migrate(sql, options, "checkpoints")

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
          ),
        // A row that doesn't exist yet stands for `0`, and is inserted whatever `from` is.
        advance: (viewModel, from, to) =>
          sql<{ readonly position: number }>`
            INSERT INTO ${table} (view_model, position) VALUES (${viewModel}, ${to})
            ON CONFLICT (view_model) DO UPDATE SET position = excluded.position
            WHERE ${table}.position = ${from}
            RETURNING position
          `.pipe(
            Effect.map((rows) => rows.length > 0),
            Effect.mapError(storeError("checkpoint"))
          )
      })
    })
  )
