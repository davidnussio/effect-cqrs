/**
 * Durable storage on PostgreSQL, through any `effect/sql` PostgreSQL driver.
 *
 * A sequence can't number events here. Transactions commit in a different order from
 * the one they drew their numbers in, so a view model that has read up to 6 would
 * never come back for a 5 that commits later. Positions come from a counter row
 * instead, which an append holds until it commits: writers wait for each other at
 * that one step, and positions follow commit order with no gaps.
 *
 * Because of that row, appends from one process are written in groups: those that
 * arrive while a group is being written go together into the next one, a single
 * statement that checks each stream's version, takes positions for the appends that
 * pass and inserts their events. An append that fails its check fails alone.
 *
 * The statement also sends a notification with the new head on a channel named
 * after the events table. A store on a `PgClient` listens to it, so view models in
 * every process wake up on every append.
 */
import {
  Clock,
  DateTime,
  type Duration,
  Effect,
  Exit,
  Layer,
  Predicate,
  type Queue,
  Schema,
  type Scope,
  Stream
} from "effect"
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
   * Listens for appends from other processes when the `SqlClient` is a `PgClient`
   * from `@effect/sql-pg`. Listening holds a connection. Defaults to `true`. Turn it
   * off behind a pooler that doesn't keep sessions, such as PgBouncer in transaction
   * mode, and set `pollInterval` instead.
   */
  readonly listen?: boolean | undefined
  /** Reads the head this often, see `EventStore.MakeOptions`. Off by default. */
  readonly pollInterval?: Duration.Input | undefined
}

/** A schema change. Migrations are listed in the order to apply them. */
export interface Migration {
  readonly id: number
  readonly name: string
  /** SQL statements with the table names filled in. Each one can run again without harm. */
  readonly statements: ReadonlyArray<string>
}

const quote = (name: string) => `"${name.replaceAll(`"`, `""`)}"`

const tableNames = (options?: Options) => {
  const events = options?.eventsTable ?? "cqrs_events"
  return {
    events: quote(events),
    head: quote(`${events}_head`),
    typeIndex: quote(`${events}_type_idx`),
    checkpoints: quote(options?.checkpointsTable ?? "cqrs_checkpoints")
  }
}

type Tables = ReturnType<typeof tableNames>

const steps: ReadonlyArray<{
  readonly id: number
  readonly name: string
  readonly table: "events" | "checkpoints"
  readonly statements: (tables: Tables) => ReadonlyArray<string>
}> = [
  {
    id: 1,
    name: "create_events",
    table: "events",
    statements: ({ events, head, typeIndex }) => [
      `CREATE TABLE IF NOT EXISTS ${events} (
  position BIGINT PRIMARY KEY,
  aggregate_name TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  "timestamp" BIGINT NOT NULL,
  UNIQUE (aggregate_name, aggregate_id, version)
)`,
      `CREATE INDEX IF NOT EXISTS ${typeIndex} ON ${events} (type, position)`,
      `CREATE TABLE IF NOT EXISTS ${head} (
  only_row BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (only_row),
  position BIGINT NOT NULL
)`,
      `INSERT INTO ${head} (position) VALUES (0) ON CONFLICT DO NOTHING`
    ]
  },
  {
    id: 2,
    name: "add_causation",
    table: "events",
    statements: ({ events }) => [
      `ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1`,
      `ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS correlation_id TEXT`,
      `ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS causation_id TEXT`
    ]
  },
  {
    id: 3,
    name: "add_metadata",
    table: "events",
    statements: ({ events }) => [`ALTER TABLE ${events} ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'`]
  },
  {
    id: 4,
    name: "create_checkpoints",
    table: "checkpoints",
    statements: ({ checkpoints }) => [
      `CREATE TABLE IF NOT EXISTS ${checkpoints} (
  view_model TEXT PRIMARY KEY,
  position BIGINT NOT NULL
)`
    ]
  }
]

/**
 * The schema of the events and checkpoints tables, for tools that apply migrations
 * themselves. `layer` and `layerCheckpoints` run the same statements on startup
 * unless `migrate` is `false`.
 */
export const migrations = (options?: Options): ReadonlyArray<Migration> => {
  const tables = tableNames(options)
  return steps.map(({ id, name, statements }) => ({ id, name, statements: statements(tables) }))
}

const storeError = (operation: string) => (cause: unknown) => new EventStore.EventStoreError({ operation, cause })

/**
 * Applies the migrations of one table in a transaction. The advisory lock keeps
 * processes that start together from creating the same table at once.
 */
const migrate = (sql: SqlClient.SqlClient, options: Options | undefined, table: "events" | "checkpoints") => {
  const tables = tableNames(options)
  return Effect.gen(function*() {
    yield* sql`SELECT pg_advisory_xact_lock(hashtext(${`effect-cqrs ${tables[table]}`}))`
    for (const step of steps) {
      if (step.table !== table) continue
      for (const statement of step.statements(tables)) yield* sql.unsafe(statement)
    }
  }).pipe(sql.withTransaction, Effect.mapError(storeError("migrate")))
}

const decodeRows = Schema.decodeUnknownEffect(Schema.Array(EventStore.StoredEvent))

const isUniqueViolation = (error: SqlError.SqlError) => error.reason._tag === "UniqueViolation"

/** What listening needs of a `PgClient`, without depending on `@effect/sql-pg`. */
interface Listener {
  readonly listen: (
    channel: string
  ) => Effect.Effect<Queue.Dequeue<{ readonly payload: string }, SqlError.SqlError>, SqlError.SqlError, Scope.Scope>
}

const isListener = (sql: SqlClient.SqlClient): sql is SqlClient.SqlClient & Listener =>
  Predicate.hasProperty(sql, "listen") && Predicate.isFunction(sql.listen)

type AppendExit = Exit.Exit<
  ReadonlyArray<EventStore.StoredEvent>,
  EventStore.ConcurrencyConflict | EventStore.EventStoreError
>

export const make = Effect.fnUntraced(function*(options?: Options) {
  const sql = yield* SqlClient.SqlClient
  const prefix = options?.eventsTable ?? "cqrs_events"
  const events = sql(prefix)
  const counter = sql(`${prefix}_head`)

  if (options?.migrate !== false) yield* migrate(sql, options, "events")

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

  const conflict = (input: EventStore.AppendInput) =>
    new EventStore.ConcurrencyConflict({
      aggregateName: input.aggregateName,
      aggregateId: input.aggregateId,
      expectedVersion: input.expectedVersion
    })

  /**
   * Writes appends to different streams in one statement, so in one transaction. An
   * append whose stream moved on is left out, and only it gets a conflict.
   *
   * The version check can't see a writer that hasn't committed, but the UNIQUE
   * constraint can, and it fails the whole statement. Rolling back gives the
   * positions back, so that leaves no gap.
   */
  const write = (inputs: ReadonlyArray<EventStore.AppendInput>) =>
    Effect.gen(function*() {
      const timestamp = yield* Clock.currentTimeMillis
      const appends = JSON.stringify(inputs.map((input, index) => ({
        index,
        name: input.aggregateName,
        id: input.aggregateId,
        expected: input.expectedVersion,
        count: input.events.length,
        correlation: input.correlationId ?? null,
        causation: input.causationId ?? null,
        metadata: input.metadata ?? {}
      })))
      // Arrays, as there can be many: the append, the index in it, type, schema
      // version and payload. `->` keeps a `null` payload as JSON rather than SQL NULL.
      const newEvents = JSON.stringify(inputs.flatMap((input, index) =>
        input.events.map((event, offset) => [index, offset, event.type, event.schemaVersion ?? 1, event.payload])
      ))
      const accepted = yield* sql<{ readonly index: number; readonly first: number }>`
        WITH input AS (
          SELECT * FROM jsonb_to_recordset(${appends}::jsonb) AS input(
            index INTEGER, name TEXT, id TEXT, expected INTEGER, count INTEGER,
            correlation TEXT, causation TEXT, metadata JSONB
          )
        ),
        accepted AS (
          SELECT input.*, SUM(count) OVER (ORDER BY index) - count AS skipped FROM input
          WHERE expected = (
            SELECT COALESCE(MAX(version), 0) FROM ${events} WHERE aggregate_name = input.name AND aggregate_id = input.id
          )
        ),
        head AS (
          UPDATE ${counter} SET position = position + (SELECT SUM(count) FROM accepted)
          WHERE EXISTS (SELECT FROM accepted)
          RETURNING position, position - (SELECT SUM(count) FROM accepted) AS start
        ),
        inserted AS (
          INSERT INTO ${events} (
            position, aggregate_name, aggregate_id, version, type, schema_version, payload, "timestamp",
            correlation_id, causation_id, metadata
          )
          SELECT
            head.start + accepted.skipped + (event->>1)::integer + 1,
            accepted.name,
            accepted.id,
            accepted.expected + (event->>1)::integer + 1,
            event->>2,
            (event->>3)::integer,
            event->4,
            ${timestamp}::bigint,
            accepted.correlation,
            accepted.causation,
            accepted.metadata
          FROM head, jsonb_array_elements(${newEvents}::jsonb) AS event
          JOIN accepted ON accepted.index = (event->>0)::integer
        )
        SELECT
          index,
          ((SELECT start FROM head) + skipped + 1)::float8 AS first,
          (SELECT pg_notify(${prefix}, position::text) FROM head)::text AS notified
        FROM accepted
      `
      const firsts = new Map(accepted.map(({ first, index }) => [index, first]))
      return inputs.map((input, index): AppendExit => {
        const first = firsts.get(index)
        return first === undefined
          ? Exit.fail(conflict(input))
          : Exit.succeed(input.events.map((event, offset) =>
            EventStore.toStored(input, event, offset, first + offset, DateTime.makeUnsafe(timestamp))
          ))
      })
    })

  const writeOne = (input: EventStore.AppendInput) =>
    write([input]).pipe(
      Effect.flatMap(([exit]) => exit!),
      Effect.catchTag("SqlError", (error) =>
        Effect.fail(isUniqueViolation(error) ? conflict(input) : storeError("append")(error)))
    )

  // A group that fails as a whole is written again one append at a time, so each
  // gets its own outcome: a writer in another process may have taken one stream.
  const appendGroup = (inputs: ReadonlyArray<EventStore.AppendInput>) =>
    inputs.length === 1
      ? Effect.map(Effect.exit(writeOne(inputs[0]!)), (exit) => [exit])
      : write(inputs).pipe(
        Effect.catchTag("SqlError", () => Effect.forEach(inputs, (input) => Effect.exit(writeOne(input))))
      )

  const grouped = yield* EventStore.groupCommit(appendGroup)

  // An append in the caller's transaction must use its connection, and a savepoint
  // keeps a conflict from aborting the rest of that transaction.
  const append = (input: EventStore.AppendInput) =>
    Effect.flatMap(
      Effect.serviceOption(sql.transactionService),
      (transaction) => transaction._tag === "Some" ? writeOne(input).pipe(sql.withTransaction) : grouped(input)
    ).pipe(Effect.catchTag("SqlError", (error) => Effect.fail(storeError("append")(error))))

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

  // The head is read once the listener is in place, so nothing in between is missed.
  const changes = options?.listen === false || !isListener(sql)
    ? undefined
    : Stream.unwrap(Effect.gen(function*() {
      const notifications = yield* sql.listen(prefix)
      const current = yield* head
      return Stream.concat(
        Stream.succeed(current),
        Stream.fromQueue(notifications).pipe(
          Stream.map(({ payload }) => Number(payload)),
          Stream.filter(Number.isSafeInteger)
        )
      )
    })).pipe(
      Stream.mapError((error) => error._tag === "EventStoreError" ? error : storeError("listen")(error))
    )

  return yield* EventStore.make({ append, readBatch, head, changes }, { pollInterval: options?.pollInterval })
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
