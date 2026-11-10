/**
 * Events a view model gave up on, so the rest of the log could move past them.
 *
 * A view model with the `ViewModel.park` failure policy records an event here
 * once it has failed often enough, skips it from then on, and handles it again
 * when `ViewModels.retryParked` asks it to.
 *
 * @module
 */
import { Context, Effect, Layer, Schema } from "effect"
import { SqlClient } from "effect/sql"
import { EventStoreError, type Migration } from "./EventStore.ts"

export const ParkedEvent = Schema.Struct({
  viewModel: Schema.String,
  /** The event's position in the store. */
  position: Schema.Int,
  /** The event's id, as `EventStore.eventId` gives it: `account/alice@4`. */
  eventId: Schema.String,
  /** The last failure, as text. */
  error: Schema.String,
  /** How many times the view model tried to handle it. */
  attempts: Schema.Int,
  /** When it was parked, or last retried without success. */
  parkedAt: Schema.DateTimeUtcFromMillis
})
export type ParkedEvent = typeof ParkedEvent.Type

export class ParkedEvents extends Context.Service<ParkedEvents, {
  /** Records an event, replacing an earlier record of the same one. */
  park(event: ParkedEvent): Effect.Effect<void, EventStoreError>
  /** The parked events of one view model, or of all of them, by view model and position. */
  list(viewModel?: string): Effect.Effect<ReadonlyArray<ParkedEvent>, EventStoreError>
  /** Forgets an event, once it has been handled. */
  remove(viewModel: string, position: number): Effect.Effect<void, EventStoreError>
}>()("effect-cqrs/ParkedEvents") {}

const byPlace = (a: ParkedEvent, b: ParkedEvent) =>
  a.viewModel < b.viewModel ? -1 : a.viewModel > b.viewModel ? 1 : a.position - b.position

const keyOf = (viewModel: string, position: number) => JSON.stringify([viewModel, position])

/** Parked events in process memory. They do not survive a restart. */
export const makeMemory: Effect.Effect<ParkedEvents["Service"]> = Effect.sync(() => {
  const events = new Map<string, ParkedEvent>()
  return ParkedEvents.of({
    park: (event) => Effect.sync(() => void events.set(keyOf(event.viewModel, event.position), event)),
    list: (viewModel) =>
      Effect.sync(() =>
        Array.from(events.values())
          .filter((event) => viewModel === undefined || event.viewModel === viewModel)
          .sort(byPlace)
      ),
    remove: (viewModel, position) => Effect.sync(() => void events.delete(keyOf(viewModel, position)))
  })
})

export const layerMemory: Layer.Layer<ParkedEvents> = Layer.effect(ParkedEvents, makeMemory)

export interface SqlOptions {
  /** Defaults to `cqrs_parked_events`. */
  readonly table?: string | undefined
  /**
   * Creates the table on startup when it is missing. Defaults to `true`. Turn it
   * off where the schema is managed elsewhere, from {@link migrations}.
   */
  readonly migrate?: boolean | undefined
}

const quote = (identifier: string) => `"${identifier.replaceAll("\"", "\"\"")}"`

/**
 * The migrations of the table, the same on SQLite and PostgreSQL. They are what
 * {@link layerSql} runs unless `migrate` is `false`, and are safe to run again.
 */
export const migrations = (options?: Pick<SqlOptions, "table">): ReadonlyArray<Migration> => [{
  id: 1,
  name: "create_parked_events",
  statements: [`CREATE TABLE IF NOT EXISTS ${quote(options?.table ?? "cqrs_parked_events")} (
  view_model TEXT NOT NULL,
  position BIGINT NOT NULL,
  event_id TEXT NOT NULL,
  error TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  parked_at BIGINT NOT NULL,
  PRIMARY KEY (view_model, position)
)`]
}]

const decodeRows = Schema.decodeUnknownEffect(Schema.Array(ParkedEvent))
const encode = Schema.encodeSync(ParkedEvent)

const storeError = (operation: string) => (cause: unknown) => new EventStoreError({ operation, cause })

/** Parked events in a SQL table, on SQLite or PostgreSQL. */
export const makeSql = Effect.fnUntraced(function*(options?: SqlOptions) {
  const sql = yield* SqlClient.SqlClient
  const table = sql(options?.table ?? "cqrs_parked_events")

  if (options?.migrate ?? true) {
    yield* Effect.forEach(
      migrations(options).flatMap(({ statements }) => statements),
      (statement) => sql.unsafe(statement),
      { discard: true }
    ).pipe(
      Effect.mapError(storeError("initialize"))
    )
  }

  // `BIGINT` comes back from PostgreSQL as text, so it is converted on the way out.
  const columns = sql.literal(`
    view_model AS "viewModel",
    CAST(position AS DOUBLE PRECISION) AS position,
    event_id AS "eventId",
    error,
    attempts,
    CAST(parked_at AS DOUBLE PRECISION) AS "parkedAt"
  `)

  return ParkedEvents.of({
    park: (event) => {
      const row = encode(event)
      return sql`
        INSERT INTO ${table} (view_model, position, event_id, error, attempts, parked_at)
        VALUES (${row.viewModel}, ${row.position}, ${row.eventId}, ${row.error}, ${row.attempts}, ${row.parkedAt})
        ON CONFLICT (view_model, position) DO UPDATE SET
          event_id = excluded.event_id,
          error = excluded.error,
          attempts = excluded.attempts,
          parked_at = excluded.parked_at
      `.pipe(Effect.asVoid, Effect.mapError(storeError("park")))
    },
    list: (viewModel) =>
      (viewModel === undefined
        ? sql`SELECT ${columns} FROM ${table} ORDER BY view_model, position`
        : sql`SELECT ${columns} FROM ${table} WHERE view_model = ${viewModel} ORDER BY position`).pipe(
          Effect.flatMap(decodeRows),
          Effect.mapError(storeError("parked"))
        ),
    remove: (viewModel, position) =>
      sql`DELETE FROM ${table} WHERE view_model = ${viewModel} AND position = ${position}`.pipe(
        Effect.asVoid,
        Effect.mapError(storeError("unpark"))
      )
  })
})

/**
 * Parked events in a SQL table, see {@link makeSql}. Give it the client of the
 * read models, and a retried event leaves the table in the same transaction as
 * its handler's writes when the view model has a `transaction`.
 */
export const layerSql = (options?: SqlOptions): Layer.Layer<ParkedEvents, EventStoreError, SqlClient.SqlClient> =>
  Layer.effect(ParkedEvents, makeSql(options))
