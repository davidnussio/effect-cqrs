/**
 * State that every replica of the server must see alike: the settings of the bot
 * and of the simulated shoppers, the presenter's chaos, who is in the room and the
 * rate limits. Behind a load balancer any replica may answer a request, so this
 * can't live in one process's memory.
 *
 * It lives in a table of the database the events are in, one JSON value per key,
 * and each change runs in a transaction that locks its row, so replicas never
 * overwrite each other's changes. The tests keep it in memory.
 */
import { Context, Effect, Layer, Option, Schema, Semaphore } from "effect"
import { SqlClient } from "effect/sql"

export class SharedState extends Context.Service<SharedState, {
  /** The JSON stored under `key`. */
  get(key: string): Effect.Effect<Option.Option<string>>
  /**
   * Replaces the JSON under `key` with what `f` makes of it, while no other
   * replica can change it, and returns what `f` returns beside it.
   */
  modify<A>(key: string, f: (current: Option.Option<string>) => readonly [A, string]): Effect.Effect<A>
}>()("workshop/SharedState") {
  static readonly layerMemory = Layer.effect(
    SharedState,
    Effect.gen(function*() {
      const values = new Map<string, string>()
      const lock = yield* Semaphore.make(1)
      return SharedState.of({
        get: (key) => Effect.sync(() => Option.fromNullishOr(values.get(key))),
        modify: (key, f) =>
          lock.withPermit(Effect.sync(() => {
            const [result, next] = f(Option.fromNullishOr(values.get(key)))
            values.set(key, next)
            return result
          }))
      })
    })
  )

  /** In the table `workshop_state`, on SQLite or PostgreSQL. */
  static readonly layerSql = Layer.effect(
    SharedState,
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const create = sql`CREATE TABLE IF NOT EXISTS workshop_state (key TEXT PRIMARY KEY, value TEXT)`
      // Replicas that start together would otherwise race to create the table.
      yield* sql.onDialectOrElse({
        pg: () =>
          sql`SELECT pg_advisory_xact_lock(hashtext('workshop_state'))`.pipe(
            Effect.andThen(create),
            sql.withTransaction
          ),
        orElse: () => create
      }).pipe(Effect.orDie)

      const get = (key: string) =>
        sql<{ readonly value: string | null }>`SELECT value FROM workshop_state WHERE key = ${key}`.pipe(
          Effect.map(([row]) => Option.fromNullishOr(row?.value)),
          Effect.orDie
        )

      const modify = <A>(key: string, f: (current: Option.Option<string>) => readonly [A, string]) =>
        Effect.gen(function*() {
          // The row exists before it is locked, so two replicas can't both insert it.
          yield* sql`INSERT INTO workshop_state (key, value) VALUES (${key}, NULL) ON CONFLICT (key) DO NOTHING`
          const [row] = yield* sql.onDialectOrElse({
            pg: () =>
              sql<{ readonly value: string | null }>`SELECT value FROM workshop_state WHERE key = ${key} FOR UPDATE`,
            orElse: () => sql<{ readonly value: string | null }>`SELECT value FROM workshop_state WHERE key = ${key}`
          })
          const [result, next] = f(Option.fromNullishOr(row?.value))
          yield* sql`UPDATE workshop_state SET value = ${next} WHERE key = ${key}`
          return result
        }).pipe(sql.withTransaction, Effect.orDie)

      return SharedState.of({ get, modify })
    })
  )
}

/** One value of the shared state, decoded with its schema. */
export interface Shared<A> {
  readonly get: Effect.Effect<A>
  modify<B>(f: (current: A) => readonly [B, A]): Effect.Effect<B>
  /** Changes the value and returns the new one. */
  update(f: (current: A) => A): Effect.Effect<A>
}

/**
 * The value under `key`, `initial` until something is stored. A value that no
 * longer decodes, written by an older version of the server, reads as `initial`.
 */
export const shared = <S extends Schema.Codec<any, any>>(key: string, schema: S, initial: S["Type"]) =>
  Effect.gen(function*(): Effect.fn.Return<Shared<S["Type"]>, never, SharedState> {
    const state = yield* SharedState
    const codec = Schema.toCodecJson(schema)
    const decode = Schema.decodeUnknownOption(codec)
    const encode = Schema.encodeSync(codec)
    const read = (json: Option.Option<string>): S["Type"] =>
      Option.match(json, {
        onNone: () => initial,
        onSome: (text) => Option.getOrElse(decode(JSON.parse(text)), () => initial)
      })
    const modify = <B>(f: (current: S["Type"]) => readonly [B, S["Type"]]) =>
      state.modify(key, (json) => {
        const [result, next] = f(read(json))
        return [result, JSON.stringify(encode(next))] as const
      })
    return {
      get: Effect.map(state.get(key), read),
      modify,
      update: (f) =>
        modify((current) => {
          const next = f(current)
          return [next, next] as const
        })
    }
  })
