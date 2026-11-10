/**
 * Decides which process runs each view model.
 *
 * Replicas that share an event store would otherwise all run the same view
 * models and race on their checkpoints. `ViewModel.layer` takes a lease on each
 * view model before running it: the process that holds the lease runs it, and the
 * others wait as standbys until it is given up.
 *
 * @module
 */
import { Context, Duration, Effect, Exit, Layer, Schedule, Scope, Semaphore, Stream, SubscriptionRef } from "effect"
import { SqlClient, type SqlConnection } from "effect/sql"

export interface Lease {
  /** Whether this process holds the lease now. */
  readonly held: Effect.Effect<boolean>
  /** Emits whether the lease is held, then every change. */
  readonly changes: Stream.Stream<boolean>
}

export class Leases extends Context.Service<Leases, {
  /**
   * Competes for the lease called `name` until the scope closes, which gives it
   * up. The first attempt is made before this returns.
   */
  acquire(name: string): Effect.Effect<Lease, never, Scope.Scope>
}>()("effect-cqrs/Leases") {}

const alwaysHeld: Lease = { held: Effect.succeed(true), changes: Stream.make(true) }

/**
 * Leases that are always held. It is the default of `ViewModel.layer`, for a
 * single process or for view models that keep their data in that process.
 */
export const memory: Leases["Service"] = Leases.of({ acquire: () => Effect.succeed(alwaysHeld) })

export const layerMemory: Layer.Layer<Leases> = Layer.succeed(Leases, memory)

export interface PostgresOptions {
  /**
   * How often a standby tries to take a lease over, and the holder checks that
   * its connection is alive. Defaults to 5 seconds.
   */
  readonly interval?: Duration.Input | undefined
  /** Prefixes the names, so that several applications can share a database. Defaults to `effect-cqrs/`. */
  readonly prefix?: string | undefined
}

interface Session {
  readonly connection: SqlConnection.Connection
  readonly scope: Scope.Closeable
}

interface Contender {
  readonly key: string
  readonly held: SubscriptionRef.SubscriptionRef<boolean>
}

/**
 * Leases on PostgreSQL advisory locks, `pg_try_advisory_lock` on a hash of the
 * name. The locks belong to one connection that the service reserves from the
 * client's pool for as long as anything competes, so a process holds them until
 * it gives them up or its connection closes: when it dies, the database releases
 * them and a standby takes over at its next attempt.
 *
 * A holder whose connection fails stops at its next check, up to `interval`
 * later, while a standby may already have started: handlers can run in two
 * processes for that long.
 */
export const makePostgres = Effect.fnUntraced(function*(options?: PostgresOptions) {
  const sql = yield* SqlClient.SqlClient
  const scope = yield* Effect.scope
  const interval = Duration.fromInputUnsafe(options?.interval ?? "5 seconds")
  const prefix = options?.prefix ?? "effect-cqrs/"
  // One statement at a time on the connection, and one change at a time to what it holds.
  const semaphore = yield* Semaphore.make(1)
  const contenders = new Set<Contender>()
  let session: Session | undefined

  const execute = (connection: SqlConnection.Connection, statement: string, params: ReadonlyArray<unknown> = []) =>
    connection.execute(statement, params, undefined)

  const connect = Effect.gen(function*() {
    if (session !== undefined) return session.connection
    const child = yield* Scope.fork(scope)
    const connection = yield* sql.reserve.pipe(
      Scope.provide(child),
      Effect.onError(() => Scope.close(child, Exit.void))
    )
    // The connection goes back to the pool, which must not hand out the locks with it.
    yield* Scope.addFinalizer(child, execute(connection, "SELECT pg_advisory_unlock_all()").pipe(Effect.ignore))
    session = { connection, scope: child }
    return connection
  })

  // Every lock went with the connection.
  const disconnect = Effect.suspend(() => {
    const current = session
    session = undefined
    return Effect.forEach(contenders, ({ held }) => SubscriptionRef.set(held, false), { discard: true }).pipe(
      Effect.andThen(current === undefined ? Effect.void : Scope.close(current.scope, Exit.void))
    )
  })

  const tick = Effect.gen(function*() {
    if (contenders.size === 0) return
    const connection = yield* connect
    let tried = false
    for (const contender of contenders) {
      if (yield* SubscriptionRef.get(contender.held)) continue
      tried = true
      const [row] = yield* execute(connection, "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked", [
        contender.key
      ])
      if (row?.locked === true) yield* SubscriptionRef.set(contender.held, true)
    }
    // A holder with nothing to try still finds out whether its connection is alive.
    if (!tried) yield* execute(connection, "SELECT 1")
  }).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Lost the connection that holds the view model leases", error).pipe(
        Effect.andThen(disconnect)
      )
    ),
    semaphore.withPermit
  )

  yield* tick.pipe(Effect.repeat(Schedule.spaced(interval)), Effect.forkScoped)
  yield* Effect.addFinalizer(() => semaphore.withPermit(disconnect))

  const release = (contender: Contender) =>
    Effect.gen(function*() {
      contenders.delete(contender)
      if ((yield* SubscriptionRef.get(contender.held)) && session !== undefined) {
        yield* execute(session.connection, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", [contender.key]).pipe(
          Effect.catch(() => disconnect)
        )
      }
      yield* SubscriptionRef.set(contender.held, false)
    }).pipe(semaphore.withPermit)

  return Leases.of({
    acquire: (name) =>
      Effect.gen(function*() {
        const contender: Contender = { key: `${prefix}${name}`, held: yield* SubscriptionRef.make(false) }
        yield* Effect.acquireRelease(
          Effect.sync(() => void contenders.add(contender)).pipe(Effect.andThen(tick)),
          () => release(contender)
        )
        return { held: SubscriptionRef.get(contender.held), changes: SubscriptionRef.changes(contender.held) }
      })
  })
})

/** Leases on PostgreSQL advisory locks, see {@link makePostgres}. */
export const layerPostgres = (options?: PostgresOptions): Layer.Layer<Leases, never, SqlClient.SqlClient> =>
  Layer.effect(Leases, makePostgres(options))
