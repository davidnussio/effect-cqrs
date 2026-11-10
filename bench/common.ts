/**
 * What the benchmark and the stress test share: an aggregate that can grow long
 * streams, the storage backends, and the measuring tools.
 */
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { DateTime, Effect, Layer, Redacted, Schema } from "effect"
import { Aggregate, Checkpoints, EventStore, PostgresEventStore, SqliteEventStore } from "effect-cqrs"
import { SqlClient } from "effect/sql"
import { execSync } from "node:child_process"
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs"
import { cpus, platform, release, tmpdir, totalmem } from "node:os"
import { join } from "node:path"

export const AccountEvent = Schema.TaggedUnion({
  Deposited: { amount: Schema.Int, reference: Schema.String, at: Schema.DateTimeUtc },
  Withdrawn: { amount: Schema.Int, reference: Schema.String, at: Schema.DateTimeUtc }
})

export interface AccountState {
  readonly balance: number
  readonly movements: number
}

const Movement = Schema.Struct({ amount: Schema.Int, reference: Schema.String })

/** A bank account: every command appends, so a stream can grow without limit. */
export const Account = Aggregate.make<AccountState>()({
  name: "account",
  events: AccountEvent,
  initial: { balance: 0, movements: 0 },
  projection: {
    Deposited: (state, { amount }) => ({ balance: state.balance + amount, movements: state.movements + 1 }),
    Withdrawn: (state, { amount }) => ({ balance: state.balance - amount, movements: state.movements + 1 })
  }
})
  .command("deposit", {
    payload: Movement,
    handler: Effect.fnUntraced(function*(_, { amount, reference }) {
      return AccountEvent.cases.Deposited.make({ amount, reference, at: yield* DateTime.now })
    })
  })
  .command("withdraw", {
    payload: Movement,
    handler: Effect.fnUntraced(function*(_, { amount, reference }) {
      return AccountEvent.cases.Withdrawn.make({ amount, reference, at: yield* DateTime.now })
    })
  })
  .command("depositMany", {
    payload: Schema.Struct({ ...Movement.fields, count: Schema.Int }),
    handler: Effect.fnUntraced(function*(_, { amount, count, reference }) {
      const at = yield* DateTime.now
      return Array.from(
        { length: count },
        (_, index) => AccountEvent.cases.Deposited.make({ amount, reference: `${reference}-${index}`, at })
      )
    })
  })

/** The size of every command the crash scenario writes, so a torn one shows in the counts. */
export const crashEventsPerCommand = 5

const encodeEvent = Schema.encodeUnknownSync(Schema.toCodecJson(AccountEvent))
const at = DateTime.makeUnsafe(0)

/** A stored event as a command would have written it, for filling a store quickly. */
export const seedEvent = (index: number, type: "Deposited" | "Withdrawn" = "Deposited"): EventStore.NewEvent => ({
  type,
  payload: encodeEvent(AccountEvent.cases[type].make({ amount: 10, reference: `seed-${index}`, at })) as Schema.Json
})

/** Fills one stream, appending in transactions of `chunk` events. */
export const seedStream = Effect.fnUntraced(function*(
  aggregateId: string,
  length: number,
  options?: { readonly chunk?: number; readonly type?: "Deposited" | "Withdrawn" }
) {
  const store = yield* EventStore.EventStore
  const chunk = options?.chunk ?? 1000
  for (let version = 0; version < length; version += chunk) {
    const size = Math.min(chunk, length - version)
    yield* store.append({
      aggregateName: Account.name,
      aggregateId,
      expectedVersion: version,
      events: Array.from({ length: size }, (_, index) => seedEvent(version + index, options?.type))
    })
  }
})

export type Backend = "memory" | "sqlite-memory" | "sqlite-file" | "postgres"

/** Where the Docker Postgres of `docker-compose.yml` is, when it is meant to be used. */
export const postgresUrl = process.env.POSTGRES_URL

/** The backends to run, as in `BENCH_BACKENDS=postgres pnpm bench`. */
export const backends: ReadonlyArray<Backend> =
  (process.env.BENCH_BACKENDS?.split(",") as Array<Backend> | undefined) ?? [
    "memory",
    "sqlite-memory",
    "sqlite-file",
    ...(postgresUrl === undefined ? [] : ["postgres" as const])
  ]

/** A temporary directory that is removed with the scope, for database files. */
export const temporaryFile = (name: string) =>
  Effect.acquireRelease(
    Effect.sync(() => mkdtempSync(join(tmpdir(), "effect-cqrs-bench-"))),
    (directory) => Effect.sync(() => rmSync(directory, { recursive: true, force: true }))
  ).pipe(Effect.map((directory) => join(directory, name)))

export const sqliteStorage = (filename: string) =>
  Layer.merge(SqliteEventStore.layer(), SqliteEventStore.layerCheckpoints()).pipe(
    Layer.provide(Layer.fresh(SqliteClient.layer({ filename })))
  )

export const postgresClient = () =>
  PgClient.layer({
    url: Redacted.make(postgresUrl ?? ""),
    maxConnections: 10,
    // Everything here is short lived, and test failures should be about the test.
    connectTimeout: "5 seconds"
  })

/** Tables of a store on Postgres, under a name of their own. */
export const postgresStorage = (table: string) => {
  const options = { eventsTable: `${table}_events`, checkpointsTable: `${table}_checkpoints` }
  return Layer.merge(PostgresEventStore.layer(options), PostgresEventStore.layerCheckpoints(options))
}

/** Removes the tables of `postgresStorage` with the scope. */
const dropTables = (table: string) =>
  Layer.effectDiscard(Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* Effect.addFinalizer(() =>
      sql.unsafe(`DROP TABLE IF EXISTS ${table}_events, ${table}_events_head, ${table}_checkpoints`).pipe(Effect.ignore)
    )
  }))

/** A store on Postgres that only this scope sees, because the server outlives it. */
export const temporaryPostgres = () => {
  const table = `bench_${crypto.randomUUID().replaceAll("-", "")}`
  return Layer.merge(postgresStorage(table), dropTables(table)).pipe(Layer.provideMerge(Layer.fresh(postgresClient())))
}

/** A store and checkpoints on one backend. Each `Effect.provide` starts empty. */
export const storage = (backend: Backend) => {
  switch (backend) {
    case "postgres":
      return temporaryPostgres()
    case "memory":
      return Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory)
    case "sqlite-memory":
      return sqliteStorage(":memory:")
    case "sqlite-file":
      return Layer.unwrap(Effect.map(temporaryFile("events.sqlite"), sqliteStorage))
  }
}

export type DurableBackend = "sqlite-file" | "postgres"

/** A store that can be closed and opened again. `target` names it to another process. */
export const openStorage = (backend: DurableBackend, target: string) =>
  backend === "postgres"
    ? postgresStorage(target).pipe(Layer.provide(Layer.fresh(postgresClient())))
    : sqliteStorage(target)

/** A durable store for one scope, with a way to ask how big it got, in bytes. */
export const temporaryDurable = (backend: DurableBackend) =>
  backend === "sqlite-file"
    ? Effect.map(temporaryFile("events.sqlite"), (target) => ({
      target,
      size: Effect.sync(() => statSync(target).size + statSync(`${target}-wal`).size)
    }))
    : Effect.gen(function*() {
      const target = `bench_${crypto.randomUUID().replaceAll("-", "")}`
      const client = Layer.fresh(postgresClient())
      yield* Effect.addFinalizer(() =>
        SqlClient.SqlClient.use((sql) =>
          sql.unsafe(`DROP TABLE IF EXISTS ${target}_events, ${target}_events_head, ${target}_checkpoints`)
        ).pipe(Effect.provide(client), Effect.scoped, Effect.ignore)
      )
      return {
        target,
        size: SqlClient.SqlClient.use((sql) =>
          sql.unsafe<{ readonly size: number }>(
            `SELECT pg_total_relation_size('${target}_events')::float8 AS size`
          )
        ).pipe(Effect.map(([row]) => row?.size ?? 0), Effect.provide(client), Effect.scoped, Effect.orDie)
      }
    })

/** Shrinks the workloads for a quick look, as in `BENCH_SCALE=0.01 pnpm bench`. */
export const scale = Number(process.env.BENCH_SCALE ?? 1)

export const scaled = (count: number) => Math.max(1, Math.round(count * scale))

export const timed = Effect.fnUntraced(function*<A, E, R>(effect: Effect.Effect<A, E, R>) {
  const start = performance.now()
  const value = yield* effect
  return { value, ms: performance.now() - start }
})

export interface Latency {
  readonly p50: number
  readonly p95: number
  readonly p99: number
  readonly max: number
}

export const summarize = (latencies: ReadonlyArray<number>): Latency => {
  const sorted = [...latencies].sort((a, b) => a - b)
  const percentile = (p: number) => sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)]!
  return { p50: percentile(50), p95: percentile(95), p99: percentile(99), max: sorted[sorted.length - 1]! }
}

/** Runs `count` operations one at a time, timing each. */
export const sample = Effect.fnUntraced(function*<E, R>(
  count: number,
  operation: (index: number) => Effect.Effect<unknown, E, R>
) {
  const latencies = new Array<number>(count)
  const started = performance.now()
  for (let index = 0; index < count; index++) {
    const start = performance.now()
    yield* operation(index)
    latencies[index] = performance.now() - start
  }
  return { ms: performance.now() - started, latency: summarize(latencies) }
})

export const median = (values: ReadonlyArray<number>) =>
  [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]!

export const megabytes = (bytes: number) => Math.round(bytes / 1024 / 1024)

/** Heap and resident memory in MB, after a collection when `--expose-gc` allows one. */
export const memoryUsage = () => {
  globalThis.gc?.()
  const { heapUsed, rss } = process.memoryUsage()
  return { heapMb: megabytes(heapUsed), rssMb: megabytes(rss) }
}

const git = (command: string) => execSync(`git ${command}`, { encoding: "utf8" }).trim()

export const environment = Effect.gen(function*() {
  const { version } = JSON.parse(readFileSync("node_modules/effect/package.json", "utf8"))
  return {
    date: DateTime.formatIso(yield* DateTime.now),
    commit: git("rev-parse --short HEAD") + (git("status --porcelain src") === "" ? "" : "-dirty"),
    node: process.version,
    effect: version as string,
    sqlite: process.versions.sqlite,
    cpu: cpus()[0]?.model,
    cores: cpus().length,
    memoryGb: Math.round(totalmem() / 1024 / 1024 / 1024),
    platform: `${platform()} ${release()}`,
    scale
  }
})

/** Writes raw results next to the benchmarks, unless the workloads were scaled down. */
export const writeResults = (name: string, results: unknown) =>
  Effect.sync(() => {
    if (scale !== 1) return
    mkdirSync("bench/results", { recursive: true })
    writeFileSync(`bench/results/${name}.json`, `${JSON.stringify(results, null, 2)}\n`)
  })
