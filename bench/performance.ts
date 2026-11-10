/**
 * Measures each operation on each backend, one at a time and without faults.
 * Run with `pnpm bench`, or `BENCH_SCALE=0.05 pnpm bench` for a quick look. The
 * raw numbers go to `bench/results/performance.json`, summarized in
 * `bench/REPORT.md`.
 */
import { NodeRuntime } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Console, Effect, Layer, Option, Schema, Stream } from "effect"
import { Aggregate, AggregateService, EventStore, PersonalData, SqliteEventStore, ViewModel } from "effect-cqrs"
import * as PersonalDataNode from "effect-cqrs/PersonalDataNode"
import { SqlClient } from "effect/sql"
import {
  Account,
  type AccountState,
  type Backend,
  backends,
  environment,
  type Latency,
  median,
  sample,
  scaled,
  seedEvent,
  seedStream,
  storage,
  timed,
  writeResults
} from "./common.ts"

interface Result {
  readonly scenario: string
  readonly backend: Backend
  readonly label: string
  readonly count: number
  /** Operations or events per second, the median of the rounds. */
  readonly rate: number
  readonly unit: "events/s" | "commands/s" | "reads/s"
  readonly range?: readonly [number, number]
  readonly latencyMs?: Latency
}

const results: Array<Result> = []

const round = (value: number, digits = 2) => Number(value.toFixed(digits))

const record = (result: Result) =>
  Effect.suspend(() => {
    results.push(result)
    const latency = result.latencyMs === undefined
      ? ""
      : `  p50 ${round(result.latencyMs.p50)}ms  p95 ${round(result.latencyMs.p95)}ms  p99 ${
        round(result.latencyMs.p99)
      }ms  max ${round(result.latencyMs.max)}ms`
    const range = result.range === undefined ? "" : `  (${result.range[0]}-${result.range[1]})`
    return Console.log(
      `${result.scenario.padEnd(22)} ${result.backend.padEnd(14)} ${result.label.padEnd(22)} ${
        String(result.rate).padStart(9)
      } ${result.unit}${range}${latency}`
    )
  })

const ROUNDS = 5

/** Repeats a workload on a fresh store. The first round warms up the JIT and doesn't count. */
const throughput = Effect.fnUntraced(function*<E>(
  options: Pick<Result, "scenario" | "backend" | "label" | "count" | "unit">,
  workload: Effect.Effect<unknown, E, EventStore.EventStore>
) {
  const rates: Array<number> = []
  for (let index = 0; index <= ROUNDS; index++) {
    const { ms } = yield* timed(workload).pipe(Effect.provide(storage(options.backend)))
    if (index > 0) rates.push(Math.round((options.count / ms) * 1000))
  }
  yield* record({ ...options, rate: median(rates), range: [Math.min(...rates), Math.max(...rates)] })
})

const appendSingle = (backend: Backend) => {
  const count = scaled(backend === "memory" ? 200_000 : 20_000)
  return throughput(
    { scenario: "append", backend, label: "1 event per append", count, unit: "events/s" },
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      for (let index = 0; index < count; index++) {
        yield* store.append({
          aggregateName: Account.name,
          aggregateId: `a-${index}`,
          expectedVersion: 0,
          events: [seedEvent(index)]
        })
      }
    })
  )
}

const appendBatch = (backend: Backend) => {
  const count = scaled(200_000)
  return throughput(
    { scenario: "append", backend, label: "100 events per append", count, unit: "events/s" },
    seedStream("batched", count, { chunk: 100 })
  )
}

const commandOnNewStreams = (backend: Backend) => {
  const count = scaled(5_000)
  return throughput(
    { scenario: "command", backend, label: "new stream each", count, unit: "commands/s" },
    Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      for (let index = 0; index < count; index++) {
        yield* accounts.commands.deposit(`a-${index}`, { amount: 1, reference: "r" })
      }
    })
  )
}

/**
 * Gives Postgres the statistics autovacuum would have within a minute, which the
 * tables here don't live long enough to get.
 */
const analyze = Effect.serviceOption(SqlClient.SqlClient).pipe(
  Effect.flatMap(Option.match({
    onNone: () => Effect.void,
    onSome: (sql) => Effect.orDie(Effect.asVoid(sql.unsafe("ANALYZE")))
  }))
)

/**
 * Loads and commands against streams of a given length, all in one store so its
 * size doesn't change between lengths. A cold load replays the whole stream, as
 * the first use after a restart does.
 */
const streamLength = (backend: Backend) =>
  Effect.gen(function*() {
    const accounts = yield* AggregateService.make(Account)
    const cold = yield* AggregateService.make(Account, { stateCacheSize: 0 })
    const levels = [
      { length: 1, streams: 200, samples: 200 },
      { length: 10, streams: 200, samples: 200 },
      { length: 100, streams: 200, samples: 200 },
      { length: 1_000, streams: 100, samples: 100 },
      { length: 10_000, streams: 10, samples: 50 },
      { length: 100_000, streams: 1, samples: 10 }
    ].map((level) => ({ ...level, streams: scaled(level.streams), samples: scaled(level.samples) }))

    for (const { length, streams } of levels) {
      for (let stream = 0; stream < streams; stream++) {
        yield* seedStream(`length-${length}-${stream}`, length)
      }
    }
    yield* analyze
    for (let index = 0; index < scaled(2_000); index++) {
      yield* accounts.commands.deposit("warm-up", { amount: 1, reference: "r" })
    }

    for (const { length, samples, streams } of levels) {
      const id = (index: number) => `length-${length}-${index % streams}`
      const reads = yield* sample(samples, (index) => cold.readModel(id(index)))
      yield* record({
        scenario: "cold load",
        backend,
        label: `stream of ${length}`,
        count: samples,
        rate: Math.round((samples / reads.ms) * 1000),
        unit: "reads/s",
        latencyMs: reads.latency
      })
      // What a running service sees: each stream was loaded once before.
      for (let stream = 0; stream < streams; stream++) {
        yield* accounts.readModel(id(stream))
      }
      const commands = yield* sample(
        samples,
        (index) => accounts.commands.deposit(id(index), { amount: 1, reference: "r" })
      )
      yield* record({
        scenario: "command",
        backend,
        label: `stream of ${length}`,
        count: samples,
        rate: Math.round((samples / commands.ms) * 1000),
        unit: "commands/s",
        latencyMs: commands.latency
      })
    }
  }).pipe(Effect.provide(storage(backend)))

/** Short commands while unrelated events pile up: the cost of store size alone. */
const storeSize = (backend: Backend) =>
  Effect.gen(function*() {
    const accounts = yield* AggregateService.make(Account)
    const store = yield* EventStore.EventStore
    const samples = scaled(200)
    let size = 0
    for (let index = 0; index < scaled(2_000); index++) {
      yield* accounts.commands.deposit(`warm-up-${index}`, { amount: 1, reference: "r" })
    }
    for (const target of [1_000, 10_000, 100_000, 1_000_000].map(scaled)) {
      const missing = target - (yield* store.head)
      for (let chunk = 0; chunk * 1_000 < missing; chunk++) {
        yield* seedStream(`filler-${target}-${chunk}`, Math.min(1_000, missing - chunk * 1_000))
      }
      size = yield* store.head
      yield* analyze
      const commands = yield* sample(
        samples,
        (index) => accounts.commands.deposit(`size-${target}-${index}`, { amount: 1, reference: "r" })
      )
      yield* record({
        scenario: "store size",
        backend,
        label: `${target} events stored`,
        count: samples,
        rate: Math.round((samples / commands.ms) * 1000),
        unit: "commands/s",
        latencyMs: commands.latency
      })
    }
    const { ms } = yield* timed(store.read().pipe(Stream.runDrain))
    yield* record({
      scenario: "read",
      backend,
      label: "whole log",
      count: size,
      rate: Math.round((size / ms) * 1000),
      unit: "events/s"
    })
  }).pipe(Effect.provide(storage(backend)))

const makeBalances = (name: string) => {
  const balances = new Map<string, number>()
  const add = (id: string, amount: number) => Effect.sync(() => void balances.set(id, (balances.get(id) ?? 0) + amount))
  return ViewModel.make({
    name,
    aggregates: [Account],
    dispose: Effect.sync(() => balances.clear()),
    handlers: {
      Deposited: ({ amount }, { aggregateId }) => add(aggregateId, amount),
      Withdrawn: ({ amount }, { aggregateId }) => add(aggregateId, -amount)
    }
  })
}

const Withdrawals = ViewModel.make({
  name: "withdrawals",
  aggregates: [Account],
  dispose: Effect.void,
  handlers: { Withdrawn: () => Effect.void }
})

const viewModelCatchUp = (backend: Backend) =>
  Effect.gen(function*() {
    for (let stream = 0; stream < scaled(2_000); stream++) {
      yield* seedStream(`a-${stream}`, 100)
    }
    // A rare event type, for the view model that only wants those.
    for (let stream = 0; stream < scaled(20); stream++) {
      yield* seedStream(`w-${stream}`, 200, { type: "Withdrawn" })
    }

    const balances = makeBalances("balances")
    const firstRun = yield* timed(
      ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(
        Effect.provide(ViewModel.layer([balances])),
        Effect.scoped
      )
    )
    const total = yield* EventStore.EventStore.use((store) => store.head)
    yield* record({
      scenario: "view model",
      backend,
      label: "catch up",
      count: total,
      rate: Math.round((total / firstRun.ms) * 1000),
      unit: "events/s"
    })

    // The catch up above ran on a table just filled. What follows is a table in use.
    yield* analyze
    const fresh = makeBalances("replayed")
    const rates = yield* Effect.gen(function*() {
      const viewModels = yield* ViewModel.ViewModels
      yield* viewModels.caughtUp()
      const rates: Array<number> = []
      for (let index = 0; index < ROUNDS; index++) {
        const { ms } = yield* timed(viewModels.replay({ viewModels: ["replayed"] }))
        rates.push(Math.round((total / ms) * 1000))
      }
      return rates
    }).pipe(Effect.provide(ViewModel.layer([fresh])), Effect.scoped)
    yield* record({
      scenario: "view model",
      backend,
      label: "replay",
      count: total,
      rate: median(rates),
      range: [Math.min(...rates), Math.max(...rates)],
      unit: "events/s"
    })

    const sparseRun = yield* timed(
      ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(
        Effect.provide(ViewModel.layer([Withdrawals])),
        Effect.scoped
      )
    )
    yield* record({
      scenario: "view model",
      backend,
      label: "catch up, 2% wanted",
      count: total,
      rate: Math.round((total / sparseRun.ms) * 1000),
      unit: "events/s"
    })
  }).pipe(Effect.provide(storage(backend)))

/** How long a client waits to read its own write through a view model. */
const readYourWrites = (backend: Backend) =>
  Effect.gen(function*() {
    const accounts = yield* AggregateService.make(Account)
    const viewModels = yield* ViewModel.ViewModels
    const count = scaled(2_000)
    const warmUp = (index: number) => accounts.commands.deposit(`warm-${index}`, { amount: 1, reference: "r" })
    yield* sample(scaled(1_000), warmUp)
    const { latency, ms } = yield* sample(count, (index) =>
      accounts.commands.deposit(`a-${index}`, { amount: 1, reference: "r" }).pipe(
        Effect.flatMap(({ position }) =>
          viewModels.caughtUp(position)
        )
      ))
    yield* record({
      scenario: "read your writes",
      backend,
      label: "command + caughtUp",
      count,
      rate: Math.round((count / ms) * 1000),
      unit: "commands/s",
      latencyMs: latency
    })
  }).pipe(
    Effect.provide(ViewModel.layer([makeBalances("balances")]).pipe(Layer.provideMerge(storage(backend))))
  )

const fanOut = (backend: Backend) =>
  Effect.forEach([1, 10, 50], (size) => {
    const count = scaled(5_000)
    const viewModels = Array.from({ length: size }, (_, index) => makeBalances(`balances-${index}`))
    return Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      const running = yield* ViewModel.ViewModels
      const { ms } = yield* timed(
        Effect.gen(function*() {
          for (let index = 0; index < count; index++) {
            yield* accounts.commands.deposit(`a-${index}`, { amount: 1, reference: "r" })
          }
          yield* running.caughtUp()
        })
      )
      yield* record({
        scenario: "fan out",
        backend,
        label: `${size} view models`,
        count,
        rate: Math.round((count / ms) * 1000),
        unit: "commands/s"
      })
    }).pipe(
      Effect.provide(ViewModel.layer(viewModels).pipe(Layer.provideMerge(storage(backend))))
    )
  }, { discard: true })

const concurrency = (backend: Backend) =>
  Effect.forEach([1, 16, 256], (workers) => {
    const count = scaled(5_000)
    return throughput(
      { scenario: "concurrency", backend, label: `${workers} workers`, count, unit: "commands/s" },
      Effect.gen(function*() {
        const accounts = yield* AggregateService.make(Account)
        yield* Effect.forEach(
          Array.from({ length: count }, (_, index) => index),
          (index) => accounts.commands.deposit(`a-${index}`, { amount: 1, reference: "r" }),
          { concurrency: workers, discard: true }
        )
      })
    )
  }, { discard: true })

// The same account, with the reference of each movement as personal data.
const personal = PersonalData.field(Schema.String, { redacted: "" })
const PersonalAccountEvent = Schema.TaggedUnion({
  Deposited: { amount: Schema.Int, reference: personal, at: Schema.DateTimeUtc },
  Withdrawn: { amount: Schema.Int, reference: personal, at: Schema.DateTimeUtc }
})
const PersonalAccount = Aggregate.make<AccountState>()({
  name: Account.name,
  events: PersonalAccountEvent,
  initial: Account.initial,
  projection: Account.projection
})
  .command("deposit", Account.commands.deposit)
  .command("depositMany", Account.commands.depositMany)

const webCryptoKeys = PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
const nodeKeys = webCryptoKeys.pipe(Layer.provide(PersonalDataNode.layerCipher))

/** What encrypting one field per event costs on each cipher, next to the same account without it. */
const personalData = (backend: Backend) =>
  Effect.forEach(
    [
      ["plain", Account, webCryptoKeys],
      ["personal WebCrypto", PersonalAccount, webCryptoKeys],
      ["personal node:crypto", PersonalAccount, nodeKeys]
    ] as const,
    ([kind, aggregate, keys]) =>
      Effect.gen(function*() {
        const count = scaled(5_000)
        yield* throughput(
          { scenario: "personal data", backend, label: `${kind}, new stream each`, count, unit: "commands/s" },
          Effect.gen(function*() {
            const accounts = yield* AggregateService.make(aggregate as typeof Account)
            for (let index = 0; index < count; index++) {
              yield* accounts.commands.deposit(`a-${index}`, { amount: 1, reference: "r" })
            }
          }).pipe(Effect.provide(keys))
        )
        yield* Effect.gen(function*() {
          const accounts = yield* AggregateService.make(aggregate as typeof Account)
          const cold = yield* AggregateService.make(aggregate as typeof Account, { stateCacheSize: 0 })
          const streams = scaled(20)
          for (let stream = 0; stream < streams; stream++) {
            yield* accounts.commands.depositMany(`long-${stream}`, { amount: 1, reference: "r", count: 1_000 })
          }
          yield* cold.readModel("long-0")
          const samples = scaled(100)
          const reads = yield* sample(samples, (index) => cold.readModel(`long-${index % streams}`))
          yield* record({
            scenario: "personal data",
            backend,
            label: `${kind}, cold load of 1000`,
            count: samples,
            rate: Math.round((samples / reads.ms) * 1000),
            unit: "reads/s",
            latencyMs: reads.latency
          })
        }).pipe(Effect.provide(Layer.merge(storage(backend), keys)))
      }),
    { discard: true }
  )

/** The plans behind the reads that matter, on a store with no `ANALYZE` statistics. */
const queryPlans = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* seedStream("a", 1_000)
  const plan = (query: string) =>
    sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`).pipe(
      Effect.map((rows) => rows.map((row) => row.detail))
    )
  return {
    "read one stream": yield* plan(
      `SELECT * FROM cqrs_events WHERE position > 0 AND version > 0 AND aggregate_name IN ('account')
       AND aggregate_id IN ('a') ORDER BY version LIMIT 500`
    ),
    "read for a view model": yield* plan(
      `SELECT * FROM cqrs_events WHERE position > 0 AND aggregate_name IN ('account')
       AND type IN ('Deposited', 'Withdrawn') ORDER BY position LIMIT 500`
    ),
    "read one event type": yield* plan(
      `SELECT * FROM cqrs_events WHERE position > 0 AND aggregate_name IN ('account') AND type IN ('Withdrawn')
       ORDER BY position LIMIT 500`
    ),
    // The append as the store runs it: the version check is a subquery of the insert.
    "append with version check": yield* plan(
      `INSERT INTO cqrs_events (
         aggregate_name, aggregate_id, version, type, schema_version, payload, timestamp, correlation_id,
         causation_id, metadata
       )
       SELECT 'account', 'a', 1000 + event.key + 1, json_extract(event.value, '$[0]'),
         json_extract(event.value, '$[1]'), json_extract(event.value, '$[2]'), 0, NULL, NULL, '{}'
       FROM json_each('[["Deposited",1,"{}"]]') AS event
       WHERE (SELECT COALESCE(MAX(version), 0) FROM cqrs_events
              WHERE aggregate_name = 'account' AND aggregate_id = 'a') = 1000
       ORDER BY event.key
       RETURNING position`
    )
  }
}).pipe(
  Effect.provide(SqliteEventStore.layer().pipe(Layer.provideMerge(SqliteClient.layer({ filename: ":memory:" }))))
)

const program = Effect.gen(function*() {
  const env = yield* environment
  yield* Console.log(JSON.stringify(env))
  const suites = {
    appendSingle,
    appendBatch,
    commandOnNewStreams,
    streamLength,
    storeSize,
    viewModelCatchUp,
    readYourWrites,
    fanOut,
    concurrency,
    personalData
  }
  // `BENCH_SUITES=personalData,fanOut` runs only those.
  const only = process.env.BENCH_SUITES?.split(",")
  for (const [name, suite] of Object.entries(suites)) {
    if (only !== undefined && !only.includes(name)) continue
    for (const backend of backends) {
      yield* suite(backend)
    }
  }
  const plans = yield* queryPlans
  yield* Console.log(JSON.stringify(plans, null, 2))
  const name = [
    "performance",
    ...(process.env.BENCH_BACKENDS === undefined ? [] : backends),
    ...(only ?? [])
  ].join("-")
  yield* writeResults(name, { environment: env, rounds: ROUNDS, plans, results })
})

NodeRuntime.runMain(program)
