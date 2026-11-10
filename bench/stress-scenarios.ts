/**
 * Loads the framework until something gives, then checks that nothing was lost,
 * duplicated or reordered. A scenario returns what it measured and the invariants
 * it saw broken, so `pnpm stress` can run them at full size and `pnpm test` at a
 * size that keeps them from rotting.
 */
import { Cause, Effect, Exit, Fiber, Layer, References, Stream } from "effect"
import type { Aggregate } from "effect-cqrs"
import { AggregateService, EventStore, ViewModel } from "effect-cqrs"
import { spawn } from "node:child_process"
import {
  Account,
  type Backend,
  crashEventsPerCommand,
  type DurableBackend,
  megabytes,
  memoryUsage,
  openStorage,
  seedStream,
  storage,
  summarize,
  temporaryDurable,
  timed
} from "./common.ts"

export interface Report {
  readonly name: string
  readonly metrics: Record<string, unknown>
  /** Invariants that broke. Anything here is a bug, not a slow result. */
  readonly violations: ReadonlyArray<string>
}

export interface Sizes {
  readonly ingestEvents: number
  readonly burstEvents: number
  readonly contentionCommands: number
  readonly contentionWorkers: ReadonlyArray<number>
  readonly flakyEvents: number
  readonly flakyFailures: number
  readonly restartEvents: number
  readonly restarts: number
  readonly crashes: number
  readonly crashWriters: number
  readonly slowEvents: number
  readonly subscribers: number
  readonly subscriberEvents: number
  readonly hugeCommands: ReadonlyArray<number>
}

export const fullSizes: Sizes = {
  ingestEvents: 1_000_000,
  burstEvents: 200_000,
  contentionCommands: 2_000,
  contentionWorkers: [2, 8, 32, 128],
  flakyEvents: 50_000,
  flakyFailures: 8,
  restartEvents: 100_000,
  restarts: 20,
  crashes: 8,
  crashWriters: 3,
  slowEvents: 20_000,
  subscribers: 20,
  subscriberEvents: 20_000,
  hugeCommands: [10_000, 100_000, 200_000, 500_000, 1_000_000]
}

export const smokeSizes: Sizes = {
  ingestEvents: 2_000,
  burstEvents: 1_000,
  contentionCommands: 50,
  contentionWorkers: [8],
  flakyEvents: 1_000,
  flakyFailures: 2,
  restartEvents: 1_000,
  restarts: 2,
  crashes: 1,
  crashWriters: 2,
  slowEvents: 100,
  subscribers: 3,
  subscriberEvents: 100,
  hugeCommands: [1_000]
}

const BATCH_SIZE = 500

const makeChecks = () => {
  const violations: Array<string> = []
  let suppressed = 0
  return {
    check: (holds: boolean, message: string) => {
      if (holds) return
      if (violations.length < 20) violations.push(message)
      else suppressed++
    },
    violations: () => suppressed === 0 ? violations : [...violations, `and ${suppressed} more`]
  }
}

type Check = ReturnType<typeof makeChecks>["check"]

/** Reads the whole log: positions must have no gaps and each stream's versions must count up. */
const auditLog = Effect.fnUntraced(function*(expected: number, check: Check) {
  const store = yield* EventStore.EventStore
  const versions = new Map<string, number>()
  let count = 0
  yield* store.read().pipe(
    Stream.runForEachArray((batch) =>
      Effect.sync(() => {
        for (const event of batch) {
          count++
          check(event.position === count, `found position ${event.position} where ${count} was due`)
          const version = (versions.get(event.aggregateId) ?? 0) + 1
          check(event.version === version, `stream ${event.aggregateId}: version ${event.version} after ${version - 1}`)
          versions.set(event.aggregateId, event.version)
        }
      })
    )
  )
  const head = yield* store.head
  check(count === expected, `the log holds ${count} events, ${expected} were acknowledged`)
  check(head === count, `head is ${head} but the log holds ${count} events`)
  return versions
})

/**
 * An idempotent view model that keeps the books on itself: it applies a stream's
 * next version, ignores one it has seen, and counts a jump as a gap.
 */
const makeLedger = (name: string, interfere?: (meta: Aggregate.EventMeta) => Effect.Effect<void, Error>) => {
  const applied = new Map<string, number>()
  const balances = new Map<string, number>()
  const stats = { deliveries: 0, duplicates: 0, gaps: 0 }
  const apply = (amount: number, meta: Aggregate.EventMeta) =>
    Effect.suspend(() => interfere?.(meta) ?? Effect.void).pipe(
      Effect.andThen(Effect.sync(() => {
        stats.deliveries++
        const last = applied.get(meta.aggregateId) ?? 0
        if (meta.version <= last) {
          stats.duplicates++
          return
        }
        if (meta.version !== last + 1) stats.gaps++
        applied.set(meta.aggregateId, meta.version)
        balances.set(meta.aggregateId, (balances.get(meta.aggregateId) ?? 0) + amount)
      }))
    )
  const reset = () => {
    applied.clear()
    balances.clear()
    stats.deliveries = 0
    stats.duplicates = 0
    stats.gaps = 0
  }
  return {
    balances,
    stats,
    viewModel: ViewModel.make({
      name,
      aggregates: [Account],
      dispose: Effect.sync(reset),
      handlers: {
        Deposited: ({ amount }, meta) => apply(amount, meta),
        Withdrawn: ({ amount }, meta) => apply(-amount, meta)
      }
    })
  }
}

const positionOf = (name: string) =>
  ViewModel.ViewModels.use((viewModels) => viewModels.status).pipe(
    Effect.map((statuses) => statuses.find((status) => status.name === name)?.position ?? 0)
  )

const quiet = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provideService(effect, References.MinimumLogLevel, "None")

/**
 * Sustained load: many commands from concurrent workers with a view model
 * following, watching for slowdowns and for memory that doesn't come back.
 *
 * Commands that arrive from the event loop give other fibers a turn in between.
 * `Effect.forEach` over synchronous work never does, because each item runs to the
 * end in a fresh fiber, so `yieldToOthers` stands in for that arrival. Without it
 * the run shows what a synchronous burst does to the view models.
 */
const ingest = (
  name: string,
  backend: DurableBackend,
  events: number,
  options: { readonly eventsPerStream: number; readonly yieldToOthers: boolean }
) =>
  Effect.scoped(Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const { size, target } = yield* temporaryDurable(backend)
    const eventsPerCommand = 10
    const commands = Math.ceil(events / eventsPerCommand)
    const streams = Math.max(1, Math.round(events / options.eventsPerStream))
    const workers = 32
    const arrival = options.yieldToOthers ? Effect.yieldNow : Effect.void
    const windows = 10
    const ledger = makeLedger("ledger")

    return yield* Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      const store = yield* EventStore.EventStore
      const viewModels = yield* ViewModel.ViewModels

      let peakLag = 0
      const measureLag = Effect.gen(function*() {
        const lag = (yield* store.head) - (yield* positionOf("ledger"))
        peakLag = Math.max(peakLag, lag)
      })

      const samples: Array<Record<string, number>> = []
      let issued = 0
      for (let window = 0; window < windows; window++) {
        const size = Math.ceil(commands / windows)
        const from = issued
        issued += size
        const { ms } = yield* timed(
          Effect.forEach(
            Array.from({ length: size }, (_, index) => from + index),
            (index) =>
              accounts.commands.depositMany(`account-${index % streams}`, {
                amount: 1,
                reference: `r-${index}`,
                count: eventsPerCommand
              }).pipe(
                Effect.andThen(arrival),
                Effect.andThen(index % 200 === 0 ? measureLag : Effect.void)
              ),
            { concurrency: workers, discard: true }
          )
        )
        const head = yield* store.head
        samples.push({
          events: head,
          eventsPerSecond: Math.round((size * eventsPerCommand / ms) * 1000),
          viewModelLag: head - (yield* positionOf("ledger")),
          ...memoryUsage()
        })
      }

      const total = issued * eventsPerCommand
      const drain = yield* timed(viewModels.caughtUp())
      yield* auditLog(total, check)

      const checkLedger = (stage: string) => {
        const balance = Array.from(ledger.balances.values()).reduce((sum, value) => sum + value, 0)
        check(balance === total, `${stage}: the view model's balances add up to ${balance}, not ${total}`)
        check(ledger.stats.gaps === 0, `${stage}: the view model saw ${ledger.stats.gaps} gaps`)
      }
      checkLedger("live")
      check((yield* positionOf("ledger")) === total, "the view model stopped short of the head")
      for (let stream = 0; stream < Math.min(streams, 100); stream++) {
        const id = `account-${stream}`
        const { balance } = yield* accounts.readModel(id)
        check(balance === ledger.balances.get(id), `${id}: read model and view model disagree`)
      }

      const replay = yield* timed(viewModels.replay({ viewModels: ["ledger"] }))
      check(replay.value.eventCount === total, `replay delivered ${replay.value.eventCount} of ${total} events`)
      checkLedger("replay")

      const database = yield* size
      return {
        name,
        metrics: {
          events: total,
          streams,
          workers,
          yieldToOthers: options.yieldToOthers,
          samples,
          peakLag,
          drainSeconds: drain.ms / 1000,
          replaySeconds: replay.ms / 1000,
          replayEventsPerSecond: Math.round((total / replay.ms) * 1000),
          databaseMb: megabytes(database)
        },
        violations: violations()
      } satisfies Report
    }).pipe(
      Effect.provide(ViewModel.layer([ledger.viewModel]).pipe(Layer.provideMerge(openStorage(backend, target))))
    )
  }))

/**
 * Many writers on one stream. Inside a service they take turns, so nobody may give
 * up. Two services on one store stand in for two processes, where conflicts are real
 * and a command may give up, but what was acknowledged must be stored once and what
 * failed must not be.
 *
 * Both stores here are synchronous, so `append` yields first: that is the gap between
 * loading and appending that any store with a real round trip has.
 */
const contention = (sizes: Sizes, backends: ReadonlyArray<Backend>) =>
  Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const cases: Array<Record<string, unknown>> = []
    for (const backend of backends) {
      for (const processes of [1, 2]) {
        for (const workers of sizes.contentionWorkers) {
          const result = yield* Effect.gen(function*() {
            const store = yield* EventStore.EventStore
            const label = `${backend}, ${processes} processes, ${workers} workers`
            let conflicts = 0
            const counting = EventStore.EventStore.of({
              ...store,
              append: (input) =>
                Effect.yieldNow.pipe(
                  Effect.andThen(store.append(input)),
                  Effect.tapError(() => Effect.sync(() => void conflicts++))
                )
            })
            const services = yield* Effect.forEach(
              Array.from({ length: processes }),
              () => AggregateService.make(Account).pipe(Effect.provideService(EventStore.EventStore, counting))
            )

            const latencies: Array<number> = []
            const acknowledged = new Set<string>()
            const { ms } = yield* timed(Effect.forEach(
              Array.from({ length: sizes.contentionCommands }, (_, index) => index),
              (index) =>
                Effect.gen(function*() {
                  const reference = `r-${index}`
                  const start = performance.now()
                  const exit = yield* Effect.exit(
                    services[index % processes]!.commands.deposit("hot", { amount: 1, reference })
                  )
                  latencies.push(performance.now() - start)
                  if (Exit.isSuccess(exit)) {
                    acknowledged.add(reference)
                    return
                  }
                  const error = Cause.squash(exit.cause)
                  check(
                    (error as { readonly _tag?: string })._tag === "ConcurrencyConflict",
                    `${label}: command failed with ${String(error)}`
                  )
                }),
              { concurrency: workers, discard: true }
            ))

            if (processes === 1) {
              check(
                acknowledged.size === sizes.contentionCommands,
                `${label}: ${sizes.contentionCommands - acknowledged.size} commands gave up with no rival process`
              )
              check(conflicts === 0, `${label}: ${conflicts} conflicts with no rival process`)
            }
            yield* auditLog(acknowledged.size, check)
            const accounts = services[0]!
            const { movements } = yield* accounts.readModel("hot")
            check(movements === acknowledged.size, `${label}: state counts ${movements} movements`)
            const history = yield* accounts.history("hot", { payload: true })
            const stored = history.flatMap(({ payload }) => payload === undefined ? [] : [payload.reference])
            check(
              stored.length === acknowledged.size,
              `${label}: ${stored.length} stored, ${acknowledged.size} acknowledged`
            )
            check(stored.every((reference) => acknowledged.has(reference)), `${label}: a failed command left an event`)

            return {
              backend,
              processes,
              workers,
              commands: sizes.contentionCommands,
              acknowledged: acknowledged.size,
              gaveUp: sizes.contentionCommands - acknowledged.size,
              conflicts,
              commandsPerSecond: Math.round((acknowledged.size / ms) * 1000),
              latencyMs: summarize(latencies)
            }
          }).pipe(Effect.provide(storage(backend)))
          cases.push(result)
        }
      }
    }
    return { name: "contention", metrics: { cases }, violations: violations() } satisfies Report
  })

/** A handler that fails now and then: the view model must stall, retry and skip nothing. */
const flakyViewModel = (sizes: Sizes, backend: Backend) =>
  Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const pending = new Set(
      Array.from(
        { length: sizes.flakyFailures },
        (_, index) => Math.floor(((index + 0.5) * sizes.flakyEvents) / sizes.flakyFailures)
      )
    )
    const ledger = makeLedger("flaky", ({ position }) =>
      pending.delete(position) ? Effect.fail(new Error(`injected failure at ${position}`)) : Effect.void)

    yield* seedStream("flaky", sizes.flakyEvents, { chunk: 100 })
    const run = yield* timed(
      ViewModel.ViewModels.use((viewModels) =>
        viewModels.caughtUp()
      ).pipe(
        Effect.provide(ViewModel.layer([ledger.viewModel])),
        Effect.scoped,
        quiet
      )
    )
    check(pending.size === 0, `${pending.size} injected failures were never reached`)
    check(ledger.stats.gaps === 0, `the view model saw ${ledger.stats.gaps} gaps`)
    check(ledger.balances.get("flaky") === sizes.flakyEvents * 10, "the balance is wrong after recovering")
    check(
      ledger.stats.duplicates <= sizes.flakyFailures * BATCH_SIZE,
      `${ledger.stats.duplicates} events were repeated, more than one batch per failure`
    )
    return {
      name: "flaky view model",
      metrics: {
        events: sizes.flakyEvents,
        injectedFailures: sizes.flakyFailures,
        duplicates: ledger.stats.duplicates,
        seconds: run.ms / 1000
      },
      violations: violations()
    } satisfies Report
  }).pipe(Effect.provide(storage(backend)))

/** Stops a durable view model mid-batch, over and over, while the log keeps growing. */
const restart = (sizes: Sizes, backend: Backend) =>
  Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const ledger = makeLedger("restarted")
    const perRound = Math.ceil(sizes.restartEvents / sizes.restarts)
    const running = Effect.provide(ViewModel.layer([ledger.viewModel]))
    for (let round = 0; round < sizes.restarts; round++) {
      yield* seedStream(`stream-${round}`, perRound, { chunk: 100 })
      yield* Effect.sleep(`${5 + (round % 5) * 5} millis`).pipe(running, Effect.scoped)
    }
    const total = perRound * sizes.restarts
    yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(running, Effect.scoped)
    yield* auditLog(total, check)
    check(ledger.stats.gaps === 0, `the view model saw ${ledger.stats.gaps} gaps`)
    check(
      ledger.stats.deliveries - ledger.stats.duplicates === total,
      `${ledger.stats.deliveries - ledger.stats.duplicates} of ${total} events were applied`
    )
    check(
      ledger.stats.duplicates <= sizes.restarts * BATCH_SIZE,
      `${ledger.stats.duplicates} events were repeated, more than one batch per restart`
    )
    return {
      name: "restart",
      metrics: { events: total, restarts: sizes.restarts, duplicates: ledger.stats.duplicates },
      violations: violations()
    } satisfies Report
  }).pipe(Effect.provide(storage(backend)))

/** Runs a writer process for a while, kills it without warning and returns what it acknowledged. */
const runUntilKilled = (backend: DurableBackend, target: string, afterFirstAck: number) =>
  Effect.callback<ReadonlyArray<number>>((resume) => {
    const child = spawn(process.execPath, ["bench/crash-writer.ts", backend, target], {
      stdio: ["ignore", "pipe", "inherit"]
    })
    const acknowledged: Array<number> = []
    let pending = ""
    child.stdout.on("data", (chunk: Buffer) => {
      if (acknowledged.length === 0) setTimeout(() => child.kill("SIGKILL"), afterFirstAck)
      const lines = (pending + chunk.toString()).split("\n")
      pending = lines.pop() ?? ""
      for (const line of lines) acknowledged.push(Number(line))
    })
    child.on("close", () => resume(Effect.succeed(acknowledged)))
  })

/**
 * Kills writer processes mid-commit, several at a time on the same streams:
 * acknowledged events must survive, commands must stay whole and the log must have
 * no gaps.
 */
const crash = (sizes: Sizes, backend: DurableBackend) =>
  Effect.scoped(Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const { target } = yield* temporaryDurable(backend)
    // Creates the tables, which the writers would otherwise race to do.
    yield* Effect.void.pipe(Effect.provide(openStorage(backend, target)))
    let acknowledged = 0
    let last = 0
    for (let round = 0; round < sizes.crashes; round++) {
      const writers = yield* Effect.forEach(
        Array.from({ length: sizes.crashWriters }, (_, writer) => writer),
        (writer) => runUntilKilled(backend, target, 100 + ((round + writer) % 4) * 150),
        { concurrency: "unbounded" }
      )
      for (const positions of writers) {
        acknowledged += positions.length
        for (const position of positions) last = Math.max(last, position)
      }
    }
    return yield* Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const head = yield* store.head
      check(head >= last, `the last acknowledged position ${last} is past the head ${head}`)
      check(
        head >= acknowledged * crashEventsPerCommand,
        `${acknowledged} commands acknowledged, ${head} events stored`
      )
      check(head % crashEventsPerCommand === 0, `the log ends in the middle of a command, at ${head}`)
      const versions = yield* auditLog(head, check)
      for (const [id, version] of versions) {
        check(
          version % crashEventsPerCommand === 0,
          `stream ${id} ends in the middle of a command, at version ${version}`
        )
      }
      return {
        name: "crash",
        metrics: {
          kills: sizes.crashes * sizes.crashWriters,
          writersAtOnce: sizes.crashWriters,
          acknowledgedCommands: acknowledged,
          storedEvents: head,
          committedButUnacknowledged: head - acknowledged * crashEventsPerCommand
        },
        violations: violations()
      } satisfies Report
    }).pipe(Effect.provide(openStorage(backend, target)))
  }))

/** A view model far slower than the writers: the backlog must drain, in order, without memory running away. */
const slowConsumer = (sizes: Sizes, backend: Backend) =>
  Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const ledger = makeLedger("slow", () => Effect.sleep("1 millis"))
    const before = memoryUsage()
    return yield* Effect.gen(function*() {
      const accounts = yield* AggregateService.make(Account)
      const viewModels = yield* ViewModel.ViewModels
      const store = yield* EventStore.EventStore
      let peakLag = 0
      const write = yield* timed(Effect.forEach(
        Array.from({ length: sizes.slowEvents }, (_, index) => index),
        (index) =>
          accounts.commands.deposit(`account-${index % 100}`, { amount: 1, reference: `r-${index}` }).pipe(
            Effect.andThen(index % 500 === 0 ? positionOf("slow") : Effect.succeed(-1)),
            Effect.flatMap((position) =>
              position < 0
                ? Effect.void
                : Effect.map(store.head, (head) => void (peakLag = Math.max(peakLag, head - position)))
            )
          ),
        { discard: true }
      ))
      const backlog = memoryUsage()
      const drain = yield* timed(viewModels.caughtUp())
      yield* auditLog(sizes.slowEvents, check)
      check(ledger.stats.gaps === 0, `the view model saw ${ledger.stats.gaps} gaps`)
      check(
        ledger.stats.deliveries - ledger.stats.duplicates === sizes.slowEvents,
        "the view model missed events"
      )
      return {
        name: "slow consumer",
        metrics: {
          events: sizes.slowEvents,
          peakLag,
          writeSeconds: write.ms / 1000,
          drainSeconds: drain.ms / 1000,
          heapGrowthMb: backlog.heapMb - before.heapMb
        },
        violations: violations()
      } satisfies Report
    }).pipe(
      Effect.provide(ViewModel.layer([ledger.viewModel]).pipe(Layer.provideMerge(storage(backend))))
    )
  })

/** Everyone following the same log must see every event, in order. */
const subscribers = (sizes: Sizes, backend: Backend) =>
  Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const store = yield* EventStore.EventStore
    const accounts = yield* AggregateService.make(Account)
    const fibers = yield* Effect.forEach(
      Array.from({ length: sizes.subscribers }, (_, index) => index),
      (index) =>
        store.subscribe().pipe(
          Stream.take(sizes.subscriberEvents),
          Stream.runFold(() => 0, (seen, event) => {
            check(event.position === seen + 1, `subscriber ${index} saw ${event.position} after ${seen}`)
            return event.position
          }),
          Effect.forkChild
        )
    )
    const { ms } = yield* timed(Effect.gen(function*() {
      for (let index = 0; index < sizes.subscriberEvents; index++) {
        yield* accounts.commands.deposit(`account-${index % 100}`, { amount: 1, reference: `r-${index}` })
      }
      for (const fiber of fibers) {
        check((yield* Fiber.join(fiber)) === sizes.subscriberEvents, "a subscriber ended early")
      }
    }))
    return {
      name: "subscribers",
      metrics: {
        subscribers: sizes.subscribers,
        events: sizes.subscriberEvents,
        deliveriesPerSecond: Math.round(((sizes.subscribers * sizes.subscriberEvents) / ms) * 1000)
      },
      violations: violations()
    } satisfies Report
  }).pipe(Effect.provide(storage(backend)))

/** One command emitting a huge batch. It may be refused, but never half applied. */
const hugeCommand = (sizes: Sizes, backends: ReadonlyArray<Backend>) =>
  Effect.gen(function*() {
    const { check, violations } = makeChecks()
    const cases: Array<Record<string, unknown>> = []
    for (const backend of backends) {
      for (const count of sizes.hugeCommands) {
        const result = yield* Effect.gen(function*() {
          const accounts = yield* AggregateService.make(Account)
          const store = yield* EventStore.EventStore
          const { ms, value: exit } = yield* timed(
            Effect.exit(accounts.commands.depositMany("huge", { amount: 1, reference: "r", count }))
          )
          const head = yield* store.head
          const stored = Exit.isSuccess(exit)
          check(head === (stored ? count : 0), `${backend}, ${count} events: head is ${head}`)
          return {
            backend,
            count,
            stored,
            seconds: ms / 1000,
            error: Exit.isFailure(exit) ? String(Cause.squash(exit.cause)).slice(0, 80) : undefined
          }
        }).pipe(Effect.provide(storage(backend)))
        cases.push(result)
      }
    }
    return { name: "huge command", metrics: { cases }, violations: violations() } satisfies Report
  })

/** The same scenarios on SQLite, with the memory store where it compares, or on Postgres. */
export const scenarios = (
  sizes: Sizes,
  database: "sqlite" | "postgres" = "sqlite"
): ReadonlyArray<{ readonly name: string; readonly run: Effect.Effect<Report, unknown> }> => {
  const onPostgres = database === "postgres"
  const durable = onPostgres ? "postgres" : "sqlite-file"
  const quick = onPostgres ? "postgres" : "sqlite-memory"
  return [
    {
      name: "ingest",
      run: ingest("ingest", durable, sizes.ingestEvents, { eventsPerStream: 100, yieldToOthers: true })
    },
    {
      name: "ingest, short streams",
      run: ingest("ingest, short streams", durable, sizes.ingestEvents, { eventsPerStream: 10, yieldToOthers: true })
    },
    { name: "burst", run: ingest("burst", durable, sizes.burstEvents, { eventsPerStream: 100, yieldToOthers: false }) },
    { name: "contention", run: contention(sizes, onPostgres ? ["postgres"] : ["memory", "sqlite-file"]) },
    { name: "flaky view model", run: flakyViewModel(sizes, quick) },
    { name: "restart", run: restart(sizes, durable) },
    { name: "crash", run: crash(sizes, durable) },
    { name: "slow consumer", run: slowConsumer(sizes, quick) },
    { name: "subscribers", run: subscribers(sizes, quick) },
    { name: "huge command", run: hugeCommand(sizes, onPostgres ? ["postgres"] : ["memory", "sqlite-memory"]) }
  ]
}
