import { assert, describe, it } from "@effect/vitest"
import { Effect, Option, References, Schedule } from "effect"
import { AggregateService, ViewModel } from "effect-cqrs"
import { configurations, Ledger, on, range, readAll, storageOf, verifies } from "./support.ts"

const makeLedgers = AggregateService.make(Ledger)

const quiet = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provideService(effect, References.MinimumLogLevel, "None")

/**
 * A view model that totals the draws of each ledger, and remembers the position of
 * every event it handled, in the order it handled them. It fails on the positions
 * in `broken`.
 */
const makeTotals = (onFailure?: ViewModel.FailurePolicy) => {
  const handled: Array<number> = []
  const totals = new Map<string, number>()
  const broken = new Set<number>()
  const handle = (position: number, apply: () => void) =>
    broken.has(position)
      ? Effect.fail(`cannot handle the event at ${position}`)
      : Effect.sync(() => {
        handled.push(position)
        apply()
      })
  const view = ViewModel.make({
    name: "totals",
    aggregates: [Ledger],
    dispose: Effect.sync(() => {
      handled.length = 0
      totals.clear()
    }),
    onFailure,
    handlers: {
      Opened: (_, { aggregateId, position }) => handle(position, () => totals.set(aggregateId, 0)),
      Drawn: ({ amount }, { aggregateId, position }) =>
        handle(position, () => totals.set(aggregateId, (totals.get(aggregateId) ?? 0) + amount))
    }
  })
  return { view, handled, totals, broken }
}

/** Runs with the view model started, and stops it afterwards, as a process does. */
const running = <V extends ViewModel.Any>(view: V) => <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(ViewModel.layer([view])), Effect.scoped)

const status = Effect.map(ViewModel.ViewModels.use((viewModels) => viewModels.status), (statuses) => statuses[0]!)

const until = <A, R>(effect: Effect.Effect<A, never, R>, done: (value: A) => boolean) =>
  effect.pipe(Effect.repeat({ until: done, schedule: Schedule.spaced("5 millis") }), Effect.timeout("5 seconds"))

/** The positions of the events the view model subscribes to, in store order. */
const subscribed = Effect.map(
  readAll({ aggregateNames: [Ledger.name], eventTypes: ["Opened", "Drawn"] }),
  (events) => events.map((event) => event.position)
)

for (const configuration of configurations) {
  const storage = storageOf(configuration)

  describe.skipIf(!configuration.available)(`view models ${on(configuration)}`, () => {
    it.live(verifies("VM-001", "handles every event in store order, and has a command's events once caught up"), () => {
      const { handled, totals, view } = makeTotals()
      return Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        const viewModels = yield* ViewModel.ViewModels
        const ids = ["l-0", "l-1", "l-2"]
        for (const id of ids) yield* ledgers.commands.open(id, { limit: 1000 })

        // Reading your own write.
        const own = yield* ledgers.commands.draw("l-0", { amount: 7, by: "nurse" })
        yield* viewModels.caughtUp(own.position)
        assert.strictEqual(totals.get("l-0"), 7)

        // Many writers at once, and an event the view model doesn't subscribe to.
        yield* Effect.forEach(
          range(60),
          (index) => ledgers.commands.draw(ids[index % 3]!, { amount: 1 + (index % 4), by: "nurse" }),
          { concurrency: 6 }
        )
        yield* ledgers.commands.close("l-2", {})
        const last = yield* ledgers.commands.drawEach("l-1", { amounts: [1, 2, 3], by: "nurse" })
        yield* viewModels.caughtUp(last.position)

        assert.deepStrictEqual(handled, yield* subscribed)
        assert.strictEqual(handled.length, 3 + 1 + 60 + 3)
        for (const id of ids) assert.strictEqual(totals.get(id), (yield* ledgers.state(id)).drawn)
        assert.strictEqual((yield* status).position, last.position)
      }).pipe(running(view), Effect.provide(storage))
    })

    it.live(
      verifies("VM-002", "stalls on an event its handler fails on, reports it, and goes on when it succeeds"),
      () => {
        const { broken, handled, view } = makeTotals()
        broken.add(2)
        return Effect.gen(function*() {
          const ledgers = yield* makeLedgers
          const viewModels = yield* ViewModel.ViewModels
          yield* ledgers.commands.open("l", { limit: 10 })
          yield* ledgers.commands.draw("l", { amount: 1, by: "nurse" })
          yield* ledgers.commands.draw("l", { amount: 2, by: "nurse" })

          const stalled = yield* until(status, ({ lastError }) => Option.isSome(lastError))
          assert.strictEqual(Option.getOrThrow(stalled.lastError).position, 2)
          assert.strictEqual(stalled.parked, 0)
          // Nothing after the event is handled, however long it takes.
          yield* Effect.sleep("300 millis")
          assert.deepStrictEqual(handled, [1])
          assert.isBelow((yield* status).position, 2)
          const waiting = yield* viewModels.caughtUp().pipe(Effect.timeout("50 millis"), Effect.flip)
          assert.strictEqual(waiting._tag, "TimeoutError")

          broken.clear()
          yield* viewModels.caughtUp().pipe(Effect.timeout("10 seconds"))
          assert.deepStrictEqual(handled, [1, 2, 3])
          yield* until(status, ({ lastError }) => Option.isNone(lastError))
        }).pipe(running(view), Effect.provide(storage), quiet)
      },
      { timeout: 30_000 }
    )

    it.live(verifies("VM-003", "continues from its checkpoint after a restart, with what it missed"), () => {
      const { handled, totals, view } = makeTotals()
      const caughtUp = ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp()).pipe(running(view))
      return Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        yield* ledgers.commands.open("l", { limit: 1000 })
        for (const _ of range(5)) yield* ledgers.commands.draw("l", { amount: 1, by: "nurse" })
        yield* caughtUp
        assert.deepStrictEqual(handled, yield* subscribed)

        // While the view model is down.
        for (const _ of range(40)) yield* ledgers.commands.draw("l", { amount: 2, by: "nurse" })
        yield* caughtUp
        yield* ledgers.commands.draw("l", { amount: 3, by: "nurse" })
        yield* caughtUp

        // Each event once: stopped cleanly, it saved where it got to.
        assert.deepStrictEqual(handled, yield* subscribed)
        assert.strictEqual(handled.length, 47)
        assert.strictEqual(totals.get("l"), 5 + 80 + 3)
      }).pipe(Effect.provide(storage))
    })

    it.live(verifies("VM-004", "rebuilds by replay what handling the events live gave"), () => {
      const { handled, totals, view } = makeTotals()
      return Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        const viewModels = yield* ViewModel.ViewModels
        for (const id of ["l-0", "l-1"]) yield* ledgers.commands.open(id, { limit: 1000 })
        for (const index of range(30)) {
          yield* ledgers.commands.draw(`l-${index % 2}`, { amount: 1 + (index % 5), by: "nurse" })
        }
        yield* viewModels.caughtUp()
        const live = { handled: [...handled], totals: new Map(totals) }

        // Damage of the kind a replay is for.
        totals.set("l-0", -1)
        totals.set("stray", 99)
        const result = yield* viewModels.replay({ viewModels: ["totals"] })

        assert.deepStrictEqual(result, { viewModels: ["totals"], eventCount: 32 })
        assert.deepStrictEqual(totals, live.totals)
        assert.deepStrictEqual(handled, live.handled)

        // It follows new events afterwards.
        const next = yield* ledgers.commands.draw("l-0", { amount: 10, by: "nurse" })
        yield* viewModels.caughtUp(next.position)
        assert.strictEqual(totals.get("l-0"), live.totals.get("l-0")! + 10)

        const unknown = yield* Effect.flip(viewModels.replay({ viewModels: ["missing"] }))
        assert.strictEqual(unknown._tag, "UnknownViewModel")
      }).pipe(running(view), Effect.provide(storage))
    })

    it.live(verifies("VM-005", "records an event it parks, with its error, and handles it on a retry"), () => {
      const { broken, handled, totals, view } = makeTotals(ViewModel.park({ attempts: 2 }))
      broken.add(2)
      return Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        const viewModels = yield* ViewModel.ViewModels
        yield* ledgers.commands.open("l", { limit: 10 })
        yield* ledgers.commands.draw("l", { amount: 1, by: "nurse" })
        yield* ledgers.commands.draw("l", { amount: 2, by: "nurse" })

        yield* viewModels.caughtUp().pipe(Effect.timeout("10 seconds"))

        assert.deepStrictEqual(handled, [1, 3])
        const parkedStatus = yield* status
        assert.strictEqual(parkedStatus.parked, 1)
        assert.isTrue(Option.isNone(parkedStatus.lastError))
        const parked = yield* viewModels.parked("totals")
        assert.strictEqual(parked.length, 1)
        assert.deepInclude(parked[0]!, { viewModel: "totals", position: 2, eventId: "ledger/l@2", attempts: 2 })
        assert.include(parked[0]!.error, "cannot handle the event at 2")

        // A retry that fails again keeps it parked.
        const again = yield* viewModels.retryParked({ viewModel: "totals" })
        assert.deepStrictEqual(again.succeeded, [])
        assert.deepStrictEqual(again.failed.map((event) => event.position), [2])
        assert.strictEqual((yield* viewModels.parked("totals")).length, 1)

        broken.clear()
        const retried = yield* viewModels.retryParked({ viewModel: "totals" })
        assert.deepStrictEqual(retried, { succeeded: [2], failed: [] })
        assert.deepStrictEqual(yield* viewModels.parked("totals"), [])
        assert.strictEqual((yield* status).parked, 0)
        assert.deepStrictEqual([...handled].sort(), [1, 2, 3])
        assert.strictEqual(totals.get("l"), 3)
      }).pipe(running(view), Effect.provide(storage), quiet)
    }, { timeout: 30_000 })
  })
}
