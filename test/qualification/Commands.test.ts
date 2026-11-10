import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect, Layer, Metric, References, Schema } from "effect"
import { Aggregate, AggregateService, EventStore, EventVersions } from "effect-cqrs"
import { KeyValueStore } from "effect/persistence"
import { TestClock } from "effect/testing"
import {
  configurations,
  head,
  Ledger,
  LedgerState,
  ledgerStream,
  LimitExceeded,
  on,
  outcome,
  range,
  readAll,
  seeded,
  storageOf,
  verifies
} from "./support.ts"

const makeLedgers = AggregateService.make(Ledger)

type Ledgers = AggregateService.AggregateService<typeof Ledger>

const quiet = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provideService(effect, References.MinimumLogLevel, "None")

const count = (outcomes: ReadonlyArray<string>, tag: string) => outcomes.filter((each) => each === tag).length

/** How many appends lost a race, as the services counted them. */
const conflicts = Effect.map(Metric.snapshot, (snapshots) =>
  snapshots
    .filter((snapshot) => snapshot.id === AggregateService.MetricNames.concurrencyConflicts)
    .reduce((sum, { state }) => sum + Number((state as { readonly count: number | bigint }).count), 0))

/** A store whose next append to each of these streams comes second to a rival's draw. */
const outrun = Effect.fnUntraced(function*(aggregateIds: ReadonlyArray<string>) {
  const store = yield* EventStore.EventStore
  const rivals = yield* makeLedgers
  const waiting = new Set(aggregateIds)
  return EventStore.EventStore.of({
    ...store,
    append: (input) =>
      waiting.delete(input.aggregateId)
        ? Effect.andThen(
          Effect.orDie(rivals.commands.draw(input.aggregateId, { amount: 1, by: "rival" })),
          store.append(input)
        )
        : store.append(input)
  })
})

// The older shape of a measurement, from before it recorded its unit.
const MeasuredV1 = Schema.Struct({ value: Schema.Number })
const ReadingEvent = Schema.TaggedUnion({
  Measured: { value: Schema.Number, unit: Schema.Literals(["mg", "g"]) }
})
const Reading = Aggregate.make<ReadonlyArray<string>>()({
  name: "reading",
  events: ReadingEvent,
  initial: [],
  projection: { Measured: (readings, { unit, value }) => [...readings, `${value} ${unit}`] },
  versions: {
    Measured: EventVersions.from(MeasuredV1).next(ReadingEvent.cases.Measured, (v1) => ({ ...v1, unit: "mg" as const }))
  }
}).command("measure", {
  payload: Schema.Struct({ value: Schema.Number, unit: Schema.Literals(["mg", "g"]) }),
  handler: (_, payload) => Effect.succeed(ReadingEvent.cases.Measured.make(payload))
})

const snapshotKey = (aggregateId: string) => `effect-cqrs/snapshot/${JSON.stringify([Ledger.name, "1", aggregateId])}`

/** A service with snapshots, as a new process would build it, recording where each read of a stream starts. */
const withSnapshots = Effect.gen(function*() {
  const store = yield* EventStore.EventStore
  const reads: Array<number> = []
  const ledgers = yield* AggregateService.make(Ledger, { snapshots: { every: 5, schema: LedgerState } }).pipe(
    Effect.provideService(
      EventStore.EventStore,
      EventStore.EventStore.of({
        ...store,
        read: (filter) => {
          reads.push(filter?.afterVersion ?? 0)
          return store.read(filter)
        }
      })
    )
  )
  return { ledgers, reads }
})

for (const configuration of configurations) {
  const storage = storageOf(configuration)

  describe.skipIf(!configuration.available)(`commands ${on(configuration)}`, () => {
    it.effect(
      verifies("CMD-001", "rejects what doesn't match a schema before deciding, and stores nothing"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers

          const invalid = yield* Effect.forEach([
            ledgers.commands.open("l", { limit: 1.5 }),
            ledgers.commands.open("l", { limit: -1 }),
            ledgers.commands.draw("l", { amount: 0, by: "nurse" }),
            ledgers.commands.draw("l", { amount: 1, by: "" }),
            ledgers.dispatch({ aggregateId: "l", type: "open", payload: { limit: "10" } }),
            ledgers.dispatch({ aggregateId: "l", type: "open", payload: {} }),
            ledgers.dispatch({ aggregateId: "l", type: "open", payload: null }),
            ledgers.dispatch({ aggregateId: "l", type: "open" }),
            ledgers.dispatch({ aggregateId: "", type: "open", payload: { limit: 1 } }),
            ledgers.dispatch({ aggregateId: 7, type: "open", payload: { limit: 1 } }),
            ledgers.dispatch({ type: "open", payload: { limit: 1 } }),
            ledgers.dispatch({ aggregateId: "l", type: "open", payload: { limit: 1 }, expectedVersion: -1 }),
            ledgers.dispatch({ aggregateId: "l", type: "open", payload: { limit: 1 }, commandId: "" }),
            ledgers.dispatch({ aggregateId: "l", type: "open", payload: { limit: 1 }, metadata: "who" }),
            ledgers.dispatch("open"),
            ledgers.dispatch(null),
            ledgers.dispatch([])
          ], outcome)
          const unknown = yield* Effect.forEach(
            ["erase", "", "Open", "toString", "constructor", "__proto__", "hasOwnProperty"],
            (type) => outcome(ledgers.dispatch({ aggregateId: "l", type, payload: { limit: 1 } }))
          )

          assert.deepStrictEqual(invalid, invalid.map(() => "InvalidCommand"))
          assert.deepStrictEqual(unknown, unknown.map(() => "UnknownCommand"))
          assert.strictEqual(yield* head, 0)
          assert.deepStrictEqual(yield* ledgers.readModel("l"), Ledger.initial)
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("CMD-002", "stores nothing of a command its handler rejects"), () =>
      Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        yield* ledgers.commands.open("l", { limit: 5 })
        yield* ledgers.commands.draw("l", { amount: 3, by: "nurse" })
        const state = yield* ledgers.readModel("l")

        const exceeded = yield* Effect.flip(ledgers.commands.draw("l", { amount: 3, by: "nurse" }))
        const several = yield* Effect.flip(ledgers.commands.drawEach("l", { amounts: [1, 1, 1], by: "nurse" }))
        const twice = yield* Effect.flip(ledgers.commands.open("l", { limit: 50 }))
        const missing = yield* Effect.flip(ledgers.commands.draw("unopened", { amount: 1, by: "nurse" }))

        assert.deepStrictEqual(exceeded, new LimitExceeded({ limit: 5, drawn: 3, amount: 3 }))
        assert.deepStrictEqual([exceeded._tag, several._tag, twice._tag, missing._tag], [
          "LimitExceeded",
          "LimitExceeded",
          "NotOpen",
          "NotOpen"
        ])
        assert.strictEqual(yield* head, 2)
        assert.deepStrictEqual(yield* ledgers.readModel("l"), state)
        assert.deepStrictEqual(yield* ledgerStream("unopened"), [])
      }).pipe(Effect.provide(storage)))

    it.effect(
      verifies("CMD-003", "gives the fold of the stream, from memory and from a replay, after 300 commands"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers
          const random = seeded(20261110)
          const ids = ["l-0", "l-1", "l-2", "l-3"]
          // What the states have to be, kept apart from the framework.
          const expected = new Map(ids.map((id) => [id, { ...Ledger.initial }]))

          for (const id of ids) {
            const limit = 40 + Math.floor(random() * 60)
            yield* ledgers.commands.open(id, { limit })
            expected.set(id, { status: "open", limit, drawn: 0, draws: 0 })
          }
          for (const index of range(300)) {
            const id = ids[Math.floor(random() * ids.length)]!
            const model = expected.get(id)!
            const amounts = range(1 + Math.floor(random() * 3)).map(() => 1 + Math.floor(random() * 6))
            const amount = amounts.reduce((sum, each) => sum + each, 0)
            const fits = model.status === "open" && model.drawn + amount <= model.limit
            const result = yield* outcome(
              index % 97 === 96
                ? ledgers.commands.close(id, {})
                : amounts.length === 1
                ? ledgers.commands.draw(id, { amount, by: `user-${index}` })
                : ledgers.commands.drawEach(id, { amounts, by: `user-${index}` })
            )
            if (index % 97 === 96) {
              assert.strictEqual(result, model.status === "open" ? "ok" : "NotOpen")
              if (model.status === "open") expected.set(id, { ...model, status: "closed" })
            } else {
              assert.strictEqual(result, fits ? "ok" : model.status === "open" ? "LimitExceeded" : "NotOpen")
              if (fits) expected.set(id, { ...model, drawn: model.drawn + amount, draws: model.draws + amounts.length })
            }
          }

          const restarted = yield* makeLedgers
          for (const id of ids) {
            const model = expected.get(id)!
            assert.deepStrictEqual(yield* ledgers.readModel(id), model)
            assert.deepStrictEqual(yield* restarted.readModel(id), model)
            const stream = yield* ledgerStream(id)
            assert.strictEqual(stream.filter((event) => event.type === "Drawn").length, model.draws)
            assert.deepStrictEqual(stream.map((event) => event.version), range(stream.length).map((index) => index + 1))
          }
        }).pipe(Effect.provide(storage)),
      { timeout: 30_000 }
    )

    it.effect(
      verifies("CMD-004", "keeps the limit when 60 draws are asked of one service at once"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers
          yield* ledgers.commands.open("l", { limit: 20 })

          const outcomes = yield* Effect.forEach(
            range(60),
            (index) => outcome(ledgers.commands.draw("l", { amount: 1, by: `user-${index}` })),
            { concurrency: "unbounded" }
          )

          assert.strictEqual(count(outcomes, "ok"), 20)
          assert.strictEqual(count(outcomes, "LimitExceeded"), 40)
          assert.deepStrictEqual(yield* ledgers.readModel("l"), { status: "open", limit: 20, drawn: 20, draws: 20 })
          const stream = yield* ledgerStream("l")
          assert.deepStrictEqual(stream.map((event) => event.version), range(21).map((index) => index + 1))
          // Every stored draw is one a caller was told about.
          assert.strictEqual(new Set(stream.slice(1).map((event) => (event.payload as { by: string }).by)).size, 20)
        }).pipe(Effect.provide(storage))
    )

    it.live(
      verifies("CMD-005", "keeps the limit when two services race for the same aggregate"),
      () =>
        Effect.gen(function*() {
          const services = [yield* makeLedgers, yield* makeLedgers]
          yield* services[0]!.commands.open("l", { limit: 15 })

          const outcomes = yield* Effect.forEach(
            range(40),
            (index) => outcome(services[index % 2]!.commands.draw("l", { amount: 1, by: `user-${index}` })),
            { concurrency: "unbounded" }
          )

          const accepted = count(outcomes, "ok")
          const gaveUp = count(outcomes, "ConcurrencyConflict")
          assert.strictEqual(accepted + gaveUp + count(outcomes, "LimitExceeded"), 40)
          // The services did collide, so this is what the requirement is about.
          assert.isAbove(yield* conflicts, 0)
          const stream = yield* ledgerStream("l")
          assert.strictEqual(stream.length - 1, accepted)
          assert.isAtMost(accepted, 15)
          if (gaveUp === 0) assert.strictEqual(accepted, 15)
          const state = { status: "open", limit: 15, drawn: accepted, draws: accepted }
          assert.deepStrictEqual(yield* (yield* makeLedgers).readModel("l"), state)
          for (const service of services) assert.deepStrictEqual(yield* service.readModel("l"), state)
        }).pipe(Effect.provide(storage), Effect.provideService(Metric.MetricRegistry, new Map())),
      { timeout: 30_000 }
    )

    it.live(
      verifies("CMD-006", "decides again after losing a race, and fails when its retries run out"),
      () =>
        Effect.gen(function*() {
          const setup = yield* makeLedgers
          yield* setup.commands.open("gives-up", { limit: 5 })
          yield* setup.commands.open("last-unit", { limit: 1 })
          yield* setup.commands.open("room-left", { limit: 2 })
          const racing = yield* outrun(["gives-up", "last-unit", "room-left"])
          const provideRacing = Effect.provideService(EventStore.EventStore, racing)
          const impatient = yield* AggregateService.make(Ledger, { maxRetries: 0 }).pipe(provideRacing)
          const patient = yield* makeLedgers.pipe(provideRacing)
          const draw = (ledgers: Ledgers, id: string) => ledgers.commands.draw(id, { amount: 1, by: "late" })
          const drawers = (id: string) =>
            Effect.map(
              ledgerStream(id),
              (events) => events.slice(1).map((event) => (event.payload as { by: string }).by)
            )

          // Without retries the conflict is the answer, and only the rival's draw is stored.
          const conflict = yield* Effect.flip(draw(impatient, "gives-up"))
          assert.strictEqual(conflict._tag, "ConcurrencyConflict")
          assert.deepStrictEqual(yield* drawers("gives-up"), ["rival"])

          // The rival took the only unit, which the second decision has to see.
          const exceeded = yield* Effect.flip(draw(patient, "last-unit"))
          assert.strictEqual(exceeded._tag, "LimitExceeded")
          assert.deepStrictEqual(yield* drawers("last-unit"), ["rival"])

          // With room left the second decision is stored after the rival's.
          const result = yield* draw(patient, "room-left")
          assert.strictEqual(result.version, 3)
          assert.deepStrictEqual(yield* drawers("room-left"), ["rival", "late"])
        }).pipe(Effect.provide(storage))
    )

    it.live(verifies("CMD-007", "answers a repeated commandId with the first result"), () =>
      Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        yield* ledgers.commands.open("l", { limit: 10 })
        const draw = (service: Ledgers, commandId: string) =>
          service.commands.drawEach("l", { amounts: [1, 2], by: "nurse" }, { commandId })
        const summary = (result: Effect.Success<ReturnType<typeof draw>>) => ({
          version: result.version,
          position: result.position,
          events: result.events,
          correlationId: result.correlationId
        })

        const first = summary(yield* draw(ledgers, "c-1"))
        const before = yield* head

        // Again in the same service, in a new one, and through an envelope.
        assert.deepStrictEqual(summary(yield* draw(ledgers, "c-1")), first)
        assert.deepStrictEqual(summary(yield* draw(yield* makeLedgers, "c-1")), first)
        const dispatched = yield* (yield* makeLedgers).dispatch({
          aggregateId: "l",
          type: "drawEach",
          payload: { amounts: [1, 2], by: "nurse" },
          commandId: "c-1"
        })
        assert.deepStrictEqual(summary(dispatched), first)
        assert.strictEqual(yield* head, before)

        // Duplicates that arrive together, in one service and in two.
        const services = [ledgers, yield* makeLedgers]
        const together = yield* Effect.forEach(range(8), (index) => draw(services[index % 2]!, "c-2"), {
          concurrency: "unbounded"
        })
        assert.strictEqual(new Set(together.map((result) => result.version)).size, 1)
        assert.strictEqual(yield* head, before + 2)
        assert.deepStrictEqual((yield* ledgers.readModel("l")).drawn, 6)

        // The id of one command doesn't answer for another.
        const reused = yield* Effect.flip(ledgers.commands.close("l", {}, { commandId: "c-1" }))
        assert.strictEqual(reused._tag, "CommandIdReused")
        assert.strictEqual((yield* ledgers.readModel("l")).status, "open")
        assert.strictEqual(yield* head, before + 2)
      }).pipe(Effect.provide(storage)))

    it.effect(
      verifies("CMD-008", "refuses a command based on another version than the aggregate's"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers
          yield* ledgers.commands.open("l", { limit: 10 }, { expectedVersion: 0 })
          const draw = (expectedVersion: number) =>
            ledgers.commands.draw("l", { amount: 1, by: "nurse" }, { expectedVersion })

          const stale = yield* Effect.flip(draw(0))
          const ahead = yield* Effect.flip(draw(2))
          const dispatched = yield* Effect.flip(ledgers.dispatch({
            aggregateId: "l",
            type: "draw",
            payload: { amount: 1, by: "nurse" },
            expectedVersion: 0
          }))

          assert.deepStrictEqual(
            [stale, ahead, dispatched].map((error) => [
              error._tag,
              error._tag === "WrongExpectedVersion" && error.expectedVersion,
              error._tag === "WrongExpectedVersion" && error.actualVersion
            ]),
            [["WrongExpectedVersion", 0, 1], ["WrongExpectedVersion", 2, 1], ["WrongExpectedVersion", 0, 1]]
          )
          assert.strictEqual(yield* head, 1)
          assert.strictEqual((yield* draw(1)).version, 2)
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("CMD-009", "stores the events of a command together"), () =>
      Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        yield* ledgers.commands.open("l", { limit: 10 })
        yield* ledgers.commands.open("other", { limit: 10 })
        yield* TestClock.adjust("5 seconds")

        const result = yield* ledgers.commands.drawEach("l", { amounts: [1, 2, 3], by: "nurse" })

        const written = (yield* ledgerStream("l")).slice(1)
        assert.deepStrictEqual(written.map((event) => event.version), [2, 3, 4])
        assert.deepStrictEqual(written.map((event) => event.position), [3, 4, 5])
        assert.deepStrictEqual(written.map((event) => DateTime.toEpochMillis(event.timestamp)), [5000, 5000, 5000])
        assert.deepStrictEqual(written.map((event) => event.correlationId), range(3).map(() => result.correlationId))
        assert.strictEqual(new Set(written.map((event) => event.causationId)).size, 1)
        assert.deepStrictEqual(written.map((event) => (event.payload as { amount: number }).amount), [1, 2, 3])
        assert.deepStrictEqual([result.version, result.position, result.events.length], [4, 5, 3])
      }).pipe(Effect.provide(storage)))

    it.effect(
      verifies("CMD-010", "fails on a stored event it can't decode, rather than skipping it"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          const ledgers = yield* makeLedgers
          const opened = { type: "Opened", payload: { _tag: "Opened", limit: 5 } }
          yield* store.append({
            aggregateName: Ledger.name,
            aggregateId: "damaged",
            expectedVersion: 0,
            events: [opened, { type: "Drawn", payload: { _tag: "Drawn", amount: "plenty", by: "nurse" } }]
          })
          yield* store.append({
            aggregateName: Ledger.name,
            aggregateId: "from-the-future",
            expectedVersion: 0,
            events: [opened, { type: "Drawn", payload: { _tag: "Drawn", amount: 1, by: "nurse" }, schemaVersion: 4 }]
          })
          const before = yield* head

          for (const [id, schemaVersion] of [["damaged", 1], ["from-the-future", 4]] as const) {
            const errors = yield* Effect.all([
              Effect.flip(ledgers.readModel(id)),
              Effect.flip(ledgers.history(id, { payload: true })),
              Effect.flip(ledgers.commands.draw(id, { amount: 1, by: "nurse" })),
              Effect.flip(ledgers.commands.close(id, {}))
            ])
            for (const error of errors) {
              assert.strictEqual(error._tag, "EventDecodeError")
              assert.deepInclude(error, {
                aggregateName: "ledger",
                aggregateId: id,
                version: 2,
                type: "Drawn",
                schemaVersion
              })
            }
          }
          assert.strictEqual(yield* head, before)
          // The damage is confined to its stream.
          yield* ledgers.commands.open("sound", { limit: 1 })
        }).pipe(Effect.provide(storage))
    )

    it.effect(verifies("CMD-011", "gives the state as it was before a past time"), () =>
      Effect.gen(function*() {
        const ledgers = yield* makeLedgers
        yield* TestClock.adjust("1 second")
        const beforeOpening = yield* DateTime.now
        yield* ledgers.commands.open("l", { limit: 10 })
        yield* TestClock.adjust("1 second")
        yield* ledgers.commands.draw("l", { amount: 2, by: "nurse" })
        const atSecondDraw = yield* DateTime.now
        yield* TestClock.adjust("1 second")
        const between = yield* DateTime.now
        yield* TestClock.adjust("1 second")
        yield* ledgers.commands.draw("l", { amount: 3, by: "nurse" })
        yield* ledgers.commands.close("l", {})
        const now = { status: "closed", limit: 10, drawn: 5, draws: 2 }

        for (const service of [ledgers, yield* makeLedgers]) {
          assert.deepStrictEqual(yield* service.readModel("l", { finishTime: beforeOpening }), Ledger.initial)
          assert.deepStrictEqual((yield* service.readModel("l", { finishTime: atSecondDraw })).drawn, 0)
          assert.deepStrictEqual(yield* service.readModel("l", { finishTime: between }), {
            status: "open",
            limit: 10,
            drawn: 2,
            draws: 1
          })
          // Looking back doesn't change what the service knows about the present.
          assert.deepStrictEqual(yield* service.readModel("l"), now)
        }
      }).pipe(Effect.provide(storage)))

    it.effect(
      verifies("CMD-012", "reads an event of an older schema version in the current one"),
      () =>
        Effect.gen(function*() {
          const store = yield* EventStore.EventStore
          const old = Aggregate.historical(Reading, "Measured", 1, { value: 5 })
          yield* store.append({ aggregateName: Reading.name, aggregateId: "r", expectedVersion: 0, events: [old] })
          const before = yield* readAll()
          const readings = yield* AggregateService.make(Reading)

          yield* readings.commands.measure("r", { value: 2, unit: "g" })

          assert.deepStrictEqual(yield* readings.readModel("r"), ["5 mg", "2 g"])
          const history = yield* readings.history("r", { payload: true })
          assert.deepStrictEqual(history.map((entry) => entry.payload), [
            ReadingEvent.cases.Measured.make({ value: 5, unit: "mg" }),
            ReadingEvent.cases.Measured.make({ value: 2, unit: "g" })
          ])
          const stored = yield* readAll()
          assert.deepStrictEqual(stored.slice(0, 1), before)
          assert.deepStrictEqual(stored.map((event) => [event.schemaVersion, event.payload]), [
            [1, { _tag: "Measured", value: 5 }],
            [2, { _tag: "Measured", value: 2, unit: "g" }]
          ])
        }).pipe(Effect.provide(storage))
    )

    const SnapshotStorage = Layer.merge(storage, KeyValueStore.layerMemory)

    it.effect(verifies("SNAP-001", "restores from a snapshot the state a replay gives"), () =>
      Effect.gen(function*() {
        // Closing the first service waits for the snapshots it is still saving.
        yield* Effect.scoped(Effect.gen(function*() {
          const { ledgers } = yield* withSnapshots
          yield* ledgers.commands.open("l", { limit: 100 })
          for (const index of range(22)) yield* ledgers.commands.draw("l", { amount: 1 + (index % 3), by: "nurse" })
        }))
        const replayed = yield* (yield* makeLedgers).readModel("l")

        const { ledgers, reads } = yield* withSnapshots
        assert.deepStrictEqual(yield* ledgers.readModel("l"), replayed)
        // The snapshot was used: the stream was read from where it ends, not from its start.
        assert.deepStrictEqual(reads, [20])

        yield* ledgers.commands.draw("l", { amount: 4, by: "nurse" })
        assert.deepStrictEqual(yield* ledgers.readModel("l"), yield* (yield* makeLedgers).readModel("l"))
        assert.strictEqual((yield* ledgers.readModel("l")).drawn, replayed.drawn + 4)
      }).pipe(Effect.provide(SnapshotStorage)))

    it.effect(
      verifies("SNAP-002", "rebuilds from the log when a snapshot can't be read"),
      () =>
        Effect.gen(function*() {
          const snapshots = yield* KeyValueStore.KeyValueStore
          yield* Effect.scoped(Effect.gen(function*() {
            const { ledgers } = yield* withSnapshots
            yield* ledgers.commands.open("l", { limit: 100 })
            for (const _ of range(9)) yield* ledgers.commands.draw("l", { amount: 2, by: "nurse" })
          }))
          const saved = yield* snapshots.get(snapshotKey("l"))
          assert.isDefined(saved)
          const expected = { status: "open", limit: 100, drawn: 18, draws: 9 }

          for (
            const damaged of [
              "",
              "not JSON",
              saved!.slice(0, saved!.length / 2),
              JSON.stringify({ version: 10, position: 10 }),
              JSON.stringify({
                version: 10,
                position: 10,
                state: { status: "open", limit: "100", drawn: 0, draws: 0 }
              }),
              JSON.stringify({ version: "10", position: 10, state: expected })
            ]
          ) {
            yield* snapshots.set(snapshotKey("l"), damaged)
            const { ledgers, reads } = yield* withSnapshots
            assert.deepStrictEqual(yield* ledgers.readModel("l"), expected)
            assert.deepStrictEqual(reads, [0])
          }
        }).pipe(Effect.provide(SnapshotStorage), quiet)
    )
  })
}
