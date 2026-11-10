import { assert, describe, it } from "@effect/vitest"
import { DateTime, Effect } from "effect"
import { AggregateService, Causation, EventStore, Metadata } from "effect-cqrs"
import { TestClock } from "effect/testing"
import { configurations, Ledger, LedgerEvent, ledgerStream, on, storageOf, verifies } from "./support.ts"

const makeLedgers = AggregateService.make(Ledger)

for (const configuration of configurations) {
  const storage = storageOf(configuration)

  describe.skipIf(!configuration.available)(`audit trail ${on(configuration)}`, () => {
    it.effect(
      verifies("AUD-001", "records when, in which correlation and why, and lists it in order"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers
          yield* TestClock.adjust("1 hour")
          const opened = yield* ledgers.commands.open("l", { limit: 10 })
          yield* ledgers.commands.open("other", { limit: 10 })
          yield* TestClock.adjust("1 minute")
          const drawn = yield* ledgers.commands.draw("l", { amount: 2, by: "nurse" })
          yield* TestClock.adjust("1 minute")
          const several = yield* ledgers.commands.drawEach("l", { amounts: [1, 1], by: "nurse" })

          const history = yield* ledgers.history("l")

          assert.deepStrictEqual(history.map((entry) => [entry.version, entry.type]), [
            [1, "Opened"],
            [2, "Drawn"],
            [3, "Drawn"],
            [4, "Drawn"]
          ])
          assert.deepStrictEqual(history.map((entry) => DateTime.toEpochMillis(entry.timestamp)), [
            3_600_000,
            3_660_000,
            3_720_000,
            3_720_000
          ])
          assert.deepStrictEqual(history.map((entry) => entry.correlationId), [
            opened.correlationId,
            drawn.correlationId,
            several.correlationId,
            several.correlationId
          ])
          assert.strictEqual(new Set(history.map((entry) => entry.correlationId)).size, 3)
          for (const entry of history) {
            assert.isString(entry.causationId)
            assert.isAbove(entry.causationId!.length, 0)
            assert.isFalse("payload" in entry)
          }

          // In pages, and with the payloads, it is the same trail.
          const pages = [
            ...(yield* ledgers.history("l", { limit: 3 })),
            ...(yield* ledgers.history("l", { afterVersion: 3, limit: 3 }))
          ]
          assert.deepStrictEqual(pages, history)
          const withPayloads = yield* ledgers.history("l", { payload: true })
          assert.deepStrictEqual(withPayloads.map(({ payload: _, ...entry }) => entry), [...history])
          assert.deepStrictEqual(withPayloads.map((entry) => entry.payload), [
            LedgerEvent.cases.Opened.make({ limit: 10 }),
            LedgerEvent.cases.Drawn.make({ amount: 2, by: "nurse" }),
            LedgerEvent.cases.Drawn.make({ amount: 1, by: "nurse" }),
            LedgerEvent.cases.Drawn.make({ amount: 1, by: "nurse" })
          ])
          // And it is what the store holds.
          const stored = yield* ledgerStream("l")
          assert.deepStrictEqual(
            stored.map((event) => ({
              version: event.version,
              timestamp: event.timestamp,
              type: event.type,
              correlationId: event.correlationId,
              causationId: event.causationId,
              metadata: event.metadata
            })),
            [...history]
          )
          // A window of time selects the events recorded in it.
          const window = yield* ledgers.history("l", {
            startTime: DateTime.makeUnsafe(3_660_000),
            finishTime: DateTime.makeUnsafe(3_720_000)
          })
          assert.deepStrictEqual(window.map((entry) => entry.version), [2])
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("AUD-002", "records who and which software on every event, and keeps its own keys"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers.pipe(
            Effect.provide(Metadata.layer({ software: "device 2.1.0", site: "lab" }))
          )
          const metadata = Effect.map(ledgerStream("l"), (events) => events.map((event) => event.metadata))

          yield* ledgers.commands.open("l", { limit: 10 }).pipe(
            Metadata.withMetadata({ user: "dr.rossi", site: "ward-3" })
          )
          yield* ledgers.dispatch({
            aggregateId: "l",
            type: "drawEach",
            payload: { amounts: [1, 1], by: "nurse" },
            metadata: {
              user: "nurse.bianchi",
              [AggregateService.commandIdKey]: "forged",
              [AggregateService.commandTypeKey]: "forged"
            }
          }).pipe(Metadata.withMetadata({ user: "gateway", station: 4 }))
          yield* ledgers.commands.draw("l", { amount: 1, by: "nurse" })

          assert.deepStrictEqual(yield* metadata, [
            { software: "device 2.1.0", site: "ward-3", user: "dr.rossi" },
            { software: "device 2.1.0", site: "lab", user: "nurse.bianchi", station: 4 },
            { software: "device 2.1.0", site: "lab", user: "nurse.bianchi", station: 4 },
            { software: "device 2.1.0", site: "lab" }
          ])

          // With a commandId the reserved keys say which command it was, whatever came in.
          yield* ledgers.dispatch({
            aggregateId: "l",
            type: "draw",
            payload: { amount: 1, by: "nurse" },
            commandId: "request-9",
            metadata: { [AggregateService.commandIdKey]: "forged", [AggregateService.commandTypeKey]: "forged" }
          })
          assert.deepStrictEqual((yield* metadata)[4], {
            software: "device 2.1.0",
            site: "lab",
            [AggregateService.commandIdKey]: "request-9",
            [AggregateService.commandTypeKey]: "draw"
          })
          // History shows the same.
          assert.deepStrictEqual((yield* ledgers.history("l")).map((entry) => entry.metadata), yield* metadata)
        }).pipe(Effect.provide(storage))
    )

    it.effect(
      verifies("AUD-003", "leads from a reaction back to the request that started it"),
      () =>
        Effect.gen(function*() {
          const ledgers = yield* makeLedgers
          const causes = (id: string) =>
            Effect.map(ledgerStream(id), (events) => events.map((event) => [event.correlationId, event.causationId]))

          // A request names itself.
          yield* ledgers.commands.open("first", { limit: 1 }).pipe(
            Causation.withCausation({ correlationId: "request-7", causationId: "request-7" })
          )
          const [first] = yield* ledgerStream("first")
          // What reacts to the event records it as the cause, and so on down the chain.
          yield* ledgers.commands.open("second", { limit: 1 }).pipe(Causation.causedBy(first!))
          const [second] = yield* ledgerStream("second")
          yield* ledgers.commands.open("third", { limit: 1 }).pipe(Causation.causedBy(second!))

          assert.deepStrictEqual(yield* causes("first"), [["request-7", "request-7"]])
          assert.deepStrictEqual(yield* causes("second"), [["request-7", "ledger/first@1"]])
          assert.deepStrictEqual(yield* causes("third"), [["request-7", "ledger/second@1"]])
          assert.strictEqual(EventStore.eventId(first!), "ledger/first@1")

          // An envelope continues a correlation started elsewhere.
          const dispatched = yield* ledgers.dispatch({
            aggregateId: "fourth",
            type: "open",
            payload: { limit: 1 },
            correlationId: "request-8",
            causationId: "message-3"
          })
          assert.strictEqual(dispatched.correlationId, "request-8")
          assert.deepStrictEqual(yield* causes("fourth"), [["request-8", "message-3"]])

          // Without any, each command starts a correlation of its own.
          yield* ledgers.commands.open("fifth", { limit: 1 })
          yield* ledgers.commands.open("sixth", { limit: 1 })
          const [[fifth], [sixth]] = [yield* causes("fifth"), yield* causes("sixth")]
          assert.strictEqual(fifth![0], fifth![1])
          assert.isString(fifth![0])
          assert.notStrictEqual(fifth![0], sixth![0])
        }).pipe(Effect.provide(storage))
    )
  })
}
