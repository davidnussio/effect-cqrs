import { NodeFileSystem } from "@effect/platform-node"
import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Schema } from "effect"
import { Aggregate, AggregateService, EventCatalog, EventStore, EventVersions, PersonalData } from "effect-cqrs"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const Currency = Schema.Literals(["CHF", "EUR"])

const owner = PersonalData.field(Schema.NonEmptyString, { redacted: "[forgotten]" })

const OpenedV1 = Schema.Struct({ owner })

const AccountEvent = Schema.TaggedUnion({
  AccountOpened: { owner, currency: Currency },
  MoneyDeposited: { amount: Schema.Int }
})

interface State {
  readonly owner: string
  readonly balance: number
}

const define = (events: typeof AccountEvent) =>
  Aggregate.make<State>()({
    name: "account",
    events,
    initial: { owner: "", balance: 0 },
    projection: {
      AccountOpened: (state, { owner }) => ({ ...state, owner }),
      MoneyDeposited: (state, { amount }) => ({ ...state, balance: state.balance + amount })
    },
    versions: {
      AccountOpened: EventVersions.from(OpenedV1).next(
        AccountEvent.cases.AccountOpened,
        ({ owner }) => ({ owner, currency: "CHF" as const })
      )
    }
  })

const Account = define(AccountEvent)
  .command("open", {
    payload: Schema.Struct({ owner: Schema.NonEmptyString, currency: Currency }),
    handler: (_, payload) => Effect.succeed(AccountEvent.cases.AccountOpened.make(payload))
  })
  .command("deposit", {
    payload: Schema.Struct({ amount: Schema.Int }),
    handler: (_, { amount }) => Effect.succeed(AccountEvent.cases.MoneyDeposited.make({ amount }))
  })

const Keys = PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))

const Storage = Layer.merge(EventStore.layerMemory, Keys)

// Stores an AccountOpened of version 1, as code from before the currency would have.
const seedVersion1 = EventStore.EventStore.use((store) =>
  store.append({
    aggregateName: "account",
    aggregateId: "old",
    expectedVersion: 0,
    events: [Aggregate.historical(Account, "AccountOpened", 1, { owner: "Bob" })]
  })
)

const capture = Effect.gen(function*() {
  const accounts = yield* AggregateService.make(Account)
  yield* accounts.commands.open("alice", { owner: "Alice", currency: "EUR" })
  yield* accounts.commands.deposit("alice", { amount: 10 })
  yield* seedVersion1
  return yield* EventCatalog.captureSamples([Account])
})

describe("EventCatalog", () => {
  it("describes every version of every event", () => {
    const entries = EventCatalog.events(Account)
    const opened = entries.find(({ type }) => type === "AccountOpened")!
    assert.strictEqual(opened.schemaVersion, 2)
    assert.deepStrictEqual(opened.versions.map(({ schemaVersion }) => schemaVersion), [1, 2])
    assert.deepStrictEqual(Object.keys(opened.versions[1]!.schema.properties ?? {}).sort(), ["_tag", "currency", "owner"])
    assert.deepStrictEqual(opened.personalFields, ["owner"])
    const deposited = entries.find(({ type }) => type === "MoneyDeposited")!
    assert.strictEqual(deposited.versions.length, 1)
    assert.deepStrictEqual(deposited.personalFields, [])
  })

  it("describes the payload of every command", () => {
    const [open] = EventCatalog.commands(Account)
    assert.strictEqual(open?.name, "open")
    assert.deepStrictEqual(open?.payload.required, ["owner", "currency"])
  })

  it("lists events and commands in an AsyncAPI document", () => {
    const document = EventCatalog.document([Account], { title: "Bank", version: "1.0.0" })
    assert.strictEqual(document.asyncapi, "3.0.0")
    assert.deepStrictEqual(Object.keys(document.channels).sort(), ["account.commands", "account.events"])
    const opened = document.components.messages["account.AccountOpened"] as Record<string, unknown>
    assert.strictEqual(opened["x-schemaVersion"], 2)
    assert.deepStrictEqual(Object.keys(opened["x-versions"] as object), ["1"])
    assert.deepStrictEqual(opened["x-personal-fields"], ["owner"])
    assert.isDefined(document.components.messages["account.commands.deposit"])
  })

  it.effect("captures samples without personal data, and they decode", () =>
    Effect.gen(function*() {
      const samples = yield* capture
      assert.deepStrictEqual(
        samples.map(({ schemaVersion, type }) => `${type}@${schemaVersion}`).sort(),
        ["AccountOpened@1", "AccountOpened@2", "MoneyDeposited@1"]
      )
      const text = JSON.stringify(samples)
      assert.notInclude(text, "Alice")
      assert.notInclude(text, "Bob")
      assert.notInclude(text, "$personal")
      assert.strictEqual(yield* EventCatalog.checkCompatibility(Account, samples), 3)
    }).pipe(Effect.provide(Storage)))

  it.effect("fails when a stored shape no longer decodes", () =>
    Effect.gen(function*() {
      const samples = yield* capture
      // MoneyDeposited gained a required field without a version that fills it in.
      const Changed = Schema.TaggedUnion({
        AccountOpened: { owner, currency: Currency },
        MoneyDeposited: { amount: Schema.Int, reference: Schema.String }
      })
      const error = yield* Effect.flip(EventCatalog.checkCompatibility(define(Changed as unknown as typeof AccountEvent), samples))
      assert.strictEqual(error._tag, "IncompatibleSamples")
      assert.deepStrictEqual(error.failures.map(({ type }) => type), ["MoneyDeposited"])
    }).pipe(Effect.provide(Storage)))

  it.effect("fails when an event type is gone", () =>
    Effect.gen(function*() {
      const error = yield* Effect.flip(EventCatalog.checkCompatibility(Account, [
        { aggregateName: "account", type: "AccountClosed", schemaVersion: 1, payload: { _tag: "AccountClosed" } }
      ]))
      assert.deepStrictEqual(error.failures.map(({ type }) => type), ["AccountClosed"])
    }))

  it.effect("keeps samples already in the file", () =>
    Effect.gen(function*() {
      const path = join(mkdtempSync(join(tmpdir(), "catalog-")), "samples.json")
      const first = yield* EventCatalog.saveSamples(path, [
        { aggregateName: "account", type: "MoneyDeposited", schemaVersion: 1, payload: { _tag: "MoneyDeposited", amount: 1 } }
      ])
      assert.strictEqual(first.length, 1)
      yield* EventCatalog.saveSamples(path, yield* capture)
      const saved = yield* EventCatalog.readSamples(path)
      assert.strictEqual(saved.length, 3)
      assert.deepStrictEqual(saved.find(({ type }) => type === "MoneyDeposited")?.payload, {
        _tag: "MoneyDeposited",
        amount: 1
      })
      assert.strictEqual(yield* EventCatalog.checkCompatibility(Account, saved), 3)
    }).pipe(Effect.provide(Layer.merge(Storage, NodeFileSystem.layer))))
})
