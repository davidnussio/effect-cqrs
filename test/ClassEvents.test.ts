import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Schema, Stream } from "effect"
import { Aggregate, AggregateService, EventCatalog, EventStore, EventVersions, Outbox, PersonalData } from "effect-cqrs"

// Events defined as classes rather than as a tagged union of structs.
class MemberJoined extends Schema.TaggedClass<MemberJoined>()("MemberJoined", {
  name: PersonalData.field(Schema.String, { redacted: "[forgotten]" }),
  plan: Schema.String
}) {}
class MemberLeft extends Schema.TaggedClass<MemberLeft>()("MemberLeft", { reason: Schema.String }) {}

// Before plans existed.
const MemberJoinedV1 = Schema.Struct({ name: PersonalData.field(Schema.String, { redacted: "[forgotten]" }) })

interface State {
  readonly name: string
  readonly plan: string
  readonly active: boolean
}

const Member = Aggregate.make<State>()({
  name: "member",
  events: Schema.Union([MemberJoined, MemberLeft]),
  initial: { name: "", plan: "", active: false },
  projection: {
    MemberJoined: (_, { name, plan }) => ({ name, plan, active: true }),
    MemberLeft: (state) => ({ ...state, active: false })
  },
  versions: {
    MemberJoined: EventVersions.from(MemberJoinedV1).next(MemberJoined, ({ name }) => ({ name, plan: "basic" }))
  }
}).command("join", {
  payload: Schema.Struct({ name: Schema.String, plan: Schema.String }),
  handler: (_, payload) => Effect.succeed(new MemberJoined(payload))
}).command("leave", {
  payload: Schema.Struct({ reason: Schema.String }),
  handler: (_, { reason }) => Effect.succeed(new MemberLeft({ reason }))
})

const Storage = Layer.merge(
  EventStore.layerMemory,
  PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
)

describe("Events defined as classes", () => {
  it.effect("encrypt their personal fields, and read as the replacement once forgotten", () =>
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      const members = yield* AggregateService.make(Member)
      yield* members.commands.join("m1", { name: "Alice Rossi", plan: "pro" })

      const [stored] = yield* Stream.runCollect(store.read())
      assert.notInclude(JSON.stringify(stored?.payload), "Alice Rossi")
      assert.deepStrictEqual(yield* members.state("m1"), { name: "Alice Rossi", plan: "pro", active: true })

      yield* PersonalData.forget("m1")
      const fresh = yield* AggregateService.make(Member)
      assert.strictEqual((yield* fresh.state("m1")).name, "[forgotten]")
    }).pipe(Effect.provide(Storage)))

  it.effect("upcast an older version into the class", () =>
    Effect.gen(function*() {
      const store = yield* EventStore.EventStore
      yield* store.append({
        aggregateName: "member",
        aggregateId: "m2",
        expectedVersion: 0,
        events: [{ type: "MemberJoined", payload: { _tag: "MemberJoined", name: "Bob" }, schemaVersion: 1 }]
      })
      const members = yield* AggregateService.make(Member, { stateCacheSize: 0 })
      assert.deepStrictEqual(yield* members.state("m2"), { name: "Bob", plan: "basic", active: true })
    }).pipe(Effect.provide(Storage)))

  it("are listed by the outbox and the catalog", () => {
    const outbox = Outbox.make({ name: "all", aggregates: [Member], publish: () => Effect.void })
    assert.deepStrictEqual(Object.keys(outbox.mappings).sort(), ["MemberJoined", "MemberLeft"])

    const entries = EventCatalog.events(Member)
    assert.deepStrictEqual(entries.map(({ type }) => type), ["MemberJoined", "MemberLeft"])
    assert.deepStrictEqual(entries[0]?.personalFields, ["name"])
    assert.strictEqual(entries[0]?.schemaVersion, 2)
  })
})
