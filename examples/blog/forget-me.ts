/**
 * The post "Forget me, from the backups too": a member asks to be forgotten, and last
 * night's backup of the events still holds her name. With crypto-shredding it holds it
 * encrypted with a key that no longer exists, so restoring the backup doesn't bring her
 * back. `forget-me.test.ts` runs it.
 */
import { Effect, Layer, Stream } from "effect"
import { AggregateService, EventStore, PersonalData } from "effect-cqrs"
import { Member } from "./library.ts"

/** Copies events into an empty store, in order: what restoring a backup does. */
const restore = (backup: ReadonlyArray<EventStore.StoredEvent>) =>
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    for (const event of backup) {
      const { aggregateId, aggregateName, payload, schemaVersion, type, version } = event
      yield* store.append({
        aggregateName,
        aggregateId,
        expectedVersion: version - 1,
        events: [{ type, payload, schemaVersion }]
      })
    }
  })

// #region keys
// The keys live apart from the events: another database, other permissions, other backups.
export const Keys = PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))
// #endregion

export const forgetting = Effect.gen(function*() {
  const members = yield* AggregateService.make(Member)
  const store = yield* EventStore.EventStore

  // #region joined
  yield* members.commands.join("alice", { name: "Alice Rossi" })
  yield* members.commands.join("bob", { name: "Bob Bianchi" })

  const [stored] = yield* store.read({ aggregateIds: ["alice"] }).pipe(Stream.runCollect)
  // stored.payload: { _tag: "MemberJoined", name: { $personal: { subject: "alice", data: "X4DnRIZB…" } } }
  // #endregion

  // #region backup
  // Last night's backup: every event, as stored.
  const backup = yield* store.read().pipe(Stream.runCollect)
  // #endregion

  // #region forget
  yield* PersonalData.forget("alice")

  const alice = yield* members.state("alice")
  // { joined: true, name: "[forgotten]", loans: {} }
  // #endregion

  // #region restore
  // Something goes wrong, and the events are restored from last night's backup.
  const restored = yield* Effect.gen(function*() {
    yield* restore(backup)
    const members = yield* AggregateService.make(Member)
    return { alice: yield* members.state("alice"), bob: yield* members.state("bob") }
  }).pipe(Effect.provide(Layer.fresh(EventStore.layerMemory)))
  // restored.alice.name: "[forgotten]", restored.bob.name: "Bob Bianchi"
  // #endregion

  return { stored, alice, restored }
})
