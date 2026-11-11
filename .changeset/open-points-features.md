---
"effect-cqrs": minor
---

Rotate the master key, manage saga triggers, and use events defined as classes.

- **Master key rotation.** `PersonalData.rewrap()` wraps every subject key that an earlier master key wrapped with the current one, a page at a time, so the earlier key can be dropped from `previous`. Keys don't change, so no event is rewritten. `layerMasterKeyConfig` reads earlier keys from `PERSONAL_DATA_MASTER_KEY_PREVIOUS`, as `id:base64` pairs separated by commas. `Keys` has a new `rewrap`, and `PersonalData.Backend` the optional `wrappedByOthers` and `replaceWrapped` it needs. A previous master key under the id of the current one now fails, rather than wrapping new keys with the old one.
- **Saga triggers.** `Saga.layer` provides `Saga.Sagas`, which has the `status`, `parked` and `retryParked` of `ViewModel.ViewModels` for the triggers. A trigger with `onFailure: ViewModel.park(...)` parked events that nothing could retry.
- **Events as classes.** A `Schema.Union` of `Schema.TaggedClass` events works throughout: personal fields in them are encrypted, `EventVersions` upcasts into the class, and `Outbox` and `EventCatalog` list them, where they skipped them before.
- **Personal fields that would be stored in plaintext.** A field transformed after `PersonalData.field`, as in `field(Schema.String, ...).pipe(Schema.decodeTo(...))`, kept its mark where it wasn't found and was written unencrypted. Building the aggregate now fails, and so does a field whose `subject` names a sibling that isn't always a string.
- **Commands.** `If-Match: *` asks `CommandApi` for no version in particular rather than failing with `400`. A typed command or `CommandGateway.send` with an `expectedVersion` that isn't a non-negative integer, or an empty `commandId`, fails with `InvalidCommand` instead of dying. `PersonalData.forget` tries a failing store a few more times, and says to call it again when it still fails.
