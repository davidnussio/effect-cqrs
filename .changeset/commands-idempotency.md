---
"effect-cqrs": minor
---

Commands can be retried safely. A typed command takes an optional third argument, `{ commandId, expectedVersion }`, and the `dispatch` envelope accepts both fields too.

With a `commandId`, a command that comes again on the same aggregate returns its first result, read back from the events it wrote, instead of running again. The id is recorded in the events' metadata under `cqrs.commandId`, and each aggregate remembers its last 100 ids, set with the new `commandIdsPerStream` option. Replays rebuild them and snapshots keep them. Commands that write no events aren't remembered.

With an `expectedVersion`, a command fails with the new `WrongExpectedVersion` error, carrying the expected and the actual version, when the aggregate has moved on since the caller read it. It isn't retried. `CommandFailure` now includes it.

The new `Ids` module holds `Ids.IdGenerator`, the reference that makes new correlation ids, so tests can make them deterministic with `Ids.sequential()`.
