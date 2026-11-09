---
"effect-cqrs": minor
---

Record metadata with commands: a JSON object for what the domain doesn't decide on, such as the software version, the user who sent a command or the address it came from.

- **Recorded on every event.** `StoredEvent`, `EventMeta` and `HistoryEntry` have a new `metadata` field, `{}` when a command carried none, so projections, view models and `history` can read it.
- **Set from the context.** `Metadata.layer({ appVersion })` provides it to the services built on top, which remember it, and `Metadata.withMetadata` adds to it where a command runs. A `dispatch` envelope can carry `metadata` too, which wins key by key.
- **Not inherited by reactions.** Commands a view model or saga runs record their own metadata, not the one of the event they react to: the `causationId` leads back to it.

Existing SQLite and PostgreSQL tables get a `metadata` column on startup, filled with `{}` for older events. `StoredEvent` and `EventMeta` have a new field, so code that builds them by hand needs updating.
