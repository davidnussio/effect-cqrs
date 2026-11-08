---
"effect-cqrs": minor
---

Evolve event schemas, snapshot long aggregates, and trace why events happened.

- **Event upcasting.** `EventVersions.from(V1).next(V2, upcast)` declares the older shapes of an event in the aggregate's new `versions` option. Old events are upcast on read through a decoder composed once with `Schema.decodeTo`, so projections and view models only see the current shape. The store records each event's `schemaVersion`.
- **Snapshots.** `AggregateService.make(aggregate, { snapshots: { every, schema, version } })` saves state through a `KeyValueStore` from `effect/persistence`, so a cold start replays only the events after the last snapshot.
- **Causation.** Every event records a `correlationId` and a `causationId`. View model handlers make the event they handle the cause of the commands they run, `Causation.causedBy` does the same for other subscribers, and `Causation.withCausation` and the `dispatch` envelope set them from outside.
- **Fixture.** `givenEvents` accepts events in older schema versions, made with `Aggregate.historical`, and `checkInvariants` runs random command sequences with Effect's `Arbitrary` and reports the shortest one that breaks a predicate.

Existing SQLite and PostgreSQL tables get the new `schema_version`, `correlation_id` and `causation_id` columns on startup. `StoredEvent` has these three new fields, and `EventMeta` has the two causation ones, so code that builds them by hand needs updating.
