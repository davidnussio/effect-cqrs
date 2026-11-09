---
"effect-cqrs": minor
---

Add `PersonalData`, crypto-shredding for personal data in an append-only log. `PersonalData.field` marks a field of an event as personal. `AggregateService` encrypts it with a key that belongs to its subject: the stream's aggregate id, or a sibling property the field names. `PersonalData.forget` destroys that key. The field then reads as the replacement it declares, and the event's other fields are left as they are. Forgetting is recorded in the log, refuses new personal data for the subject and destroys sealed snapshots that hold it.

Data keys are stored wrapped by a `MasterKey`, which can be backed by a KMS or by a key from configuration. They are kept in memory with `layerMemory`, or in a SQLite or PostgreSQL table with `layerSql`. View models erase what they stored about a forgotten subject in the new `onForget` handler. The bank demo encrypts owners' names and has a **forget** button on each account.

Aggregates with personal fields need `PersonalData.Keys`. `makeEventDecoder` and `makeEventEncoder` take the keys as an optional argument, and `ReadError` and `CommandFailure` now include `PersonalDataError` and `SubjectForgotten`.
