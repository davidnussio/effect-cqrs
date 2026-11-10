---
"effect-cqrs": minor
---

Read `history` in pages. It takes `afterVersion` and `limit`, and lists events without decoding them unless `payload` is set, so an event that no longer decodes doesn't fail a listing that doesn't show it. The HTTP endpoint of `CommandApi` takes the same two query parameters and answers with at most `historyLimit` events, 1,000 unless `CommandApi.layer` is given another.
