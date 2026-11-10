---
"effect-cqrs": patch
---

Document that an append to the PostgreSQL store inside a transaction of your own holds the counter row until that transaction ends, so every other writer waits for it.
