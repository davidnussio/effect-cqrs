---
"effect-cqrs": minor
---

Rename `readModel` to `state`. `todos.state(id)` folds one aggregate's events into its state, optionally as it was before a `finishTime`, as `readModel` did. The old name came from the `read-model` action of moleculer-cqrs and read as one of the read models that view models keep. It isn't one: it is the state of the write side, the one the next command is decided on, and it is never behind.

`readModel` stays on the service as a deprecated name for `state`. Three things change without an alias:

- The `GET /<aggregate>/:aggregateId` endpoint of `CommandApi.group` is named `state`, so a typed client calls `client.todo.state(...)` and the OpenAPI operation id is `todo.state`. The path and the response are the same.
- `CommandApi.group` refuses a command named `state`, and accepts one named `readModel`.
- The span of a read is named `<aggregate>.state`.
