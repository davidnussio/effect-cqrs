---
"effect-cqrs": minor
---

Run sagas as durable workflows from `effect/workflow`.

- `Saga.workflow` defines the process and `Saga.toLayer` registers its body. Commands the body runs record the event that started the saga as their cause.
- `Saga.command` runs a command as an `Activity`: its outcome is kept, so a replayed workflow skips it. Domain errors listed in `error` are kept too, lost races and store errors are retried, and anything else fails the workflow and runs its compensations.
- `Saga.make` is the trigger, a view model whose handlers call `Saga.start`, and `Saga.layer` runs triggers apart from the view models. The workflow's idempotency key turns a second start of the same saga into a join.
