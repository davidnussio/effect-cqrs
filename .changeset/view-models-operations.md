---
"effect-cqrs": minor
---

View models can park events that keep failing, commit their checkpoints with their writes, and run in one process out of several.

`onFailure: ViewModel.park({ attempts })` gives up on an event after it failed that many times: the view model records it in the new `ParkedEvents` service and goes on with the next one. `viewModels.parked()` lists parked events with their error and attempts, `viewModels.retryParked({ viewModel })` handles them again, `status` counts them and the `cqrs_view_model_parked_events_total` metric counts every event parked. `ParkedEvents.layerSql()` keeps them in a SQLite or PostgreSQL table, created on startup unless `migrate` is `false`, with `ParkedEvents.migrations()` for its migrations. Without a policy a view model still stalls on a failing event, as before.

`transaction: ViewModel.sqlTransaction` runs each batch of handlers in one transaction with the checkpoint it reaches, which is then saved after every batch. A failed batch leaves neither its writes nor its checkpoint, and a crash repeats nothing.

`ViewModel.layer` takes a lease on each view model from the new `Leases` service before running it. Leases are always held by default. `Leases.layerPostgres()` holds them as PostgreSQL advisory locks, so replicas sharing a database run each view model once, and a standby takes over when the holder stops or dies. `status` shows `holdsLease`, and `caughtUp` doesn't wait for view models another process runs.

`ViewModels` has two new methods and `Status` two new fields, so code that implements `ViewModels` itself has to add them.
