---
"effect-cqrs": patch
---

Fence view model checkpoints. `Checkpoints` has an optional `advance(viewModel, from, to)`, which moves a checkpoint only if it is still at `from`, implemented by the memory, SQLite and PostgreSQL stores. A view model moves its checkpoint on from where it last saved it, so while a lease changes hands and two processes run it, a view model with a `transaction` rolls a batch back rather than committing it a second time, and neither process moves the checkpoint back. Custom stores without `advance` keep the previous behaviour.
