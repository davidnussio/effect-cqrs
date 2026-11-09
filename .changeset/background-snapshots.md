---
"effect-cqrs": patch
---

Save snapshots in the background and remember the state a command leaves.

- **Snapshots no longer hold up commands.** `AggregateService.make` with `snapshots` saves them in a fiber of its own and needs a `Scope`, which `Layer.effect` provides. Closing the scope waits for the saves still running. A save that fails is retried after `every` more events instead of on every command.
- **The next command reads nothing it already knows.** After a write the service folds the events it just stored into the state it remembers, decoding them as a replay would, so the next command on that aggregate only reads what others wrote since.
