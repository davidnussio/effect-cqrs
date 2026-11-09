---
"effect-cqrs": patch
---

Learn the framework from a tutorial and two demos that show its traces.

- **Tutorial.** A new page builds a small shop step by step: events, aggregates and commands, a view model, a saga, metadata, event versioning and metrics. Its code runs as `pnpm tutorial` and is tested.
- **Shop demo.** The tutorial's shop runs in the browser with devtools for every Effect span, each saga's steps and execution, the stored rows and the metrics.
- **Traces in the bank demo.** Its new Trace tab shows each command on the bus, the aggregate's spans and the transfer saga's workflow and activities.
