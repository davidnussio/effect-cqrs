---
"effect-cqrs": minor
---

Record metrics in `AggregateService` through Effect's `Metric` module, so remembered states and snapshots can be watched in production.

- Each series carries an `aggregate` attribute, and `AggregateService.MetricNames` lists them: where each load found its state (`memory`, `snapshot` or `replay`), how many events it read on top, how many states are in memory and how many were dropped, snapshots saved and failed, lost races, and how long commands take by `command` and `outcome`.
- Export them with `OtlpMetrics` or `PrometheusMetrics` from `effect/observability`, or read them with `Metric.snapshot`. On the in-memory store, with no I/O to hide it, they cost about 5% of command throughput.
- The bank demo keeps four accounts in memory, saves a snapshot every five events and shows the metrics in a new tab.
