# Performance and stress report

Measured on 31 October 2026 on one laptop. "Before" is commit `b46c324`, and "after"
is the same code with the seven fixes below. The raw numbers are in
[results/](results/) and [results/baseline/](results/baseline/), and
[Reproduce](#reproduce) has the commands.

## Summary

**Nothing was lost, before or after the fixes.** Ten stress scenarios broke no
invariant. They included two runs of a million events, 8 `SIGKILL`s of a writer
process, 20 view model restarts, injected handler failures and up to 128 writers on
one stream. Every acknowledged event was stored exactly once, positions and versions
had no gaps, and no killed command left part of its events behind.

**The first run found seven problems, and all seven are fixed.** On a SQLite file
unless noted:

| | Before | After |
| --- | ---: | ---: |
| Command on a stream of 1,000 events | 3.09 ms | 0.11 ms |
| Command on a stream of 100,000 events | 1,153 ms | 0.15 ms |
| First load of a stream of 100,000 events | 1,153 ms | 276 ms |
| 128 writers on one stream: commands that gave up | 92% | 0% |
| 128 writers on one stream: commands per second | 42 | 7,401 |
| View model behind during a million-event ingest | 723,180 events | 0 at every sample |
| Ingest and project a million events | 45.4 s | 33.4 s |
| 8 transient handler failures over 50,000 events | 25.9 s | 1.1 s |
| Commands per second with 50 view models | 270 | 2,179 |
| Memory store, command with 1,000,000 events stored | 7.65 ms | 0.02 ms |
| Memory store, one command of 1,000,000 events | `RangeError` | stored in 1.2 s |

**The fixes have a price**, mostly on writes, because of one more index.

| | Before | After | Change |
| --- | ---: | ---: | ---: |
| Append, 100 events per transaction | 130,667 events/s | 111,816 events/s | -14% |
| Append, 1 event per transaction | 13,535 events/s | 12,972 events/s | -4% |
| Command on a new stream | 10,391/s | 9,770/s | -6% |
| Ingest and project on very short streams | 25.8 s | 32.5 s | +26% |
| Database for a million events | 196 MB | 231 MB | +18% |

The ingest got slower where nothing needed fixing: with one command per stream there
is no replay to save. The view model now also does its work during the run rather
than after it, and each of those commands pays for the extra index.

Two rules are new for applications. A handler must not run a command on its own stream,
because it would wait for itself, and handlers and projections must not mutate the
state, which is now kept between commands. A crash can also repeat up to two batches
of events in a view model instead of one, which at-least-once handlers already allow.

[Remaining limits](#remaining-limits) lists what the fixes don't solve.

## Setup

| | |
| --- | --- |
| Machine | Apple M4 Pro, 12 cores, 24 GB, internal SSD |
| System | macOS, Darwin 27.2.0, in an ordinary desktop session |
| Runtime | Node 26.13.0, Effect 4.0.6, SQLite 3.54.1 |
| SQLite settings | WAL, `synchronous=FULL`, `fullfsync` off, one connection |

Three backends were measured: `memory`, `sqlite-memory` (`:memory:`) and `sqlite-file`
(a temporary file on the SSD).

The workload is a small `Account` aggregate in [common.ts](common.ts): payloads
under 100 bytes and a one-line projection. An aggregate with larger events or a
richer schema will spend longer decoding.

Throughput figures are the median of 5 rounds on a fresh store, after one warm-up
round that doesn't count. Rounds agreed within about 10%, and the ranges are in the raw
data. Latency figures come from a single pass, after warming up on other streams.

## The fixes

### 1. Commands on a hot stream mostly failed

Commands on one stream raced and retried at once, so only one could win each round.
Now they take turns inside a service, through a lock per stream (`makeKeyedLock` in
[AggregateService.ts](../src/AggregateService.ts)). Conflicts can only come from another
process, and those retries now back off with jitter (`conflictBackoff`).

Each case runs 2,000 commands on one stream, with a yield between loading and
appending, the gap any store with a real round trip has. Two services on one store
stand in for two processes. On a file:

| Workers | Before: gave up | Before: per second | One process: gave up | One process: per second | Two processes: gave up | Two processes: conflicts | Two processes: per second |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 3% | 268 | 0 | 7,246 | 0 | 667 | 239 |
| 8 | 26% | 104 | 0 | 7,183 | 0 | 134 | 1,351 |
| 32 | 78% | 54 | 0 | 7,413 | 0 | 40 | 4,209 |
| 128 | 92% | 42 | 0 | 7,401 | 0 | 29 | 4,395 |

### 2. Long streams were slow, and worse than linearly on SQLite

There were two causes, and there are two fixes.

- Every command replayed its whole stream. The service now remembers the state of the
  10,000 most recently used aggregates (`makeStateCache`, set with `stateCacheSize`), so
  a command only reads events it hasn't seen. Those can come from another process,
  because a state plus what came after it is always the current state.
- Each page of a stream read sorted the stream again. A new index on
  `(aggregate_name, aggregate_id, position)`, created on startup, removes the sort.

p50 on a file. A cold load is what the first use after a restart costs.

| Events in the stream | Before: command | After: cold load | After: command |
| ---: | ---: | ---: | ---: |
| 1 | 0.11 ms | 0.02 ms | 0.10 ms |
| 10 | 0.13 ms | 0.05 ms | 0.10 ms |
| 100 | 0.38 ms | 0.30 ms | 0.12 ms |
| 1,000 | 3.09 ms | 2.77 ms | 0.11 ms |
| 10,000 | 36.71 ms | 27.90 ms | 0.14 ms |
| 100,000 | 1,153 ms | 276 ms | 0.15 ms |

### 3. View models lagged at full load and stopped during synchronous bursts

`Effect.forEach` with `concurrency` starts each item in a fresh fiber and carries on as
long as items finish without suspending. `node:sqlite` is synchronous, so no other
fiber got a turn until the loop ended.

The service now yields before a command once a millisecond has passed (`makeTimeSlice`).
A first attempt yielded after every command. That halved the memory store's throughput
and still let `concurrency: "unbounded"` run to the end, because each item ran before
its yield.

In the 200,000-event burst the view model went from processing nothing until the end
to a peak lag of 10 events. In both million-event runs it was level with the head at
every sample, taken every 200 commands, against 723,180 events behind before.

### 4. A view model's retry backoff never reset

The schedule wrapped the whole catch-up, so batches that succeeded between failures
didn't reset it. Now the delay only doubles when a view model fails again in the same
place (`backOff` in [ViewModel.ts](../src/ViewModel.ts)). 8 failures over 50,000 events
went from 25.9 s to 1.1 s. An event that keeps failing still backs off up to 30 seconds.

### 5. Each view model added a fixed cost per event

Each one read for itself and committed a checkpoint on every wake. Now view models that
are within 500 events of the head share one read and its decoded events (`makeTail`).
Checkpoints are saved every 500 events and when the view model stops
(`checkpointInterval`), and a wake no longer queries the head.

| View models | Before | After |
| ---: | ---: | ---: |
| 1 | 5,638 | 7,120 |
| 10 | 1,302 | 4,854 |
| 50 | 270 | 2,179 |

Commands per second on a file. The cost per view model per event fell from about 72 µs
to about 6.5 µs. Sharing the read accounted for most of the gain at 50 view models:
coalescing checkpoints alone reached about 640 commands/s in a quick run.

### 6. The memory store scanned the whole log on every read

It now keeps each stream's events apart and finds a position by binary search (`candidates`
and `firstAfter` in [EventStore.ts](../src/EventStore.ts)).

| Events stored | Before | After |
| ---: | ---: | ---: |
| 1,000 | 34,189/s | 63,588/s |
| 10,000 | 8,153/s | 54,494/s |
| 100,000 | 1,218/s | 31,900/s |
| 1,000,000 | 133/s | 30,485/s |

### 7. The memory store died on a command of 200,000 events

`events.push(...stored)` passed every event as an argument and overflowed the stack.
It is now a loop, and one command of 1,000,000 events takes 1.2 s.

## Performance

Current numbers for what the fixes above don't already show.

### Writes

Events per second through `store.append`, and commands per second through the full
path: validate, load, decide, encode, append.

| | memory | sqlite-memory | sqlite-file |
| --- | ---: | ---: | ---: |
| Append, 1 event per transaction | 265,292 | 32,214 | 12,972 |
| Append, 100 events per transaction | 1,057,616 | 123,604 | 111,816 |
| Command on a new stream | 62,940 | 17,257 | 9,770 |

Batching matters more than anything else on a file: 100 events per transaction is
almost 9 times faster than one, because the commit is what costs.

### Store size

p50 of a command on a new stream, with unrelated events already stored.

| Events stored | memory | sqlite-memory | sqlite-file |
| ---: | ---: | ---: | ---: |
| 1,000 | 0.01 ms | 0.06 ms | 0.10 ms |
| 10,000 | 0.01 ms | 0.06 ms | 0.11 ms |
| 100,000 | 0.02 ms | 0.06 ms | 0.10 ms |
| 1,000,000 | 0.02 ms | 0.06 ms | 0.12 ms |

### Reads and view models

| Events per second | memory | sqlite-memory | sqlite-file |
| --- | ---: | ---: | ---: |
| Read the whole log, 1,000,000 events | 36,727,573 | 525,082 | 514,771 |
| View model catching up, 204,000 events | 632,506 | 258,514 | 254,430 |
| Replay, 204,000 events | 658,218 | 260,149 | 255,594 |

The memory read does no decoding, so it says little about the framework. It also fell
from 101 million events/s, which has no practical effect at that speed. A view model
that wants 2% of the events skipped the rest through the type index, finishing a
204,000-event log in 15 ms on a file.

| Read your own write, command + `caughtUp` | p50 | p99 | per second |
| --- | ---: | ---: | ---: |
| memory | 0.04 ms | 0.07 ms | 23,056 |
| sqlite-memory | 0.10 ms | 0.19 ms | 9,131 |
| sqlite-file | 0.15 ms | 0.34 ms | 6,389 |

### Concurrency

Commands per second on separate streams, by number of concurrent workers:

| Workers | memory | sqlite-memory | sqlite-file |
| ---: | ---: | ---: | ---: |
| 1 | 67,374 | 17,080 | 9,539 |
| 16 | 74,265 | 18,045 | 9,659 |
| 256 | 72,445 | 18,134 | 9,634 |

Concurrency adds almost no throughput. Everything runs on one thread and `node:sqlite` is
synchronous, so concurrent workers take turns. It adds no meaningful overhead either.

## Stress

All 10 scenarios held their invariants.

| Scenario | Load | Result |
| --- | --- | --- |
| Ingest | 1,000,000 events, 10,000 streams, 32 workers, view model following | 33 s, view model level with the head |
| Ingest, short streams | The same over 100,000 streams | 33 s, view model level with the head |
| Burst | 200,000 events with nothing yielding | Peak view model lag of 10 events |
| Contention | 2,000 commands on one stream, 2 to 128 workers, 1 or 2 processes | Nobody gave up, and conflicts only came from the other process |
| Flaky view model | 8 injected failures over 50,000 events | Recovered, no gaps, 1,992 events repeated, 1.1 s |
| Restart | 20 stops mid-batch over 100,000 events | No gaps, 1,962 events repeated |
| Crash | 8 `SIGKILL`s of a writer process | All 9,359 acknowledged commands survived, none torn |
| Slow consumer | 1 ms per event against a sequential writer | Peak lag 19,115 events, drained in 22 s, 3 MB of heap growth |
| Subscribers | 20 subscribers, 20,000 events | All in order, 41,622 deliveries per second |
| Huge command | One command of up to 1,000,000 events | Memory stored it in 1.2 s, in-memory SQLite in 8.7 s |

"Repeated" means a handler saw an event twice, which at-least-once delivery allows.
The count stayed within one batch per failure or restart.

### A million events

Throughput and memory per 100,000 events, on 10,000 streams. Heap is measured after a
forced collection.

| Events | Before: events/s | Before: lag | After: events/s | After: lag | After: heap | After: RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 37,823 | 72,680 | 33,111 | 0 | 40 MB | 298 MB |
| 200,000 | 31,998 | 144,680 | 30,300 | 0 | 36 MB | 298 MB |
| 300,000 | 27,890 | 217,180 | 29,919 | 0 | 36 MB | 298 MB |
| 400,000 | 28,986 | 289,180 | 29,804 | 0 | 36 MB | 215 MB |
| 500,000 | 26,239 | 361,680 | 29,725 | 0 | 36 MB | 215 MB |
| 600,000 | 23,768 | 433,680 | 29,650 | 0 | 36 MB | 220 MB |
| 700,000 | 19,920 | 506,180 | 29,535 | 0 | 36 MB | 220 MB |
| 800,000 | 19,326 | 578,680 | 29,268 | 0 | 36 MB | 212 MB |
| 900,000 | 18,513 | 650,680 | 29,080 | 0 | 36 MB | 212 MB |
| 1,000,000 | 18,024 | 723,180 | 28,966 | 0 | 36 MB | 216 MB |

Throughput used to halve as streams grew. It now falls by about 12%, and the view model's
work is included in it. The heap stays flat, so there is no sign of a leak over this
distance. The short-stream run rose from 38 to 48 MB, which fits its 100,000 streams
filling the state cache and the view model's own maps.

One window of the burst run dropped to 7,238 events/s. It didn't happen in 3 further
runs of that scenario, which stayed between 22,731 and 29,533, so it looks like noise
from the machine.

## Remaining limits

- **Restarts cost a replay.** The state cache is only in memory, so the first use of a
  stream after a restart replays it, at about 2.8 µs per event. Persistent snapshots
  would remove that, and would need a schema for the state.
- **Rival processes on one stream are slow.** With a rival on every command, conflicts
  hit one command in three and throughput falls to about 240 per second, because each
  conflict backs off for at least 8 ms. Nothing gave up in these runs, but 5 retries can
  still run out.
- **The stream lock isn't first come, first served.** A fiber that arrives just as the
  lock is released can take it ahead of those waiting, which `Semaphore` allows. Under
  a synchronous burst of 128 workers p50 was 0.1 ms and p99 was 142 ms.
- **Slow handlers fall behind without slowing writers.** There is no backpressure: a
  handler taking 1 ms per event left the view model 19,115 events behind before it
  drained. Memory stays low, but `caughtUp` waits.
- **One thread.** Throughput doesn't grow with concurrency, and synchronous SQLite calls
  block the event loop while they run.

## Not covered

- **Power loss.** `SIGKILL` doesn't test it, and with `fullfsync` off macOS doesn't
  flush the drive's own cache on commit. Commit rates on a file are therefore
  optimistic, and hardware with a real flush will be slower.
- **Other machines.** Only this laptop was measured, and only Node 26 although the
  package allows Node 24.
- **Real separate processes writing to one database.** The rivals in the contention
  scenario are two services in one process.
- **Other work on the event loop**, such as an HTTP server. The time slice yields to
  fibers, and whether that is enough for I/O wasn't measured.
- **Runs longer than a few minutes.** The flat heap covers a million events, not days.
- **Tracing.** Spans ran with no exporter attached.

## Reproduce

```bash
pnpm bench
pnpm stress
```

These take about 4 and 5 minutes here, and write to `bench/results/`.
`BENCH_SCALE=0.05 pnpm bench` gives a quick pass that writes nothing, and
`STRESS_ONLY=crash,burst pnpm stress` runs named scenarios. `pnpm test` runs every
stress scenario at a small size, so they keep working.
