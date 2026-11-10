# Performance and stress report

Measured on 31 October 2026 on one laptop. "Before" is commit `b46c324`, and "after"
is the same code with the seven fixes below and a Postgres backend. The raw numbers are
in [results/](results/) and [results/baseline/](results/baseline/), and
[Reproduce](#reproduce) has the commands.

## Summary

**Nothing was lost, before or after the fixes, on SQLite or on Postgres.** Ten stress
scenarios broke no invariant. They included two runs of a million events, 24 `SIGKILL`s
of writer processes running three at a time on the same streams, 20 view model
restarts, injected handler failures and up to 128 writers on one stream. Every
acknowledged event was stored exactly once, positions and versions had no gaps, and no
killed command left part of its events behind.

**The first run found seven problems, and all seven are fixed.** On a SQLite file
unless noted:

| | Before | After |
| --- | ---: | ---: |
| Command on a stream of 1,000 events | 3.09 ms | 0.13 ms |
| Command on a stream of 100,000 events | 1,153 ms | 0.14 ms |
| First load of a stream of 100,000 events | 1,153 ms | 284 ms |
| 128 writers on one stream: commands that gave up | 92% | 0% |
| 128 writers on one stream: commands per second | 42 | 7,406 |
| View model behind during a million-event ingest | 723,180 events | 0 at every sample |
| Ingest and project a million events | 45.4 s | 35.0 s |
| 8 transient handler failures over 50,000 events | 25.9 s | 1.1 s |
| Commands per second with 50 view models | 270 | 1,920 |
| Memory store, command with 1,000,000 events stored | 7.65 ms | 0.02 ms |
| Memory store, one command of 1,000,000 events | `RangeError` | stored in 1.2 s |

**The fixes have a small price.**

| | Before | After | Change |
| --- | ---: | ---: | ---: |
| Append, 1 event per transaction | 13,535 events/s | 13,567 events/s | 0% |
| Append, 100 events per transaction | 130,667 events/s | 117,367 events/s | -10% |
| Command on a new stream | 10,391/s | 8,858/s | -15% |
| Ingest and project on very short streams | 25.8 s | 32.5 s | +26% |
| Database for a million events | 196 MB | 196 MB | 0% |

The final run was about 10% slower on paths the fixes didn't touch, such as reading the
whole log, so only the last two changes clearly come from the code. The ingest got
slower where nothing needed fixing: with one command per stream there is no replay to
save, and the view model now does its work during the run rather than after it.

Two rules are new for applications. A handler must not run a command on its own stream,
because it would wait for itself, and handlers and projections must not mutate the
state, which is now kept between commands. A crash can also repeat up to two batches
of events in a view model instead of one, which at-least-once handlers already allow.

**Postgres works, at about a tenth of the commit rate of a SQLite file.** A command
takes about 1 ms, and the store tops out near 1,100 commits per second, which batching
works around. See [Postgres](#postgres), which also covers what running on it found.

[Remaining limits](#remaining-limits) lists what the fixes don't solve.

## Setup

| | |
| --- | --- |
| Machine | Apple M4 Pro, 12 cores, 24 GB, internal SSD |
| System | macOS, Darwin 27.2.0, in an ordinary desktop session |
| Runtime | Node 26.13.0, Effect 4.0.6, SQLite 3.54.1 |
| SQLite settings | WAL, `synchronous=FULL`, `fullfsync` off, one connection |
| Postgres | 18.6 in Docker (OrbStack) on the same laptop, default settings, 10 connections |

Four backends were measured: `memory`, `sqlite-memory` (`:memory:`), `sqlite-file`
(a temporary file on the SSD) and `postgres`, with `synchronous_commit` and `fsync` on
and a store of its own for each run.

The workload is a small `Account` aggregate in [common.ts](common.ts): payloads
under 100 bytes and a one-line projection. An aggregate with larger events or a
richer schema will spend longer decoding.

Throughput figures are the median of 5 rounds on a fresh store, after one warm-up
round that doesn't count. Rounds agreed within about 10% on SQLite and less well on
Postgres, and the ranges are in the raw data. Latency figures come from a single pass,
after warming up on other streams.

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
| 2 | 3% | 268 | 0 | 7,396 | 0 | 667 | 251 |
| 8 | 26% | 104 | 0 | 6,647 | 0 | 134 | 1,349 |
| 32 | 78% | 54 | 0 | 7,682 | 0 | 43 | 3,547 |
| 128 | 92% | 42 | 0 | 7,406 | 0 | 27 | 4,385 |

### 2. Long streams were slow, and worse than linearly on SQLite

There were two causes, and there are two fixes.

- Every command replayed its whole stream. The service now remembers the state of the
  10,000 most recently used aggregates (`makeStateCache`, set with `stateCacheSize`), so
  a command only reads events it hasn't seen. Those can come from another process,
  because a state plus what came after it is always the current state.
- Each page of a stream read sorted the stream again, because no index gives a
  stream's events by position. Reads of one stream now go by `version`, which the
  `UNIQUE` constraint already indexes (`afterVersion` in the event filter).

p50 on a file. A cold load is what the first use after a restart costs.

| Events in the stream | Before: command | After: cold load | After: command |
| ---: | ---: | ---: | ---: |
| 1 | 0.11 ms | 0.02 ms | 0.10 ms |
| 10 | 0.13 ms | 0.05 ms | 0.10 ms |
| 100 | 0.38 ms | 0.30 ms | 0.11 ms |
| 1,000 | 3.09 ms | 2.81 ms | 0.13 ms |
| 10,000 | 36.71 ms | 30.94 ms | 0.16 ms |
| 100,000 | 1,153 ms | 284 ms | 0.14 ms |

A first version added an index on `(aggregate_name, aggregate_id, position)` instead.
Reading by version needs no index of its own, which gave back its 18% of disk space.

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

| View models | Before | After | Postgres |
| ---: | ---: | ---: | ---: |
| 1 | 5,638 | 6,726 | 687 |
| 10 | 1,302 | 4,797 | 621 |
| 50 | 270 | 1,920 | 245 |

Commands per second on a file, and on Postgres. The cost per view model per event on a
file fell from about 72 µs to about 7.6 µs. Sharing the read accounted for most of the
gain at 50 view models: coalescing checkpoints alone reached about 640 commands/s in a
quick run.

### 6. The memory store scanned the whole log on every read

It now keeps each stream's events apart and finds a position by binary search (`candidates`
and `firstAfter` in [EventStore.ts](../src/EventStore.ts)).

| Events stored | Before | After |
| ---: | ---: | ---: |
| 1,000 | 34,189/s | 56,869/s |
| 10,000 | 8,153/s | 61,653/s |
| 100,000 | 1,218/s | 26,211/s |
| 1,000,000 | 133/s | 29,078/s |

### 7. The memory store died on a command of 200,000 events

`events.push(...stored)` passed every event as an argument and overflowed the stack.
It is now a loop, and one command of 1,000,000 events takes 1.2 s.

## Postgres

`PostgresEventStore` runs the same store tests as the other backends, plus its own in
`test/PostgresEventStore.test.ts`, and the same benchmark and stress scenarios.

A sequence can't number events: transactions commit in a different order from the one
they drew their numbers in, so a view model that has read up to 6 would never come back
for a 5 that commits later. Positions come from a counter row instead, which an append
holds until it commits. That keeps them in commit order with no gaps, and it is also
why writes are serial.

- **Commits top out near 1,100 per second.** Turning off `synchronous_commit` raised
  that to 1,882, so waiting for the disk is about 40% of a commit. The rest is the 5 round
  trips of an append, each to a server on the same machine, and a real network will
  add to them. Workers don't help, because each commit holds the counter.
- **Batching is the way around it.** 100 events per transaction reached 31,423 events/s,
  and a command of 10 events sustained about 9,500 events/s.
- **Bulk inserts are good.** One command of 1,000,000 events took 6.4 s, faster than
  the 8.0 s of in-memory SQLite, because Postgres gets them in multi-row statements.
- **Length of stream doesn't matter.** The benchmark showed 3 to 4 ms on streams of 10,000
  events and more, but a controlled run alternating a stream of 10 events and one of
  100,000 gave 1.1-1.3 ms and 1.0-1.5 ms. So that was something else happening at that
  point of the run, most likely the work Postgres does after a bulk load.

Running on it found three things, all handled or described here.

- **`LIMIT` could send the planner through the whole log.** Looking for new events in a
  long stream, it walked the primary key through every later event, removing 204,000 rows
  and taking 11 ms. Reading by version, the fix in [finding 2](#2-long-streams-were-slow-and-worse-than-linearly-on-sqlite),
  uses the stream's index whatever the statistics say.
- **Tables are slow until they have statistics.** Autovacuum takes up to a minute to
  analyze a new table, and until then each page for a view model sorted every matching
  row: 19.5 ms against 0.06 ms after `ANALYZE`. That is why a catch-up on a table just
  filled ran at 22,079 events/s against 83,789 after, and why ingest started at 5,153
  events/s. It passes by itself. After a bulk load, run `ANALYZE` to skip the wait.
- **This client binds at most 32,767 parameters to a statement**, not the 65,535 of the
  protocol. A test of a large batch caught the first version of the adapter assuming
  otherwise.

## Performance

Measured again on 10 November 2026, after group commit, cross-process notifications and
the other changes since. The laptop ran about 20% slower that day than on 31 October:
the commit before those changes, measured beside the new code, gave the same numbers
within noise, so compare rows within these tables rather than with the summary above.

### Writes

Events per second through `store.append`, and commands per second through the full
path: validate, load, decide, encode, append.

| | memory | sqlite-memory | sqlite-file | postgres |
| --- | ---: | ---: | ---: | ---: |
| Append, 1 event per transaction | 266,505 | 34,342 | 13,461 | 1,650 |
| Append, 100 events per transaction | 1,110,570 | 358,366 | 252,768 | 71,316 |
| Command on a new stream | 46,347 | 16,917 | 9,396 | 1,097 |

Batching matters more than anything else on disk: 100 events per transaction is almost
19 times faster than one on a file and 43 times on Postgres, because the commit is what
costs. Appends now check the version and insert in one statement, which is why the
batched rows doubled.

### Store size

p50 of a command on a new stream, with unrelated events already stored.

| Events stored | memory | sqlite-memory | sqlite-file | postgres |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 0.02 ms | 0.06 ms | 0.12 ms | 0.75 ms |
| 10,000 | 0.02 ms | 0.06 ms | 0.12 ms | 0.67 ms |
| 100,000 | 0.02 ms | 0.06 ms | 0.11 ms | 0.77 ms |
| 1,000,000 | 0.02 ms | 0.06 ms | 0.11 ms | 0.76 ms |

### Reads and view models

| Events per second | memory | sqlite-memory | sqlite-file | postgres |
| --- | ---: | ---: | ---: | ---: |
| Read the whole log, 1,000,000 events | 113,342,279 | 364,785 | 353,050 | 428,931 |
| View model catching up, 204,000 events | 479,333 | 183,602 | 185,079 | 22,595 |
| Replay, 204,000 events | 482,035 | 186,520 | 185,843 | 160,712 |

The memory read does no decoding, so it says little about the framework. The Postgres
catch-up ran on a table just filled, and the replay after `ANALYZE`. A view model that
wants 2% of the events skipped the rest through the type index, finishing a 204,000-event
log in about 20 ms on a file and 27 ms on Postgres.

| Read your own write, command + `caughtUp` | p50 | p99 | per second |
| --- | ---: | ---: | ---: |
| memory | 0.04 ms | 0.07 ms | 21,068 |
| sqlite-memory | 0.11 ms | 0.21 ms | 8,591 |
| sqlite-file | 0.17 ms | 0.83 ms | 5,191 |
| postgres | 0.84 ms | 1.21 ms | 1,153 |

### Concurrency

Commands per second on separate streams, by number of concurrent workers:

| Workers | memory | sqlite-memory | sqlite-file | postgres |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 50,046 | 16,396 | 9,319 | 1,307 |
| 16 | 54,954 | 16,054 | 14,592 | 4,108 |
| 256 | 54,834 | 16,659 | 15,895 | 16,958 |

On disk, concurrency now pays: appends that queue behind one being written go together
into the next transaction, so a SQLite file nearly doubles and Postgres grows thirteen
times with 256 workers. With 16 workers on Postgres the result varies from run to run,
from about 4,000 to 8,000, depending on how many appends happen to queue. In memory
there is no commit to share, and everything still runs on one thread.

## Stress

All 10 scenarios held their invariants on both databases.

| Scenario | Load | SQLite | Postgres |
| --- | --- | --- | --- |
| Ingest | 1,000,000 events, 10,000 streams, 32 workers, view model following | 35 s, view model level with the head | 118 s, peak lag 3,830 events |
| Ingest, short streams | The same over 100,000 streams | 33 s, view model level with the head | 128 s, peak lag 620 events |
| Burst | 200,000 events with nothing yielding | Peak lag 10 events | Peak lag 100 events |
| Contention | 2,000 commands on one stream, 2 to 128 workers, 1 or 2 services | Nobody gave up | Nobody gave up with one service, 4 of 2,000 with two at 128 workers |
| Flaky view model | 8 injected failures over 50,000 events | No gaps, 1.1 s | No gaps, 1.9 s |
| Restart | 20 stops mid-batch over 100,000 events | No gaps, 2,509 events repeated | No gaps, 2,110 events repeated |
| Crash | 24 `SIGKILL`s, 3 writer processes at a time on the same streams | All 13,593 acknowledged commands survived, none torn | All 3,745 survived, none torn |
| Slow consumer | 1 ms per event against a sequential writer | Peak lag 19,057 events, drained in 22 s | Writer slower than the handler, peak lag 2 |
| Subscribers | 20 subscribers, 20,000 events | All in order, 41,455 deliveries per second | All in order, 10,157 per second |
| Huge command | One command of up to 1,000,000 events | Memory store 1.2 s, in-memory SQLite 8.0 s | 6.4 s |

"Repeated" means a handler saw an event twice, which at-least-once delivery allows.
The count stayed within one batch per failure or restart. The flaky scenario repeated
1,992 events on both. The Postgres lag came from the table being new, as above, and
went to 0 once autovacuum had analyzed it.

### A million events

Throughput and memory per 100,000 events, on 10,000 streams. Heap is measured after a
forced collection, on the SQLite run.

| Events | Before: events/s | Before: lag | SQLite: events/s | SQLite: lag | SQLite: heap | Postgres: events/s | Postgres: lag |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 37,823 | 72,680 | 31,032 | 0 | 41 MB | 5,153 | 100 |
| 200,000 | 31,998 | 144,680 | 27,763 | 0 | 36 MB | 8,465 | 100 |
| 300,000 | 27,890 | 217,180 | 28,223 | 0 | 36 MB | 8,053 | 530 |
| 400,000 | 28,986 | 289,180 | 29,104 | 0 | 36 MB | 8,823 | 1,420 |
| 500,000 | 26,239 | 361,680 | 29,305 | 0 | 36 MB | 9,458 | 0 |
| 600,000 | 23,768 | 433,680 | 28,567 | 0 | 36 MB | 9,705 | 0 |
| 700,000 | 19,920 | 506,180 | 27,169 | 0 | 36 MB | 9,905 | 0 |
| 800,000 | 19,326 | 578,680 | 28,274 | 0 | 36 MB | 8,560 | 0 |
| 900,000 | 18,513 | 650,680 | 28,306 | 0 | 36 MB | 10,078 | 0 |
| 1,000,000 | 18,024 | 723,180 | 28,197 | 0 | 36 MB | 9,208 | 0 |

Throughput used to halve as streams grew. It now stays flat, with the view model's work
included in it. The heap stays flat too, on Postgres as well (36-38 MB), so there is no
sign of a leak over this distance. The short-stream run rose from 40 to 50 MB, which fits
its 100,000 streams filling the state cache and the view model's own maps.

A million events take 196 MB on SQLite and 312 MB on Postgres, indexes included, and
replay in 4.6 s and 12.4 s.

## Remaining limits

- **Restarts cost a replay without snapshots.** The state cache is only in memory, so
  the first use of a stream after a restart replays it, at about 3 to 4 µs per event,
  unless snapshots are on.
- **Rival processes on one stream are slow.** With a rival on every command, conflicts
  hit one command in three and throughput falls to about 250 per second, or 116 on
  Postgres, because each conflict backs off for at least 8 ms. Few commands gave up in
  these runs, but 5 retries can still run out.
- **Notifications across processes need Postgres or polling.** Postgres stores notify
  every process on each append. Processes sharing a SQLite file only see each other's
  events with `pollInterval`.
- **One commit at a time per Postgres store.** Appends wait for the counter, which group
  commit offsets by writing the appends that queue up together: about 1,300 commits per
  second for a single writer here, and 17,000 commands per second with 256 workers.
- **The stream lock isn't first come, first served.** A fiber that arrives just as the
  lock is released can take it ahead of those waiting, which `Semaphore` allows. Under
  a synchronous burst of 128 workers p50 was 0.1 ms and p99 was 144 ms.
- **Slow handlers fall behind without slowing writers.** There is no backpressure: a
  handler taking 1 ms per event left the view model 19,057 events behind before it
  drained. Memory stays low, but `caughtUp` waits.
- **One thread.** In memory, throughput doesn't grow with concurrency, and synchronous
  SQLite calls block the event loop while they run.

## Not covered

- **Power loss.** `SIGKILL` doesn't test it, and with `fullfsync` off macOS doesn't
  flush the drive's own cache on commit. Commit rates on a file are therefore
  optimistic, and hardware with a real flush will be slower.
- **Other machines.** Only this laptop was measured, and only Node 26 although the
  package allows Node 24.
- **Postgres across a network, or tuned.** It ran on the same machine with default
  settings and 128 MB of `shared_buffers`, in a Linux VM, so a real server differs both ways.
- **Sustained contention between real processes.** The crash scenario writes from real
  processes, but the rivals in the contention scenario are two services in one process.
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

For Postgres, start the database of `docker-compose.yml` and say where it is. With
`POSTGRES_URL` set, `pnpm test` covers Postgres too.

```bash
docker compose up -d --wait
```

```bash
export POSTGRES_URL=postgres://postgres:postgres@localhost:5432/cqrs
```

```bash
pnpm test
```

```bash
BENCH_BACKENDS=postgres pnpm bench
```

```bash
STRESS_DATABASE=postgres pnpm stress
```

These take about 8 minutes each.
