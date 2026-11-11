# The workshop on several replicas

The workshop server, scaled out behind Traefik, with every replica on one
PostgreSQL. It answers two questions: can `effect-cqrs` run in several pods at
once, and what does an application on it have to do about it.

```
            http://localhost:8080
                     │
                 ┌───▼────┐   API paths, in turn   ┌────────────┐
   phones ──────►│Traefik ├───────────────────────►│ workshop 1 │──┐
   big screen    │        ├───────────────────────►│ workshop 2 │──┤  appends, reads,
                 │        ├───────────────────────►│ workshop 3 │──┤  LISTEN/NOTIFY,
                 │        │   everything else      └────────────┘  │  advisory locks,
                 │        ├──────────► ui (nginx)                  │  shared state
                 └────────┘                                ┌───────▼──────┐
                                                           │  PostgreSQL  │
                                                           └──────────────┘
```

## Running it

From the root of the repository:

```bash
docker compose -f examples/workshop/cluster/compose.yml up -d --build --wait
```

The UI is on <http://localhost:8080>, the API reference on
<http://localhost:8080/docs> and Traefik's dashboard on <http://localhost:8081>. The
presenter joins with the code `prova`. Every value in `compose.yml` is a development
one: set `AUTH_SECRET`, `ADMIN_CODE` and `PERSONAL_DATA_MASTER_KEY` in the
environment, or in a `.env` file next to it, before anyone else joins.

Each call may reach another replica, and `/health` says which one answered:

```bash
for i in 1 2 3 4 5 6; do curl -s localhost:8080/health | jq -r .replica; done
```

Scale up or down while the room is using it. A new replica follows the log from the
start, as on any restart, and joins the rotation once `/health` answers:

```bash
docker compose -f examples/workshop/cluster/compose.yml up -d --scale workshop=5
```

Stop the replica that runs the sagas and the bot, and watch another one take them
over within two seconds. The logs say which one holds them, since only that one
logs the bot's restocks:

```bash
docker compose -f examples/workshop/cluster/compose.yml logs -f workshop
docker kill effect-cqrs-cluster-workshop-1
```

Rehearse a room against the whole cluster:

```bash
LOAD_URL=http://localhost:8080 ADMIN_CODE=prova LOAD_PARTICIPANTS=50 LOAD_SECONDS=60 pnpm workshop:load
```

Phones on the same network reach it at `http://<this machine's address>:8080`: the UI
calls the API on whatever origin served it. To start again from an empty log, drop
the database with the stack:

```bash
docker compose -f examples/workshop/cluster/compose.yml down -v
```

## What already works across pods

The framework was built with replicas on one database in mind, and most of it
needs nothing:

| Piece | Across replicas |
| --- | --- |
| Appends | Every replica appends to the same log. Positions come from one counter row, so they follow commit order with no gaps, and a write that loses a race gets `ConcurrencyConflict` and is retried. |
| The event bus | Each append sends `NOTIFY` with the new head. Every replica listens, so `store.subscribe`, view models and the live streams wake up on appends made anywhere. It needs a session, not PgBouncer's transaction mode; behind one, turn `listen` off and set `pollInterval`. |
| `commandId` | Kept in the events of the stream, so a retry that lands on another replica returns the first result. |
| View models in memory | Every replica follows the log and builds its own copy. A read that passes `position` waits until the replica it reaches has seen that position, so a client reads its own writes from any replica. |
| View models in a database, outboxes | `Leases.layerPostgres()` runs each on one replica, as an advisory lock; a standby takes over from the shared checkpoint when the holder stops. |
| Keys of personal data | In a SQL table. A replica keeps a key in memory for a minute, so after `forget` the others can read the subject for that long. |

So, to the question: the event bus works in pods, as long as the store is
PostgreSQL. The memory and SQLite stores are for one process. It is a log that
replicas follow, not a broker: it delivers each event to every replica, in order and
at least once, and it doesn't share work out. Each view model runs on one replica,
or on all of them, never split across them. Other systems get the events through an
`Outbox`.

## What the workshop had to change

The application kept state in memory that replicas must share, and ran things that
must run once:

| Gap | What went wrong with three replicas | What it does now |
| --- | --- | --- |
| Sagas | Every replica ran every saga, with its own in-memory workflow engine, so each step ran three times | The saga triggers take their leases from the `Leases` in context, `Leases.layerPostgres`, and run on one replica. The read models get leases that are always held, since every replica needs its own copy. See `domain/workshop.ts`. |
| Restock bot, simulated shoppers | Each replica restocked on its own schedule | Each runs where its lease is held, `workshop/bot` and `workshop/simulator` |
| What the presenter sets | Settings, the chaos counter and the bot's counters lived in the replica that got the request, while the bot and the sagas ran elsewhere | In a `workshop_state` table, one JSON value per key, changed in a transaction that locks the row. See `domain/shared.ts`. |
| Who is in the room | Each replica counted the streams it held, so the control room saw a third of the room | Each replica writes who it holds to the shared state when someone comes or goes and every 3 seconds, and reads the others' every second. A replica not heard from for 10 seconds is gone. |
| Rate limits | Each replica counted on its own, so the limits were three times looser | Counted in the shared state |
| Creating tables | Replicas that start together raced to create the keys table, and one could fail | The framework now takes an advisory lock first, as the event store already did |
| Which replica answered | Nothing said | `/health` reports `replica` |

Still per replica, on purpose: the spans and command timings the control room shows
are those of the replica its stream reached.

The saga engine stays in memory. When the replica that runs the sagas stops, the next
one starts every saga again from the beginning of the log, which joins nothing in
its new engine, and so runs each step again. Every step is idempotent, so that is
safe, and it is what a restart did already; but it costs more as the log grows, and
for the length of a lease's handover, up to two seconds, two replicas can run the
same step.

## Plan for the framework

What the workshop does by hand above points at what the framework could do itself.
In order:

1. **Lock every migration.** Done: `PersonalData.layerSql` and
   `ParkedEvents.layerSql` take an advisory lock named after the table on
   PostgreSQL. `test/Migrations.test.ts` starts eight at once on one table, which
   failed every time before.

2. **Durable sagas across replicas.** A saga is safe on one replica at a time, but
   its in-memory engine forgets running workflows, so its trigger can't keep a durable
   checkpoint and a takeover replays the whole log. `ClusterWorkflowEngine` from
   `effect/cluster` keeps workflows in SQL, through `SqlMessageStorage`, on the
   database the events are in. Each replica becomes a runner, `NodeClusterSocket.layer`
   with `SqlRunnerStorage`, and the triggers keep their checkpoints with
   `PostgresEventStore.layerCheckpoints()`. To do: a test with two runners, one
   stopped in the middle of a workflow, then a `Saga.layerCluster` helper and a
   section in the sagas guide. The workshop then drops the replay.

3. **Read your writes on a standby.** `caughtUp` doesn't wait for a view model that
   another replica runs. For one kept in a database, a standby could poll the
   shared checkpoint instead, so a read on any replica sees its own writes.

4. **Share work out.** One replica runs each view model. A view model too slow for
   one could be split by stream, a lease and a checkpoint for each of N partitions, or
   run as cluster entities sharded by aggregate id. Running commands for an aggregate
   on the runner that owns it would also keep its state warm and end the races that
   `ConcurrencyConflict` retries.

5. **One view of telemetry.** Export spans and metrics with OTLP, from
   `effect/observability`, with the replica as an attribute, and add a collector and
   a viewer to this compose file, so the control room's trace covers every replica.

6. **Kubernetes.** The same as this compose file: a Deployment with readiness on
   `/health`, a `preStop` pause so a replica leaves the Service before it gives its
   leases up, the session pooler, and the runner health of `effect/cluster` from the
   Kubernetes API once step 2 is in.

Until step 2, `Singleton` from `effect/cluster` is the other way to run view models
and sagas once in a cluster, see the view models guide.
