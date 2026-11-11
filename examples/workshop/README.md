# Workshop demo

A bank and a multi-user shop on `effect-cqrs`, for a hands-on workshop: participants
use it from their phones while a big screen shows what happens inside. The plan, in
Italian, is in [PLAN.md](PLAN.md). So far it has the bank and the shop: participants
join with a nickname, add money, send it to each other, buy from a shared catalog
with real stock, wait in a queue for what is sold out, and close their account,
which forgets their nickname. A bot restocks the shop, simulated shoppers keep it
busy when the room is quiet, and a control room on the projector shows who is in,
every event as it is stored, the measures, the trace and personal data before and
after it is forgotten.

| Path | What it is |
| --- | --- |
| `api.ts` | The HTTP API, shared by the server and the UI's typed client |
| `domain/account.ts` | The account aggregate |
| `domain/bank.ts` | The account service, the transfer saga and the view models |
| `domain/shop.ts` | The product and order aggregates, the order sagas and the shop's view models |
| `domain/automation.ts` | The restock bot and the simulated shoppers |
| `domain/workshop.ts` | The bank and the shop together, with every view model in one layer |
| `server/auth.ts` | The tokens the server issues |
| `server/http.ts` | The handlers of the API |
| `server/live.ts` | The live streams: events, presence, metrics, spans, and the AsyncAPI document |
| `server/limits.ts` | The rate limits |
| `server/main.ts` | The server: configuration, storage, Scalar |
| `server/vite.config.ts` | Bundles the server into one file for the Docker image |
| `ui/` | The UI, a Vite and React app, and the control room at `#sala` |
| `load.ts` | The rehearsal: many virtual participants at once |
| `reset.ts` | Empties the database |
| `domain/shared.ts` | The state replicas share: settings, chaos, presence, rate limits |
| `Dockerfile` | The image Render runs, built from the root of the repository |
| `ui/Dockerfile` | The UI served by nginx, for the cluster |
| `cluster/` | Several replicas behind Traefik with Docker Compose, and what scaling out took |

`render.yaml` and `vercel.json` at the root of the repository deploy it, and
`test/Workshop.test.ts` tests it.

## Running it locally

```bash
pnpm workshop
```

```bash
pnpm ui:workshop
```

The server listens on port 3000 and stores events in `examples/workshop.sqlite`. To use the
Postgres of `docker-compose.yml` instead:

```bash
docker compose up -d --wait
docker compose exec postgres createdb -U postgres workshop
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/workshop pnpm workshop
```

The UI runs on <http://localhost:5190> and calls `http://localhost:3000`, or the
server named by `VITE_API_URL`.

| Route | What it does |
| --- | --- |
| `GET /health` | Says the server is up, and reads the position of the log from the store |
| `POST /auth/join` | Opens an account for a nickname and returns a token for it |
| `GET /me` | The caller's account |
| `POST /me/deposit` | Adds up to 100, 10 times a minute |
| `POST /me/transfers` | Sends money to another participant |
| `GET /me/ledger` | The caller's movements |
| `POST /me/close` | Pays out the balance, closes the account and forgets the nickname |
| `GET /participants` | Everyone who joined, with their balance |
| `GET /shop/products` | The catalog, with stock and the queue of waiting orders |
| `POST /shop/orders` | Places an order; a saga then pays for it and takes the goods |
| `GET /me/orders` | The caller's orders and where each one is |
| `POST /me/orders/:orderId/cancel` | Cancels an order waiting in the queue and gives the money back |
| `GET /shop/bot` | What the restock bot does and when it runs next |
| `POST /admin/products/:productId/restock` | Adds stock, which goes to the queue first. Admin only |
| `PATCH /admin/bot` | Pauses, resumes or tunes the bot. Admin only |
| `POST /admin/bot/run` | Makes the bot run now. Admin only |
| `GET /admin/simulator` | What the simulated shoppers do. Admin only |
| `PATCH /admin/simulator` | Starts, stops or tunes them. Admin only |
| `POST /admin/chaos/break-next-order` | Makes the next order fail after payment, so the saga refunds it. Admin only |
| `POST /admin/replay` | Rebuilds a view model from the log. Admin only |
| `GET /admin/metrics` | The control room's measures, once. Admin only |
| `GET /stream/events` | Server-Sent Events: events with personal fields hidden, and presence |
| `GET /stream/control` | Server-Sent Events: everything the control room shows. Admin only |
| `GET /asyncapi.json` | The AsyncAPI 3 document of the events and the two streams |
| `GET /docs` | The API reference in Scalar, where you can try the routes |
| `GET /openapi.json` | The OpenAPI document |

Every route under `/me`, `/participants`, `/shop` and `/admin` needs `Authorization: Bearer <token>`. In
Scalar, call `/auth/join`, copy the token and paste it in the bearer field of **Authentication**. The reads
take a `position` from an earlier answer and wait until the view models have seen it,
so a client reads its own writes.

## The shop

An order touches three aggregates, so a saga carries it: it takes the money from
the account, then the goods from the product. When there are none, an order placed
with `backorder: true` joins the product's queue; any other gets its money back and
is rejected. Each step does nothing the second time, so the saga can run again after
a restart.

The queue lives in the product aggregate. Restocking hands the goods to waiting
orders in the order they arrived, in the same command, so two orders never get the
same piece. While a queue exists, new orders join it instead of taking goods ahead
of it. A waiting order can be cancelled; one the goods already reached can't.

Closing an account cancels its waiting orders and refunds them before paying out the
balance.

## The bot and the simulated shoppers

Both run inside the server, through the same aggregate services as the API, and
sign the events they cause with `actor` in the metadata: `bot` or `simulator`, and
`presenter` for a restock by hand.

The bot wakes up every second and, when its interval has passed, restocks each
product at or below its threshold, and each product with orders waiting: it adds its
batch plus what the queue still lacks. It starts with the server, every 30 seconds
for products with 2 pieces or fewer, 5 at a time.

The simulated shoppers start switched off. Once on, they open accounts named with a
🤖, add money, buy, wait in queues, send money to anyone, people included, and now
and then cancel. Their accounts have fixed ids, so a restart finds the same ones.

Both are tuned from the **Regia** section, which only the presenter sees. Settings live
in the `workshop_state` table, beside the events, so every replica of the server sees
the same ones and a restart keeps them; `pnpm workshop:reset` brings back the defaults.

## Live streams and the control room

The server follows the log once and sends each event on two Server-Sent Events
streams. On `/stream/events` the personal fields read `[personal]`; on
`/stream/control`, for the presenter, they are decrypted, next to the payload as the
store keeps it, encrypted. Each event carries its `actor`: the participant's id for
what they did through the API, `bot`, `simulator` or `presenter`. Events without one
come from a saga, and share the `correlationId` of the request that started it.

Opening `/stream/events` counts as being in the room: presence goes up with the first
tab and down when the last one closes. The control stream also sends the metrics
every second and the spans as they end, leaving out SQL statements, CORS preflights
and the streams themselves.

`EventSource` can't send a token, so the UI opens the streams with Effect's HTTP
client and parses them with `Sse.decode`, reconnecting with a growing delay. The
participants' screens read their data again whenever the log moves.

The presenter opens the control room from the **Regia** section, or at `#sala`. It shows
who is in, the log as it grows, coloured by correlation, the measures, the shelves
and the queues, the trace, and personal data: the nickname as the log keeps it,
encrypted, and the moment its key is destroyed. It also checks that the money adds
up: the participants' balances, from one view model, against the books, another
one folded from the same events, and says when they can't be compared because one
is behind. Its buttons break the next order after payment, so the saga shows its
compensation, and rebuild a view model from the log.

`/asyncapi.json` describes the events of the three aggregates, from
`EventCatalog.document`, and the two streams. It opens in
[AsyncAPI Studio](https://studio.asyncapi.com/) by URL.

## Limits

Deposits are limited to 10 a minute per participant, and joins to 120 a minute per
address, which leaves room for a classroom behind one router. Both answer `429`. They
are counted in the shared state, so they hold however many replicas answer.

## Rehearsal and reset

`pnpm workshop:load` plays many participants at once through the typed client: each
joins, opens the live stream, reads the bank and the shop as the page does every two
seconds, and deposits, buys and sends money for a while. Each has connections of its
own, at most six as a browser keeps, and they start one after another over
`LOAD_RAMP_SECONDS`, 10 by default. It ends with how long each call took, what the
domain refused, and, given `ADMIN_CODE`, whether the books balance and the view models
caught up.

```bash
ADMIN_CODE=prova LOAD_PARTICIPANTS=50 LOAD_SECONDS=60 pnpm workshop:load
```

Started all in the same instant, 50 participants open some 350 connections at once.
macOS keeps at most 128 waiting for the server to accept them (`kern.ipc.somaxconn`)
and resets the others right after the handshake, so their first request fails with
`ECONNRESET` while the server logs nothing. A room never arrives that fast, which is
what the ramp is for. If the laptop that runs the workshop must take such a burst,
`sudo sysctl kern.ipc.somaxconn=1024` raises the limit until the next restart.

To rehearse against something as small as Render's free plan, run the image with
its limits:

```bash
docker run --rm --cpus 0.1 --memory 512m -p 3000:3000 -e ADMIN_CODE=prova \
  -e AUTH_SECRET=... -e PERSONAL_DATA_MASTER_KEY_ID=k -e PERSONAL_DATA_MASTER_KEY=... \
  -e POSTGRES_URL=... effect-cqrs-workshop
```

With 50 participants for 60 seconds, against the image with Postgres, measured when
the test read the bank's screen alone:

| CPU | Screen read p50 / p95 | Order p50 / p95 |
| --- | --- | --- |
| 1, a laptop | 6 / 39 ms | 7 / 18 ms |
| 0.5, Render Starter | 66 / 196 ms | 16 / 120 ms |
| 0.1, Render free | 6 s / 60 s | 6 s / 48 s |

None of them lost a call, and every time the books balanced and the view models
caught up: the tenth of a CPU is simply too little.

`pnpm workshop:reset` drops the framework's tables, events and keys alike, so the
next start is a clean one. Stop the server first; it asks for `RESET=yes`:

```bash
RESET=yes POSTGRES_URL=... pnpm workshop:reset
```

## Authentication

The server issues its own tokens: there is no identity provider. `/auth/join` makes
up an id, opens the account under that id and returns a JWT signed with HMAC SHA-256
through WebCrypto. The token names the account, the nickname and the role, and
expires after a day. Every command runs on the account in the token, never on one
named in the request, and a token whose account is closed is refused.

The token lives in the browser's storage only. Clearing it, or changing phone, loses
the account, which is why the UI has no "log out".

Joining with `adminCode` equal to `ADMIN_CODE` makes the session an admin one, for
the presenter's controls in the **Regia** section.

## Server configuration

| Variable | Default | |
| --- | --- | --- |
| `PORT` | `3000` | Render sets it |
| `POSTGRES_URL` | | Stores events in PostgreSQL. Without it they go to SQLite |
| `POSTGRES_CA_CERT` | | A PEM certificate to verify the database with |
| `SQLITE_FILE` | `workshop.sqlite` | |
| `CORS_ORIGIN` | any origin | The origins of the UI, separated by commas |
| `AUTH_SECRET` | development secret | Signs the tokens |
| `ADMIN_CODE` | | What the presenter types to join as admin. Without it nobody can |
| `PERSONAL_DATA_MASTER_KEY` | development key | 32 bytes in base64 that wrap the keys of personal data |
| `PERSONAL_DATA_MASTER_KEY_ID` | `development` | The id of that key |

With `NODE_ENV=production`, as in the Docker image, the server refuses to start
without `AUTH_SECRET` and the master key.

## Putting it online

Everything runs on free plans: Supabase for Postgres, Render for the server and
Vercel for the UI. Set them up in this order, since each needs the address of the
one before.

Render's free instance has a tenth of a CPU, which is enough for the demo online and
a few people at a time, but not for a room: with 50 phones reading their screens,
calls take seconds. For the workshop itself, run the server on a laptop behind a
Cloudflare tunnel, as described in "The laptop", with the same database.

### 1. Supabase

1. Create a project at <https://supabase.com/dashboard>, in a region close to the
   Render one, such as Frankfurt.
2. Open **Connect** and copy the **Session pooler** connection string. The direct
   connection is IPv6 only, and the transaction pooler can't hold the `LISTEN` the
   event store uses. Put the database password in it and add `?sslmode=require`.
3. Nothing else: the server creates its tables when it starts.

A free project pauses after a week without activity. Open the dashboard before the
workshop to wake it.

If the server fails with a certificate error, download the certificate from
**Database settings → SSL configuration** and put its contents in
`POSTGRES_CA_CERT`.

### 2. Render

1. At <https://dashboard.render.com>, choose **New → Blueprint** and connect this
   repository. Render reads `render.yaml` and creates the `effect-cqrs-workshop` web
   service on the free plan.
2. Set `POSTGRES_URL` to the Supabase string, `ADMIN_CODE` to a code of your
   choice and `PERSONAL_DATA_MASTER_KEY` to the output of `openssl rand -base64 32`.
   Keep a copy of the key: without it no nickname can be read again. Render makes
   up `AUTH_SECRET`. Leave `CORS_ORIGIN` empty for now.
3. Once deployed, `https://<service>.onrender.com/health` answers with
   `"store": "postgres"`.

Render rebuilds on every push to `main` that touches the server, the framework or the
lockfile. A free instance stops after 15 minutes without requests and takes about a
minute to wake up: the UI says so while it waits.

### 3. Vercel

1. At <https://vercel.com/new>, import this repository. Leave the root directory as
   the repository root: `vercel.json` sets the build.
2. Add the environment variable `VITE_API_URL` with the Render address, without a
   trailing slash, and deploy.
3. Back on Render, set `CORS_ORIGIN` to the Vercel address, for example
   `https://effect-cqrs-workshop.vercel.app`.

Vercel skips a build when a push changes nothing the UI uses.

### The laptop

For the workshop, run the same image on a laptop and expose it with a Cloudflare
quick tunnel, which needs no account. Give it the variables Render has, so it uses
the same database. Two servers on one PostgreSQL share the work: the sagas, the bot
and the simulated shoppers run on the one that holds their lease. Behind a load
balancer that is how the workshop scales, see [cluster/README.md](cluster/README.md).

```bash
docker build -f examples/workshop/Dockerfile -t effect-cqrs-workshop .
docker run --rm -p 3000:3000 -e POSTGRES_URL='...' effect-cqrs-workshop
cloudflared tunnel --url http://localhost:3000
```

Then point `VITE_API_URL` on Vercel at the tunnel's address and redeploy.
