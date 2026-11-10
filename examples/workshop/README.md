# Workshop demo

A bank and a multi-user shop on `effect-cqrs`, for a hands-on workshop: participants
use it from their phones while a big screen shows what happens inside. The plan, in
Italian, is in [PLAN.md](PLAN.md). So far it has the bank: participants join with a
nickname, add money, send it to each other and close their account, which forgets
their nickname.

| Path | What it is |
| --- | --- |
| `api.ts` | The HTTP API, shared by the server and the UI's typed client |
| `domain/account.ts` | The account aggregate |
| `domain/bank.ts` | The account service, the transfer saga and the view models |
| `server/auth.ts` | The tokens the server issues |
| `server/http.ts` | The handlers of the API |
| `server/main.ts` | The server: configuration, storage, Swagger |
| `server/vite.config.ts` | Bundles the server into one file for the Docker image |
| `ui/` | The UI, a Vite and React app |
| `Dockerfile` | The image Render runs, built from the root of the repository |

`render.yaml` and `vercel.json` at the root of the repository deploy it, and
`test/Workshop.test.ts` tests it.

## Running it locally

```bash
pnpm workshop
```

```bash
pnpm ui:workshop
```

The server listens on port 3000 and stores events in `workshop.sqlite`. To use the
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
| `POST /me/deposit` | Adds up to 100 |
| `POST /me/transfers` | Sends money to another participant |
| `GET /me/ledger` | The caller's movements |
| `POST /me/close` | Pays out the balance, closes the account and forgets the nickname |
| `GET /participants` | Everyone who joined, with their balance |
| `GET /docs` | Swagger UI |
| `GET /openapi.json` | The OpenAPI document |

Every route under `/me` and `/participants` needs `Authorization: Bearer <token>`. In
Swagger, call `/auth/join`, copy the token and paste it under **Authorize**. The reads
take a `position` from an earlier answer and wait until the view models have seen it,
so a client reads its own writes.

## Authentication

The server issues its own tokens: there is no identity provider. `/auth/join` makes
up an id, opens the account under that id and returns a JWT signed with HMAC SHA-256
through WebCrypto. The token names the account, the nickname and the role, and
expires after a day. Every command runs on the account in the token, never on one
named in the request, and a token whose account is closed is refused.

The token lives in the browser's storage only. Clearing it, or changing phone, loses
the account, which is why the UI has no "log out".

Joining with `adminCode` equal to `ADMIN_CODE` makes the session an admin one, for
the presenter's controls to come.

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

### Plan B: the laptop

If Render misbehaves on the day, run the same image on a laptop and expose it with a
Cloudflare quick tunnel, which needs no account:

```bash
docker build -f examples/workshop/Dockerfile -t effect-cqrs-workshop .
docker run --rm -p 3000:3000 -e POSTGRES_URL='...' effect-cqrs-workshop
cloudflared tunnel --url http://localhost:3000
```

Then point `VITE_API_URL` on Vercel at the tunnel's address and redeploy.
