# Todo server

The todo aggregate of `examples/todo` behind an HTTP server: commands and reads through
`CommandApi`, the todo list view model, OpenAPI documentation, and OpenTelemetry export
when an endpoint is configured.

```bash
pnpm example:server
```

| Route | What it does |
| --- | --- |
| `POST /todo/:id/createTodo` | Creates a todo, body `{ "title": "..." }` |
| `POST /todo/:id/completeTodo` | Completes it, no body needed |
| `POST /todo/:id/deleteTodo` | Deletes it, body `{ "message": "..." }` |
| `GET /todo/:id` | The state of one todo, folded from its events |
| `GET /todo/:id/history?payload=true` | Its events |
| `GET /todos` | The view model. With a `Read-After: N` header, after it has seen position `N` |
| `GET /docs` | Scalar documentation |
| `GET /openapi.json` | The OpenAPI document |

```bash
curl -X POST localhost:3000/todo/t1/createTodo -H 'content-type: application/json' -d '{"title":"Buy milk"}'
curl -i -X POST localhost:3000/todo/t1/completeTodo
curl localhost:3000/todo/t1
curl localhost:3000/todos -H 'read-after: 2'
```

A command answers with an `Event-Position` header, the position of its last event. Send it
back as `Read-After` and the list waits until it includes that command, for up to 5
seconds, then answers `503` if it is still behind.

An invalid payload is answered with `400`, a declared domain error such as
`TodoAlreadyCreated` with `422` and its fields, and a lost race with `409`.

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `PORT` | `3000` | |
| `SQLITE_FILE` | `todo-server.sqlite` | Where events go without `POSTGRES_URL` |
| `POSTGRES_URL` | | Stores events in PostgreSQL instead, for example `postgres://postgres:postgres@localhost:5432/cqrs` with the database of `docker-compose.yml` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | Exports traces, metrics and logs over OTLP/HTTP, for example `http://localhost:4318`. Nothing is exported without it |

To see traces locally, run a collector such as Jaeger and point the server at it:

```bash
docker run --rm -p 16686:16686 -p 4318:4318 jaegertracing/jaeger:latest
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 pnpm example:server
```

The view model keeps the list in memory with an in-memory checkpoint, so it is rebuilt
from the store on every start. `api.ts` holds only the API definition, which a client can
import to call the server with `HttpApiClient`.
