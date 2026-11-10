---
"effect-cqrs": minor
---

Add modules that connect aggregates to the rest of a system, described in the new integration guide.

`CommandApi` serves an aggregate over HTTP with `effect/http-api`. `CommandApi.group` builds an `HttpApiGroup` with a `POST` endpoint per command, taking the command's payload as body, and `GET` endpoints for `history` and, given a schema of the state, `readModel`. `CommandApi.layer` implements it with an aggregate service. An invalid payload is answered with `400`, a lost race with `409`, and a handler's error with `422`: as it is when listed in `errors`, otherwise as `CommandRejected` with its tag and message. OpenAPI comes from the group.

`EventCatalog` gives the JSON Schema of every event, in every version declared with `EventVersions`, and of every command payload, and lists them in an AsyncAPI 3.0 document. `captureSamples` keeps a few stored payloads of each event type and version, with personal fields replaced by their redacted value, `saveSamples` adds them to a JSON file, and `checkCompatibility` fails a test when one no longer decodes with the current aggregate.

`Outbox` publishes events to brokers such as Kafka, NATS or SQS. An outbox is a view model, so it publishes at least once from its checkpoint, in batches, retrying a failed batch, and never moves the checkpoint past a message that wasn't published. Events are mapped to integration events by tag, and each message has the event's id, which stays the same, for consumers to deduplicate.

`examples/server`, run with `pnpm example:server`, serves the todo aggregate over HTTP with OpenAPI documentation, a view model, SQLite or PostgreSQL, and OpenTelemetry export when an endpoint is configured. The integration guide also shows tenant isolation with Effect's `LayerMap`.
