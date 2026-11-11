---
"effect-cqrs": minor
---

Add `CommandGateway`, one entry point for the commands of several aggregates, like Axon's `CommandGateway`. `CommandGateway.make([Todo, Account])` builds it over aggregates, or over services built with `AggregateService.make` to choose their options or share them. `send` takes a typed `{ aggregateName, aggregateId, type, payload }` command, with `commandId` and `expectedVersion`, and its errors are those of the command's handler. `dispatch` takes JSON envelopes and routes them by `aggregateName`, or by `type` when a single aggregate has a command of that name, and fails with `UnroutableCommand` when it can't tell where one goes. Interceptors, declared as `CommandGateway.Interceptor<E, R>`, run around every command to authorize it, log it or add metadata, and their errors and requirements are added to the commands' types. `gateway.services` gives each aggregate's service for reading.
