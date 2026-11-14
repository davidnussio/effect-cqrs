---
"effect-cqrs": minor
---

Read your own writes over HTTP. A command served by `CommandApi` answers with an `Event-Position` header, the store position of its last event, as well as the `position` in the body. `CommandApi.ReadAfter` is middleware for your own query endpoints: a request with `Read-After: <position>` waits until the view models of the process have handled that position before the endpoint runs. Implement it with `CommandApi.layerReadAfter({ timeout })`, which needs `ViewModel.ViewModels`. A request that waits longer than the timeout, 5 seconds by default, is answered with `503` and the new `NotCaughtUp` error, and a header that isn't a position with `400`.
