---
"effect-cqrs": patch
---

Run command handlers, view model handlers and outbox publishers under the caller's span and log annotations. A service built inside a span put every span of its handlers, and of the appends they made, under that span instead of under the request.
