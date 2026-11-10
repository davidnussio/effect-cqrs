---
"effect-cqrs": patch
---

Keep view model batches to one page of the store under load. A view model slower than the appends read its whole backlog as one batch, which with a `transaction` became one transaction and was handled again in full after a failure. It now takes the latest head of the store each time it catches up, and reads a backlog in pages. A view model that waits to retry a failing event no longer queues a notification for every append made meanwhile, and parking an event clears `Status.lastError`.
