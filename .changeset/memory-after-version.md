---
"effect-cqrs": patch
---

Make the memory store honour `afterVersion` on a read of one stream, as the SQL stores do. Running a command again with its `commandId` on the memory store returned the events of every command before it in the stream as well as its own.
