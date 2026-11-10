---
"effect-cqrs": minor
---

Refuse a `commandId` that another command already ran with on the aggregate. The command's name is recorded beside its id, under the reserved metadata key `cqrs.command` (`AggregateService.commandTypeKey`), and a different command under a known id fails with `CommandIdReused` instead of returning the first command's result. Through `CommandApi`, a reused `Idempotency-Key` is answered with `422`. `CommandFailure` includes the new error.
