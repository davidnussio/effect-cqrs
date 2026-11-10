---
"effect-cqrs": patch
---

Don't write an append whose caller was interrupted while it waited for a group commit on SQLite or PostgreSQL. A command that timed out in that queue failed for its caller and was written anyway. An append interrupted once its group is being written may still be written, as the documentation of `groupCommit` now says.
