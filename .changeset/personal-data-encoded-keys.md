---
"effect-cqrs": patch
---

Reject personal fields inside a struct whose encoded form differs from its decoded one, such as a struct piped through `Schema.encodeKeys`. The fields were looked up under their decoded keys in the encoded payload, missed, and stored in plaintext. Building the aggregate service now fails with an error that names the struct.
