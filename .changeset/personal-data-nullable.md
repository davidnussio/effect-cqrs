---
"effect-cqrs": patch
---

Encrypt personal fields marked around a nullable schema, such as `PersonalData.field(Schema.NullOr(Schema.String), { redacted: null })`. They went unnoticed and were stored in plaintext. Fields marked inside the union, `Schema.NullOr(PersonalData.field(...))`, were already found.
