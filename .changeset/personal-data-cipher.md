---
"effect-cqrs": minor
---

`PersonalDataNode.layerCipher`, from `effect-cqrs/PersonalDataNode`, encrypts personal fields with `node:crypto` instead of WebCrypto. It is synchronous, which roughly halves what reading a personal field costs, and writes the same format, so data written with either cipher reads with the other. The cipher is a `PersonalData.Cipher`, WebCrypto unless another is provided, and keys are now `CipherKey`s rather than WebCrypto `CryptoKey`s.

`PersonalData.layerSql` and `makeSqlBackend` take `migrate: false` to leave the key table to your own migrations, with its migrations from `PersonalData.migrations({ table })`, in the same shape as the stores' `migrations`.

The key cache now uses Effect's `Cache`. Concurrent requests for a subject that isn't cached share one read of the key store and one call to the master key, and writers racing for a new subject create a single key, where each used to wrap a key of its own, a paid call with a KMS.

Reading checks that an encrypted field belongs to the subject it would be encrypted for, so a field copied into another stream fails to decrypt instead of reading as someone else's data. Keys must be 32 bytes, and errors no longer carry a malformed master key from configuration or the text of a field that isn't JSON.
