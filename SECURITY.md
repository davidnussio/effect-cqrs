# Security

## Supported versions

effect-cqrs is at 0.x, so only the latest release gets fixes. A fix ships as a new release
rather than a backport.

| Version | Supported |
| --- | --- |
| Latest 0.x release | Yes |
| Earlier releases | No |

## Reporting a vulnerability

Report it privately through GitHub's private vulnerability reporting: open the
[Security tab](https://github.com/davidnussio/effect-cqrs/security) of the repository and
choose **Report a vulnerability**. Don't open a public issue.

Say which version you tested, what an attacker can do, and how to reproduce it. The
discussion stays in the private advisory until a fix is released, and the advisory is then
published with credit to you, unless you'd rather stay anonymous.

Vulnerabilities in Effect itself belong to the [Effect repository](https://github.com/Effect-TS/effect),
and those in a database driver to that driver's project.

## Threat model of crypto-shredding

`PersonalData` lets an append-only log forget a person: the fields marked with
`PersonalData.field` are encrypted with a key that belongs to their subject, and
`PersonalData.forget` destroys that key. The
[personal data guide](https://davidnussio.github.io/effect-cqrs/guides/personal-data/)
explains how to use it. This section says what it protects and what it doesn't.

### How it works

- Each subject has its own random AES-256 data key, created the first time one of its
  fields is written.
- Every personal field is encrypted with AES-GCM under a fresh 96-bit random IV. The
  additional authenticated data binds the ciphertext to the aggregate name, aggregate id,
  event type, field path, subject and key id, so it can't be moved to another stream,
  field or subject without failing to decrypt.
- The key store only holds data keys wrapped by a `MasterKey`, with AES-GCM as well when
  the master key is held by the process. A `MasterKey` can also be backed by a KMS, which
  keeps the master key out of the process.
- `forget` deletes the wrapped key and records the subject as forgotten in the same step,
  then appends a `SubjectForgotten` event to the log. A subject without a key record fails
  to decrypt rather than reading as forgotten, so losing the key store doesn't pass for
  forgetting everyone.
- Snapshots of a state that holds personal data are encrypted with the key of every
  subject they hold data of, and deleted when one of them can't be read.

### What it protects against

- **Someone who reads the event store but not the key store or the master key**, such as a
  leaked database dump, a replica or a backup of the events. They see the events' other
  fields, but not the personal ones.
- **Reading a forgotten subject's data from the log**, after its key is destroyed and every
  copy of the key store that still held it has expired. That includes old backups of the
  events, which hold ciphertext only.
- **Tampering with a personal field.** A changed or moved ciphertext fails authentication
  and the read fails with `PersonalDataError` rather than returning a wrong value.

### What it doesn't protect against

- **An attacker who can run code in the application** or read its memory. It decrypts
  personal fields to run commands and view models, and caches unwrapped keys for
  `cacheTtl`, a minute by default.
- **An attacker who has the key store and the master key**, or a KMS role that can unwrap
  keys. Keep the key store in another database or behind another client, with its own
  permissions, and keep the master key out of both.
- **Copies of the keys.** Backups, replicas and snapshots of the key store keep a key after
  `forget` for as long as they are kept. Let them expire.
- **Plaintext outside the log.** View models receive decrypted fields, and keep them until
  `onForget` erases them or the view model is replayed. Logs, traces, caches, exports and
  anything a handler sent elsewhere keep what they saw. Don't log payloads of events with
  personal fields.
- **Metadata.** The subject id, the key id, the event type, which fields are present and the
  length of each value are stored in plaintext, and the `SubjectForgotten` event records
  which subject was forgotten and when. Use opaque subject ids rather than email addresses,
  and don't put personal data in aggregate ids, unmarked fields, causation or command
  metadata.
- **Data written before a field was marked.** The log is never rewritten, so older events
  keep that field in plaintext.
- **Other processes during the cache window.** A process that cached a subject's key keeps
  using it for up to `cacheTtl` after another process forgot the subject.
- **Deleted bytes on disk.** Databases keep overwritten rows for a while in free pages, dead
  tuples or their write-ahead log until they are vacuumed or checkpointed.
- **Keys kept by `layerMemory` and `layerMasterKeyEphemeral`.** They are for tests and
  demos: the keys live in the process, and everything they encrypted is unreadable after a
  restart.

Crypto-shredding is a way to honour a request to be forgotten in an append-only log, not a
replacement for access control on the event store.
