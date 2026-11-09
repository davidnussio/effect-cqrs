/**
 * Crypto-shredding for personal data. The log is append-only, so the personal
 * fields of an event are encrypted with a key that belongs to the person they
 * describe, the subject. Forgetting a subject destroys that key: the events stay
 * where they are and keep their amounts, dates and ids, while their personal
 * fields read as the replacement each field declares.
 *
 * ```ts
 * const AccountEvent = Schema.TaggedUnion({
 *   AccountOpened: {
 *     owner: PersonalData.field(Schema.String, { redacted: "[forgotten]" }),
 *     currency: Schema.Literals(["CHF", "EUR"])
 *   },
 *   TransferSent: {
 *     to: Schema.String,
 *     // Encrypted with the key of the receiver rather than the account holder.
 *     beneficiary: PersonalData.field(Schema.String, { redacted: "[forgotten]", subject: "to" }),
 *     amount: Schema.Number
 *   }
 * })
 * ```
 *
 * A field belongs to the aggregate id of its stream unless it names a sibling
 * property that holds its subject. Schemas stay plain: encryption happens where
 * `AggregateService` writes and reads the store, which then needs {@link Keys}.
 *
 * Each subject has its own data key. The key store keeps it only wrapped by a
 * {@link MasterKey}, which can live in a KMS, so the master key can change without
 * touching events. {@link forget} destroys the data key, refuses keys for that
 * subject from then on and records the request in the log.
 */
import { Clock, Config, Context, Duration, Effect, Layer, Option, Predicate, Redacted, Schema, type SchemaAST } from "effect"
import { Base64 } from "effect/encoding"
import { SqlClient } from "effect/sql"
import { CurrentCausation } from "./Causation.ts"
import { EventStore, type EventStoreError } from "./EventStore.ts"
import { CurrentMetadata } from "./Metadata.ts"

export class SubjectForgotten extends Schema.TaggedError<SubjectForgotten>()("SubjectForgotten", {
  subject: Schema.String
}) {}

/** A key that couldn't be read, created or used, or a field that didn't decrypt. */
export class PersonalDataError extends Schema.TaggedError<PersonalDataError>()("PersonalDataError", {
  operation: Schema.String,
  subject: Schema.String,
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}) {}

// ---------------------------------------------------------------------------
// Fields
// ---------------------------------------------------------------------------

const annotationKey = "effect-cqrs/PersonalData"

interface FieldAnnotation {
  /** The replacement, in the field's JSON form. */
  readonly redacted: Schema.Json
  readonly subject: string | undefined
}

type PlainSchema = Schema.Top & { readonly DecodingServices: never; readonly EncodingServices: never }

/**
 * Marks a field as personal data. `redacted` is what the field reads as once
 * its subject is forgotten, and has to be a valid value of the field. `subject`
 * names a property next to the field that holds the id of the person it
 * describes, by default the aggregate id of the stream.
 */
export const field = <S extends PlainSchema>(
  schema: S,
  options: { readonly redacted: S["Type"]; readonly subject?: string | undefined }
): S => {
  let redacted: Schema.Json
  try {
    const value = Schema.decodeUnknownSync(Schema.toType(schema as PlainSchema))(options.redacted)
    redacted = Schema.encodeSync(Schema.toCodecJson(schema as PlainSchema))(value) as Schema.Json
  } catch (error) {
    throw new Error(`PersonalData.field: the redacted value is not a valid value of the field: ${String(error)}`)
  }
  const annotation: FieldAnnotation = { redacted, subject: options.subject }
  return schema.annotate({ [annotationKey]: annotation }) as S
}

/** Where a personal field sits in an encoded payload, and how to find its subject. */
export interface Field {
  readonly path: ReadonlyArray<string>
  /** The sibling property holding the subject, or `undefined` for the aggregate id. */
  readonly subject: string | undefined
  readonly redacted: Schema.Json
}

// A schema with checks keeps its annotations on the last check, and a field marked
// before more checks were added keeps them on an earlier one, so all are read: a
// personal field that goes unnoticed would be stored in plaintext.
const annotationOf = (ast: SchemaAST.AST): FieldAnnotation | undefined => {
  const found = ast.annotations?.[annotationKey] ??
    ast.checks?.findLast((check) => check.annotations?.[annotationKey] !== undefined)?.annotations?.[annotationKey]
  return found as FieldAnnotation | undefined
}

const isNullish = (ast: SchemaAST.AST) => ast._tag === "Null" || ast._tag === "Undefined" || ast._tag === "Void"

// `optional`, `NullOr` and `UndefinedOr` wrap a field in a union with nothing.
const unwrapNullable = (ast: SchemaAST.AST): SchemaAST.AST => {
  if (ast._tag !== "Union") return ast
  const present = ast.types.filter((member) => !isNullish(member))
  return present.length === 1 ? present[0]! : ast
}

const mentionsPersonal = (ast: SchemaAST.AST, seen: Set<SchemaAST.AST> = new Set()): boolean => {
  if (seen.has(ast)) return false
  seen.add(ast)
  if (annotationOf(ast) !== undefined) return true
  switch (ast._tag) {
    case "Objects":
      return ast.propertySignatures.some(({ type }) => mentionsPersonal(type, seen)) ||
        ast.indexSignatures.some(({ type }) => mentionsPersonal(type, seen))
    case "Union":
      return ast.types.some((type) => mentionsPersonal(type, seen))
    case "Arrays":
      return [...ast.elements, ...ast.rest].some((type) => mentionsPersonal(type, seen))
    case "Suspend":
      return mentionsPersonal(ast.thunk(), seen)
    case "Declaration":
      return ast.typeParameters.some((type) => mentionsPersonal(type, seen))
    default:
      return false
  }
}

const describe = (path: ReadonlyArray<string>) => path.length === 0 ? "the top level" : path.join(".")

const collect = (ast: SchemaAST.AST, path: ReadonlyArray<string>, fields: Array<Field>, where: string): void => {
  const node = unwrapNullable(ast)
  const annotation = annotationOf(node)
  if (annotation !== undefined) {
    fields.push({ path, subject: annotation.subject, redacted: annotation.redacted })
    return
  }
  if (node._tag !== "Objects") {
    if (mentionsPersonal(node)) {
      throw new Error(
        `${where}: personal fields can be properties of structs, nested structs and optional or nullable properties, ` +
          `but ${describe(path)} is a ${node._tag}`
      )
    }
    return
  }
  const before = fields.length
  for (const property of node.propertySignatures) {
    if (!Predicate.isString(property.name)) {
      if (mentionsPersonal(property.type)) throw new Error(`${where}: a personal field needs a string key`)
      continue
    }
    collect(property.type, [...path, property.name], fields, where)
  }
  if (node.indexSignatures.some(({ type }) => mentionsPersonal(type))) {
    throw new Error(`${where}: personal fields can't be values of a record at ${describe(path)}`)
  }
  // A field takes its subject from a sibling, which has to exist and be readable.
  for (const field of fields.slice(before)) {
    if (field.subject === undefined || field.path.length !== path.length + 1) continue
    const sibling = node.propertySignatures.find(({ name }) => name === field.subject)
    if (sibling === undefined) {
      throw new Error(`${where}: ${describe(field.path)} takes its subject from "${field.subject}", which is not next to it`)
    }
    if (annotationOf(unwrapNullable(sibling.type)) !== undefined) {
      throw new Error(`${where}: ${describe(field.path)} takes its subject from "${field.subject}", which is personal itself`)
    }
  }
}

/** The personal fields of one schema, such as an old version of an event. */
export const fieldsOf = (schema: Schema.Top, where: string): ReadonlyArray<Field> => {
  const fields: Array<Field> = []
  collect(schema.ast, [], fields, where)
  return fields
}

const tagOf = (ast: SchemaAST.Objects): string | undefined => {
  const tag = ast.propertySignatures.find(({ name }) => name === "_tag")?.type
  return tag?._tag === "Literal" && Predicate.isString(tag.literal) ? tag.literal : undefined
}

/** The personal fields of each event in a union of tagged events. Events without any are left out. */
export const fieldsByTag = (schema: Schema.Top, where: string): ReadonlyMap<string, ReadonlyArray<Field>> => {
  const byTag = new Map<string, ReadonlyArray<Field>>()
  const visit = (ast: SchemaAST.AST): void => {
    if (ast._tag === "Union") return ast.types.forEach(visit)
    const tag = ast._tag === "Objects" ? tagOf(ast) : undefined
    if (tag === undefined) {
      if (mentionsPersonal(ast)) throw new Error(`${where}: events with personal fields need to be tagged structs`)
      return
    }
    const fields: Array<Field> = []
    collect(ast, [], fields, `${where}, ${tag}`)
    if (fields.length > 0) byTag.set(tag, fields)
  }
  visit(schema.ast)
  return byTag
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/**
 * Wraps the data keys of subjects, so the key store only ever holds them
 * encrypted. Implement it on top of a KMS to keep the master key out of the
 * process, or use {@link layerMasterKey} with a key from configuration.
 */
export class MasterKey extends Context.Service<MasterKey, {
  /** The key new data keys are wrapped with. */
  readonly keyId: string
  wrap(
    dataKey: Uint8Array<ArrayBuffer>,
    context: { readonly subject: string; readonly keyId: string }
  ): Effect.Effect<Uint8Array<ArrayBuffer>, PersonalDataError>
  /** Unwraps a key wrapped by `masterKeyId`, which may be an earlier master key. */
  unwrap(
    wrapped: Uint8Array<ArrayBuffer>,
    context: { readonly subject: string; readonly keyId: string; readonly masterKeyId: string }
  ): Effect.Effect<Uint8Array<ArrayBuffer>, PersonalDataError>
}>()("effect-cqrs/PersonalData/MasterKey") {}

/** What the key store holds for a subject. */
export type KeyRecord =
  | {
    readonly _tag: "Active"
    readonly keyId: string
    readonly masterKeyId: string
    /** The data key, wrapped by the master key. */
    readonly wrapped: Uint8Array<ArrayBuffer>
  }
  | { readonly _tag: "Forgotten"; readonly forgottenAt: number }

/** Where keys are kept. Keep it apart from the events, with backups that expire. */
export interface Backend {
  readonly get: (subject: string) => Effect.Effect<Option.Option<KeyRecord>, PersonalDataError>
  /**
   * Stores a key unless the subject already has a record, and returns the record
   * the subject has afterwards: a racing writer's key, or `Forgotten`.
   */
  readonly create: (
    subject: string,
    key: Extract<KeyRecord, { readonly _tag: "Active" }>
  ) => Effect.Effect<KeyRecord, PersonalDataError>
  /** Deletes the key and records that the subject is forgotten, in one step. */
  readonly forget: (subject: string, forgottenAt: number) => Effect.Effect<void, PersonalDataError>
}

/** A key from WebCrypto, which works in Node and in browsers. */
export type AesKey = InstanceType<typeof globalThis.CryptoKey>

export interface DataKey {
  readonly keyId: string
  readonly key: AesKey
}

/** The data keys of subjects, created on first use and cached for a while. */
export class Keys extends Context.Service<Keys, {
  /** The key to encrypt with, created when the subject has none. Fails for a forgotten subject. */
  encryptionKey(subject: string): Effect.Effect<DataKey, SubjectForgotten | PersonalDataError>
  /**
   * The key to decrypt with, `None` when the subject was forgotten. A subject
   * without any key fails: losing the key store must not pass for forgetting.
   */
  decryptionKey(subject: string, keyId: string): Effect.Effect<Option.Option<AesKey>, PersonalDataError>
  isForgotten(subject: string): Effect.Effect<boolean, PersonalDataError>
  /** Destroys the key of a subject. Prefer {@link forget}, which also records it in the log. */
  forget(subject: string): Effect.Effect<void, PersonalDataError>
}>()("effect-cqrs/PersonalData/Keys") {}

export interface KeysOptions {
  /** How many unwrapped keys to keep in memory. Defaults to 10,000. */
  readonly cacheSize?: number | undefined
  /**
   * How long an unwrapped key is used before the store is asked again. It bounds
   * how long another process keeps using the key of a subject forgotten here.
   * Defaults to one minute.
   */
  readonly cacheTtl?: Duration.Input | undefined
}

const subtle = () => globalThis.crypto.subtle

const failure = (operation: string, subject: string, message: string) => (cause: unknown) =>
  new PersonalDataError({ operation, subject, message, cause })

const importAesKey = (bytes: Uint8Array<ArrayBuffer>, operation: string, subject: string) =>
  Effect.tryPromise({
    try: () => subtle().importKey("raw", bytes, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]),
    catch: failure(operation, subject, "The key is not a valid AES-256 key")
  })

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

const randomBytes = (length: number): Uint8Array<ArrayBuffer> => globalThis.crypto.getRandomValues(new Uint8Array(length))

const ivLength = 12

/** AES-GCM with a random IV, which goes in front of the ciphertext. */
const encrypt = (
  key: AesKey,
  plaintext: Uint8Array<ArrayBuffer>,
  additionalData: Uint8Array<ArrayBuffer>,
  operation: string,
  subject: string
) => {
  const iv = randomBytes(ivLength)
  return Effect.tryPromise({
    try: () => subtle().encrypt({ name: "AES-GCM", iv, additionalData }, key, plaintext),
    catch: failure(operation, subject, "Encryption failed")
  }).pipe(Effect.map((ciphertext) => {
    const sealed = new Uint8Array(ivLength + ciphertext.byteLength)
    sealed.set(iv)
    sealed.set(new Uint8Array(ciphertext), ivLength)
    return sealed
  }))
}

const decrypt = (
  key: AesKey,
  sealed: Uint8Array<ArrayBuffer>,
  additionalData: Uint8Array<ArrayBuffer>,
  operation: string,
  subject: string
) =>
  Effect.tryPromise({
    try: () =>
      subtle().decrypt(
        { name: "AES-GCM", iv: sealed.subarray(0, ivLength), additionalData },
        key,
        sealed.subarray(ivLength)
      ),
    catch: failure(operation, subject, "The data doesn't decrypt: it was altered, or belongs to another key or place")
  }).pipe(Effect.map((plaintext) => new Uint8Array(plaintext)))

const label = (...parts: ReadonlyArray<string | number | ReadonlyArray<string>>) =>
  textEncoder.encode(JSON.stringify(["effect-cqrs/PersonalData", ...parts]))

const decodeBase64 = (text: string, operation: string, subject: string) => {
  const result = Base64.decode(text)
  return result._tag === "Success"
    ? Effect.succeed(new Uint8Array(result.success))
    : Effect.fail(new PersonalDataError({ operation, subject, message: "Malformed base64", cause: result.failure }))
}

/**
 * A master key held by the process, given as 32 bytes. Keys wrapped by earlier
 * master keys stay readable while those are listed in `previous`.
 */
export const makeMasterKey = Effect.fnUntraced(function*(options: {
  readonly keyId: string
  readonly key: Redacted.Redacted<Uint8Array>
  readonly previous?: ReadonlyArray<{ readonly keyId: string; readonly key: Redacted.Redacted<Uint8Array> }> | undefined
}) {
  const keys = new Map<string, AesKey>()
  for (const { key, keyId } of [options, ...(options.previous ?? [])]) {
    const bytes = new Uint8Array(Redacted.value(key))
    if (bytes.byteLength !== 32) {
      return yield* new PersonalDataError({
        operation: "masterKey",
        subject: "",
        message: `Master key ${keyId} has ${bytes.byteLength} bytes instead of 32`
      })
    }
    keys.set(keyId, yield* importAesKey(bytes, "masterKey", ""))
    bytes.fill(0)
  }
  const context = (subject: string, keyId: string) => label("key", subject, keyId)
  return MasterKey.of({
    keyId: options.keyId,
    wrap: (dataKey, { keyId, subject }) =>
      encrypt(keys.get(options.keyId)!, dataKey, context(subject, keyId), "wrap", subject),
    unwrap: (wrapped, { keyId, masterKeyId, subject }) => {
      const key = keys.get(masterKeyId)
      return key === undefined
        ? Effect.fail(
          new PersonalDataError({ operation: "unwrap", subject, message: `Unknown master key ${masterKeyId}` })
        )
        : decrypt(key, wrapped, context(subject, keyId), "unwrap", subject)
    }
  })
})

export const layerMasterKey = (options: Parameters<typeof makeMasterKey>[0]): Layer.Layer<MasterKey, PersonalDataError> =>
  Layer.effect(MasterKey, makeMasterKey(options))

/**
 * A master key from configuration: an id, and 32 bytes in base64. By default
 * `PERSONAL_DATA_MASTER_KEY_ID` and `PERSONAL_DATA_MASTER_KEY`.
 */
export const layerMasterKeyConfig = (options?: {
  readonly keyId?: Config.Config<string> | undefined
  readonly key?: Config.Config<Redacted.Redacted<string>> | undefined
}): Layer.Layer<MasterKey, PersonalDataError | Config.ConfigError> =>
  Layer.effect(
    MasterKey,
    Effect.gen(function*() {
      const keyId = yield* (options?.keyId ?? Config.String("PERSONAL_DATA_MASTER_KEY_ID"))
      const key = yield* (options?.key ?? Config.Redacted("PERSONAL_DATA_MASTER_KEY"))
      const bytes = yield* decodeBase64(Redacted.value(key), "masterKey", "")
      return yield* makeMasterKey({ keyId, key: Redacted.make(bytes) })
    })
  )

/**
 * A random master key that lasts as long as the layer. Keys it wrapped can't be
 * read by another process or after a restart, so it suits tests and demos.
 */
export const layerMasterKeyEphemeral: Layer.Layer<MasterKey> = Layer.effect(
  MasterKey,
  Effect.suspend(() => Effect.orDie(makeMasterKey({ keyId: "ephemeral", key: Redacted.make(randomBytes(32)) })))
)

interface Cached {
  readonly keyId: string
  readonly key: AesKey
  readonly expiresAt: number
}

/** Builds {@link Keys} on a key store. */
export const make = Effect.fnUntraced(function*(backend: Backend, options?: KeysOptions) {
  const masterKey = yield* MasterKey
  const capacity = options?.cacheSize ?? 10_000
  const ttl = Duration.toMillis(Duration.fromInputUnsafe(options?.cacheTtl ?? "1 minute"))
  // A `Map` iterates in insertion order, which makes it a least recently used cache.
  const cache = new Map<string, Cached>()

  const cached = (subject: string, now: number): Cached | undefined => {
    const entry = cache.get(subject)
    if (entry === undefined) return undefined
    cache.delete(subject)
    if (entry.expiresAt <= now) return undefined
    cache.set(subject, entry)
    return entry
  }

  const remember = (subject: string, keyId: string, key: AesKey, now: number) => {
    cache.delete(subject)
    cache.set(subject, { keyId, key, expiresAt: now + ttl })
    if (cache.size > capacity) cache.delete(cache.keys().next().value!)
  }

  const unwrap = Effect.fnUntraced(function*(subject: string, record: Extract<KeyRecord, { _tag: "Active" }>) {
    const bytes = yield* masterKey.unwrap(record.wrapped, { subject, ...record })
    const key = yield* importAesKey(bytes, "unwrap", subject)
    bytes.fill(0)
    return key
  })

  const activate = Effect.fnUntraced(function*(subject: string, record: KeyRecord, now: number) {
    if (record._tag === "Forgotten") return yield* new SubjectForgotten({ subject })
    const key = yield* unwrap(subject, record)
    remember(subject, record.keyId, key, now)
    return { keyId: record.keyId, key }
  })

  const encryptionKey = Effect.fnUntraced(function*(subject: string) {
    const now = yield* Clock.currentTimeMillis
    const hit = cached(subject, now)
    if (hit !== undefined) return { keyId: hit.keyId, key: hit.key }
    const existing = yield* backend.get(subject)
    if (Option.isSome(existing)) return yield* activate(subject, existing.value, now)
    const keyId = crypto.randomUUID()
    const bytes = randomBytes(32)
    const wrapped = yield* masterKey.wrap(bytes, { subject, keyId })
    bytes.fill(0)
    const record = yield* backend.create(subject, { _tag: "Active", keyId, masterKeyId: masterKey.keyId, wrapped })
    return yield* activate(subject, record, now)
  })

  const decryptionKey = Effect.fnUntraced(function*(subject: string, keyId: string) {
    const now = yield* Clock.currentTimeMillis
    const hit = cached(subject, now)
    if (hit !== undefined && hit.keyId === keyId) return Option.some(hit.key)
    const record = yield* backend.get(subject)
    if (Option.isNone(record)) {
      return yield* new PersonalDataError({
        operation: "decryptionKey",
        subject,
        message: "The subject has no key and was never forgotten: the key store is missing data"
      })
    }
    if (record.value._tag === "Forgotten") return Option.none()
    if (record.value.keyId !== keyId) {
      return yield* new PersonalDataError({
        operation: "decryptionKey",
        subject,
        message: `The data was encrypted with key ${keyId}, which the subject doesn't have`
      })
    }
    const key = yield* unwrap(subject, record.value)
    remember(subject, keyId, key, now)
    return Option.some(key)
  })

  const isForgotten = Effect.fnUntraced(function*(subject: string) {
    const now = yield* Clock.currentTimeMillis
    if (cached(subject, now) !== undefined) return false
    const record = yield* backend.get(subject)
    if (Option.isNone(record)) return false
    if (record.value._tag === "Forgotten") return true
    // Caching the key spares the store a read on every command until it expires.
    remember(subject, record.value.keyId, yield* unwrap(subject, record.value), now)
    return false
  })

  const forget = Effect.fnUntraced(function*(subject: string) {
    yield* backend.forget(subject, yield* Clock.currentTimeMillis)
    cache.delete(subject)
  })

  return Keys.of({ encryptionKey, decryptionKey, isForgotten, forget })
})

/** A key store in process memory, gone after a restart along with access to everything it encrypted. */
export const makeMemoryBackend = (): Backend => {
  const records = new Map<string, KeyRecord>()
  return {
    get: (subject) => Effect.sync(() => Option.fromNullishOr(records.get(subject))),
    create: (subject, key) =>
      Effect.sync(() => {
        const existing = records.get(subject)
        if (existing !== undefined) return existing
        records.set(subject, key)
        return key
      }),
    forget: (subject, forgottenAt) =>
      Effect.sync(() => {
        if (records.get(subject)?._tag !== "Forgotten") records.set(subject, { _tag: "Forgotten", forgottenAt })
      })
  }
}

/** Keys kept in process memory, for tests and demos. */
export const layerMemory = (options?: KeysOptions): Layer.Layer<Keys, never, MasterKey> =>
  Layer.effect(Keys, Effect.suspend(() => make(makeMemoryBackend(), options)))

export interface SqlOptions {
  /** Defaults to `cqrs_personal_data_keys`. */
  readonly table?: string | undefined
}

const KeyRow = Schema.Struct({
  keyId: Schema.NullOr(Schema.String),
  masterKeyId: Schema.NullOr(Schema.String),
  wrapped: Schema.NullOr(Schema.String),
  forgottenAt: Schema.NullOr(Schema.Number)
})
const decodeKeyRows = Schema.decodeUnknownEffect(Schema.Array(KeyRow))

/**
 * A key store in a SQL table, on SQLite or PostgreSQL. The table is created on
 * startup. Provide it a client of its own to keep keys in another database than
 * the events, with other permissions and backups.
 *
 * Forgetting overwrites the row. The database may keep the old bytes for a
 * while, in free pages, dead rows or its write-ahead log, and backups keep them
 * as long as they are kept: let backups of this table expire.
 */
export const makeSqlBackend = Effect.fnUntraced(function*(options?: SqlOptions) {
  const sql = yield* SqlClient.SqlClient
  const table = sql(options?.table ?? "cqrs_personal_data_keys")
  const sqlError = (operation: string, subject: string) => failure(operation, subject, "The key store failed")

  yield* sql`
    CREATE TABLE IF NOT EXISTS ${table} (
      subject TEXT PRIMARY KEY,
      key_id TEXT,
      master_key_id TEXT,
      wrapped TEXT,
      forgotten_at BIGINT,
      CHECK ((key_id IS NOT NULL AND master_key_id IS NOT NULL AND wrapped IS NOT NULL) <> (forgotten_at IS NOT NULL))
    )
  `.pipe(Effect.mapError(sqlError("initialize", "")))

  const get = (subject: string) =>
    sql`
      SELECT
        key_id AS "keyId",
        master_key_id AS "masterKeyId",
        wrapped,
        CAST(forgotten_at AS DOUBLE PRECISION) AS "forgottenAt"
      FROM ${table} WHERE subject = ${subject}
    `.pipe(
      Effect.flatMap(decodeKeyRows),
      Effect.mapError(sqlError("get", subject)),
      Effect.flatMap(([row]) => {
        if (row === undefined) return Effect.succeed(Option.none<KeyRecord>())
        if (row.forgottenAt !== null) {
          return Effect.succeed(Option.some<KeyRecord>({ _tag: "Forgotten", forgottenAt: row.forgottenAt }))
        }
        return Effect.map(decodeBase64(row.wrapped!, "get", subject), (wrapped) =>
          Option.some<KeyRecord>({ _tag: "Active", keyId: row.keyId!, masterKeyId: row.masterKeyId!, wrapped }))
      })
    )

  return {
    get,
    create: (subject, key) =>
      sql`
        INSERT INTO ${table} (subject, key_id, master_key_id, wrapped)
        VALUES (${subject}, ${key.keyId}, ${key.masterKeyId}, ${Base64.encode(key.wrapped)})
        ON CONFLICT (subject) DO NOTHING
      `.pipe(
        Effect.mapError(sqlError("create", subject)),
        // Whoever inserted first, this is the key the subject has now.
        Effect.andThen(get(subject)),
        Effect.flatMap(Option.match({
          onNone: () =>
            Effect.fail(new PersonalDataError({ operation: "create", subject, message: "The key was not stored" })),
          onSome: Effect.succeed
        }))
      ),
    forget: (subject, forgottenAt) =>
      sql`
        INSERT INTO ${table} (subject, forgotten_at) VALUES (${subject}, ${forgottenAt})
        ON CONFLICT (subject) DO UPDATE SET
          key_id = NULL,
          master_key_id = NULL,
          wrapped = NULL,
          forgotten_at = COALESCE(${table}.forgotten_at, excluded.forgotten_at)
      `.pipe(Effect.asVoid, Effect.mapError(sqlError("forget", subject)))
  } satisfies Backend
})

/** Keys in a SQL table, see {@link makeSqlBackend}. */
export const layerSql = (
  options?: SqlOptions & KeysOptions
): Layer.Layer<Keys, PersonalDataError, MasterKey | SqlClient.SqlClient> =>
  Layer.effect(Keys, Effect.flatMap(makeSqlBackend(options), (backend) => make(backend, options)))

// ---------------------------------------------------------------------------
// Forgetting
// ---------------------------------------------------------------------------

/** The stream that records forgotten subjects, one stream per subject. */
export const streamName = "effect-cqrs/personal-data"

/** The type of the event {@link forget} records. Its payload is empty. */
export const subjectForgottenType = "SubjectForgotten"

/**
 * Forgets a subject: destroys its key, so its personal fields read as their
 * replacements everywhere, and records a `SubjectForgotten` event in the stream
 * of the subject, with the current causation and metadata as the audit trail.
 * Running it again records nothing new.
 *
 * Other processes stop using the key once their cache expires, see
 * {@link KeysOptions.cacheTtl}.
 */
export const forget = Effect.fn("PersonalData.forget")(
  function*(subject: string): Effect.fn.Return<void, PersonalDataError | EventStoreError, Keys | EventStore> {
    const keys = yield* Keys
    const store = yield* EventStore
    yield* keys.forget(subject)
    const causation = yield* CurrentCausation
    const metadata = yield* CurrentMetadata
    const id = causation?.correlationId ?? crypto.randomUUID()
    yield* store.append({
      aggregateName: streamName,
      aggregateId: subject,
      expectedVersion: 0,
      events: [{ type: subjectForgottenType, payload: {} }],
      correlationId: id,
      causationId: causation?.causationId ?? id,
      metadata
    }).pipe(
      // The subject was forgotten before, which the stream already records.
      Effect.catchTag("ConcurrencyConflict", () => Effect.void)
    )
  }
)

// ---------------------------------------------------------------------------
// Encrypting payloads
// ---------------------------------------------------------------------------

const wireKey = "$personal"
const wireVersion = 1

const Wire = Schema.Struct({
  version: Schema.Int,
  subject: Schema.String,
  keyId: Schema.String,
  data: Schema.String
})
const decodeWire = Schema.decodeUnknownOption(Wire)

/** Where a payload is stored, which the ciphertext of its fields is bound to. */
export interface Location {
  readonly aggregateName: string
  readonly type: string
}

type JsonRecord = { [key: string]: Schema.Json }

const isRecord = (value: Schema.Json | undefined): value is JsonRecord =>
  Predicate.isObject(value) && !Array.isArray(value)

/**
 * The object that holds the last key of `path`, copying each object on the way,
 * so a payload is changed without touching the one it was copied from. Cheaper
 * than a deep copy on every event of a replay.
 */
const writableParent = (root: JsonRecord, path: ReadonlyArray<string>): JsonRecord | undefined => {
  let current = root
  for (let index = 0; index < path.length - 1; index++) {
    const next = current[path[index]!]
    if (!isRecord(next)) return undefined
    const copy = { ...next }
    current[path[index]!] = copy
    current = copy
  }
  return current
}

const fieldLabel = (location: Location, path: ReadonlyArray<string>, subject: string, keyId: string) =>
  label("field", wireVersion, location.aggregateName, location.type, path, subject, keyId)

/**
 * Encrypts the personal fields of an encoded payload. Fields without a value are
 * left alone. `aggregateId` is the subject of fields that don't name one.
 * Returns the subjects whose keys were used.
 */
export const seal = (
  keys: Keys["Service"],
  fields: ReadonlyArray<Field>,
  location: Location & { readonly aggregateId: string | undefined },
  payload: Schema.Json
): Effect.Effect<WithSubjects, SubjectForgotten | PersonalDataError> =>
  Effect.gen(function*() {
    if (!isRecord(payload)) return { payload, subjects: [] }
    const copy = { ...payload }
    const subjects = new Set<string>()
    for (const field of fields) {
      const parent = writableParent(copy, field.path)
      const name = field.path[field.path.length - 1]!
      const value = parent?.[name]
      if (parent === undefined || value === undefined || value === null) continue
      const subject = field.subject === undefined ? location.aggregateId : parent[field.subject]
      if (!Predicate.isString(subject) || subject.length === 0) {
        return yield* Effect.die(
          new Error(
            `${location.aggregateName}.${location.type}: ${describe(field.path)} has no subject: ` +
              (field.subject === undefined ? "the aggregate id is missing" : `"${field.subject}" is not a string`)
          )
        )
      }
      const { key, keyId } = yield* keys.encryptionKey(subject)
      const sealed = yield* encrypt(
        key,
        textEncoder.encode(JSON.stringify(value)),
        fieldLabel(location, field.path, subject, keyId),
        "seal",
        subject
      )
      parent[name] = { [wireKey]: { version: wireVersion, subject, keyId, data: Base64.encode(sealed) } }
      subjects.add(subject)
    }
    return { payload: copy, subjects: Array.from(subjects) }
  })

/** A payload, with the subjects whose personal data it holds. */
export interface WithSubjects {
  readonly payload: Schema.Json
  readonly subjects: ReadonlyArray<string>
}

/**
 * Decrypts the personal fields of a stored payload, putting the replacement in
 * place of the fields of forgotten subjects. A field stored in plaintext, from
 * before it was marked personal, is left as it is.
 */
export const open = (
  keys: Keys["Service"],
  fields: ReadonlyArray<Field>,
  location: Location,
  payload: Schema.Json
): Effect.Effect<WithSubjects, PersonalDataError> =>
  Effect.gen(function*() {
    if (!isRecord(payload)) return { payload, subjects: [] }
    const copy = { ...payload }
    const subjects = new Set<string>()
    for (const field of fields) {
      const parent = writableParent(copy, field.path)
      const name = field.path[field.path.length - 1]!
      const value = parent?.[name]
      if (parent === undefined || !Predicate.isObject(value) || !Object.hasOwn(value, wireKey)) continue
      const wire = decodeWire((value as JsonRecord)[wireKey])
      if (Option.isNone(wire) || wire.value.version !== wireVersion) {
        return yield* new PersonalDataError({
          operation: "open",
          subject: Option.match(wire, { onNone: () => "", onSome: ({ subject }) => subject }),
          message: `${location.aggregateName}.${location.type}: ${describe(field.path)} is not encrypted data this version can read`
        })
      }
      const { data, keyId, subject } = wire.value
      const key = yield* keys.decryptionKey(subject, keyId)
      if (Option.isNone(key)) {
        parent[name] = field.redacted
        continue
      }
      const plaintext = yield* decrypt(
        key.value,
        yield* decodeBase64(data, "open", subject),
        fieldLabel(location, field.path, subject, keyId),
        "open",
        subject
      )
      parent[name] = JSON.parse(textDecoder.decode(plaintext)) as Schema.Json
      subjects.add(subject)
    }
    return { payload: copy, subjects: Array.from(subjects) }
  })

/** Text encrypted once with the key of each subject, so destroying any one of the keys makes it unreadable. */
export const Sealed = Schema.Struct({
  subjects: Schema.Array(Schema.Struct({ subject: Schema.String, keyId: Schema.String })),
  data: Schema.String
})
export type Sealed = typeof Sealed.Type

/** Seals text such as a snapshot of a state that holds the personal data of `subjects`. */
export const sealText = Effect.fnUntraced(function*(
  keys: Keys["Service"],
  subjects: ReadonlyArray<string>,
  text: string,
  purpose: string
) {
  let data = textEncoder.encode(text)
  const used: Array<{ readonly subject: string; readonly keyId: string }> = []
  for (const subject of [...subjects].sort()) {
    const { key, keyId } = yield* keys.encryptionKey(subject)
    data = yield* encrypt(key, data, label("text", purpose, subject, keyId), "sealText", subject)
    used.push({ subject, keyId })
  }
  return { subjects: used, data: Base64.encode(data) } satisfies Sealed
})

/** Opens text from {@link sealText}, `None` when one of its subjects was forgotten. */
export const openText = Effect.fnUntraced(function*(keys: Keys["Service"], sealed: Sealed, purpose: string) {
  let data = yield* decodeBase64(sealed.data, "openText", "")
  for (const { keyId, subject } of [...sealed.subjects].reverse()) {
    const key = yield* keys.decryptionKey(subject, keyId)
    if (Option.isNone(key)) return Option.none<string>()
    data = yield* decrypt(key.value, data, label("text", purpose, subject, keyId), "openText", subject)
  }
  return Option.some(textDecoder.decode(data))
})
