/**
 * Describes what an aggregate writes and accepts, for consumers outside the
 * process, and guards stored events against schema changes that would break them.
 *
 * - {@link events} and {@link commands} give the JSON Schema of every event, in
 *   every version declared with `EventVersions`, and of every command payload.
 * - {@link document} lists them for several aggregates in an AsyncAPI 3 document.
 * - {@link captureSamples} takes a few stored payloads of each event and version,
 *   {@link saveSamples} keeps them in a JSON file, and {@link checkCompatibility}
 *   fails when one of them no longer decodes with the current aggregate.
 *
 * JSON Schema describes the JSON form of a schema, as `Schema.toCodecJson` encodes
 * it. Personal fields, see `PersonalData`, are described in plaintext: in the store
 * they are encrypted.
 */
import {
  DateTime,
  Effect,
  FileSystem,
  JsonSchema,
  Predicate,
  Result,
  Schema,
  type SchemaAST,
  Stream
} from "effect"
import type * as Aggregate from "./Aggregate.ts"
import { makeEventDecoder } from "./AggregateService.ts"
import { EventStore, type EventStoreError, type StoredEvent } from "./EventStore.ts"
import type * as EventVersions from "./EventVersions.ts"
import * as PersonalData from "./PersonalData.ts"

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

export interface EventVersion {
  readonly schemaVersion: number
  /** A JSON Schema draft 2020-12 of the payload in this version. */
  readonly schema: JsonSchema.JsonSchema
}

export interface EventEntry {
  readonly aggregate: string
  readonly type: string
  /** The version new events are written in, the last of `versions`. */
  readonly schemaVersion: number
  /** Oldest first. Events written before versions were declared are version 1. */
  readonly versions: ReadonlyArray<EventVersion>
  /** Paths of the personal fields in the current version, encrypted in the store. */
  readonly personalFields: ReadonlyArray<string>
}

export interface CommandEntry {
  readonly aggregate: string
  readonly name: string
  /** A JSON Schema draft 2020-12 of the payload. */
  readonly payload: JsonSchema.JsonSchema
}

type JsonRecord = { [key: string]: Schema.Json }

const isRecord = (value: unknown): value is JsonRecord => Predicate.isObject(value) && !Array.isArray(value)

const standalone = (schema: Schema.Top): JsonSchema.JsonSchema => {
  const { definitions, schema: root } = Schema.toJsonSchemaDocument(schema)
  return {
    $schema: JsonSchema.META_SCHEMA_URI_DRAFT_2020_12,
    ...root,
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions })
  }
}

const tagOf = (ast: SchemaAST.Objects): string | undefined => {
  const tag = ast.propertySignatures.find(({ name }) => name === "_tag")?.type
  return tag?._tag === "Literal" && Predicate.isString(tag.literal) ? tag.literal : undefined
}

/** The current schema of each event, by tag. */
const eventSchemas = (aggregate: Aggregate.Any): ReadonlyArray<readonly [string, Schema.Top]> => {
  const found: Array<readonly [string, Schema.Top]> = []
  const visit = (ast: SchemaAST.AST): void => {
    if (ast._tag === "Union") return ast.types.forEach(visit)
    const tag = ast._tag === "Objects" ? tagOf(ast) : undefined
    if (tag !== undefined) found.push([tag, Schema.make(ast)])
  }
  visit(aggregate.events.ast)
  return found
}

const versionsOf = (aggregate: Aggregate.Any, tag: string): EventVersions.Any | undefined =>
  (aggregate.versions as Record<string, EventVersions.Any | undefined>)[tag]

/** The JSON Schema of every event of an aggregate, in every version it declares. */
export const events = (aggregate: Aggregate.Any): ReadonlyArray<EventEntry> =>
  eventSchemas(aggregate).map(([type, schema]) => {
    const old = versionsOf(aggregate, type)?.schemas.slice(0, -1) ?? []
    return {
      aggregate: aggregate.name,
      type,
      schemaVersion: old.length + 1,
      versions: [...old, schema].map((each, index) => ({ schemaVersion: index + 1, schema: standalone(each) })),
      personalFields: PersonalData.fieldsOf(schema, `${aggregate.name}, ${type}`).map(({ path }) => path.join("."))
    }
  })

/** The JSON Schema of the payload of every command of an aggregate. */
export const commands = (aggregate: Aggregate.Any): ReadonlyArray<CommandEntry> =>
  Object.entries(aggregate.commands).map(([name, command]) => ({
    aggregate: aggregate.name,
    name,
    payload: standalone(command.payload)
  }))

// ---------------------------------------------------------------------------
// AsyncAPI
// ---------------------------------------------------------------------------

export interface DocumentOptions {
  readonly title: string
  readonly version: string
  readonly description?: string | undefined
}

/** An AsyncAPI 3.0 document, with JSON Schema draft-07 payloads as AsyncAPI expects. */
export interface AsyncApiDocument {
  readonly asyncapi: "3.0.0"
  readonly info: { readonly title: string; readonly version: string; readonly description?: string }
  readonly channels: Record<string, unknown>
  readonly operations: Record<string, unknown>
  readonly components: {
    readonly messages: Record<string, unknown>
    readonly schemas: Record<string, JsonSchema.JsonSchema>
  }
}

const definitionsPrefix = "#/definitions/"

const toComponents = <A>(value: A): A => {
  if (Array.isArray(value)) return value.map(toComponents) as A
  if (!isRecord(value)) return value
  return Object.fromEntries(Object.entries(value).map(([key, each]) => [
    key,
    key === "$ref" && Predicate.isString(each) && each.startsWith(definitionsPrefix)
      ? `#/components/schemas/${each.slice(definitionsPrefix.length)}`
      : toComponents(each)
  ])) as A
}

/**
 * Lists the events and commands of some aggregates in an AsyncAPI 3.0 document.
 * Each aggregate has a channel of events it sends and a channel of commands it
 * receives. Older versions of an event are under the message's `x-versions`,
 * and its personal fields under `x-personal-fields`.
 */
export const document = (aggregates: ReadonlyArray<Aggregate.Any>, options: DocumentOptions): AsyncApiDocument => {
  const schemas: Record<string, JsonSchema.JsonSchema> = {}
  // Definitions go to the components, which payloads refer to from the root of the document.
  const draft07 = (schema: Schema.Top): JsonSchema.JsonSchema => {
    const document = JsonSchema.toDocumentDraft07(Schema.toJsonSchemaDocument(schema))
    for (const [key, definition] of Object.entries(document.definitions)) {
      schemas[key] = toComponents(definition)
    }
    return toComponents(document.schema)
  }

  const channels: Record<string, unknown> = {}
  const operations: Record<string, unknown> = {}
  const messages: Record<string, unknown> = {}

  for (const aggregate of aggregates) {
    const eventMessages: Record<string, unknown> = {}
    for (const [type, schema] of eventSchemas(aggregate)) {
      const old = versionsOf(aggregate, type)?.schemas.slice(0, -1) ?? []
      const key = `${aggregate.name}.${type}`
      const personal = PersonalData.fieldsOf(schema, `${aggregate.name}, ${type}`).map(({ path }) => path.join("."))
      messages[key] = {
        name: type,
        title: type,
        payload: draft07(schema),
        "x-schemaVersion": old.length + 1,
        ...(old.length === 0 ? {} : {
          "x-versions": Object.fromEntries(old.map((each, index) => [String(index + 1), draft07(each)]))
        }),
        ...(personal.length === 0 ? {} : { "x-personal-fields": personal })
      }
      eventMessages[type] = { $ref: `#/components/messages/${key}` }
    }
    const commandMessages: Record<string, unknown> = {}
    for (const [name, command] of Object.entries(aggregate.commands)) {
      const key = `${aggregate.name}.commands.${name}`
      messages[key] = { name, title: name, payload: draft07(command.payload) }
      commandMessages[name] = { $ref: `#/components/messages/${key}` }
    }

    const events = `${aggregate.name}.events`
    const commands = `${aggregate.name}.commands`
    channels[events] = {
      address: aggregate.name,
      title: `Events of ${aggregate.name}`,
      messages: eventMessages
    }
    channels[commands] = {
      address: `${aggregate.name}/commands`,
      title: `Commands of ${aggregate.name}`,
      messages: commandMessages
    }
    operations[events] = {
      action: "send",
      channel: { $ref: `#/channels/${events}` },
      messages: Object.keys(eventMessages).map((type) => ({ $ref: `#/channels/${events}/messages/${type}` }))
    }
    operations[commands] = {
      action: "receive",
      channel: { $ref: `#/channels/${commands}` },
      messages: Object.keys(commandMessages).map((name) => ({ $ref: `#/channels/${commands}/messages/${name}` }))
    }
  }

  return {
    asyncapi: "3.0.0",
    info: {
      title: options.title,
      version: options.version,
      ...(options.description === undefined ? {} : { description: options.description })
    },
    channels,
    operations,
    components: { messages, schemas }
  }
}

// ---------------------------------------------------------------------------
// Compatibility
// ---------------------------------------------------------------------------

/** A payload as the store keeps it, to check that the current aggregate still reads it. */
export const Sample = Schema.Struct({
  aggregateName: Schema.String,
  type: Schema.String,
  schemaVersion: Schema.Int,
  payload: Schema.Json
})
export type Sample = typeof Sample.Type

const SamplesJson = Schema.fromJsonString(Schema.Array(Sample))

export class IncompatibleSamples extends Schema.TaggedError<IncompatibleSamples>()("IncompatibleSamples", {
  aggregateName: Schema.String,
  failures: Schema.Array(Schema.Struct({
    type: Schema.String,
    schemaVersion: Schema.Int,
    message: Schema.String
  }))
}) {
  override get message() {
    return this.failures.map(({ message, schemaVersion, type }) =>
      `${this.aggregateName}.${type} version ${schemaVersion}: ${message}`
    ).join("\n")
  }
}

const sampleKey = (sample: Pick<Sample, "aggregateName" | "type" | "schemaVersion">) =>
  JSON.stringify([sample.aggregateName, sample.type, sample.schemaVersion])

/** The personal fields of an event as it was stored, or none for a type or version the aggregate doesn't know. */
const personalFieldsAt = (aggregate: Aggregate.Any, type: string, schemaVersion: number) => {
  const old = versionsOf(aggregate, type)?.schemas.slice(0, -1) ?? []
  const schema = schemaVersion === old.length + 1
    ? eventSchemas(aggregate).find(([tag]) => tag === type)?.[1]
    : old[schemaVersion - 1]
  return schema === undefined ? [] : PersonalData.fieldsOf(schema, `${aggregate.name}, ${type}`)
}

const setAt = (payload: Schema.Json, path: ReadonlyArray<string>, value: Schema.Json): Schema.Json => {
  if (!isRecord(payload) || path.length === 0) return payload
  const [head, ...rest] = path as [string, ...Array<string>]
  if (!Object.hasOwn(payload, head)) return payload
  const current = payload[head]!
  // Encrypted or not, a value is replaced, and a missing optional field stays missing.
  if (rest.length === 0) return current === null ? payload : { ...payload, [head]: value }
  return { ...payload, [head]: setAt(current, rest, value) }
}

const redact = (aggregate: Aggregate.Any, event: StoredEvent): Schema.Json =>
  personalFieldsAt(aggregate, event.type, event.schemaVersion).reduce(
    (payload, field) => setAt(payload, field.path, field.redacted),
    event.payload
  )

/**
 * Reads the store and keeps the last `perVersion` payloads, one by default, of
 * every event type and schema version of the given aggregates.
 *
 * Personal fields are replaced by the value they read as once their subject is
 * forgotten, so samples hold no personal data and can be committed, and checking
 * them needs no keys. The rest of the payload is kept as it was stored.
 */
export const captureSamples = (
  aggregates: ReadonlyArray<Aggregate.Any>,
  options?: { readonly perVersion?: number | undefined }
): Effect.Effect<ReadonlyArray<Sample>, EventStoreError, EventStore> =>
  Effect.gen(function*() {
    const store = yield* EventStore
    const perVersion = options?.perVersion ?? 1
    const byName = new Map(aggregates.map((aggregate) => [aggregate.name, aggregate]))
    const kept = new Map<string, Array<Sample>>()
    yield* store.read({ aggregateNames: Array.from(byName.keys()) }).pipe(
      Stream.runForEach((event) =>
        Effect.sync(() => {
          const sample: Sample = {
            aggregateName: event.aggregateName,
            type: event.type,
            schemaVersion: event.schemaVersion,
            payload: redact(byName.get(event.aggregateName)!, event)
          }
          const key = sampleKey(sample)
          const samples = kept.get(key) ?? []
          samples.push(sample)
          if (samples.length > perVersion) samples.shift()
          kept.set(key, samples)
        })
      )
    )
    return Array.from(kept.values()).flat()
  })

/** Reads samples written by {@link saveSamples}. */
export const readSamples = (path: string) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    return yield* Schema.decodeEffect(SamplesJson)(yield* fs.readFileString(path))
  })

/**
 * Adds samples to a JSON file, creating it if needed. Samples of an event type and
 * version already in the file are left as they are, so the file keeps the shapes
 * the store held over time, also after those events are gone from it.
 */
export const saveSamples = (path: string, samples: ReadonlyArray<Sample>) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const existing = (yield* fs.exists(path)) ? yield* readSamples(path) : []
    const known = new Set(existing.map(sampleKey))
    const merged = [...existing, ...samples.filter((sample) => !known.has(sampleKey(sample)))]
    yield* fs.writeFileString(path, `${JSON.stringify(merged, null, 2)}\n`)
    return merged
  })

/**
 * Decodes every sample of the aggregate with its current schemas and upcasts, as
 * a store read would, and fails with {@link IncompatibleSamples} listing the
 * ones that no longer decode. Returns how many samples were checked.
 *
 * Personal fields in samples are expected in plaintext, as {@link captureSamples}
 * leaves them, so no keys are needed.
 */
export const checkCompatibility = (
  aggregate: Aggregate.Any,
  samples: ReadonlyArray<Sample>
): Effect.Effect<number, IncompatibleSamples> =>
  Effect.gen(function*() {
    // A plaintext field is read as it is, so any keys do. These are thrown away.
    const keys = yield* PersonalData.make(PersonalData.makeMemoryBackend()).pipe(
      Effect.provide(PersonalData.layerMasterKeyEphemeral)
    )
    const decode = makeEventDecoder(aggregate, keys)
    const timestamp = DateTime.nowUnsafe()
    const own = samples.filter((sample) => sample.aggregateName === aggregate.name)
    const failures: Array<{ type: string; schemaVersion: number; message: string }> = []
    for (const [index, sample] of own.entries()) {
      const result = yield* Effect.result(decode({
        ...sample,
        position: index + 1,
        aggregateId: "sample",
        version: index + 1,
        timestamp,
        correlationId: null,
        causationId: null,
        metadata: {}
      }))
      if (Result.isFailure(result)) {
        failures.push({ type: sample.type, schemaVersion: sample.schemaVersion, message: result.failure.message })
      }
    }
    if (failures.length > 0) {
      return yield* new IncompatibleSamples({ aggregateName: aggregate.name, failures })
    }
    return own.length
  })
