/**
 * The older shapes of one event, so that events stored long ago still reach
 * projections and view models in today's shape. The log is never rewritten:
 * old payloads are upcast as they are read.
 *
 * ```ts
 * const AccountOpenedV1 = Schema.Struct({ owner: Schema.String })
 *
 * const Account = Aggregate.make<State>()({
 *   name: "account",
 *   events: AccountEvent,
 *   initial,
 *   projection,
 *   versions: {
 *     AccountOpened: EventVersions.from(AccountOpenedV1).next(
 *       AccountEvent.cases.AccountOpened,
 *       ({ owner }) => ({ owner, currency: "CHF" })
 *     )
 *   }
 * })
 * ```
 *
 * The first schema is version 1, the shape events had before the history was
 * declared, and every `next` adds a version. The last one must be the event's
 * current schema, which is what new events are written with.
 */
import { Schema, SchemaGetter } from "effect"

/** A schema that decodes and encodes without services, as stored payloads need. */
export type VersionSchema = Schema.Top & {
  readonly DecodingServices: never
  readonly EncodingServices: never
}

/** What an upcast returns: the next version, without the `_tag` the event's name already gives. */
export type Untagged<T> = T extends { readonly _tag: string } ? Omit<T, "_tag"> : T

type Last<Schemas extends ReadonlyArray<VersionSchema>> = Schemas extends
  readonly [...any, infer L extends VersionSchema] ? L
  : VersionSchema

export interface EventVersions<Schemas extends ReadonlyArray<VersionSchema>, Latest> {
  /** Oldest first: `schemas[0]` is version 1. */
  readonly schemas: Schemas
  /** `upcasts[i]` turns version `i + 1` into version `i + 2`. */
  readonly upcasts: ReadonlyArray<(previous: any) => unknown>
  /** The type of the last version, which an aggregate checks against its event. */
  readonly "~latest"?: Latest
  /** Adds a version, with the function that turns the previous one into it. */
  next<Next extends VersionSchema>(
    schema: Next,
    upcast: (previous: Latest) => Untagged<Next["Type"]>
  ): EventVersions<readonly [...Schemas, Next], Next["Type"]>
}

/** Any history whose last version is `Latest`. */
export type Any<Latest = any> = EventVersions<ReadonlyArray<VersionSchema>, Latest>

/** The schema of one version, or `never` when the history has no such version. */
export type SchemaAt<H extends Any, V extends number> = H extends EventVersions<infer Schemas, any>
  ? [never, ...Schemas][V] extends infer S extends VersionSchema ? S : never
  : never

const build = <Schemas extends ReadonlyArray<VersionSchema>>(
  schemas: Schemas,
  upcasts: ReadonlyArray<(previous: any) => unknown>
): EventVersions<Schemas, Last<Schemas>["Type"]> => ({
  schemas,
  upcasts,
  next: (schema, upcast) => build([...schemas, schema] as const, [...upcasts, upcast]) as any
})

/** Starts a history at version 1. */
export const from = <V1 extends VersionSchema>(schema: V1): EventVersions<readonly [V1], V1["Type"]> =>
  build([schema] as const, [])

/** The version new events of this kind are written with. */
export const current = (versions: Any | undefined): number => versions?.schemas.length ?? 1

/**
 * Builds one decoder per old version, each going all the way to the current one.
 * The chains are composed once with `Schema.decodeTo`, so reading an old event
 * costs one decode however many versions it is behind.
 *
 * `decoders[v - 1]` decodes a JSON payload stored at version `v`. The current
 * version has none, as the aggregate's own schema reads it.
 */
export const decoders = (tag: string, versions: Any): ReadonlyArray<Schema.Decoder<unknown>> =>
  versions.schemas.slice(0, -1).map((schema, index) =>
    versions.schemas.slice(index + 1).reduce<Schema.Top>(
      (chain, next, step) =>
        chain.pipe(Schema.decodeTo(Schema.toType(next), {
          // Adding the tag lets the last step decode into the tagged event, and a
          // schema in between without one drops it again.
          decode: SchemaGetter.transform((previous) => ({
            ...(versions.upcasts[index + step]!(previous) as object),
            _tag: tag
          })),
          // A payload is only ever written in its current version.
          encode: SchemaGetter.forbidden(() => `${tag} can only be written in its current version`)
        })),
      Schema.toCodecJson(schema)
    ) as Schema.Decoder<unknown>
  )
