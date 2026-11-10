/**
 * Metadata: what a command carries beside its payload that the domain doesn't
 * decide on, such as the version of the software that ran it, the user who sent
 * it or the address it came from. It is a JSON object recorded on every event
 * the command writes, and projections, view models and `history` can read it.
 *
 * Like `Causation`, it travels in the fiber's context rather than in payloads.
 * A command records, key by key, from the lowest precedence:
 *
 * 1. the metadata where its aggregate service was built, such as the software
 *    version provided once for the whole application with {@link layer};
 * 2. the metadata where it runs, added with {@link withMetadata};
 * 3. the `metadata` of a `dispatch` envelope.
 *
 * A view model or saga reacting to an event doesn't inherit that event's
 * metadata: its commands record their own. The `causationId` leads back to the
 * event, and from there to its metadata.
 *
 * @module
 */
import { Context, Effect, Function, Layer, Schema } from "effect"

/** A JSON object. */
export type Metadata = Schema.JsonObject

export const Metadata: Schema.Codec<Metadata> = Schema.JsonObject

/** The metadata commands run in this fiber record, `{}` outside any. */
export const CurrentMetadata = Context.Reference<Metadata>("effect-cqrs/Metadata", {
  defaultValue: () => ({})
})

/** Runs an effect whose commands record this metadata on top of the current one. */
export const withMetadata: {
  (metadata: Metadata): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  <A, E, R>(effect: Effect.Effect<A, E, R>, metadata: Metadata): Effect.Effect<A, E, R>
} = Function.dual(
  2,
  <A, E, R>(effect: Effect.Effect<A, E, R>, metadata: Metadata) =>
    CurrentMetadata.use((current) => Effect.provideService(effect, CurrentMetadata, { ...current, ...metadata }))
)

/**
 * Provides metadata to everything built on top of this layer, for values that
 * hold for the whole process such as the software version:
 *
 * ```ts
 * AppLayer.pipe(Layer.provide(Metadata.layer({ appVersion: "1.4.0" })))
 * ```
 *
 * Aggregate services remember it, so it is recorded even by commands run where
 * the layer isn't visible.
 */
export const layer = (metadata: Metadata): Layer.Layer<never> => Layer.succeed(CurrentMetadata, metadata)
