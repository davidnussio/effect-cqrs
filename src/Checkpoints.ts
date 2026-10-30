/**
 * Remembers how far each view model has read.
 *
 * A checkpoint has to live as long as the view model it tracks: pair an
 * in-memory view model with {@link layerMemory} so it rebuilds from the event
 * store on restart, and a durable one with a durable checkpoint store.
 */
import { Context, Effect, Layer } from "effect"
import type { EventStoreError } from "./EventStore.ts"

export class Checkpoints extends Context.Service<Checkpoints, {
  /** The last processed position, `0` when the view model never ran. */
  get(viewModel: string): Effect.Effect<number, EventStoreError>
  set(viewModel: string, position: number): Effect.Effect<void, EventStoreError>
}>()("effect-cqrs/Checkpoints") {}

export const layerMemory: Layer.Layer<Checkpoints> = Layer.sync(Checkpoints, () => {
  const positions = new Map<string, number>()
  return Checkpoints.of({
    get: (viewModel) => Effect.sync(() => positions.get(viewModel) ?? 0),
    set: (viewModel, position) => Effect.sync(() => void positions.set(viewModel, position))
  })
})
