/**
 * Remembers how far each view model has read.
 *
 * A checkpoint has to live as long as the view model it tracks: pair an
 * in-memory view model with {@link layerMemory} so it rebuilds from the event
 * store on restart, and a durable one with a durable checkpoint store.
 *
 * @module
 */
import { Context, Effect, Layer } from "effect"
import type { EventStoreError } from "./EventStore.ts"

export class Checkpoints extends Context.Service<Checkpoints, {
  /** The last processed position, `0` when the view model never ran. */
  get(viewModel: string): Effect.Effect<number, EventStoreError>
  set(viewModel: string, position: number): Effect.Effect<void, EventStoreError>
  /**
   * Moves a checkpoint to `to` only if it is still at `from`, and returns whether it
   * moved. View models use it to notice that another process moved their checkpoint,
   * which can happen for a moment while a lease changes hands: in a `transaction`
   * the batch then rolls back instead of being committed twice. Stores without it
   * fall back to `set`, which can't notice.
   */
  readonly advance?:
    | ((viewModel: string, from: number, to: number) => Effect.Effect<boolean, EventStoreError>)
    | undefined
}>()("effect-cqrs/Checkpoints") {}

export const layerMemory: Layer.Layer<Checkpoints> = Layer.sync(Checkpoints, () => {
  const positions = new Map<string, number>()
  return Checkpoints.of({
    get: (viewModel) => Effect.sync(() => positions.get(viewModel) ?? 0),
    set: (viewModel, position) => Effect.sync(() => void positions.set(viewModel, position)),
    advance: (viewModel, from, to) =>
      Effect.sync(() => {
        if ((positions.get(viewModel) ?? 0) !== from) return false
        positions.set(viewModel, to)
        return true
      })
  })
})
