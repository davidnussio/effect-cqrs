/**
 * The ids the framework makes up: the correlation of a command run outside any
 * causation, and of a saga started outside one. They are random UUIDs, and a test
 * that compares them provides a generator of its own:
 *
 * ```ts
 * program.pipe(Effect.provideService(Ids.IdGenerator, Ids.sequential("id")))
 * ```
 */
import { Context, Effect } from "effect"

/** Makes a new id each time it is called. Defaults to `crypto.randomUUID()`. */
export const IdGenerator = Context.Reference<() => string>("effect-cqrs/Ids", {
  defaultValue: () => () => crypto.randomUUID()
})

/** A new id from the current generator. */
export const next: Effect.Effect<string> = IdGenerator.use((generate) => Effect.sync(generate))

/** A generator of `id-1`, `id-2` and so on, for tests. Each call starts its own count. */
export const sequential = (prefix = "id"): () => string => {
  let count = 0
  return () => `${prefix}-${++count}`
}
