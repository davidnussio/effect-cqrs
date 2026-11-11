/**
 * Sliding-window limits per key, in the `SharedState`, so that they hold however
 * many replicas share the load: enough to keep one enthusiastic participant from
 * flooding a demo everyone is watching.
 */
import { Clock, Context, Duration, Effect, Layer, Option, Schema } from "effect"
import { TooManyRequests } from "../api.ts"
import { shared, SharedState } from "../domain/shared.ts"

export class RateLimiter extends Context.Service<RateLimiter, {
  /** Counts one use of `key`, failing when it was used `limit` times in the last `window`. */
  take(key: string, limit: number, window: Duration.Input): Effect.Effect<void, TooManyRequests>
}>()("workshop/RateLimiter") {}

const Uses = Schema.Array(Schema.Number)

export const make = Effect.gen(function*() {
  const state = yield* SharedState

  const take = Effect.fnUntraced(function*(key: string, limit: number, window: Duration.Input) {
    const now = yield* Clock.currentTimeMillis
    const millis = Duration.toMillis(Duration.fromInputUnsafe(window))
    const uses = yield* shared(`limit:${key}`, Uses, []).pipe(Effect.provideService(SharedState, state))
    // Uses that left the window are dropped as the key is touched again, so each value stays small.
    const retryAfter = yield* uses.modify((all) => {
      const recent = all.filter((at) => at > now - millis)
      return recent.length >= limit
        ? [Option.some(Math.ceil((recent[0]! + millis - now) / 1000)), recent] as const
        : [Option.none<number>(), [...recent, now]] as const
    })
    if (Option.isSome(retryAfter)) return yield* new TooManyRequests({ retryAfterSeconds: retryAfter.value })
  })

  return RateLimiter.of({ take })
})

export const layer = Layer.effect(RateLimiter, make)

/** The limits of the API. */
export const limits = {
  /** A whole classroom may share one address behind the venue's router. */
  joinsPerAddressPerMinute: 120,
  depositsPerMinute: 10
} as const
