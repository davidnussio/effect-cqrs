/**
 * Sliding-window limits per key, in memory: enough for one server, and enough to
 * keep one enthusiastic participant from flooding a demo everyone is watching.
 */
import { Clock, Context, Duration, Effect, Layer, Ref } from "effect"
import { TooManyRequests } from "../api.ts"

export class RateLimiter extends Context.Service<RateLimiter, {
  /** Counts one use of `key`, failing when it was used `limit` times in the last `window`. */
  take(key: string, limit: number, window: Duration.Input): Effect.Effect<void, TooManyRequests>
}>()("workshop/RateLimiter") {}

export const make = Effect.gen(function*() {
  const uses = yield* Ref.make<ReadonlyMap<string, ReadonlyArray<number>>>(new Map())

  const take = Effect.fnUntraced(function*(key: string, limit: number, window: Duration.Input) {
    const now = yield* Clock.currentTimeMillis
    const millis = Duration.toMillis(Duration.fromInputUnsafe(window))
    const recent = ((yield* Ref.get(uses)).get(key) ?? []).filter((at) => at > now - millis)
    if (recent.length >= limit) {
      return yield* new TooManyRequests({ retryAfterSeconds: Math.ceil((recent[0]! + millis - now) / 1000) })
    }
    // Keys that went quiet are dropped as they are touched again, so the map stays small.
    yield* Ref.update(uses, (map) => new Map(map).set(key, [...recent, now]))
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
