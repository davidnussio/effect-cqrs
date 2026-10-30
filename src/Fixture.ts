/**
 * Tests an aggregate in isolation: given these events, when this command runs,
 * what happens? The moleculer-cqrs `CQRSFixture`, returning values to assert on
 * instead of calling a test framework itself.
 *
 * Commands run through the real {@link AggregateService} on an in-memory store,
 * so payload validation and event encoding are exercised too.
 */
import { Effect } from "effect"
import type * as Aggregate from "./Aggregate.ts"
import * as AggregateService from "./AggregateService.ts"
import { EventStore, makeMemory } from "./EventStore.ts"

export interface Outcome<A extends Aggregate.Any> {
  readonly events: ReadonlyArray<Aggregate.Event<A>>
  readonly state: Aggregate.State<A>
  readonly version: number
  /** Continues from here, with this outcome's events as the new history. */
  readonly fixture: Fixture<A>
}

export interface Fixture<A extends Aggregate.Any> {
  /** Replaces the history the next command runs against. */
  given(events: ReadonlyArray<Aggregate.Event<A>>): Fixture<A>
  /** The state after the given events, without running a command. */
  readonly state: Effect.Effect<Aggregate.State<A>, AggregateService.ReadError, Aggregate.Context<A>>
  when<K extends Aggregate.CommandName<A>>(
    command: K,
    payload: Aggregate.Payload<A, K>
  ): Effect.Effect<Outcome<A>, Aggregate.CommandError<A, K> | AggregateService.CommandFailure, Aggregate.Context<A>>
  /** Runs a command that is expected to fail and returns the error. Dies if it succeeds. */
  whenFails<K extends Aggregate.CommandName<A>>(
    command: K,
    payload: Aggregate.Payload<A, K>
  ): Effect.Effect<Aggregate.CommandError<A, K> | AggregateService.CommandFailure, never, Aggregate.Context<A>>
}

export const make = <A extends Aggregate.Any>(
  aggregate: A,
  options?: {
    readonly aggregateId?: string
    readonly events?: ReadonlyArray<Aggregate.Event<A>>
  }
): Fixture<A> => {
  const aggregateId = options?.aggregateId ?? "fixture-id"
  const history = options?.events ?? []
  const encodeEvent = AggregateService.makeEventEncoder(aggregate)

  const seeded = Effect.gen(function*() {
    const store = yield* makeMemory
    if (history.length > 0) {
      yield* store.append({
        aggregateName: aggregate.name,
        aggregateId,
        expectedVersion: 0,
        events: yield* Effect.forEach(history, encodeEvent)
      })
    }
    const service = yield* AggregateService.make(aggregate).pipe(Effect.provideService(EventStore, store))
    return service
  }).pipe(
    // A store with nothing but memory behind it can't fail, and a history that
    // doesn't fit a fresh stream is a mistake in the test.
    Effect.orDie
  )

  const when = <K extends Aggregate.CommandName<A>>(command: K, payload: Aggregate.Payload<A, K>) =>
    Effect.gen(function*() {
      const service = yield* seeded
      const result = yield* service.commands[command](aggregateId, payload)
      return {
        events: result.events,
        state: yield* service.readModel(aggregateId),
        version: result.version,
        fixture: make(aggregate, { aggregateId, events: [...history, ...result.events] })
      } satisfies Outcome<A>
    })

  return {
    given: (events) => make(aggregate, { aggregateId, events }),
    state: Effect.flatMap(seeded, (service) => service.readModel(aggregateId)),
    when,
    whenFails: (command, payload) =>
      when(command, payload).pipe(
        Effect.flip,
        Effect.catch(() => Effect.die(new Error(`Expected command "${command}" to fail, but it succeeded`)))
      )
  }
}
