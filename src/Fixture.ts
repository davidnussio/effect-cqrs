/**
 * Tests an aggregate in isolation: given these events, when this command runs,
 * what happens? The moleculer-cqrs `CQRSFixture`, as a chain that runs when it
 * is yielded:
 *
 * ```ts
 * yield* Fixture.make(Todo)
 *   .givenEvents([created])
 *   .when("deleteTodo", { message: "Done" })
 *   .expectEvent("TodoDeleted")
 *   .inspectState((state) => assert.strictEqual(state.status, "deleted"))
 * ```
 *
 * Commands run through the real {@link AggregateService} on an in-memory store,
 * so payload validation and event encoding are exercised too.
 */
import { Effect, Effectable, Schema } from "effect"
import type * as Aggregate from "./Aggregate.ts"
import * as AggregateService from "./AggregateService.ts"
import { EventStore, makeMemory } from "./EventStore.ts"

/** Raised as a defect, so a command's error channel only holds what the command can do. */
export class ExpectationFailed extends Schema.TaggedError<ExpectationFailed>()("ExpectationFailed", {
  message: Schema.String,
  expected: Schema.Unknown,
  actual: Schema.Unknown
}) {}

export interface Outcome<A extends Aggregate.Any> {
  readonly events: ReadonlyArray<Aggregate.Event<A>>
  readonly state: Aggregate.State<A>
  readonly version: number
  /** Continues from here, with this outcome's events as the new history. */
  readonly fixture: Fixture<A>
}

/** An event to compare with, or only its tag when the rest doesn't matter. */
export type ExpectedEvent<A extends Aggregate.Any> = Aggregate.Event<A> | Aggregate.Event<A>["_tag"]

/**
 * The steps so far. Nothing runs until it is yielded or run as an Effect, which
 * produces the {@link Outcome} of the last command.
 */
export interface Scenario<A extends Aggregate.Any, E, R> extends Effect.Effect<Outcome<A>, E, R> {
  /** Runs another command on top of the previous one. */
  when<K extends Aggregate.CommandName<A>>(
    command: K,
    payload: Aggregate.Payload<A, K>
  ): Scenario<A, E | Aggregate.CommandError<A, K> | AggregateService.CommandFailure, R | Aggregate.Context<A>>
  /** Runs a command that is expected to fail and returns the error. Dies if it succeeds. */
  whenFails<K extends Aggregate.CommandName<A>>(
    command: K,
    payload: Aggregate.Payload<A, K>
  ): Effect.Effect<Aggregate.CommandError<A, K> | AggregateService.CommandFailure, E, R | Aggregate.Context<A>>
  /** Expects the last command to have emitted exactly this event. */
  expectEvent(event: ExpectedEvent<A>): Scenario<A, E, R>
  /** Expects the last command to have emitted exactly these events, in order. */
  expectEvents(events: ReadonlyArray<ExpectedEvent<A>>): Scenario<A, E, R>
  /** Passes the current state to a callback, which throws to fail the test. */
  inspectState(check: (state: Aggregate.State<A>) => void): Scenario<A, E, R>
}

export interface Fixture<A extends Aggregate.Any> {
  /** Replaces the history the next command runs against. */
  given(events: ReadonlyArray<Aggregate.Event<A>>): Fixture<A>
  /** Like `given`, starting from an empty history by default and optionally switching aggregate id. */
  givenEvents(events?: ReadonlyArray<Aggregate.Event<A>>, aggregateId?: string): Fixture<A>
  /** The state after the given events, without running a command. */
  readonly state: Effect.Effect<Aggregate.State<A>, AggregateService.ReadError, Aggregate.Context<A>>
  /** Passes the state after the given events to a callback, which throws to fail the test. */
  inspectState(
    check: (state: Aggregate.State<A>) => void
  ): Scenario<A, AggregateService.ReadError, Aggregate.Context<A>>
  when<K extends Aggregate.CommandName<A>>(
    command: K,
    payload: Aggregate.Payload<A, K>
  ): Scenario<A, Aggregate.CommandError<A, K> | AggregateService.CommandFailure, Aggregate.Context<A>>
  /** Runs a command that is expected to fail and returns the error. Dies if it succeeds. */
  whenFails<K extends Aggregate.CommandName<A>>(
    command: K,
    payload: Aggregate.Payload<A, K>
  ): Effect.Effect<Aggregate.CommandError<A, K> | AggregateService.CommandFailure, never, Aggregate.Context<A>>
}

class ScenarioImpl<A extends Aggregate.Any, E, R> extends Effectable.Class<Outcome<A>, E, R>
  implements Scenario<A, E, R>
{
  readonly aggregate: A
  readonly run: Effect.Effect<Outcome<A>, E, R>

  constructor(aggregate: A, run: Effect.Effect<Outcome<A>, E, R>) {
    super()
    this.aggregate = aggregate
    this.run = run
  }

  asEffect() {
    return this.run
  }

  when<K extends Aggregate.CommandName<A>>(command: K, payload: Aggregate.Payload<A, K>) {
    return new ScenarioImpl(
      this.aggregate,
      Effect.flatMap(this.run, (outcome) => outcome.fixture.when(command, payload))
    )
  }

  whenFails<K extends Aggregate.CommandName<A>>(command: K, payload: Aggregate.Payload<A, K>) {
    return Effect.flatMap(this.run, (outcome) => outcome.fixture.whenFails(command, payload))
  }

  expectEvent(event: ExpectedEvent<A>) {
    return this.expectEvents([event])
  }

  expectEvents(expected: ReadonlyArray<ExpectedEvent<A>>) {
    const isSameEvent = Schema.toEquivalence<unknown>(this.aggregate.events)
    const matches = (want: ExpectedEvent<A>, event: Aggregate.Event<A>) =>
      typeof want === "string" ? event._tag === want : isSameEvent(want, event)

    return new ScenarioImpl(
      this.aggregate,
      Effect.tap(this.run, ({ events }) =>
        events.length === expected.length && expected.every((want, index) => matches(want, events[index]))
          ? Effect.void
          : Effect.die(
            new ExpectationFailed({
              message: "The command did not emit the expected events",
              expected,
              actual: events
            })
          ))
    )
  }

  inspectState(check: (state: Aggregate.State<A>) => void) {
    return new ScenarioImpl(
      this.aggregate,
      Effect.tap(this.run, ({ state }) => Effect.sync(() => check(state)))
    )
  }
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

  const state = Effect.flatMap(seeded, (service) => service.readModel(aggregateId))

  const when = <K extends Aggregate.CommandName<A>>(command: K, payload: Aggregate.Payload<A, K>) =>
    new ScenarioImpl(
      aggregate,
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
    )

  const fixture: Fixture<A> = {
    given: (events) => make(aggregate, { aggregateId, events }),
    givenEvents: (events = [], id = aggregateId) => make(aggregate, { aggregateId: id, events }),
    state,
    inspectState: (check) =>
      new ScenarioImpl(
        aggregate,
        Effect.map(state, (current): Outcome<A> => ({
          events: [],
          state: current,
          version: history.length,
          fixture
        }))
      ).inspectState(check),
    when,
    whenFails: (command, payload) =>
      when(command, payload).pipe(
        Effect.flip,
        Effect.catch(() => Effect.die(new Error(`Expected command "${command}" to fail, but it succeeded`)))
      )
  }

  return fixture
}
