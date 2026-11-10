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
 *
 * The history can include events stored in older versions of their schema, made
 * with `Aggregate.historical`, which then go through the same upcasting as events
 * read from a real store.
 *
 * @module
 */
import { Arbitrary, Effect, Effectable, Option, Schema } from "effect"
import * as Aggregate from "./Aggregate.ts"
import * as AggregateService from "./AggregateService.ts"
import { EventStore, makeMemory } from "./EventStore.ts"
import type * as EventVersions from "./EventVersions.ts"
import * as PersonalData from "./PersonalData.ts"

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

/** An event in today's shape, or one stored in an older version of its schema. */
export type GivenEvent<A extends Aggregate.Any> = Aggregate.Event<A> | Aggregate.Historical

/** One generated step of {@link Fixture.checkInvariants}. */
export type Step<A extends Aggregate.Any> = {
  readonly [K in Aggregate.CommandName<A>]: { readonly command: K; readonly payload: Aggregate.Payload<A, K> }
}[Aggregate.CommandName<A>]

export interface InvariantOptions<A extends Aggregate.Any> {
  /** Must hold for the given state and after every command that succeeds. */
  readonly invariant: (state: Aggregate.State<A>) => boolean
  /**
   * How to generate each command's payload. By default it comes from the payload
   * schema. `false` leaves a command out.
   */
  readonly commands?:
    | {
      readonly [K in Aggregate.CommandName<A>]?: Arbitrary.Arbitrary<Aggregate.Payload<A, K>> | false
    }
    | undefined
  /** The longest sequence of commands to try. Defaults to 20. */
  readonly maxCommands?: number | undefined
  /** Passed on to `Arbitrary.checkEffect`, for the number of runs or a seed. */
  readonly check?: Arbitrary.CheckOptions | undefined
}

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
  given(events: ReadonlyArray<GivenEvent<A>>): Fixture<A>
  /** Like `given`, starting from an empty history by default and optionally switching aggregate id. */
  givenEvents(events?: ReadonlyArray<GivenEvent<A>>, aggregateId?: string): Fixture<A>
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
  /**
   * Runs random sequences of commands from the given history and checks that the
   * invariant holds after each one that succeeds. Commands that fail are part of
   * the game and are skipped. A violation dies with {@link ExpectationFailed},
   * holding the shortest sequence found that breaks it.
   */
  checkInvariants(options: InvariantOptions<A>): Effect.Effect<void, never, Aggregate.Context<A>>
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
      Effect.tap(
        this.run,
        ({ events }) =>
          events.length === expected.length && expected.every((want, index) => matches(want, events[index]))
            ? Effect.void
            : Effect.die(
              new ExpectationFailed({
                message: "The command did not emit the expected events",
                expected,
                actual: events
              })
            )
      )
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
    readonly events?: ReadonlyArray<GivenEvent<A>>
  }
): Fixture<A> => {
  const aggregateId = options?.aggregateId ?? "fixture-id"
  const history = options?.events ?? []
  const hasPersonalData = PersonalData.fieldsByTag(aggregate.events, aggregate.name).size > 0 ||
    Object.values(aggregate.versions as Record<string, EventVersions.Any | undefined>).some((versions) =>
      versions?.schemas.some((schema) => PersonalData.fieldsOf(schema, aggregate.name).length > 0)
    )

  const ownKeys = Effect.suspend(() => PersonalData.make(PersonalData.makeMemoryBackend())).pipe(
    Effect.provide(PersonalData.layerMasterKeyEphemeral)
  )

  const seeded = Effect.gen(function*() {
    const store = yield* makeMemory
    // Personal fields are encrypted with the keys the test provides, or with keys of its own.
    const provided = yield* Effect.serviceOption(PersonalData.Keys)
    const keys = !hasPersonalData ? undefined : Option.isSome(provided) ? provided.value : yield* ownKeys
    const encodeEvent = AggregateService.makeEventEncoder(aggregate, keys)
    const encodeGiven = (event: GivenEvent<A>) =>
      event instanceof Aggregate.Historical ? Effect.succeed(event) : encodeEvent(event, aggregateId)
    if (history.length > 0) {
      yield* store.append({
        aggregateName: aggregate.name,
        aggregateId,
        expectedVersion: 0,
        events: yield* Effect.forEach(history, encodeGiven)
      })
    }
    const built = AggregateService.make(aggregate).pipe(Effect.provideService(EventStore, store))
    const service = yield* (keys === undefined ? built : Effect.provideService(built, PersonalData.Keys, keys))
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

  const checkInvariants = (options: InvariantOptions<A>) =>
    Effect.gen(function*() {
      const commands = Object.entries(aggregate.commands).flatMap(([name, command]) => {
        const given = options.commands?.[name as Aggregate.CommandName<A>] as
          | Arbitrary.Arbitrary<unknown>
          | false
          | undefined
        if (given === false) return []
        const payload = given ?? Arbitrary.schema(Schema.toType(command.payload))
        return [[name, Arbitrary.map(payload, (payload: unknown) => ({ command: name, payload }))] as const]
      })
      if (commands.length === 0) {
        return yield* Effect.die(new Error("checkInvariants needs at least one command to run"))
      }
      const byName = new Map(commands)
      const steps = Arbitrary.array(
        Arbitrary.flatMap(
          Arbitrary.schema(Schema.Literals(commands.map(([name]) => name))),
          (name) => byName.get(name)!
        ),
        { maxLength: options.maxCommands ?? 20 }
      ) as Arbitrary.Arbitrary<ReadonlyArray<Step<A>>>

      // Each run starts from a fresh store with the given history.
      const holds = (sequence: ReadonlyArray<Step<A>>) =>
        Effect.gen(function*() {
          const service = yield* seeded
          if (!options.invariant(yield* service.readModel(aggregateId))) return false
          for (const step of sequence) {
            const succeeded = yield* service.commands[step.command](aggregateId, step.payload).pipe(
              Effect.as(true),
              Effect.catch(() => Effect.succeed(false))
            )
            if (succeeded && !options.invariant(yield* service.readModel(aggregateId))) return false
          }
          return true
        }).pipe(Effect.orDie)

      const result = yield* Arbitrary.checkEffect(steps, holds, options.check)
      if (result._tag !== "Passed") {
        return yield* Effect.die(
          new ExpectationFailed({
            message: Arbitrary.formatCheckFailure(result) ?? "The invariant does not hold",
            expected: "the invariant to hold after every command",
            actual: result._tag === "Falsified" ? result.shrunkInput : result
          })
        )
      }
    })

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
      ),
    checkInvariants
  }

  return fixture
}
