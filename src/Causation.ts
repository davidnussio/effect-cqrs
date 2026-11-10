/**
 * Why an event was recorded. Every event carries two ids:
 *
 * - `correlationId` is shared by everything that follows from one original
 *   request, across aggregates and view models.
 * - `causationId` names what directly caused the command that recorded it: the
 *   request, or the event a view model or saga was reacting to.
 *
 * They travel in the fiber's context rather than in payloads. A command run
 * outside any causation starts a new correlation with a fresh id. Inside a view
 * model handler the event being handled is the cause, so a chain of reactions
 * can be drawn as a tree without passing ids around by hand.
 *
 * @module
 */
import { Context, Effect, Function } from "effect"
import { eventId, type StoredEvent } from "./EventStore.ts"

export interface Causation {
  readonly correlationId: string
  readonly causationId: string
}

/** The causation of commands run in this fiber, `undefined` outside any. */
export const CurrentCausation = Context.Reference<Causation | undefined>("effect-cqrs/Causation", {
  defaultValue: () => undefined
})

/** Runs an effect with this causation, for instance the id of an incoming HTTP request. */
export const withCausation: {
  (causation: Causation): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  <A, E, R>(effect: Effect.Effect<A, E, R>, causation: Causation): Effect.Effect<A, E, R>
} = Function.dual(
  2,
  <A, E, R>(effect: Effect.Effect<A, E, R>, causation: Causation) =>
    Effect.provideService(effect, CurrentCausation, causation)
)

/** The causation of commands run in reaction to a stored event. */
export const fromEvent = (event: StoredEvent): Causation => {
  const id = eventId(event)
  return { correlationId: event.correlationId ?? id, causationId: id }
}

/**
 * Runs an effect in reaction to a stored event, so commands it runs record that
 * event as their cause. View models do this for their handlers already.
 */
export const causedBy = (event: StoredEvent) => <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.provideService(effect, CurrentCausation, fromEvent(event))
