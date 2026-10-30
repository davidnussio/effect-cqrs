/**
 * The write side of the domain: a schema of events, a projection that folds them
 * into state, and commands that inspect that state and decide on new events.
 *
 * An aggregate is plain data. {@link AggregateService} gives it a store to run
 * against, and `Fixture` runs it in tests.
 */
import type { DateTime, Effect, Schema } from "effect"

export interface Tagged {
  readonly _tag: string
}

export type EventSchema<Event extends Tagged = Tagged> = Schema.Top & {
  readonly Type: Event
  readonly DecodingServices: never
  readonly EncodingServices: never
}

export type PayloadSchema = Schema.Top & {
  readonly DecodingServices: never
  readonly EncodingServices: never
}

/** Where an event sits in its stream and in the store. */
export interface EventMeta {
  readonly aggregateName: string
  readonly aggregateId: string
  readonly version: number
  readonly position: number
  readonly timestamp: DateTime.Utc
}

export interface CommandContext {
  readonly aggregateId: string
  readonly version: number
}

/** Exhaustive by event tag, so adding an event to the schema without folding it does not compile. */
export type Projection<State, Event extends Tagged> = {
  readonly [Tag in Event["_tag"]]: (state: State, event: Extract<Event, { readonly _tag: Tag }>, meta: EventMeta) => State
}

/**
 * Decides on the events a command produces. It may run several times when
 * concurrent writers collide, so it should only decide and leave side effects to
 * view models. Returning an empty array records nothing.
 */
export type CommandHandler<State, Payload, Event, E, R> = (
  state: State,
  payload: Payload,
  context: CommandContext
) => Effect.Effect<Event | ReadonlyArray<Event>, E, R>

export interface Command<Payload extends PayloadSchema, State, Event, E, R> {
  readonly payload: Payload
  readonly handler: CommandHandler<State, Payload["Type"], Event, E, R>
}

export interface AnyCommand {
  readonly payload: PayloadSchema
  readonly handler: CommandHandler<any, any, any, any, any>
}

export interface Aggregate<
  Name extends string,
  State,
  Events extends EventSchema<any>,
  Commands extends Record<string, AnyCommand>
> {
  readonly name: Name
  readonly events: Events
  readonly initial: State
  readonly projection: Projection<State, Events["Type"]>
  readonly commands: Commands
  /** Returns a new aggregate that also accepts this command. */
  command<const CommandName extends string, Payload extends PayloadSchema, E = never, R = never>(
    name: CommandName,
    options: {
      readonly payload: Payload
      readonly handler: CommandHandler<State, Payload["Type"], Events["Type"], E, R>
    }
  ): Aggregate<Name, State, Events, Commands & Record<CommandName, Command<Payload, State, Events["Type"], E, R>>>
}

export type Any = Aggregate<string, any, EventSchema<any>, Record<string, AnyCommand>>

export type Name<A extends Any> = A["name"]
export type State<A extends Any> = A["initial"]
export type Event<A extends Any> = A["events"]["Type"]
export type Commands<A extends Any> = A["commands"]
export type CommandName<A extends Any> = keyof A["commands"] & string
export type Payload<A extends Any, K extends CommandName<A>> = A["commands"][K]["payload"]["Type"]
export type CommandError<A extends Any, K extends CommandName<A> = CommandName<A>> = A["commands"][K] extends
  Command<any, any, any, infer E, any> ? E : never
export type Context<A extends Any> = A["commands"][CommandName<A>] extends Command<any, any, any, any, infer R> ? R
  : never

const build = <
  Name extends string,
  State,
  Events extends EventSchema<any>,
  Commands extends Record<string, AnyCommand>
>(
  definition: Pick<Aggregate<Name, State, Events, Commands>, "name" | "events" | "initial" | "projection" | "commands">
): Aggregate<Name, State, Events, Commands> => ({
  ...definition,
  command: (name, options) =>
    build({
      ...definition,
      commands: { ...definition.commands, [name]: options } as any
    })
})

/**
 * The state is given explicitly, `Aggregate.make<State>()({ ... })`, so that the
 * projection's return values are checked against it rather than widened.
 */
export const make = <State>() =>
<const Name extends string, Events extends EventSchema<any>>(options: {
  readonly name: Name
  readonly events: Events
  readonly initial: State
  readonly projection: Projection<State, Events["Type"]>
}): Aggregate<Name, State, Events, {}> => build({ ...options, commands: {} })

/** Folds one event into the state. */
export const evolve = <A extends Any>(aggregate: A, state: State<A>, event: Event<A>, meta: EventMeta): State<A> =>
  (aggregate.projection as Record<string, (state: State<A>, event: Event<A>, meta: EventMeta) => State<A>>)[event._tag]!(
    state,
    event,
    meta
  )
