/**
 * The write side of the domain: a schema of events, a projection that folds them
 * into state, and commands that inspect that state and decide on new events.
 *
 * An aggregate is plain data. {@link AggregateService} gives it a store to run
 * against, and `Fixture` runs it in tests.
 */
import { type DateTime, type Effect, Predicate, Schema } from "effect"
import * as EventVersions from "./EventVersions.ts"

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
  /** Shared by everything that follows from one original request, see `Causation`. */
  readonly correlationId: string | null
  /** What directly caused the command that recorded the event. */
  readonly causationId: string | null
  /** What the command carried beside its payload, such as the software version, see `Metadata`. */
  readonly metadata: Schema.JsonObject
}

export interface CommandContext {
  readonly aggregateId: string
  readonly version: number
}

/** Exhaustive by event tag, so adding an event to the schema without folding it does not compile. */
export type Projection<State, Event extends Tagged> = {
  readonly [Tag in Event["_tag"]]: (
    state: State,
    event: Extract<Event, { readonly _tag: Tag }>,
    meta: EventMeta
  ) => State
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

/**
 * The older shapes of some events, keyed by tag. Each history has to end in the
 * event's current shape.
 */
export type Versions<Event extends Tagged> = {
  readonly [Tag in Event["_tag"]]?: EventVersions.Any<Extract<Event, { readonly _tag: Tag }>>
}

export interface Aggregate<
  Name extends string,
  State,
  Events extends EventSchema<any>,
  Commands extends Record<string, AnyCommand>,
  V extends Versions<Events["Type"]> = {}
> {
  readonly name: Name
  readonly events: Events
  readonly initial: State
  readonly projection: Projection<State, Events["Type"]>
  readonly commands: Commands
  readonly versions: V
  /** Returns a new aggregate that also accepts this command. */
  command<const CommandName extends string, Payload extends PayloadSchema, E = never, R = never>(
    name: CommandName,
    options: {
      readonly payload: Payload
      readonly handler: CommandHandler<State, Payload["Type"], Events["Type"], E, R>
    }
  ): Aggregate<Name, State, Events, Commands & Record<CommandName, Command<Payload, State, Events["Type"], E, R>>, V>
}

export type Any = Aggregate<string, any, EventSchema<any>, Record<string, AnyCommand>, any>

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
  Commands extends Record<string, AnyCommand>,
  V extends Versions<Events["Type"]>
>(
  definition: Pick<
    Aggregate<Name, State, Events, Commands, V>,
    "name" | "events" | "initial" | "projection" | "commands" | "versions"
  >
): Aggregate<Name, State, Events, Commands, V> => ({
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
export const make =
  <State>() =>
  <const Name extends string, Events extends EventSchema<any>, const V extends Versions<Events["Type"]> = {}>(options: {
    readonly name: Name
    readonly events: Events
    readonly initial: State
    readonly projection: Projection<State, Events["Type"]>
    /** Older shapes of events already in the store, see `EventVersions`. */
    readonly versions?: V | undefined
  }): Aggregate<Name, State, Events, {}, V> =>
    build({ ...options, versions: options.versions ?? ({} as V), commands: {} })

/** Folds one event into the state. */
export const evolve = <A extends Any>(aggregate: A, state: State<A>, event: Event<A>, meta: EventMeta): State<A> =>
  (aggregate.projection as Record<string, (state: State<A>, event: Event<A>, meta: EventMeta) => State<A>>)[
    event._tag
  ]!(
    state,
    event,
    meta
  )

/** The schema version new events with this tag are written with. */
export const schemaVersion = (aggregate: Any, tag: string): number =>
  EventVersions.current((aggregate.versions as Record<string, EventVersions.Any | undefined>)[tag])

/**
 * An event as it was stored in the past, in an older version of its schema.
 * Construct one directly to replay JSON taken from a database.
 */
export class Historical {
  readonly type: string
  readonly schemaVersion: number
  readonly payload: Schema.Json
  constructor(options: { readonly type: string; readonly schemaVersion: number; readonly payload: Schema.Json }) {
    this.type = options.type
    this.schemaVersion = options.schemaVersion
    this.payload = options.payload
  }
}

/**
 * Encodes an event in an older version of its schema, as code from that time
 * would have stored it. The value is checked against that version, so a test
 * can't seed a shape that never existed.
 */
export const historical = <
  A extends Any,
  const Tag extends keyof A["versions"] & string,
  const Version extends number
>(
  aggregate: A,
  tag: Tag,
  version: Version,
  value: EventVersions.SchemaAt<NonNullable<A["versions"][Tag]>, Version>["Type"]
): Historical => {
  const versions = (aggregate.versions as Record<string, EventVersions.Any | undefined>)[tag]
  const schema = versions?.schemas[version - 1]
  if (schema === undefined) {
    throw new Error(`${aggregate.name} has no version ${version} of ${tag}`)
  }
  const payload = Schema.encodeSync(Schema.toCodecJson(schema))(value) as Schema.Json
  return new Historical({
    type: tag,
    schemaVersion: version,
    // Events were always stored with their tag, whatever the schema of the time said.
    payload: Predicate.isObject(payload) ? { ...(payload as object), _tag: tag } as Schema.Json : payload
  })
}
