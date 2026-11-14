/**
 * One entry point for the commands of several aggregates, like Axon's
 * `CommandGateway`: the caller hands over a command and the gateway finds the
 * aggregate that takes it.
 *
 * ```ts
 * const gateway = yield* CommandGateway.make([Todo, Account], {
 *   interceptors: [audit]
 * })
 *
 * // Typed: the payload and the errors follow from the aggregate and the type.
 * yield* gateway.send({ aggregateName: "todo", aggregateId: "a", type: "createTodo", payload: { title: "Buy Milk" } })
 *
 * // Untyped JSON from a transport, routed by `aggregateName`, or by `type` alone
 * // when a single aggregate has a command of that name.
 * yield* gateway.dispatch({ aggregateId: "a", type: "completeTodo", payload: {} })
 * ```
 *
 * The gateway composes aggregate services. Give it an aggregate and it builds the
 * service with the default options; give it a service built with
 * `AggregateService.make` to choose them, or to share the service, and its state
 * cache, with code that uses it directly.
 *
 * Interceptors run around every command the gateway routes, for what Axon's
 * dispatch interceptors do: authorize, log, add metadata with
 * `Metadata.withMetadata`. They run in the caller's fiber, so what they need, such
 * as the current user, comes from the caller's context rather than the gateway's.
 *
 * @module
 */
import { Effect, Predicate, Schema } from "effect"
import type * as Aggregate from "./Aggregate.ts"
import * as AggregateService from "./AggregateService.ts"
import type { EventStore } from "./EventStore.ts"

/**
 * The gateway can't tell which aggregate a command is for: the envelope names an
 * aggregate the gateway doesn't have, or it names none and the type is a command
 * of no aggregate, or of more than one.
 */
export class UnroutableCommand extends Schema.TaggedError<UnroutableCommand>()("UnroutableCommand", {
  type: Schema.String,
  /** The aggregate the envelope named, if it named one. */
  aggregateName: Schema.optional(Schema.String),
  /** The aggregates of the gateway that have a command of this type. */
  candidates: Schema.Array(Schema.String),
  message: Schema.String
}) {}

/** What an interceptor learns about the command it runs around. */
export interface CommandInfo {
  readonly aggregateName: string
  readonly aggregateId: string
  readonly type: string
}

/**
 * Runs around a command. It may run effects before and after `next`, fail
 * instead of running it, or run it with more context, and adds its own errors
 * `E` and requirements `R` to every command of the gateway.
 *
 * ```ts
 * const audit: CommandGateway.Interceptor<Unauthorized, CurrentUser> = (command, next) =>
 *   Effect.gen(function*() {
 *     const user = yield* CurrentUser
 *     if (!user.may(command.type)) return yield* new Unauthorized({ type: command.type })
 *     return yield* Metadata.withMetadata(next, { userId: user.id })
 *   })
 * ```
 */
export interface Interceptor<E = never, R = never> {
  <A, E1, R1>(command: CommandInfo, next: Effect.Effect<A, E1, R1>): Effect.Effect<A, E | E1, R | R1>
}

/** What a gateway routes to: an aggregate, or the service of one. */
export type Entry = Aggregate.Any | AggregateService.AggregateService<any>

/** The aggregate of an entry. */
export type AggregateOf<X> = X extends { readonly aggregate: infer A extends Aggregate.Any } ? A
  : X extends Aggregate.Any ? X
  : never

/** What building the services of the entries requires: entries that are services are already built. */
export type Requirements<X> = X extends Aggregate.Any ? EventStore | Aggregate.Context<X> : never

/**
 * The errors an interceptor adds. They are read from its type, so declare an
 * interceptor as an `Interceptor<E, R>` rather than inline in the options.
 */
export type InterceptorError<I> = I extends Interceptor<infer E, any> ? E : never

/** What an interceptor requires from the caller, read from its type like {@link InterceptorError}. */
export type InterceptorContext<I> = I extends Interceptor<any, infer R> ? R : never

/** A typed command for one of the aggregates `A`, what {@link CommandGateway.send} takes. */
export type Command<A extends Aggregate.Any> = A extends Aggregate.Any ? {
    readonly [K in Aggregate.CommandName<A>]:
      & {
        readonly aggregateName: Aggregate.Name<A>
        readonly aggregateId: string
        readonly type: K
        readonly payload: Aggregate.Payload<A, K>
      }
      & AggregateService.CommandOptions
  }[Aggregate.CommandName<A>]
  : never

/** The aggregate of `A` with this name. */
export type Named<A extends Aggregate.Any, Name> = Extract<A, { readonly name: Name }>

/** The errors the handler of a command declares. */
export type HandlerError<A extends Aggregate.Any, Type> = A extends Aggregate.Any
  ? Aggregate.CommandError<A, Type & Aggregate.CommandName<A>>
  : never

/** The events of any aggregate of `A`. */
export type EventOf<A extends Aggregate.Any> = A extends Aggregate.Any ? Aggregate.Event<A> : never

export interface CommandGateway<A extends Aggregate.Any, E = never, R = never> {
  /**
   * The service of each aggregate, by name, for reading with `state` and
   * `history`. Commands run through a service directly skip the interceptors.
   */
  readonly services: { readonly [X in A as Aggregate.Name<X>]: AggregateService.AggregateService<X> }
  /** Runs a typed command on the aggregate it names. */
  readonly send: <const C extends Command<A>>(
    command: C
  ) => Effect.Effect<
    AggregateService.CommandResult<Aggregate.Event<Named<A, C["aggregateName"]>>>,
    HandlerError<Named<A, C["aggregateName"]>, C["type"]> | AggregateService.CommandFailure | E,
    R
  >
  /**
   * Runs a `{ aggregateName, aggregateId, type, payload }` envelope received as
   * JSON from outside the process, with the other fields `AggregateService.dispatch`
   * takes. `aggregateName` can be left out when a single aggregate of the gateway
   * has a command of that type.
   */
  readonly dispatch: (
    input: unknown
  ) => Effect.Effect<
    AggregateService.CommandResult<EventOf<A>>,
    | HandlerError<A, string>
    | AggregateService.CommandFailure
    | AggregateService.UnknownCommand
    | UnroutableCommand
    | E,
    R
  >
}

export interface Options<Interceptors extends ReadonlyArray<Interceptor<any, any>>> {
  /** Run around every command, the first outermost. */
  readonly interceptors?: Interceptors | undefined
}

const Route = Schema.Struct({
  aggregateName: Schema.optional(Schema.NonEmptyString),
  aggregateId: Schema.NonEmptyString,
  type: Schema.String
})

const decodeRoute = Schema.decodeUnknownEffect(Route)

const isService = (entry: Entry): entry is AggregateService.AggregateService<any> =>
  Predicate.hasProperty(entry, "aggregate")

const makeGateway = Effect.fnUntraced(function*(
  entries: ReadonlyArray<Entry>,
  options: Options<ReadonlyArray<Interceptor<any, any>>> | undefined
) {
  const services = new Map<string, AggregateService.AggregateService<any>>()
  // The aggregates that have a command of each type.
  const byType = new Map<string, Array<string>>()
  for (const entry of entries) {
    const service = isService(entry) ? entry : yield* AggregateService.make(entry)
    const name = service.aggregate.name
    if (services.has(name)) {
      throw new Error(`The gateway has the aggregate ${name} twice`)
    }
    services.set(name, service)
    for (const type of Object.keys(service.aggregate.commands)) {
      const names = byType.get(type)
      if (names === undefined) byType.set(type, [name])
      else names.push(name)
    }
  }

  const interceptors = options?.interceptors ?? []
  const intercept = <X, E, R>(
    command: CommandInfo,
    effect: Effect.Effect<X, E, R>
  ): Effect.Effect<X, unknown, unknown> =>
    interceptors.reduceRight<Effect.Effect<X, unknown, unknown>>(
      (next, interceptor) => interceptor(command, next),
      effect
    )

  const send = (command: {
    readonly aggregateName: string
    readonly aggregateId: string
    readonly type: string
    readonly payload: unknown
    readonly commandId?: string | undefined
    readonly expectedVersion?: number | undefined
  }) => {
    const run = services.get(command.aggregateName)?.commands[command.type] as
      | ((aggregateId: string, payload: unknown, options?: AggregateService.CommandOptions) => Effect.Effect<any, any>)
      | undefined
    if (run === undefined) {
      // Only reachable from untyped code: the types allow known commands only.
      return Effect.die(new Error(`The gateway has no command ${command.aggregateName}.${command.type}`))
    }
    const { aggregateId, aggregateName, commandId, expectedVersion, payload, type } = command
    return intercept(
      { aggregateName, aggregateId, type },
      run(aggregateId, payload, { commandId, expectedVersion })
    )
  }

  const unroutable = (type: string, aggregateName: string | undefined, candidates: ReadonlyArray<string>) =>
    new UnroutableCommand({
      type,
      aggregateName,
      candidates,
      message: aggregateName !== undefined
        ? `The gateway has no aggregate ${aggregateName}`
        : candidates.length === 0
        ? `No aggregate of the gateway has a command ${type}`
        : `${type} is a command of ${candidates.join(", ")}, so the envelope has to name its aggregateName`
    })

  const dispatch = (input: unknown) =>
    Effect.gen(function*() {
      const route = yield* decodeRoute(input).pipe(
        Effect.mapError((error) =>
          new AggregateService.InvalidCommand({ aggregateName: "unknown", type: "unknown", message: error.message })
        )
      )
      const candidates = byType.get(route.type) ?? []
      const aggregateName = route.aggregateName ?? (candidates.length === 1 ? candidates[0] : undefined)
      const service = aggregateName === undefined ? undefined : services.get(aggregateName)
      if (aggregateName === undefined || service === undefined) {
        return yield* unroutable(route.type, route.aggregateName, candidates)
      }
      return yield* intercept(
        { aggregateName, aggregateId: route.aggregateId, type: route.type },
        service.dispatch(input)
      )
    })

  return {
    services: Object.fromEntries(services),
    send,
    dispatch
  } as unknown as CommandGateway<any, any, any>
})

/**
 * Builds a gateway over aggregates, or over services of them. Two entries for
 * the same aggregate are a defect.
 */
export const make = <
  const Entries extends ReadonlyArray<Entry>,
  const Interceptors extends ReadonlyArray<Interceptor<any, any>> = []
>(
  entries: Entries,
  options?: Options<Interceptors>
): Effect.Effect<
  CommandGateway<
    AggregateOf<Entries[number]>,
    InterceptorError<Interceptors[number]>,
    InterceptorContext<Interceptors[number]>
  >,
  never,
  Requirements<Entries[number]>
> => makeGateway(entries, options) as any
