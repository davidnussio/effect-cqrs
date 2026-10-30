/**
 * Runs an aggregate against the {@link EventStore}: the `command`, `read-model`
 * and `history` actions of moleculer-cqrs, as a typed service.
 */
import { Array as Arr, type DateTime, Effect, Schema, Stream } from "effect"
import * as Aggregate from "./Aggregate.ts"
import { ConcurrencyConflict, EventStore, type EventStoreError, type StoredEvent } from "./EventStore.ts"

export class UnknownCommand extends Schema.TaggedError<UnknownCommand>()("UnknownCommand", {
  aggregateName: Schema.String,
  type: Schema.String
}) {}

export class InvalidCommand extends Schema.TaggedError<InvalidCommand>()("InvalidCommand", {
  aggregateName: Schema.String,
  type: Schema.String,
  message: Schema.String
}) {}

/** A stored event no longer matches the schema, which means the data needs a migration. */
export class EventDecodeError extends Schema.TaggedError<EventDecodeError>()("EventDecodeError", {
  aggregateName: Schema.String,
  aggregateId: Schema.String,
  version: Schema.Int,
  type: Schema.String,
  message: Schema.String
}) {}

export type ReadError = EventStoreError | EventDecodeError

/** How a command can fail, on top of the errors its own handler declares. */
export type CommandFailure = ConcurrencyConflict | InvalidCommand | ReadError

export interface CommandResult<Event> {
  readonly aggregateName: string
  readonly aggregateId: string
  /** The stream version after the command. */
  readonly version: number
  /** The store position of the last event, to pass to `ViewModels.caughtUp`. */
  readonly position: number
  readonly events: ReadonlyArray<Event>
}

export interface HistoryEntry<Event> {
  readonly version: number
  readonly timestamp: DateTime.Utc
  readonly type: string
  readonly payload?: Event
}

export interface AggregateService<A extends Aggregate.Any> {
  readonly aggregate: A
  /** One typed function per command. */
  readonly commands: {
    readonly [K in Aggregate.CommandName<A>]: (
      aggregateId: string,
      payload: Aggregate.Payload<A, K>
    ) => Effect.Effect<CommandResult<Aggregate.Event<A>>, Aggregate.CommandError<A, K> | CommandFailure>
  }
  /** Runs a `{ aggregateId, type, payload }` envelope received as JSON from outside the process. */
  readonly dispatch: (
    input: unknown
  ) => Effect.Effect<CommandResult<Aggregate.Event<A>>, Aggregate.CommandError<A> | CommandFailure | UnknownCommand>
  /** Materializes the state on the fly, optionally as it was before `finishTime`. */
  readonly readModel: (
    aggregateId: string,
    options?: { readonly finishTime?: DateTime.Utc | undefined }
  ) => Effect.Effect<Aggregate.State<A>, ReadError>
  /** Lists the events of one aggregate, with their decoded payload when `payload` is set. */
  readonly history: (
    aggregateId: string,
    options?: {
      readonly payload?: boolean | undefined
      readonly startTime?: DateTime.Utc | undefined
      readonly finishTime?: DateTime.Utc | undefined
    }
  ) => Effect.Effect<ReadonlyArray<HistoryEntry<Aggregate.Event<A>>>, ReadError>
}

const CommandEnvelope = Schema.Struct({
  aggregateId: Schema.NonEmptyString,
  type: Schema.String,
  payload: Schema.Unknown
})

/** Decodes a stored event with the schema of the aggregate it belongs to. */
export const makeEventDecoder = <A extends Aggregate.Any>(aggregate: A) => {
  const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  return (stored: StoredEvent): Effect.Effect<Aggregate.Event<A>, EventDecodeError> =>
    decode(stored.payload).pipe(
      Effect.mapError((error) =>
        new EventDecodeError({
          aggregateName: stored.aggregateName,
          aggregateId: stored.aggregateId,
          version: stored.version,
          type: stored.type,
          message: error.message
        })
      )
    ) as Effect.Effect<Aggregate.Event<A>, EventDecodeError>
}

/** Encodes events into the JSON form the store keeps. */
export const makeEventEncoder = <A extends Aggregate.Any>(aggregate: A) => {
  const encode = Schema.encodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  return (event: Aggregate.Event<A>) =>
    encode(event).pipe(
      // Events come from the aggregate's own handlers, so this is a programming error.
      Effect.orDie,
      Effect.map((payload) => ({ type: event._tag, payload: payload as Schema.Json }))
    )
}

export const make = Effect.fnUntraced(function*<A extends Aggregate.Any>(
  aggregate: A,
  options?: {
    /** How many times a command is re-decided after losing a race. Defaults to 5. */
    readonly maxRetries?: number
  }
) {
  const store = yield* EventStore
  const context = yield* Effect.context<Aggregate.Context<A>>()
  const maxRetries = options?.maxRetries ?? 5
  const aggregateName = aggregate.name
  const decodeEvent = makeEventDecoder(aggregate)
  const encodeEvent = makeEventEncoder(aggregate)

  const replay = <Acc>(
    stream: Stream.Stream<StoredEvent, EventStoreError>,
    initial: Acc,
    step: (acc: Acc, event: Aggregate.Event<A>, stored: StoredEvent) => Acc
  ) =>
    stream.pipe(
      Stream.runFoldEffect(
        () => initial,
        (acc, stored) => Effect.map(decodeEvent(stored), (event) => step(acc, event, stored))
      )
    )

  const streamOf = (
    aggregateId: string,
    range?: { readonly startTime?: DateTime.Utc | undefined; readonly finishTime?: DateTime.Utc | undefined }
  ) => store.read({ aggregateNames: [aggregateName], aggregateIds: [aggregateId], ...range })

  const load = (aggregateId: string, finishTime?: DateTime.Utc) =>
    replay(
      streamOf(aggregateId, { finishTime }),
      { state: aggregate.initial as Aggregate.State<A>, version: 0, position: 0 },
      (acc, event, stored) => ({
        state: Aggregate.evolve(aggregate, acc.state, event, stored),
        version: stored.version,
        position: stored.position
      })
    )

  const decide = Effect.fnUntraced(function*(type: string, aggregateId: string, payload: unknown) {
    const { position, state, version } = yield* load(aggregateId)
    const { handler } = aggregate.commands[type]!
    const events: ReadonlyArray<Aggregate.Event<A>> = Arr.ensure(yield* handler(state, payload, { aggregateId, version }))
    if (events.length === 0) {
      return { aggregateName, aggregateId, version, position, events }
    }
    const stored = yield* store.append({
      aggregateName,
      aggregateId,
      expectedVersion: version,
      events: yield* Effect.forEach(events, encodeEvent)
    })
    const last = stored[stored.length - 1]!
    return { aggregateName, aggregateId, version: last.version, position: last.position, events }
  })

  const execute = (type: string, aggregateId: string, payload: unknown) =>
    decide(type, aggregateId, payload).pipe(
      Effect.retry({
        times: maxRetries,
        while: (error) => error instanceof ConcurrencyConflict
      }),
      Effect.provideContext(context),
      Effect.withSpan(`${aggregateName}.${type}`, { attributes: { "aggregate.id": aggregateId } })
    )

  const decodePayload = (type: string, schema: Aggregate.PayloadSchema, input: unknown) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError((error) => new InvalidCommand({ aggregateName, type, message: error.message }))
    )

  const commands = Object.fromEntries(
    Object.entries(aggregate.commands).map(([type, command]) => [
      type,
      (aggregateId: string, payload: unknown) =>
        decodePayload(type, Schema.toType(command.payload), payload).pipe(
          Effect.flatMap((decoded) => execute(type, aggregateId, decoded))
        )
    ])
  )

  const decodeEnvelope = Schema.decodeUnknownEffect(CommandEnvelope)

  const dispatch = (input: unknown) =>
    Effect.gen(function*() {
      const envelope = yield* decodeEnvelope(input).pipe(
        Effect.mapError((error) => new InvalidCommand({ aggregateName, type: "unknown", message: error.message }))
      )
      if (!Object.hasOwn(aggregate.commands, envelope.type)) {
        return yield* new UnknownCommand({ aggregateName, type: envelope.type })
      }
      const payload = yield* decodePayload(
        envelope.type,
        Schema.toCodecJson(aggregate.commands[envelope.type]!.payload),
        envelope.payload
      )
      return yield* execute(envelope.type, envelope.aggregateId, payload)
    })

  const readModel: AggregateService<A>["readModel"] = (aggregateId, options) =>
    load(aggregateId, options?.finishTime).pipe(
      Effect.map(({ state }) => state),
      Effect.withSpan(`${aggregateName}.readModel`, { attributes: { "aggregate.id": aggregateId } })
    )

  const history: AggregateService<A>["history"] = (aggregateId, options) =>
    replay(
      streamOf(aggregateId, { startTime: options?.startTime, finishTime: options?.finishTime }),
      [] as Array<HistoryEntry<Aggregate.Event<A>>>,
      (entries, event, stored) => {
        entries.push({
          version: stored.version,
          timestamp: stored.timestamp,
          type: stored.type,
          ...(options?.payload === true ? { payload: event } : {})
        })
        return entries
      }
    ).pipe(Effect.withSpan(`${aggregateName}.history`, { attributes: { "aggregate.id": aggregateId } }))

  return {
    aggregate,
    commands,
    dispatch,
    readModel,
    history
  } as unknown as AggregateService<A>
})
