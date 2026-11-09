/**
 * Runs an aggregate against the {@link EventStore}: the `command`, `read-model`
 * and `history` actions of moleculer-cqrs, as a typed service.
 */
import { Array as Arr, type DateTime, Effect, FiberSet, Option, Schedule, Schema, type Scope, Semaphore, Stream } from "effect"
import { KeyValueStore } from "effect/persistence"
import * as Aggregate from "./Aggregate.ts"
import { CurrentCausation } from "./Causation.ts"
import {
  ConcurrencyConflict,
  type EventFilter,
  EventStore,
  type EventStoreError,
  type StoredEvent
} from "./EventStore.ts"
import * as EventVersions from "./EventVersions.ts"

export class UnknownCommand extends Schema.TaggedError<UnknownCommand>()("UnknownCommand", {
  aggregateName: Schema.String,
  type: Schema.String
}) {}

export class InvalidCommand extends Schema.TaggedError<InvalidCommand>()("InvalidCommand", {
  aggregateName: Schema.String,
  type: Schema.String,
  message: Schema.String
}) {}

/**
 * A stored event doesn't match the schema of its version, or was written in a
 * version the aggregate doesn't declare. Old shapes are declared with `EventVersions`.
 */
export class EventDecodeError extends Schema.TaggedError<EventDecodeError>()("EventDecodeError", {
  aggregateName: Schema.String,
  aggregateId: Schema.String,
  version: Schema.Int,
  type: Schema.String,
  schemaVersion: Schema.Int,
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
  /** Recorded on the events, see `Causation`. */
  readonly correlationId: string
}

export interface HistoryEntry<Event> {
  readonly version: number
  readonly timestamp: DateTime.Utc
  readonly type: string
  readonly correlationId: string | null
  readonly causationId: string | null
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
  payload: Schema.Unknown,
  /** Continues a correlation started elsewhere, see `Causation`. */
  correlationId: Schema.optional(Schema.String),
  causationId: Schema.optional(Schema.String)
})

const versionsOf = (aggregate: Aggregate.Any) =>
  Object.entries(aggregate.versions as Record<string, EventVersions.Any | undefined>).flatMap(([tag, versions]) =>
    versions === undefined ? [] : [[tag, versions] as const]
  )

/**
 * Decodes a stored event with the schema of the aggregate it belongs to. An
 * event stored in an older version is upcast to the current one on the way.
 */
export const makeEventDecoder = <A extends Aggregate.Any>(aggregate: A) => {
  const decodeCurrent = Schema.decodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  const upcasters = new Map(versionsOf(aggregate).map(([tag, versions]) => [tag, {
    current: EventVersions.current(versions),
    decoders: EventVersions.decoders(tag, versions).map((decoder) => Schema.decodeUnknownEffect(decoder))
  }]))
  return (stored: StoredEvent): Effect.Effect<Aggregate.Event<A>, EventDecodeError> => {
    const fail = (message: string) =>
      new EventDecodeError({
        aggregateName: stored.aggregateName,
        aggregateId: stored.aggregateId,
        version: stored.version,
        type: stored.type,
        schemaVersion: stored.schemaVersion,
        message
      })
    const upcaster = upcasters.get(stored.type)
    const current = upcaster?.current ?? 1
    const decode = stored.schemaVersion === current ? decodeCurrent : upcaster?.decoders[stored.schemaVersion - 1]
    if (decode === undefined) {
      return Effect.fail(fail(`${aggregate.name} knows ${stored.type} up to schema version ${current}`))
    }
    return decode(stored.payload).pipe(
      Effect.mapError((error) => fail(error.message))
    ) as Effect.Effect<Aggregate.Event<A>, EventDecodeError>
  }
}

/** Encodes events into the JSON form the store keeps, in the current version of their schema. */
export const makeEventEncoder = <A extends Aggregate.Any>(aggregate: A) => {
  const encode = Schema.encodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  const versions = new Map(versionsOf(aggregate).map(([tag, versions]) => [tag, EventVersions.current(versions)]))
  return (event: Aggregate.Event<A>) =>
    encode(event).pipe(
      // Events come from the aggregate's own handlers, so this is a programming error.
      Effect.orDie,
      Effect.map((payload) => ({
        type: event._tag,
        payload: payload as Schema.Json,
        schemaVersion: versions.get(event._tag) ?? 1
      }))
    )
}

/** Saves the state of each aggregate every so often, so a cold start replays only what came after. */
export interface SnapshotOptions<State> {
  /** How many events to fold after a snapshot before saving the next one. */
  readonly every: number
  /** How the state is stored. It is encoded as JSON. */
  readonly schema: Schema.Codec<State, any, never, never>
  /**
   * Part of every key. Change it whenever the state or the projection changes,
   * so that snapshots of the old state are ignored and rebuilt from the log.
   * Defaults to `1`.
   */
  readonly version?: string | number | undefined
}

export interface Options {
  /** How many times a command is re-decided after losing a race. Defaults to 5. */
  readonly maxRetries?: number | undefined
  /** How many aggregates keep their state in memory between commands. Defaults to 10,000. */
  readonly stateCacheSize?: number | undefined
}

interface Loaded<State> {
  readonly state: State
  readonly version: number
  readonly position: number
  /** The version of the last snapshot saved or loaded, `0` when there is none. */
  readonly snapshotVersion: number
}

/** Remembers recent states, dropping the least recently used. A `Map` iterates in insertion order. */
const makeStateCache = <State>(capacity: number) => {
  const entries = new Map<string, Loaded<State>>()
  return {
    get: (aggregateId: string) => {
      const entry = entries.get(aggregateId)
      if (entry !== undefined) {
        entries.delete(aggregateId)
        entries.set(aggregateId, entry)
      }
      return entry
    },
    set: (aggregateId: string, loaded: Loaded<State>) => {
      if (capacity === 0 || (entries.get(aggregateId)?.position ?? 0) > loaded.position) return
      entries.delete(aggregateId)
      entries.set(aggregateId, loaded)
      if (entries.size > capacity) entries.delete(entries.keys().next().value!)
    }
  }
}

/** Runs effects with the same key one at a time. A key only exists while it is in use. */
const makeKeyedLock = () => {
  const locks = new Map<string, { readonly semaphore: Semaphore.Semaphore; users: number }>()
  return (key: string) => <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const lock = locks.get(key) ?? { semaphore: Semaphore.makeUnsafe(1), users: 0 }
        lock.users++
        locks.set(key, lock)
        return lock
      }),
      (lock) => lock.semaphore.withPermit(effect),
      (lock) =>
        Effect.sync(() => {
          if (--lock.users === 0) locks.delete(key)
        })
    )
}

/**
 * Gives other fibers a turn once in a while. `Effect.forEach` with `concurrency`
 * carries on as long as items don't suspend, and a store on `node:sqlite` never does,
 * so a bulk run would finish before any view model ran. Yielding per command is
 * expensive, so it only happens when a millisecond has passed, using real time
 * because this is about the CPU. The clock restarts after the yield rather than
 * before it, so commands started meanwhile suspend too and the loop lets go.
 */
const makeTimeSlice = () => {
  let started = performance.now()
  return Effect.suspend(() =>
    performance.now() - started < 1
      ? Effect.void
      : Effect.andThen(Effect.yieldNow, Effect.sync(() => void (started = performance.now())))
  )
}

// Only a rival outside this service can cause a conflict, so give it room to finish.
const conflictBackoff = Schedule.exponential("10 millis").pipe(Schedule.jittered)

const makeSnapshots = <State>(aggregateName: string, options: SnapshotOptions<State>) =>
  Effect.gen(function*() {
    const store = KeyValueStore.toSchemaStore(
      yield* KeyValueStore.KeyValueStore,
      Schema.Struct({ version: Schema.Int, position: Schema.Int, state: options.schema })
    )
    const key = (aggregateId: string) =>
      `effect-cqrs/snapshot/${JSON.stringify([aggregateName, String(options.version ?? 1), aggregateId])}`

    // A snapshot is only a shortcut, so a failure to read or save one falls back to the log.
    const get = (aggregateId: string) =>
      store.get(key(aggregateId)).pipe(
        Effect.map(Option.map((snapshot): Loaded<State> => ({ ...snapshot, snapshotVersion: snapshot.version }))),
        Effect.catch((error) =>
          Effect.as(Effect.logWarning(`Ignoring the ${aggregateName} snapshot of ${aggregateId}`, error), Option.none())
        )
      )

    // Saves run beside the commands rather than in their way. Closing the service
    // waits for the ones still running, so a clean shutdown keeps its last snapshots.
    const saving = yield* FiberSet.make<void>()
    yield* Effect.addFinalizer(() => FiberSet.awaitEmpty(saving))

    // Starts a save once `every` events have been folded since the last snapshot, and
    // returns the state to remember. A failed save is not retried until `every` more.
    const save = (aggregateId: string, loaded: Loaded<State>): Effect.Effect<Loaded<State>> =>
      loaded.version - loaded.snapshotVersion < options.every
        ? Effect.succeed(loaded)
        : FiberSet.run(
          saving,
          store.set(key(aggregateId), { version: loaded.version, position: loaded.position, state: loaded.state }).pipe(
            Effect.catch((error) => Effect.logWarning(`Could not save the ${aggregateName} snapshot of ${aggregateId}`, error))
          )
        ).pipe(Effect.as({ ...loaded, snapshotVersion: loaded.version }))

    return { get, save }
  })

const makeService = Effect.fnUntraced(function*<A extends Aggregate.Any>(
  aggregate: A,
  options?: Options & { readonly snapshots?: SnapshotOptions<Aggregate.State<A>> | undefined }
) {
  const store = yield* EventStore
  const context = yield* Effect.context<Aggregate.Context<A>>()
  const maxRetries = options?.maxRetries ?? 5
  const aggregateName = aggregate.name
  const decodeEvent = makeEventDecoder(aggregate)
  const encodeEvent = makeEventEncoder(aggregate)
  const states = makeStateCache<Aggregate.State<A>>(options?.stateCacheSize ?? 10_000)
  const lockStream = makeKeyedLock()
  const timeSlice = makeTimeSlice()
  const snapshots = options?.snapshots === undefined
    ? undefined
    : yield* makeSnapshots<Aggregate.State<A>>(aggregateName, options.snapshots)

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
    range?: Pick<EventFilter, "startTime" | "finishTime" | "afterPosition" | "afterVersion">
  ) => store.read({ aggregateNames: [aggregateName], aggregateIds: [aggregateId], ...range })

  const fold = (
    acc: Loaded<Aggregate.State<A>>,
    event: Aggregate.Event<A>,
    stored: StoredEvent
  ): Loaded<Aggregate.State<A>> => ({
    state: Aggregate.evolve(aggregate, acc.state, event, stored),
    version: stored.version,
    position: stored.position,
    snapshotVersion: acc.snapshotVersion
  })

  const replayFrom = (
    start: Loaded<Aggregate.State<A>>,
    aggregateId: string,
    finishTime?: DateTime.Utc
  ) =>
    replay(
      streamOf(aggregateId, { finishTime, afterPosition: start.position, afterVersion: start.version }),
      start,
      fold
    )

  const initial: Loaded<Aggregate.State<A>> = {
    state: aggregate.initial as Aggregate.State<A>,
    version: 0,
    position: 0,
    snapshotVersion: 0
  }

  // Remembered states come first, as they are the most recent. Without one the
  // last snapshot saves replaying the start of the stream.
  const start = (aggregateId: string) => {
    const remembered = states.get(aggregateId)
    return remembered !== undefined || snapshots === undefined
      ? Effect.succeed(remembered ?? initial)
      : Effect.map(snapshots.get(aggregateId), Option.getOrElse(() => initial))
  }

  // Returns the state as remembered, with the version of a snapshot it just started.
  const remember = (aggregateId: string, loaded: Loaded<Aggregate.State<A>>) =>
    (snapshots === undefined ? Effect.succeed(loaded) : snapshots.save(aggregateId, loaded)).pipe(
      Effect.tap((remembered) => Effect.sync(() => states.set(aggregateId, remembered)))
    )

  // Streams only grow, so a remembered state plus what came after it is the current
  // state, even if another process wrote those events.
  const load = (aggregateId: string, finishTime?: DateTime.Utc) =>
    finishTime === undefined
      ? start(aggregateId).pipe(
        Effect.flatMap((from) => replayFrom(from, aggregateId)),
        Effect.flatMap((loaded) => remember(aggregateId, loaded))
      )
      : replayFrom(initial, aggregateId, finishTime)

  // Folds what a command just wrote into the remembered state, so the next command
  // finds nothing new to read. The events are decoded from what was stored, as a
  // replay would, so the state is the one a restart would rebuild. If they don't
  // decode, the state isn't remembered and the next load reports the error.
  const rememberWritten = (aggregateId: string, before: Loaded<Aggregate.State<A>>, written: ReadonlyArray<StoredEvent>) =>
    Effect.forEach(written, decodeEvent).pipe(
      Effect.flatMap((events) =>
        remember(aggregateId, events.reduce((acc, event, index) => fold(acc, event, written[index]!), before))
      ),
      Effect.ignore
    )

  const decide = Effect.fnUntraced(function*(
    type: string,
    aggregateId: string,
    payload: unknown,
    causation: { readonly correlationId: string; readonly causationId: string }
  ) {
    const loaded = yield* load(aggregateId)
    const { position, state, version } = loaded
    const { handler } = aggregate.commands[type]!
    const events: ReadonlyArray<Aggregate.Event<A>> = Arr.ensure(yield* handler(state, payload, { aggregateId, version }))
    const { correlationId } = causation
    if (events.length === 0) {
      return { aggregateName, aggregateId, version, position, events, correlationId }
    }
    const stored = yield* store.append({
      aggregateName,
      aggregateId,
      expectedVersion: version,
      events: yield* Effect.forEach(events, encodeEvent),
      ...causation
    })
    yield* rememberWritten(aggregateId, loaded, stored)
    const last = stored[stored.length - 1]!
    return { aggregateName, aggregateId, version: last.version, position: last.position, events, correlationId }
  })

  // Commands on one stream take turns, so they can't race each other and only a
  // rival in another process causes a conflict. A handler must not run a command on
  // its own stream, which would wait for itself.
  const execute = (
    type: string,
    aggregateId: string,
    payload: unknown,
    given?: { readonly correlationId?: string | undefined; readonly causationId?: string | undefined }
  ) =>
    // Read outside the service's own context, which would hide the caller's.
    CurrentCausation.use((current) => {
      const correlationId = given?.correlationId ?? current?.correlationId
      const causationId = given?.causationId ?? current?.causationId
      // Without a cause the command starts its own correlation.
      const id = correlationId ?? causationId ?? crypto.randomUUID()
      const causation = { correlationId: correlationId ?? id, causationId: causationId ?? id }
      return decide(type, aggregateId, payload, causation).pipe(
        Effect.retry({
          times: maxRetries,
          while: (error) => error instanceof ConcurrencyConflict,
          schedule: conflictBackoff
        }),
        Effect.provideContext(context),
        lockStream(aggregateId),
        (decided) => Effect.andThen(timeSlice, decided),
        Effect.withSpan(`${aggregateName}.${type}`, {
          attributes: { "aggregate.id": aggregateId, "cqrs.correlation_id": causation.correlationId }
        })
      )
    })

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
      return yield* execute(envelope.type, envelope.aggregateId, payload, envelope)
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
          correlationId: stored.correlationId,
          causationId: stored.causationId,
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

/**
 * Builds the service for one aggregate. With `snapshots` it saves states through
 * a `KeyValueStore` from `effect/persistence`, which has layers for memory, files,
 * SQL and the browser's `localStorage`. Saves run in the background, and closing
 * the scope waits for the ones still running.
 */
export const make: {
  <A extends Aggregate.Any>(
    aggregate: A,
    options: Options & { readonly snapshots: SnapshotOptions<Aggregate.State<A>> }
  ): Effect.Effect<
    AggregateService<A>,
    never,
    EventStore | KeyValueStore.KeyValueStore | Scope.Scope | Aggregate.Context<A>
  >
  <A extends Aggregate.Any>(
    aggregate: A,
    options?: Options
  ): Effect.Effect<AggregateService<A>, never, EventStore | Aggregate.Context<A>>
} = makeService as any
