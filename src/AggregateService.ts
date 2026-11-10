/**
 * Runs an aggregate against the {@link EventStore}: the `command`, `read-model`
 * and `history` actions of moleculer-cqrs, as a typed service.
 *
 * @module
 */
import {
  Array as Arr,
  Cause,
  Context,
  type DateTime,
  Duration,
  Effect,
  Exit,
  FiberSet,
  Metric,
  Option,
  Predicate,
  Schedule,
  Schema,
  type Scope,
  Semaphore,
  Stream
} from "effect"
import { KeyValueStore } from "effect/persistence"
import * as Aggregate from "./Aggregate.ts"
import { CurrentCausation } from "./Causation.ts"
import {
  ConcurrencyConflict,
  type EventFilter,
  EventStore,
  type EventStoreError,
  type NewEvent as EventStoreNewEvent,
  type StoredEvent
} from "./EventStore.ts"
import * as EventVersions from "./EventVersions.ts"
import { IdGenerator } from "./Ids.ts"
import { CurrentMetadata, Metadata } from "./Metadata.ts"
import * as PersonalData from "./PersonalData.ts"

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

/**
 * The caller's `expectedVersion` is not the aggregate's version: someone else
 * changed it since the caller read it. Unlike a `ConcurrencyConflict`, this is
 * not retried, as deciding again would overwrite a change the caller hasn't seen.
 */
export class WrongExpectedVersion extends Schema.TaggedError<WrongExpectedVersion>()("WrongExpectedVersion", {
  aggregateName: Schema.String,
  aggregateId: Schema.String,
  expectedVersion: Schema.Int,
  actualVersion: Schema.Int
}) {}

/** How reading events can fail. `PersonalDataError` comes from aggregates with personal fields, see `PersonalData`. */
export type ReadError = EventStoreError | EventDecodeError | PersonalData.PersonalDataError

/**
 * How a command can fail, on top of the errors its own handler declares.
 * `SubjectForgotten` means an event would record personal data of a forgotten subject.
 */
export type CommandFailure =
  | ConcurrencyConflict
  | InvalidCommand
  | WrongExpectedVersion
  | ReadError
  | PersonalData.SubjectForgotten

/** What a typed command takes besides the aggregate id and the payload. */
export interface CommandOptions {
  /**
   * Identifies the command, so that running it again on the same aggregate
   * returns the first result instead of deciding again. See {@link commandIdKey}.
   */
  readonly commandId?: string | undefined
  /** The version the caller based its command on. If the aggregate is at another, the command fails with `WrongExpectedVersion`. */
  readonly expectedVersion?: number | undefined
}

/**
 * The metadata key that records the `commandId` on the events a command writes.
 * It is reserved: metadata given under this key is dropped.
 */
export const commandIdKey = "cqrs.commandId"

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
  readonly metadata: Metadata
  readonly payload?: Event
}

export interface AggregateService<A extends Aggregate.Any> {
  readonly aggregate: A
  /** One typed function per command. */
  readonly commands: {
    readonly [K in Aggregate.CommandName<A>]: (
      aggregateId: string,
      payload: Aggregate.Payload<A, K>,
      options?: CommandOptions
    ) => Effect.Effect<CommandResult<Aggregate.Event<A>>, Aggregate.CommandError<A, K> | CommandFailure>
  }
  /**
   * Runs a `{ aggregateId, type, payload }` envelope received as JSON from outside
   * the process. It can also carry `commandId` and `expectedVersion`, see `CommandOptions`,
   * `correlationId`, `causationId` and `metadata`.
   */
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
  causationId: Schema.optional(Schema.String),
  /** Recorded on top of the current metadata, see `Metadata`. */
  metadata: Schema.optional(Metadata),
  /** See `CommandOptions`. */
  commandId: Schema.optional(Schema.NonEmptyString),
  expectedVersion: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)))
})

const versionsOf = (aggregate: Aggregate.Any) =>
  Object.entries(aggregate.versions as Record<string, EventVersions.Any | undefined>).flatMap(([tag, versions]) =>
    versions === undefined ? [] : [[tag, versions] as const]
  )

interface PersonalFields {
  /** By tag, for events in their current version. */
  readonly current: ReadonlyMap<string, ReadonlyArray<PersonalData.Field>>
  /** By tag, then by schema version minus one, for events stored in an older version. */
  readonly old: ReadonlyMap<string, ReadonlyArray<ReadonlyArray<PersonalData.Field>>>
}

/** The personal fields of every event and every older version, `undefined` when there are none. */
const personalFieldsOf = (aggregate: Aggregate.Any): PersonalFields | undefined => {
  const current = PersonalData.fieldsByTag(aggregate.events, aggregate.name)
  const old = new Map(
    versionsOf(aggregate).flatMap(([tag, versions]) => {
      const fields = versions.schemas.slice(0, -1).map((schema, index) =>
        PersonalData.fieldsOf(schema, `${aggregate.name}, ${tag} version ${index + 1}`)
      )
      return fields.some((each) => each.length > 0) ? [[tag, fields] as const] : []
    })
  )
  return current.size === 0 && old.size === 0 ? undefined : { current, old }
}

/** The keys an aggregate with personal fields needs, checked when a service is built rather than at the first event. */
const requireKeys = (aggregate: Aggregate.Any, keys: PersonalData.Keys["Service"] | undefined) => {
  if (keys === undefined) {
    throw new Error(`${aggregate.name} has personal fields, so it needs PersonalData.Keys`)
  }
  return keys
}

/** An event decoded from the store, with the subjects whose personal data it holds in plaintext. */
interface Decoded<Event> {
  readonly event: Event
  readonly subjects: ReadonlyArray<string>
}

const noSubjects: ReadonlyArray<string> = []

// How many events of a stream decrypt at once while it is replayed.
const decryptConcurrency = 32

const makeDecoder = <A extends Aggregate.Any>(aggregate: A, keys: PersonalData.Keys["Service"] | undefined) => {
  const decodeCurrent = Schema.decodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  const upcasters = new Map(
    versionsOf(aggregate).map(([tag, versions]) => [tag, {
      current: EventVersions.current(versions),
      decoders: EventVersions.decoders(tag, versions).map((decoder) => Schema.decodeUnknownEffect(decoder))
    }])
  )
  const personal = personalFieldsOf(aggregate)
  const personalKeys = personal === undefined ? undefined : requireKeys(aggregate, keys)
  return (
    stored: StoredEvent
  ): Effect.Effect<Decoded<Aggregate.Event<A>>, EventDecodeError | PersonalData.PersonalDataError> => {
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
    const decodePayload = (payload: unknown) =>
      decode(payload).pipe(
        Effect.mapError((error) => fail(error.message))
      ) as Effect.Effect<Aggregate.Event<A>, EventDecodeError>
    const fields = personal === undefined
      ? undefined
      : stored.schemaVersion === current
      ? personal.current.get(stored.type)
      : personal.old.get(stored.type)?.[stored.schemaVersion - 1]
    if (fields === undefined || fields.length === 0) {
      return Effect.map(decodePayload(stored.payload), (event) => ({ event, subjects: noSubjects }))
    }
    return Effect.flatMap(
      PersonalData.open(
        personalKeys!,
        fields,
        { aggregateName: stored.aggregateName, type: stored.type, aggregateId: stored.aggregateId },
        stored.payload
      ),
      ({ payload, subjects }) => Effect.map(decodePayload(payload), (event) => ({ event, subjects }))
    )
  }
}

/**
 * Decodes a stored event with the schema of the aggregate it belongs to. An
 * event stored in an older version is upcast to the current one on the way.
 * An aggregate with personal fields needs `keys` to decrypt them, see `PersonalData`.
 */
export const makeEventDecoder = <A extends Aggregate.Any>(aggregate: A, keys?: PersonalData.Keys["Service"]) => {
  const decode = makeDecoder(aggregate, keys)
  return (stored: StoredEvent): Effect.Effect<Aggregate.Event<A>, EventDecodeError | PersonalData.PersonalDataError> =>
    Effect.map(decode(stored), ({ event }) => event)
}

/** An event encoded for the store, with its payload before encryption and the subjects it encrypted for. */
interface Encoded {
  readonly event: EventStoreNewEvent
  readonly plain: Schema.Json
  readonly subjects: ReadonlyArray<string>
}

const makeEncoder = <A extends Aggregate.Any>(aggregate: A, keys: PersonalData.Keys["Service"] | undefined) => {
  const encode = Schema.encodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  const versions = new Map(versionsOf(aggregate).map(([tag, versions]) => [tag, EventVersions.current(versions)]))
  const personal = personalFieldsOf(aggregate)?.current
  const personalKeys = personal === undefined ? undefined : requireKeys(aggregate, keys)
  return (
    event: Aggregate.Event<A>,
    aggregateId?: string
  ): Effect.Effect<Encoded, PersonalData.SubjectForgotten | PersonalData.PersonalDataError> => {
    const fields = personal?.get(event._tag)
    const schemaVersion = versions.get(event._tag) ?? 1
    return encode(event).pipe(
      // Events come from the aggregate's own handlers, so this is a programming error.
      Effect.orDie,
      Effect.flatMap((encoded) => {
        const plain = encoded as Schema.Json
        if (fields === undefined) {
          return Effect.succeed({
            event: { type: event._tag, payload: plain, schemaVersion },
            plain,
            subjects: noSubjects
          })
        }
        return Effect.map(
          PersonalData.seal(
            personalKeys!,
            fields,
            { aggregateName: aggregate.name, type: event._tag, aggregateId },
            plain
          ),
          ({ payload, subjects }) => ({ event: { type: event._tag, payload, schemaVersion }, plain, subjects })
        )
      })
    )
  }
}

/**
 * Encodes events into the JSON form the store keeps, in the current version of
 * their schema. An aggregate with personal fields needs `keys` to encrypt them,
 * and the id of the stream the event goes to, the default subject.
 */
export const makeEventEncoder = <A extends Aggregate.Any>(aggregate: A, keys?: PersonalData.Keys["Service"]) => {
  const encode = makeEncoder(aggregate, keys)
  return (
    event: Aggregate.Event<A>,
    aggregateId?: string
  ): Effect.Effect<EventStoreNewEvent, PersonalData.SubjectForgotten | PersonalData.PersonalDataError> =>
    Effect.map(encode(event, aggregateId), ({ event }) => event)
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
  /**
   * How many of its latest command ids each aggregate remembers, so that a command
   * run again with one of them returns its first result. Defaults to 100.
   */
  readonly commandIdsPerStream?: number | undefined
}

/** A command that wrote events with a `commandId`: the versions of its first and last event. */
interface RecentCommand {
  readonly id: string
  readonly first: number
  readonly last: number
}

const RecentCommand = Schema.Struct({ id: Schema.String, first: Schema.Int, last: Schema.Int })

const noCommands: ReadonlyArray<RecentCommand> = []

interface Loaded<State> {
  readonly state: State
  readonly version: number
  readonly position: number
  /** The version of the last snapshot saved or loaded, `0` when there is none. */
  readonly snapshotVersion: number
  /** The subjects whose personal data the state may hold, see `PersonalData`. */
  readonly subjects: ReadonlyArray<string>
  /** The latest commands that carried a `commandId`, oldest first. */
  readonly commands: ReadonlyArray<RecentCommand>
}

/** Adds subjects, keeping the same array when there is nothing new. */
const addSubjects = (subjects: ReadonlyArray<string>, more: ReadonlyArray<string>): ReadonlyArray<string> => {
  const fresh = more.filter((subject) => !subjects.includes(subject))
  return fresh.length === 0 ? subjects : [...subjects, ...fresh]
}

/**
 * The names of the metrics every aggregate service records through Effect's
 * `Metric` module. Each series carries an `aggregate` attribute with the
 * aggregate's name. Read them with `Metric.snapshot`, or send them on with an
 * exporter such as `OtlpMetrics` from `effect/observability`.
 */
export const MetricNames = {
  /**
   * Timer: how long a command took, including its wait for an earlier command on
   * the same stream, by `command` and `outcome`. The outcome is `success`, the tag
   * of the error, `defect` or `interrupted`. A payload that doesn't decode, or a
   * type the aggregate doesn't know, fails before this point and isn't recorded.
   */
  commandDuration: "cqrs_command_duration",
  /**
   * Counter: where loading the current state found a state to start from, by
   * `source`: `memory`, `snapshot`, or `replay` when it starts from the first event.
   */
  stateLoads: "cqrs_state_loads_total",
  /** Histogram: how many events a load read from the store on top of that state. */
  eventsReplayed: "cqrs_events_replayed",
  /** Gauge: how many states are remembered in memory. */
  stateCacheSize: "cqrs_state_cache_size",
  /** Counter: states dropped from memory to make room for others, see `stateCacheSize`. */
  stateCacheEvictions: "cqrs_state_cache_evictions_total",
  /** Counter: snapshots saved, by `result`: `success` or `failure`. */
  snapshotSaves: "cqrs_snapshot_saves_total",
  /** Counter: snapshots that couldn't be read, so the stream was replayed instead. */
  snapshotReadFailures: "cqrs_snapshot_read_failures_total",
  /** Counter: appends that lost a race with a writer in another process. Each is retried until `maxRetries` runs out. */
  concurrencyConflicts: "cqrs_concurrency_conflicts_total"
} as const

/**
 * Builds each series once with its attributes: a metric's key is cached, while
 * `Metric.withAttributes` would compute it again on every update.
 */
const makeMetrics = (aggregateName: string) => {
  const attributes = { aggregate: aggregateName }
  const counter = (name: string, description: string, extra?: Record<string, string>) =>
    Metric.counter(name, { description, incremental: true, attributes: { ...attributes, ...extra } })
  const loads = (source: "memory" | "snapshot" | "replay") =>
    counter(MetricNames.stateLoads, "Where loading an aggregate found a state to start from", { source })
  const saves = (result: "success" | "failure") => counter(MetricNames.snapshotSaves, "Snapshots saved", { result })
  const durations = new Map<string, Map<string, Metric.Metric<Duration.Duration, unknown>>>()
  return {
    loadedFromMemory: loads("memory"),
    loadedFromSnapshot: loads("snapshot"),
    loadedFromReplay: loads("replay"),
    eventsReplayed: Metric.histogram(MetricNames.eventsReplayed, {
      description: "Events read from the store to bring a state up to date",
      attributes,
      boundaries: [0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000]
    }),
    cacheSize: Metric.gauge(MetricNames.stateCacheSize, { description: "States remembered in memory", attributes }),
    evictions: counter(MetricNames.stateCacheEvictions, "States dropped from memory to make room"),
    snapshotSaved: saves("success"),
    snapshotFailed: saves("failure"),
    snapshotReadFailures: counter(MetricNames.snapshotReadFailures, "Snapshots that couldn't be read"),
    conflicts: counter(MetricNames.concurrencyConflicts, "Appends that lost a race with another writer"),
    duration: (command: string, outcome: string) => {
      let byOutcome = durations.get(command)
      if (byOutcome === undefined) durations.set(command, byOutcome = new Map())
      let timer = byOutcome.get(outcome)
      if (timer === undefined) {
        timer = Metric.timer(MetricNames.commandDuration, {
          description: "How long commands take",
          attributes: { ...attributes, command, outcome }
        })
        byOutcome.set(outcome, timer)
      }
      return timer
    }
  }
}

type Metrics = ReturnType<typeof makeMetrics>

/** Counts one occurrence and returns `value`, in a single step rather than an effect per update. */
const recording = <X>(counter: Metric.Counter<number>, value: X): Effect.Effect<X> =>
  Effect.contextWith((context: Context.Context<never>) => {
    counter.updateUnsafe(1, context)
    return Effect.succeed(value)
  })

const outcomeOf = (exit: Exit.Exit<unknown, unknown>): string => {
  if (Exit.isSuccess(exit)) return "success"
  const error = Cause.findErrorOption(exit.cause)
  if (Option.isSome(error)) {
    const value = error.value
    return Predicate.hasProperty(value, "_tag") && Predicate.isString(value._tag) ? value._tag : "error"
  }
  return Cause.hasInterruptsOnly(exit.cause) ? "interrupted" : "defect"
}

/** Remembers recent states, dropping the least recently used. A `Map` iterates in insertion order. */
const makeStateCache = <State>(capacity: number) => {
  const entries = new Map<string, Loaded<State>>()
  return {
    size: () => entries.size,
    get: (aggregateId: string) => {
      const entry = entries.get(aggregateId)
      if (entry !== undefined) {
        entries.delete(aggregateId)
        entries.set(aggregateId, entry)
      }
      return entry
    },
    delete: (aggregateId: string) => void entries.delete(aggregateId),
    /** Returns whether another state was dropped to make room. */
    set: (aggregateId: string, loaded: Loaded<State>): boolean => {
      if (capacity === 0 || (entries.get(aggregateId)?.position ?? 0) > loaded.position) return false
      entries.delete(aggregateId)
      entries.set(aggregateId, loaded)
      if (entries.size <= capacity) return false
      entries.delete(entries.keys().next().value!)
      return true
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

const makeSnapshots = <State>(
  aggregateName: string,
  options: SnapshotOptions<State>,
  metrics: Metrics,
  keys: PersonalData.Keys["Service"] | undefined,
  commandIdsPerStream: number
) =>
  Effect.gen(function*() {
    const keyValueStore = yield* KeyValueStore.KeyValueStore
    const key = (aggregateId: string) =>
      `effect-cqrs/snapshot/${JSON.stringify([aggregateName, String(options.version ?? 1), aggregateId])}`

    // Recent command ids are kept beside the state, outside the sealed part: the
    // events hold them in plain text too. Snapshots from before them have none.
    const commands = Schema.optional(Schema.Array(RecentCommand))
    const recent = (saved: ReadonlyArray<RecentCommand> | undefined) =>
      saved === undefined || saved.length === 0 || commandIdsPerStream <= 0
        ? noCommands
        : saved.slice(-commandIdsPerStream)

    let read: (aggregateId: string) => Effect.Effect<Option.Option<Loaded<State>>, unknown>
    let write: (aggregateId: string, loaded: Loaded<State>) => Effect.Effect<void, unknown>
    if (keys === undefined) {
      const store = KeyValueStore.toSchemaStore(
        keyValueStore,
        Schema.Struct({ version: Schema.Int, position: Schema.Int, state: options.schema, commands })
      )
      read = (aggregateId) =>
        store.get(key(aggregateId)).pipe(
          Effect.map(Option.map(({ commands, position, state, version }): Loaded<State> => ({
            state,
            version,
            position,
            snapshotVersion: version,
            subjects: noSubjects,
            commands: recent(commands)
          })))
        )
      write = (aggregateId, { commands, position, state, version }) =>
        store.set(key(aggregateId), { version, position, state, commands })
    } else {
      // The state of an aggregate with personal fields is sealed with the key of
      // every subject it holds data of, so forgetting any of them destroys it.
      const store = KeyValueStore.toSchemaStore(
        keyValueStore,
        Schema.Struct({ version: Schema.Int, position: Schema.Int, sealed: PersonalData.Sealed, commands })
      )
      const stateText = Schema.fromJsonString(Schema.toCodecJson(options.schema))
      const purpose = (aggregateId: string) => `snapshot ${JSON.stringify([aggregateName, aggregateId])}`
      read = (aggregateId) =>
        store.get(key(aggregateId)).pipe(
          Effect.flatMap((found): Effect.Effect<Option.Option<Loaded<State>>, unknown> =>
            Option.match(found, {
              onNone: () => Effect.succeed(Option.none<Loaded<State>>()),
              onSome: ({ commands, position, sealed, version }) =>
                PersonalData.openText(keys, sealed, purpose(aggregateId)).pipe(
                  Effect.flatMap((text): Effect.Effect<Option.Option<Loaded<State>>, unknown> =>
                    Option.match(text, {
                      // A subject was forgotten, so the snapshot can't be read again.
                      onNone: () => Effect.as(store.remove(key(aggregateId)), Option.none<Loaded<State>>()),
                      onSome: (text) =>
                        Effect.map(Schema.decodeEffect(stateText)(text), (state) =>
                          Option.some<Loaded<State>>({
                            state,
                            version,
                            position,
                            snapshotVersion: version,
                            subjects: sealed.subjects.map(({ subject }) => subject),
                            commands: recent(commands)
                          }))
                    })
                  )
                )
            })
          )
        )
      write = (aggregateId, { commands, position, state, subjects, version }) =>
        Schema.encodeEffect(stateText)(state).pipe(
          Effect.flatMap((text) => PersonalData.sealText(keys, subjects, text, purpose(aggregateId))),
          Effect.flatMap((sealed) => store.set(key(aggregateId), { version, position, sealed, commands }))
        )
    }

    // A snapshot is only a shortcut, so a failure to read or save one falls back to the log.
    const get = (aggregateId: string) =>
      read(aggregateId).pipe(
        Effect.catch((error) =>
          Metric.update(metrics.snapshotReadFailures, 1).pipe(
            Effect.andThen(Effect.logWarning(`Ignoring the ${aggregateName} snapshot of ${aggregateId}`, error)),
            Effect.as(Option.none<Loaded<State>>())
          )
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
          write(aggregateId, loaded).pipe(
            Effect.andThen(Metric.update(metrics.snapshotSaved, 1)),
            Effect.catch((error) =>
              Effect.andThen(
                Metric.update(metrics.snapshotFailed, 1),
                Effect.logWarning(`Could not save the ${aggregateName} snapshot of ${aggregateId}`, error)
              )
            )
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
  // Metadata provided where the service was built, such as the software version,
  // reaches commands run where that layer isn't visible.
  const builtWith = Context.get(context as Context.Context<never>, CurrentMetadata)
  const mergeMetadata = (current: Metadata, given: Metadata | undefined): Metadata =>
    current === builtWith && given === undefined ? builtWith : { ...builtWith, ...current, ...given }
  // The command id key is reserved, so a command without one records none.
  const withCommandId = (metadata: Metadata, commandId: string | undefined): Metadata => {
    if (commandId !== undefined) return { ...metadata, [commandIdKey]: commandId }
    if (!Object.hasOwn(metadata, commandIdKey)) return metadata
    const { [commandIdKey]: _, ...rest } = metadata
    return rest
  }
  const maxRetries = options?.maxRetries ?? 5
  const commandIdsPerStream = options?.commandIdsPerStream ?? 100
  const aggregateName = aggregate.name
  const keys = personalFieldsOf(aggregate) === undefined
    ? undefined
    : requireKeys(aggregate, Option.getOrUndefined(yield* Effect.serviceOption(PersonalData.Keys)))
  const decodeEvent = makeDecoder(aggregate, keys)
  const encodeEvent = makeEncoder(aggregate, keys)
  // What a command just wrote is decoded from its payload before encryption, which
  // is what decrypting it would give back.
  const decodeWritten = Schema.decodeUnknownEffect(Schema.toCodecJson(aggregate.events))
  const states = makeStateCache<Aggregate.State<A>>(options?.stateCacheSize ?? 10_000)
  const lockStream = makeKeyedLock()
  const timeSlice = makeTimeSlice()
  const metrics = makeMetrics(aggregateName)
  const snapshots = options?.snapshots === undefined
    ? undefined
    : yield* makeSnapshots<Aggregate.State<A>>(aggregateName, options.snapshots, metrics, keys, commandIdsPerStream)

  const replay = <Acc>(
    stream: Stream.Stream<StoredEvent, EventStoreError>,
    initial: Acc,
    step: (acc: Acc, decoded: Decoded<Aggregate.Event<A>>, stored: StoredEvent) => Acc
  ) =>
    keys === undefined || !keys.cipher.asynchronous
      ? stream.pipe(
        Stream.runFoldEffect(
          () => initial,
          (acc, stored) => Effect.map(decodeEvent(stored), (decoded) => step(acc, decoded, stored))
        )
      )
      // Decrypting waits on WebCrypto, so the events of a page decrypt at once, still
      // folded in order. Working page by page costs nothing for a stream with nothing
      // new, which is most loads, and no fibers for a page of one event.
      : Effect.suspend(() => {
        let acc = initial
        return stream.pipe(
          Stream.runForEachArray((page) =>
            Effect.map(
              page.length === 1
                ? Effect.map(decodeEvent(page[0]!), (decoded) => [decoded])
                : Effect.forEach(page, decodeEvent, { concurrency: decryptConcurrency }),
              (decoded) => {
                for (let index = 0; index < page.length; index++) acc = step(acc, decoded[index]!, page[index]!)
              }
            )
          ),
          Effect.map(() => acc)
        )
      })

  const streamOf = (
    aggregateId: string,
    range?: Pick<EventFilter, "startTime" | "finishTime" | "afterPosition" | "afterVersion">
  ) => store.read({ aggregateNames: [aggregateName], aggregateIds: [aggregateId], ...range })

  // The events of one command are appended together and carry the same id, so an
  // event continuing the last command extends it rather than adding another.
  const addCommand = (commands: ReadonlyArray<RecentCommand>, stored: StoredEvent): ReadonlyArray<RecentCommand> => {
    const id = stored.metadata[commandIdKey]
    if (!Predicate.isString(id) || commandIdsPerStream <= 0) return commands
    const last = commands[commands.length - 1]
    if (last !== undefined && last.id === id && last.last === stored.version - 1) {
      return [...commands.slice(0, -1), { id, first: last.first, last: stored.version }]
    }
    const kept = commands.length < commandIdsPerStream
      ? commands
      : commands.slice(commands.length - commandIdsPerStream + 1)
    return [...kept, { id, first: stored.version, last: stored.version }]
  }

  const fold = (
    acc: Loaded<Aggregate.State<A>>,
    { event, subjects }: Decoded<Aggregate.Event<A>>,
    stored: StoredEvent
  ): Loaded<Aggregate.State<A>> => ({
    state: Aggregate.evolve(aggregate, acc.state, event, stored),
    version: stored.version,
    position: stored.position,
    snapshotVersion: acc.snapshotVersion,
    subjects: addSubjects(acc.subjects, subjects),
    commands: addCommand(acc.commands, stored)
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
    snapshotVersion: 0,
    subjects: noSubjects,
    commands: noCommands
  }

  // Remembered states come first, as they are the most recent. Without one the
  // last snapshot saves replaying the start of the stream.
  const start = (aggregateId: string): Effect.Effect<Loaded<Aggregate.State<A>>, PersonalData.PersonalDataError> => {
    const remembered = states.get(aggregateId)
    if (remembered === undefined) return startOver(aggregateId)
    if (keys === undefined || remembered.subjects.length === 0) return recording(metrics.loadedFromMemory, remembered)
    // A state holding personal data of a subject forgotten since, here or in
    // another process, is rebuilt so the data reads as its replacement.
    return Effect.flatMap(
      Effect.forEach(remembered.subjects, keys.isForgotten),
      (forgotten) => {
        if (!forgotten.includes(true)) return recording(metrics.loadedFromMemory, remembered)
        states.delete(aggregateId)
        return startOver(aggregateId)
      }
    )
  }

  const startOver = (aggregateId: string): Effect.Effect<Loaded<Aggregate.State<A>>> => {
    if (snapshots === undefined) return recording(metrics.loadedFromReplay, initial)
    return Effect.flatMap(
      snapshots.get(aggregateId),
      Option.match({
        onNone: () => recording(metrics.loadedFromReplay, initial),
        onSome: (snapshot) => recording(metrics.loadedFromSnapshot, snapshot)
      })
    )
  }

  // Returns the state as remembered, with the version of a snapshot it just started.
  // `read` is how many events the load read from the store, when one did.
  const remember = (aggregateId: string, loaded: Loaded<Aggregate.State<A>>, read?: number) =>
    (snapshots === undefined ? Effect.succeed(loaded) : snapshots.save(aggregateId, loaded)).pipe(
      Effect.tap((remembered) =>
        Effect.contextWith((context: Context.Context<never>) => {
          const evicted = states.set(aggregateId, remembered)
          if (read !== undefined) metrics.eventsReplayed.updateUnsafe(read, context)
          metrics.cacheSize.updateUnsafe(states.size(), context)
          if (evicted) metrics.evictions.updateUnsafe(1, context)
          return Effect.void
        })
      )
    )

  // Streams only grow, so a remembered state plus what came after it is the current
  // state, even if another process wrote those events. Versions have no gaps, so the
  // difference between two is the number of events read.
  const load = (aggregateId: string, finishTime?: DateTime.Utc) =>
    finishTime === undefined
      ? Effect.flatMap(start(aggregateId), (from) =>
        Effect.flatMap(
          replayFrom(from, aggregateId),
          (loaded) => remember(aggregateId, loaded, loaded.version - from.version)
        ))
      : replayFrom(initial, aggregateId, finishTime)

  // Measures from the moment the command asks for its turn on the stream.
  const timed = (type: string) => <X, E, R>(effect: Effect.Effect<X, E, R>): Effect.Effect<X, E, R> =>
    Effect.clockWith((clock) => {
      const started = clock.currentTimeNanosUnsafe()
      return Effect.onExit(effect, (exit) =>
        Effect.contextWith((context: Context.Context<never>) => {
          const elapsed = Duration.nanos(clock.currentTimeNanosUnsafe() - started)
          metrics.duration(type, outcomeOf(exit)).updateUnsafe(elapsed, context)
          return Effect.void
        }))
    })

  // Folds what a command just wrote into the remembered state, so the next command
  // finds nothing new to read. The events are decoded from what was stored, as a
  // replay would, so the state is the one a restart would rebuild. If they don't
  // decode, the state isn't remembered and the next load reports the error.
  const rememberWritten = (
    aggregateId: string,
    before: Loaded<Aggregate.State<A>>,
    written: ReadonlyArray<StoredEvent>,
    encoded: ReadonlyArray<Encoded>
  ) =>
    Effect.forEach(written, (_, index) =>
      Effect.map(
        decodeWritten(encoded[index]!.plain) as Effect.Effect<Aggregate.Event<A>, Schema.SchemaError>,
        (event): Decoded<Aggregate.Event<A>> => ({ event, subjects: encoded[index]!.subjects })
      )).pipe(
        Effect.flatMap((decoded) =>
          remember(aggregateId, decoded.reduce((acc, event, index) => fold(acc, event, written[index]!), before))
        ),
        Effect.ignore
      )

  // The result of a command that ran before, read back from the events it wrote.
  const previousResult = (aggregateId: string, previous: RecentCommand, correlationId: string) =>
    replay(
      streamOf(aggregateId, { afterVersion: previous.first - 1 }).pipe(
        Stream.takeWhile((stored) => stored.version <= previous.last)
      ),
      [] as Array<readonly [Aggregate.Event<A>, StoredEvent]>,
      (acc, { event }, stored) => {
        acc.push([event, stored])
        return acc
      }
    ).pipe(
      Effect.map((written): CommandResult<Aggregate.Event<A>> => {
        const last = written[written.length - 1]?.[1]
        return {
          aggregateName,
          aggregateId,
          version: last?.version ?? previous.last,
          position: last?.position ?? 0,
          events: written.map(([event]) => event),
          correlationId: written[0]?.[1].correlationId ?? correlationId
        }
      })
    )

  const findCommand = (commands: ReadonlyArray<RecentCommand>, id: string) => {
    for (let index = commands.length - 1; index >= 0; index--) {
      if (commands[index]!.id === id) return commands[index]
    }
    return undefined
  }

  const decide = Effect.fnUntraced(function*(
    type: string,
    aggregateId: string,
    payload: unknown,
    causation: { readonly correlationId: string; readonly causationId: string },
    metadata: Metadata,
    options: CommandOptions | undefined
  ) {
    const loaded = yield* load(aggregateId)
    const { position, state, version } = loaded
    // A command that ran before is answered first: its own events moved the
    // version past the one it expected.
    const commandId = options?.commandId
    if (commandId !== undefined) {
      const previous = findCommand(loaded.commands, commandId)
      if (previous !== undefined) return yield* previousResult(aggregateId, previous, causation.correlationId)
    }
    const expectedVersion = options?.expectedVersion
    if (expectedVersion !== undefined && expectedVersion !== version) {
      return yield* new WrongExpectedVersion({ aggregateName, aggregateId, expectedVersion, actualVersion: version })
    }
    const { handler } = aggregate.commands[type]!
    const events: ReadonlyArray<Aggregate.Event<A>> = Arr.ensure(
      yield* handler(state, payload, { aggregateId, version })
    )
    const { correlationId } = causation
    if (events.length === 0) {
      return { aggregateName, aggregateId, version, position, events, correlationId }
    }
    const encoded = yield* Effect.forEach(events, (event) => encodeEvent(event, aggregateId))
    const stored = yield* store.append({
      aggregateName,
      aggregateId,
      expectedVersion: version,
      events: encoded.map(({ event }) => event),
      ...causation,
      metadata
    })
    yield* rememberWritten(aggregateId, loaded, stored, encoded)
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
    given?: CommandOptions & {
      readonly correlationId?: string | undefined
      readonly causationId?: string | undefined
      readonly metadata?: Metadata | undefined
    }
  ) =>
    // Read outside the service's own context, which would hide the caller's.
    Effect.contextWith((callerContext: Context.Context<never>) => {
      const current = Context.get(callerContext, CurrentCausation)
      const correlationId = given?.correlationId ?? current?.correlationId
      const causationId = given?.causationId ?? current?.causationId
      // Without a cause the command starts its own correlation.
      const id = correlationId ?? causationId ?? Context.get(callerContext, IdGenerator)()
      const causation = { correlationId: correlationId ?? id, causationId: causationId ?? id }
      const commandId = given?.commandId
      const metadata = withCommandId(
        mergeMetadata(Context.get(callerContext, CurrentMetadata), given?.metadata),
        commandId
      )
      return decide(type, aggregateId, payload, causation, metadata, given).pipe(
        Effect.tapError((error) =>
          error instanceof ConcurrencyConflict ? Metric.update(metrics.conflicts, 1) : Effect.void
        ),
        Effect.retry({
          times: maxRetries,
          while: (error) => error instanceof ConcurrencyConflict,
          schedule: conflictBackoff
        }),
        Effect.provideContext(context),
        lockStream(aggregateId),
        (decided) => Effect.andThen(timeSlice, decided),
        timed(type),
        Effect.withSpan(`${aggregateName}.${type}`, {
          attributes: commandId === undefined
            ? { "aggregate.id": aggregateId, "cqrs.correlation_id": causation.correlationId }
            : {
              "aggregate.id": aggregateId,
              "cqrs.correlation_id": causation.correlationId,
              "cqrs.command_id": commandId
            }
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
      (aggregateId: string, payload: unknown, options?: CommandOptions) =>
        decodePayload(type, Schema.toType(command.payload), payload).pipe(
          Effect.flatMap((decoded) => execute(type, aggregateId, decoded, options))
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
      (entries, { event }, stored) => {
        entries.push({
          version: stored.version,
          timestamp: stored.timestamp,
          type: stored.type,
          correlationId: stored.correlationId,
          causationId: stored.causationId,
          metadata: stored.metadata,
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
