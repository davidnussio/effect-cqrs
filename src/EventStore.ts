/**
 * The append-only log every aggregate and view model is built on.
 *
 * Each event belongs to a stream, identified by aggregate name and id, and has
 * two coordinates: a `version` that orders it inside its stream and a `position`
 * that orders it across the whole store. Writers use the version for optimistic
 * concurrency, readers use the position as a durable cursor.
 *
 * @module
 */
import {
  Cause,
  Context,
  DateTime,
  Deferred,
  type Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
  SubscriptionRef
} from "effect"

/**
 * A schema change for a SQL table, with the table names filled in. Each module that
 * keeps a table exports its migrations in the order to apply them, for teams that
 * run them with their own tooling and turn off `migrate`.
 */
export interface Migration {
  readonly id: number
  readonly name: string
  readonly statements: ReadonlyArray<string>
}

export class EventStoreError extends Schema.TaggedError<EventStoreError>()("EventStoreError", {
  operation: Schema.String,
  cause: Schema.Defect()
}) {}

export class ConcurrencyConflict extends Schema.TaggedError<ConcurrencyConflict>()("ConcurrencyConflict", {
  aggregateName: Schema.String,
  aggregateId: Schema.String,
  expectedVersion: Schema.Int
}) {}

export const StoredEvent = Schema.Struct({
  position: Schema.Int,
  aggregateName: Schema.String,
  aggregateId: Schema.String,
  version: Schema.Int,
  type: Schema.String,
  /** The version of the event's schema the payload was written with. Events from before versions existed are `1`. */
  schemaVersion: Schema.Int,
  payload: Schema.Json,
  timestamp: Schema.DateTimeUtcFromMillis,
  /** Shared by everything that follows from one original request. */
  correlationId: Schema.NullOr(Schema.String),
  /** What directly caused the command that recorded the event: a request, a command or another event. */
  causationId: Schema.NullOr(Schema.String),
  /** What the command carried beside its payload, see `Metadata`. Events from before metadata existed have `{}`. */
  metadata: Schema.JsonObject
})
export type StoredEvent = typeof StoredEvent.Type

export interface NewEvent {
  readonly type: string
  readonly payload: Schema.Json
  /** Defaults to `1`. */
  readonly schemaVersion?: number | undefined
}

export interface AppendInput {
  readonly aggregateName: string
  readonly aggregateId: string
  /** The stream version the writer based its decision on, `0` for a new stream. */
  readonly expectedVersion: number
  readonly events: ReadonlyArray<NewEvent>
  readonly correlationId?: string | undefined
  readonly causationId?: string | undefined
  /** Recorded on every event of the append. Defaults to `{}`. */
  readonly metadata?: Schema.JsonObject | undefined
}

/**
 * Identifies an event wherever it is stored, as the `causationId` of what it causes:
 * `account/alice@4`. Name and id are URI-encoded, so a `/` or `@` inside one can't
 * make two events look alike.
 */
export const eventId = (event: Pick<StoredEvent, "aggregateName" | "aggregateId" | "version">): string =>
  `${encodeURIComponent(event.aggregateName)}/${encodeURIComponent(event.aggregateId)}@${event.version}`

/** The columns every adapter stores for a new event, with the defaults filled in. */
export const toStored = (
  input: AppendInput,
  event: NewEvent,
  index: number,
  position: number,
  timestamp: DateTime.Utc
): StoredEvent => ({
  position,
  aggregateName: input.aggregateName,
  aggregateId: input.aggregateId,
  version: input.expectedVersion + index + 1,
  type: event.type,
  schemaVersion: event.schemaVersion ?? 1,
  payload: event.payload,
  timestamp,
  correlationId: input.correlationId ?? null,
  causationId: input.causationId ?? null,
  metadata: input.metadata ?? {}
})

/**
 * Selects events by stream, type and time. Omitted fields match everything.
 * `startTime` is inclusive and `finishTime` is exclusive, so adding one
 * millisecond to a history timestamp selects up to and including that event.
 */
export interface EventFilter {
  readonly aggregateNames?: ReadonlyArray<string> | undefined
  readonly aggregateIds?: ReadonlyArray<string> | undefined
  readonly eventTypes?: ReadonlyArray<string> | undefined
  readonly startTime?: DateTime.Utc | undefined
  readonly finishTime?: DateTime.Utc | undefined
  readonly afterPosition?: number | undefined
  /**
   * For a filter on one stream: where to continue in it. Versions follow the stream's
   * own index, so resuming there costs the same however far the log has moved on.
   */
  readonly afterVersion?: number | undefined
}

/** A filter on exactly one stream, which a store can read through that stream's index. */
export const isOneStream = ({ aggregateIds, aggregateNames }: EventFilter): boolean =>
  aggregateNames?.length === 1 && aggregateIds?.length === 1

export class EventStore extends Context.Service<EventStore, {
  /** Appends atomically, failing if the stream moved past `expectedVersion`. */
  append(input: AppendInput): Effect.Effect<ReadonlyArray<StoredEvent>, ConcurrencyConflict | EventStoreError>
  /** Reads the events stored so far, ordered by position, then completes. */
  read(filter?: EventFilter): Stream.Stream<StoredEvent, EventStoreError>
  /** Reads the events stored so far, then keeps following new ones. Never completes. */
  subscribe(filter?: EventFilter): Stream.Stream<StoredEvent, EventStoreError>
  /** The position of the latest event, `0` when the store is empty. */
  readonly head: Effect.Effect<number, EventStoreError>
  /**
   * Emits the current latest position, then each newer one. Consumers can treat an
   * element as "there may be something new". Appends from this process are always
   * observed, appends from other processes when the backend announces them or the
   * store polls for them: see `MakeOptions`.
   */
  readonly changes: Stream.Stream<number>
}>()("effect-cqrs/EventStore") {}

/** What a storage adapter implements. {@link make} builds the rest on top. */
export interface Backend {
  readonly append: EventStore["Service"]["append"]
  readonly readBatch: (
    filter: EventFilter,
    afterPosition: number,
    limit: number
  ) => Effect.Effect<ReadonlyArray<StoredEvent>, EventStoreError>
  readonly head: Effect.Effect<number, EventStoreError>
  /**
   * Head positions written by other processes, as the database announces them. It
   * should start with the head once it listens, so nothing between two runs is
   * missed. When it fails or ends, the store logs it and runs it again a second later.
   */
  readonly changes?: Stream.Stream<number, EventStoreError> | undefined
}

export interface MakeOptions {
  /** Events per read. Defaults to 500. */
  readonly batchSize?: number | undefined
  /**
   * Reads the head this often, to notice appends from other processes when the
   * backend doesn't announce them, or as a safety net when it does. Off by default.
   */
  readonly pollInterval?: Duration.Input | undefined
}

/** How long a backend's `changes` rests after it failed, before it runs again. */
const restartDelay = "1 second"

const logUnlessInterrupted = (message: string) => <E>(cause: Cause.Cause<E>) =>
  Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : Effect.logWarning(message, cause)

/**
 * Builds a store on a backend. Watching for appends from other processes takes
 * fibers that live as long as the `Scope`, which `Layer.effect` provides.
 */
export const make = Effect.fnUntraced(function*(backend: Backend, options?: MakeOptions) {
  const { latest, service } = yield* assemble(backend, options)

  const advance = (position: number) =>
    SubscriptionRef.updateSome(latest, (current) => position > current ? Option.some(position) : Option.none())

  if (backend.changes !== undefined) {
    yield* Stream.runForEach(backend.changes, advance).pipe(
      Effect.catchCause(logUnlessInterrupted("Event store notifications stopped, listening again")),
      Effect.andThen(Effect.sleep(restartDelay)),
      Effect.forever,
      Effect.forkScoped
    )
  }
  if (options?.pollInterval !== undefined) {
    const interval = options.pollInterval
    yield* backend.head.pipe(
      Effect.flatMap(advance),
      Effect.catchCause(logUnlessInterrupted("Event store could not poll its head")),
      Effect.andThen(Effect.sleep(interval)),
      Effect.forever,
      Effect.forkScoped
    )
  }
  return service
})

/** The store without fibers of its own, which only sees appends made through it. */
const assemble = Effect.fnUntraced(function*(backend: Backend, options?: MakeOptions) {
  const batchSize = options?.batchSize ?? 500
  const latest = yield* SubscriptionRef.make(yield* backend.head)

  const append = Effect.fn("EventStore.append")((input: AppendInput) =>
    backend.append(input).pipe(
      Effect.tap((stored) => {
        const last = stored.at(-1)
        return last === undefined
          ? Effect.void
          : SubscriptionRef.updateSome(latest, (position) =>
            last.position > position ? Option.some(last.position) : Option.none())
      })
    )
  )

  const read = (filter: EventFilter = {}) =>
    Stream.paginate(
      { afterPosition: filter.afterPosition ?? 0, afterVersion: filter.afterVersion ?? 0 },
      ({ afterPosition, afterVersion }) =>
        backend.readBatch({ ...filter, afterVersion }, afterPosition, batchSize).pipe(
          Effect.map((events) => {
            const last = events.at(-1)
            return [
              events,
              last === undefined || events.length < batchSize
                ? Option.none()
                : Option.some({ afterPosition: last.position, afterVersion: last.version })
            ] as const
          })
        )
    )

  const subscribe = (filter: EventFilter = {}) =>
    Stream.unwrap(Effect.gen(function*() {
      const cursor = yield* Ref.make({ position: filter.afterPosition ?? 0, version: filter.afterVersion ?? 0 })
      return SubscriptionRef.changes(latest).pipe(
        Stream.filterEffect((position) => Effect.map(Ref.get(cursor), (seen) => position > seen.position)),
        Stream.flatMap((position) =>
          Stream.unwrap(
            Effect.map(
              Ref.get(cursor),
              (seen) => read({ ...filter, afterPosition: seen.position, afterVersion: seen.version })
            )
          ).pipe(
            Stream.tap((event) => Ref.set(cursor, { position: event.position, version: event.version })),
            // Events the filter skipped must not trigger another read.
            Stream.ensuring(
              Ref.update(cursor, (seen) => seen.position >= position ? seen : { ...seen, position })
            )
          )
        )
      )
    }))

  return {
    latest,
    service: EventStore.of({
      append,
      read,
      subscribe,
      head: backend.head,
      changes: SubscriptionRef.changes(latest)
    })
  }
})

type AppendExit = Exit.Exit<ReadonlyArray<StoredEvent>, ConcurrencyConflict | EventStoreError>

/** How often a lone writer with `gather` looks for others to share a commit with. */
const probeEvery = 16

interface Pending {
  readonly input: AppendInput
  readonly result: Deferred.Deferred<ReadonlyArray<StoredEvent>, ConcurrencyConflict | EventStoreError>
}

/**
 * Group commit for a backend. An append that finds nothing being written goes
 * straight through `appendGroup`, in its own fiber. Appends that arrive meanwhile
 * wait, then go together into the next call, which writes them in one transaction
 * and returns an exit for each, in order: however many wait, they pay for one
 * commit.
 *
 * A group holds at most one append per stream, so they can't conflict with each
 * other, and at most `maxEvents` events unless a single append has more. An append
 * left out waits for the next group, where it comes first, so appends to a stream
 * are written in the order they came. Groups of waiting appends are written by a
 * fiber of the `Scope`.
 */
export const groupCommit = Effect.fnUntraced(function*(
  appendGroup: (inputs: ReadonlyArray<AppendInput>) => Effect.Effect<ReadonlyArray<AppendExit>>,
  options?: {
    /** Closes a group once it holds this many events. Defaults to 10,000. */
    readonly maxEvents?: number | undefined
    /**
     * Lets other fibers run before writing now and then, and before every write
     * while that brings company, so appends that become ready together share a
     * commit even on a synchronous driver, where nobody would otherwise wait. Worth
     * it where a commit costs more than a turn of the scheduler, such as an fsync.
     * Without it, a writer only yields after a write that had others waiting.
     */
    readonly gather?: boolean | undefined
  }
) {
  const maxEvents = options?.maxEvents ?? 10_000
  const gather = options?.gather ?? false
  const scope = yield* Effect.scope
  let writing = false
  let waiting: Array<Pending> = []

  const nextGroup = () => {
    const group: Array<Pending> = []
    const rest: Array<Pending> = []
    const streams = new Set<string>()
    let events = 0
    for (const item of waiting) {
      const key = streamKey(item.input.aggregateName, item.input.aggregateId)
      if (streams.has(key) || (group.length > 0 && events + item.input.events.length > maxEvents)) {
        rest.push(item)
      } else {
        group.push(item)
        streams.add(key)
        events += item.input.events.length
      }
    }
    waiting = rest
    return group
  }

  const complete = (group: ReadonlyArray<Pending>, exit: Exit.Exit<ReadonlyArray<AppendExit>>) =>
    Effect.forEach(group, ({ result }, index) =>
      Deferred.done(
        result,
        Exit.isSuccess(exit)
          ? exit.value[index] ?? Exit.die(new Error("A group commit lost an append"))
          : Exit.failCause(exit.cause)
      ), { discard: true })

  const drain = Effect.gen(function*() {
    while (true) {
      const group = nextGroup()
      if (group.length === 0) {
        writing = false
        return
      }
      yield* Effect.exit(writeGroup(group))
    }
  })

  // A synchronous driver writes before other fibers get to run, so they never wait
  // and never share a commit. Once others did wait, or joined a writer that let them
  // run first, the next writer lets them run first too. With `gather`, a writer
  // that finds no company does so on every `probeEvery`th append, to notice when
  // others start writing without slowing down one that writes alone.
  let contended = false
  let lastGroup = 0
  let appends = 0

  const release = Effect.suspend(() => {
    contended = waiting.length > 0 || lastGroup > 1
    if (waiting.length > 0) return Effect.asVoid(Effect.forkIn(drain, scope))
    writing = false
    return Effect.void
  })

  const writeGroup = (group: ReadonlyArray<Pending>) =>
    appendGroup(group.map(({ input }) => input)).pipe(Effect.onExit((exit) => complete(group, exit)))

  const lead = (input: AppendInput) => {
    if (!contended && !(gather && ++appends % probeEvery === 0)) {
      lastGroup = 1
      return Effect.flatMap(
        appendGroup([input]),
        ([exit]) => exit ?? Effect.die(new Error("A group commit lost an append"))
      )
    }
    const result = Deferred.makeUnsafe<ReadonlyArray<StoredEvent>, ConcurrencyConflict | EventStoreError>()
    waiting.push({ input, result })
    return Effect.yieldNow.pipe(
      Effect.andThen(Effect.suspend(() => {
        const group = nextGroup()
        lastGroup = group.length
        return writeGroup(group)
      })),
      Effect.andThen(Deferred.await(result))
    )
  }

  // Writers still waiting when the store closes don't hang.
  yield* Effect.addFinalizer(() =>
    Effect.suspend(() => {
      const left = waiting
      waiting = []
      return Effect.forEach(left, ({ result }) => Deferred.interrupt(result), { discard: true })
    })
  )

  return (input: AppendInput): Effect.Effect<ReadonlyArray<StoredEvent>, ConcurrencyConflict | EventStoreError> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        if (!writing) {
          writing = true
          return restore(lead(input)).pipe(Effect.ensuring(release))
        }
        const result = Deferred.makeUnsafe<ReadonlyArray<StoredEvent>, ConcurrencyConflict | EventStoreError>()
        waiting.push({ input, result })
        return restore(Deferred.await(result))
      })
    )
})

const matches = (filter: EventFilter, event: StoredEvent): boolean =>
  (filter.aggregateNames?.includes(event.aggregateName) ?? true) &&
  (filter.aggregateIds?.includes(event.aggregateId) ?? true) &&
  (filter.eventTypes?.includes(event.type) ?? true) &&
  (filter.startTime === undefined || !DateTime.isLessThan(event.timestamp, filter.startTime)) &&
  (filter.finishTime === undefined || DateTime.isLessThan(event.timestamp, filter.finishTime))

/** The index of the first event past `position`, in events sorted by position. */
const firstAfter = (events: ReadonlyArray<StoredEvent>, position: number): number => {
  let low = 0
  let high = events.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if (events[middle]!.position <= position) low = middle + 1
    else high = middle
  }
  return low
}

const streamKey = (aggregateName: string, aggregateId: string) => JSON.stringify([aggregateName, aggregateId])

export const makeMemory: Effect.Effect<EventStore["Service"], EventStoreError> = Effect.gen(function*() {
  const events: Array<StoredEvent> = []
  const streams = new Map<string, Array<StoredEvent>>()

  // Every command reads one stream, which shouldn't cost a walk through the whole log.
  const candidates = (filter: EventFilter): ReadonlyArray<StoredEvent> =>
    isOneStream(filter)
      ? streams.get(streamKey(filter.aggregateNames![0]!, filter.aggregateIds![0]!)) ?? []
      : events

  const { service } = yield* assemble({
    append: Effect.fnUntraced(function*(input) {
      const timestamp = yield* DateTime.now
      const key = streamKey(input.aggregateName, input.aggregateId)
      const stream = streams.get(key) ?? []
      const version = stream.length
      if (version !== input.expectedVersion) {
        return yield* new ConcurrencyConflict({
          aggregateName: input.aggregateName,
          aggregateId: input.aggregateId,
          expectedVersion: input.expectedVersion
        })
      }
      const stored = input.events.map((event, index) =>
        toStored(input, event, index, events.length + index + 1, timestamp)
      )
      for (const event of stored) {
        events.push(event)
        stream.push(event)
      }
      streams.set(key, stream)
      return stored
    }),
    readBatch: (filter, afterPosition, limit) =>
      Effect.sync(() => {
        const source = candidates(filter)
        const batch: Array<StoredEvent> = []
        for (let index = firstAfter(source, afterPosition); index < source.length && batch.length < limit; index++) {
          const event = source[index]!
          if (matches(filter, event)) batch.push(event)
        }
        return batch
      }),
    head: Effect.sync(() => events.length)
  })
  return service
})

/** Keeps events in process memory. They do not survive a restart. */
export const layerMemory: Layer.Layer<EventStore, EventStoreError> = Layer.effect(EventStore, makeMemory)
