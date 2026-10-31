/**
 * The append-only log every aggregate and view model is built on.
 *
 * Each event belongs to a stream, identified by aggregate name and id, and has
 * two coordinates: a `version` that orders it inside its stream and a `position`
 * that orders it across the whole store. Writers use the version for optimistic
 * concurrency, readers use the position as a durable cursor.
 */
import { Context, DateTime, Effect, Layer, Option, Ref, Schema, Stream, SubscriptionRef } from "effect"

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
  payload: Schema.Json,
  timestamp: Schema.DateTimeUtcFromMillis
})
export type StoredEvent = typeof StoredEvent.Type

export interface NewEvent {
  readonly type: string
  readonly payload: Schema.Json
}

export interface AppendInput {
  readonly aggregateName: string
  readonly aggregateId: string
  /** The stream version the writer based its decision on, `0` for a new stream. */
  readonly expectedVersion: number
  readonly events: ReadonlyArray<NewEvent>
}

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
   * Emits the current latest position, then each newly appended one. Consumers
   * can treat an element as "there may be something new": appends from other
   * processes are not observed.
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
}

export const make = Effect.fnUntraced(function*(backend: Backend, options?: { readonly batchSize?: number }) {
  const batchSize = options?.batchSize ?? 500
  const latest = yield* SubscriptionRef.make(yield* backend.head)

  const append = Effect.fn("EventStore.append")((input: AppendInput) =>
    backend.append(input).pipe(
      Effect.tap((stored) => {
        const last = stored.at(-1)
        return last === undefined
          ? Effect.void
          : SubscriptionRef.update(latest, (position) => Math.max(position, last.position))
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
            Effect.map(Ref.get(cursor), (seen) =>
              read({ ...filter, afterPosition: seen.position, afterVersion: seen.version }))
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

  return EventStore.of({
    append,
    read,
    subscribe,
    head: backend.head,
    changes: SubscriptionRef.changes(latest)
  })
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

  return yield* make({
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
      const stored = input.events.map((event, index): StoredEvent => ({
        position: events.length + index + 1,
        aggregateName: input.aggregateName,
        aggregateId: input.aggregateId,
        version: version + index + 1,
        type: event.type,
        payload: event.payload,
        timestamp
      }))
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
})

/** Keeps events in process memory. They do not survive a restart. */
export const layerMemory: Layer.Layer<EventStore, EventStoreError> = Layer.effect(EventStore, makeMemory)
