/**
 * The read side: projections of events into whatever storage a query needs.
 *
 * Moleculer-cqrs pushes events to view models over the broker, so an event sent
 * while a service is down is lost until someone replays. Here each view model
 * pulls from the store from its own checkpoint, so it can't miss anything, and
 * delivery is at least once: a handler can see an event again after a crash and
 * should be idempotent.
 */
import {
  Context,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
  Schedule,
  Schema,
  Semaphore,
  Stream,
  SubscriptionRef
} from "effect"
import type { DateTime } from "effect"
import type * as Aggregate from "./Aggregate.ts"
import { makeEventDecoder } from "./AggregateService.ts"
import { Checkpoints } from "./Checkpoints.ts"
import { type EventFilter, EventStore, type EventStoreError, type StoredEvent } from "./EventStore.ts"

export class UnknownViewModel extends Schema.TaggedError<UnknownViewModel>()("UnknownViewModel", {
  name: Schema.String
}) {}

export class ViewModelError extends Schema.TaggedError<ViewModelError>()("ViewModelError", {
  viewModel: Schema.String,
  /** The event being processed, `0` when the failure happened outside any event. */
  position: Schema.Int,
  cause: Schema.Defect()
}) {}

export type Handlers<Event extends Aggregate.Tagged> = {
  readonly [Tag in Event["_tag"]]?: (
    event: Extract<Event, { readonly _tag: Tag }>,
    meta: Aggregate.EventMeta
  ) => Effect.Effect<unknown, any, any>
}

type HandlerError<H> = {
  readonly [K in keyof H]-?: NonNullable<H[K]> extends (...args: any) => Effect.Effect<any, infer E, any> ? E : never
}[keyof H]

type HandlerContext<H> = {
  readonly [K in keyof H]-?: NonNullable<H[K]> extends (...args: any) => Effect.Effect<any, any, infer R> ? R : never
}[keyof H]

export interface ViewModel<out Name extends string, out E, out R> {
  readonly name: Name
  readonly aggregates: ReadonlyArray<Aggregate.Any>
  readonly handlers: Record<string, (event: never, meta: Aggregate.EventMeta) => Effect.Effect<unknown, E, R>>
  /** Clears everything the view model stored, before a replay rebuilds it. */
  readonly dispose: Effect.Effect<void, E, R>
}

export type Any = ViewModel<string, any, any>

export type Context<V extends Any> = V extends ViewModel<any, any, infer R> ? R : never

/** Handlers are keyed by event tag, and events without one are skipped. */
export const make = <
  const Name extends string,
  const Aggregates extends ReadonlyArray<Aggregate.Any>,
  const H extends Handlers<Aggregate.Event<Aggregates[number]>>,
  E = never,
  R = never
>(options: {
  readonly name: Name
  readonly aggregates: Aggregates
  readonly handlers: H
  readonly dispose: Effect.Effect<void, E, R>
}): ViewModel<Name, E | HandlerError<H>, R | HandlerContext<H>> => options as any

export interface ReplayOptions {
  readonly viewModels: ReadonlyArray<string>
  readonly startTime?: DateTime.Utc | undefined
  readonly finishTime?: DateTime.Utc | undefined
}

export interface ReplayResult {
  readonly viewModels: ReadonlyArray<string>
  readonly eventCount: number
}

export interface Status {
  readonly name: string
  readonly position: number
  /** The failure the view model is retrying, while it is stalled on one. */
  readonly lastError: Option.Option<ViewModelError>
}

export class ViewModels extends Context.Service<ViewModels, {
  /**
   * Disposes the named view models and rebuilds them from their events. A time
   * window limits which events are replayed: the view model then resumes from
   * the present and events outside the window stay out of it.
   */
  replay(options: ReplayOptions): Effect.Effect<ReplayResult, UnknownViewModel | ViewModelError>
  /**
   * Waits until every view model has processed `position`, defaulting to the
   * latest event. Pair it with `CommandResult.position` to read your own writes.
   * A view model stalled on a failure doesn't catch up, so consider a timeout.
   */
  caughtUp(position?: number): Effect.Effect<void, EventStoreError>
  readonly status: Effect.Effect<ReadonlyArray<Status>>
}>()("effect-cqrs/ViewModels") {}

// Stalled view models keep their place so events are never processed out of order.
const retryDelay = (attempt: number) => Duration.millis(Math.min(100 * 2 ** attempt, 30_000))

// Following the log moves a few events at a time, and a checkpoint for each of those
// moves is a commit per view model per event. Saving less often only means that a
// crash repeats more, which handlers already have to cope with.
const checkpointInterval = 500

// How far behind a view model can be and still read the tail everyone shares. It is
// one page of the store, so a shared read never has to page.
const tailSize = 500

/**
 * Reads the end of the log once for all view models. When they are caught up they
 * wake on the same append and want the same few events, so each reading them for
 * itself is most of what a view model costs.
 */
const makeTail = (store: EventStore["Service"], viewModels: ReadonlyArray<Any>) => {
  const filter: EventFilter = {
    aggregateNames: Array.from(new Set(viewModels.flatMap((viewModel) => viewModel.aggregates.map(({ name }) => name)))),
    eventTypes: Array.from(new Set(viewModels.flatMap((viewModel) => Object.keys(viewModel.handlers))))
  }
  const decoded = new WeakMap<StoredEvent, unknown>()
  let last: { readonly afterPosition: number; readonly latest: number; readonly events: ReadonlyArray<StoredEvent> } | undefined

  return {
    read: (afterPosition: number, latest: number) =>
      Effect.suspend(() =>
        last?.afterPosition === afterPosition && last.latest === latest
          ? Effect.succeed(last.events)
          : Stream.runCollect(store.read({ ...filter, afterPosition })).pipe(
            Effect.tap((events) => Effect.sync(() => void (last = { afterPosition, latest, events })))
          )
      ),
    decoded
  }
}

type Tail = ReturnType<typeof makeTail>

const makeRunner = Effect.fnUntraced(function*<V extends Any>(viewModel: V, tail: Tail) {
  const store = yield* EventStore
  const checkpoints = yield* Checkpoints
  const context = yield* Effect.context<Context<V>>()
  const provide = <A, E>(effect: Effect.Effect<A, E, any>) => Effect.provideContext(effect, context) as Effect.Effect<A, E>
  const semaphore = yield* Semaphore.make(1)
  const name = viewModel.name

  const fail = (position: number) => (cause: unknown) => new ViewModelError({ viewModel: name, position, cause })

  const checkpoint = yield* checkpoints.get(name)
  const position = yield* SubscriptionRef.make(checkpoint)
  const saved = yield* Ref.make(checkpoint)
  const lastError = yield* Ref.make(Option.none<ViewModelError>())
  const stall = yield* Ref.make({ position: -1, attempts: 0 })

  const decoders = new Map(viewModel.aggregates.map((aggregate) => [aggregate.name, makeEventDecoder(aggregate)]))
  const filter: EventFilter = {
    aggregateNames: Array.from(decoders.keys()),
    eventTypes: Object.keys(viewModel.handlers)
  }
  const wants = (stored: StoredEvent) =>
    decoders.has(stored.aggregateName) && Object.hasOwn(viewModel.handlers, stored.type)

  const decode = (stored: StoredEvent) => decoders.get(stored.aggregateName)!(stored)

  // Events from the tail are the same objects for everyone, so the first to decode one
  // does it for the rest.
  const decodeShared = (stored: StoredEvent) =>
    tail.decoded.has(stored)
      ? Effect.succeed(tail.decoded.get(stored))
      : decode(stored).pipe(Effect.tap((event) => Effect.sync(() => void tail.decoded.set(stored, event))))

  const handle = (decodeEvent: (stored: StoredEvent) => Effect.Effect<unknown, unknown>) => (stored: StoredEvent) =>
    Effect.gen(function*() {
      const event = yield* decodeEvent(stored)
      yield* viewModel.handlers[stored.type]!(event as never, stored)
    }).pipe(
      Effect.mapError(fail(stored.position)),
      Effect.catchDefect((defect) => Effect.fail(fail(stored.position)(defect)))
    )

  const save = (to: number) =>
    checkpoints.set(name, to).pipe(
      Effect.mapError(fail(0)),
      Effect.andThen(Ref.set(saved, to))
    )

  const advance = (to: number) =>
    Ref.get(saved).pipe(
      Effect.flatMap((last) => to < last || to - last >= checkpointInterval ? save(to) : Effect.void),
      Effect.andThen(SubscriptionRef.set(position, to))
    )

  // Finalizers run last to first, so this one sees the fiber below already stopped.
  yield* Effect.addFinalizer(() => SubscriptionRef.get(position).pipe(Effect.flatMap(save), Effect.ignore))

  // A crash repeats at most what came after the last saved checkpoint: two batches.
  const processBatch = (batch: ReadonlyArray<StoredEvent>, decodeEvent: typeof decode | typeof decodeShared) =>
    Effect.forEach(batch, handle(decodeEvent), { discard: true }).pipe(
      Effect.andThen(advance(batch[batch.length - 1]!.position))
    )

  const consume = (eventFilter: EventFilter, onBatch: (size: number) => Effect.Effect<void>) =>
    store.read(eventFilter).pipe(
      Stream.mapError(fail(0)),
      Stream.runForEachArray((batch) => processBatch(batch, decode).pipe(Effect.andThen(onBatch(batch.length))))
    )

  const follow = (afterPosition: number, latest: number) =>
    tail.read(afterPosition, latest).pipe(
      Effect.mapError(fail(0)),
      Effect.map((events) => events.filter(wants)),
      Effect.flatMap((events) => events.length === 0 ? Effect.void : processBatch(events, decodeShared))
    )

  // Moves past the end of the log even when the last events were not for this view model.
  const settle = (head: number) =>
    SubscriptionRef.get(position).pipe(
      Effect.flatMap((current) => current < head ? advance(head) : Effect.void)
    )

  const catchUp = (latest: number) =>
    Effect.gen(function*() {
      const afterPosition = yield* SubscriptionRef.get(position)
      if (latest <= afterPosition) return
      if (latest - afterPosition <= tailSize) yield* follow(afterPosition, latest)
      else yield* consume({ ...filter, afterPosition }, () => Effect.void)
      yield* settle(latest)
    }).pipe(
      semaphore.withPermit,
      provide
    )

  // Failing again at the same place doubles the delay. Having moved on means that
  // stall is over, so the next one starts from the shortest delay.
  const backOff = (error: ViewModelError) =>
    Effect.gen(function*() {
      const current = yield* SubscriptionRef.get(position)
      const attempt = yield* Ref.modify(stall, (last) => {
        const attempts = last.position === current ? last.attempts + 1 : 0
        return [attempts, { position: current, attempts }] as const
      })
      yield* Ref.set(lastError, Option.some(error))
      yield* Effect.logError(`View model ${name} failed, retrying`, error)
      yield* Effect.sleep(retryDelay(attempt))
    })

  yield* store.changes.pipe(
    Stream.filterEffect((latest) => Effect.map(SubscriptionRef.get(position), (current) => latest > current)),
    Stream.runForEach((latest) =>
      catchUp(latest).pipe(
        Effect.tapError(backOff),
        Effect.retry(Schedule.forever),
        Effect.andThen(Ref.set(lastError, Option.none()))
      )
    ),
    Effect.forkScoped
  )

  const replay = (range: Pick<ReplayOptions, "startTime" | "finishTime">) =>
    Effect.gen(function*() {
      // The checkpoint is cleared first: a crash in between then replays over
      // old data, which is safe for idempotent handlers, instead of leaving a
      // disposed view model that resumes from a stale position.
      yield* advance(0)
      yield* viewModel.dispose.pipe(Effect.mapError(fail(0)))
      const head = yield* store.head.pipe(Effect.mapError(fail(0)))
      const count = yield* Ref.make(0)
      yield* consume({ ...filter, ...range }, (size) => Ref.update(count, (n) => n + size))
      yield* settle(head)
      return yield* Ref.get(count)
    }).pipe(semaphore.withPermit, provide)

  const caughtUp = (target: number) =>
    SubscriptionRef.changes(position).pipe(
      Stream.filter((current) => current >= target),
      Stream.runHead,
      Effect.asVoid
    )

  const status = Effect.gen(function*() {
    return {
      name,
      position: yield* SubscriptionRef.get(position),
      lastError: yield* Ref.get(lastError)
    } satisfies Status
  })

  return { name, replay, caughtUp, status }
})

/** Starts the view models in the background for as long as the layer lives. */
export const layer = <const VMs extends ReadonlyArray<Any>>(
  viewModels: VMs
): Layer.Layer<ViewModels, EventStoreError, EventStore | Checkpoints | Context<VMs[number]>> =>
  Layer.effect(
    ViewModels,
    Effect.gen(function*() {
      const store = yield* EventStore
      const tail = makeTail(store, viewModels)
      const runners = yield* Effect.forEach(viewModels, (viewModel) => makeRunner(viewModel, tail))

      const replay = Effect.fn("ViewModels.replay")(function*(options: ReplayOptions) {
        const selected = yield* Effect.forEach(options.viewModels, (name) => {
          const runner = runners.find((runner) => runner.name === name)
          return runner === undefined ? new UnknownViewModel({ name }) : Effect.succeed(runner)
        })
        const counts = yield* Effect.forEach(
          selected,
          (runner) => runner.replay({ startTime: options.startTime, finishTime: options.finishTime }),
          { concurrency: "unbounded" }
        )
        return {
          viewModels: options.viewModels,
          eventCount: counts.reduce((total, count) => total + count, 0)
        }
      })

      const caughtUp = Effect.fn("ViewModels.caughtUp")(function*(position?: number) {
        const target = position ?? (yield* store.head)
        yield* Effect.forEach(runners, (runner) => runner.caughtUp(target), {
          concurrency: "unbounded",
          discard: true
        })
      })

      return ViewModels.of({
        replay,
        caughtUp,
        status: Effect.forEach(runners, (runner) => runner.status)
      })
    })
  )
