/**
 * The read side: projections of events into whatever storage a query needs.
 *
 * Moleculer-cqrs pushes events to view models over the broker, so an event sent
 * while a service is down is lost until someone replays. Here each view model
 * pulls from the store from its own checkpoint, so it can't miss anything, and
 * delivery is at least once: a handler can see an event again after a crash and
 * should be idempotent.
 */
import { Context, Effect, Layer, Option, Ref, Schedule, Schema, Semaphore, Stream, SubscriptionRef } from "effect"
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
const retrySchedule = Schedule.min([Schedule.exponential("100 millis"), Schedule.spaced("30 seconds")])

const makeRunner = Effect.fnUntraced(function*<V extends Any>(viewModel: V) {
  const store = yield* EventStore
  const checkpoints = yield* Checkpoints
  const context = yield* Effect.context<Context<V>>()
  const provide = <A, E>(effect: Effect.Effect<A, E, any>) => Effect.provideContext(effect, context) as Effect.Effect<A, E>
  const semaphore = yield* Semaphore.make(1)
  const name = viewModel.name

  const fail = (position: number) => (cause: unknown) => new ViewModelError({ viewModel: name, position, cause })

  const position = yield* SubscriptionRef.make(yield* checkpoints.get(name))
  const lastError = yield* Ref.make(Option.none<ViewModelError>())

  const decoders = new Map(viewModel.aggregates.map((aggregate) => [aggregate.name, makeEventDecoder(aggregate)]))
  const filter: EventFilter = {
    aggregateNames: Array.from(decoders.keys()),
    eventTypes: Object.keys(viewModel.handlers)
  }

  const handle = (stored: StoredEvent) =>
    Effect.gen(function*() {
      const event = yield* decoders.get(stored.aggregateName)!(stored)
      yield* viewModel.handlers[stored.type]!(event as never, stored)
    }).pipe(
      Effect.mapError(fail(stored.position)),
      Effect.catchDefect((defect) => Effect.fail(fail(stored.position)(defect)))
    )

  const advance = (to: number) =>
    checkpoints.set(name, to).pipe(
      Effect.mapError(fail(0)),
      Effect.andThen(SubscriptionRef.set(position, to))
    )

  // Progress is saved per batch, so a crash repeats at most one batch.
  const consume = (eventFilter: EventFilter, onBatch: (size: number) => Effect.Effect<void>) =>
    store.read(eventFilter).pipe(
      Stream.mapError(fail(0)),
      Stream.runForEachArray((batch) =>
        Effect.forEach(batch, handle, { discard: true }).pipe(
          Effect.andThen(advance(batch[batch.length - 1].position)),
          Effect.andThen(onBatch(batch.length))
        )
      )
    )

  // Moves past the end of the log even when the last events were not for this view model.
  const settle = (head: number) =>
    SubscriptionRef.get(position).pipe(
      Effect.flatMap((current) => current < head ? advance(head) : Effect.void)
    )

  const catchUp = Effect.gen(function*() {
    const head = yield* store.head.pipe(Effect.mapError(fail(0)))
    const afterPosition = yield* SubscriptionRef.get(position)
    if (head <= afterPosition) return
    yield* consume({ ...filter, afterPosition }, () => Effect.void)
    yield* settle(head)
  }).pipe(semaphore.withPermit, provide)

  yield* store.changes.pipe(
    Stream.filterEffect((latest) => Effect.map(SubscriptionRef.get(position), (current) => latest > current)),
    Stream.runForEach(() =>
      catchUp.pipe(
        Effect.tapError((error) =>
          Ref.set(lastError, Option.some(error)).pipe(
            Effect.andThen(Effect.logError(`View model ${name} failed, retrying`, error))
          )
        ),
        Effect.retry(retrySchedule),
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
      const runners = yield* Effect.forEach(viewModels, (viewModel) => makeRunner(viewModel))

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
