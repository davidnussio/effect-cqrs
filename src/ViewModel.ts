/**
 * The read side: projections of events into whatever storage a query needs.
 *
 * Moleculer-cqrs pushes events to view models over the broker, so an event sent
 * while a service is down is lost until someone replays. Here each view model
 * pulls from the store from its own checkpoint, so it can't miss anything, and
 * delivery is at least once: a handler can see an event again after a crash and
 * should be idempotent, unless the view model has a `transaction`.
 *
 * @module
 */
import {
  Cause,
  Context,
  DateTime,
  Duration,
  Effect,
  Fiber,
  Layer,
  Metric,
  Option,
  Ref,
  Schedule,
  Schema,
  Semaphore,
  Stream,
  SubscriptionRef
} from "effect"
import { SqlClient } from "effect/sql"
import type * as Aggregate from "./Aggregate.ts"
import { makeEventDecoder } from "./AggregateService.ts"
import * as Causation from "./Causation.ts"
import { Checkpoints } from "./Checkpoints.ts"
import { type EventFilter, eventId, EventStore, type EventStoreError, type StoredEvent } from "./EventStore.ts"
import { withoutAmbient } from "./internal/ambient.ts"
import * as Leases from "./Leases.ts"
import * as ParkedEvents from "./ParkedEvents.ts"
import * as PersonalData from "./PersonalData.ts"

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
  /** Erases what the view model stored about a forgotten subject, see `PersonalData.forget`. */
  readonly onForget?: ((subject: string, meta: Aggregate.EventMeta) => Effect.Effect<unknown, E, R>) | undefined
  /** What happens to an event its handler keeps failing on. Defaults to {@link stall}. */
  readonly onFailure?: FailurePolicy | undefined
  /** Runs each batch of handlers together with the checkpoint they reach, see {@link sqlTransaction}. */
  readonly transaction?: Transaction<R> | undefined
}

/**
 * Wraps a batch of handlers and the checkpoint after them, so that they commit
 * or roll back together.
 */
export type Transaction<R> = (effect: Effect.Effect<void, ViewModelError>) => Effect.Effect<void, unknown, R>

/**
 * A transaction of the `SqlClient` in context. The read model and the checkpoints
 * have to use that same client for their writes to join it.
 */
export const sqlTransaction: Transaction<SqlClient.SqlClient> = (effect) =>
  SqlClient.SqlClient.use((sql) => sql.withTransaction(effect))

/**
 * What a view model does about an event whose handler keeps failing.
 *
 * - `Stall` retries it forever, with a growing delay, and nothing after it is
 *   processed in the meantime. Nothing is ever skipped.
 * - `Park` retries it until it has failed `attempts` times, then records it in
 *   `ParkedEvents` and moves on. `ViewModels.retryParked` handles it again later.
 */
export type FailurePolicy =
  | { readonly _tag: "Stall" }
  | { readonly _tag: "Park"; readonly attempts: number }

/** Retries a failing event until it succeeds, holding everything after it back. The default. */
export const stall: FailurePolicy = { _tag: "Stall" }

/** Gives up on an event after it failed `attempts` times, and goes on with the next ones. */
export const park = (options: { readonly attempts: number }): FailurePolicy => ({
  _tag: "Park",
  attempts: Math.max(1, Math.floor(options.attempts))
})

export type Any = ViewModel<string, any, any>

export type Context<V extends Any> = V extends ViewModel<any, any, infer R> ? R : never

/**
 * Handlers are keyed by event tag, and events without one are skipped.
 *
 * A view model that keeps personal data handles `onForget`, which runs when
 * `PersonalData.forget` records that a subject was forgotten, in order with the
 * events around it. Events the view model reads afterwards already carry the
 * replacements, so it only has to erase what it stored before.
 *
 * `onFailure` decides what happens to an event that keeps failing, see
 * {@link FailurePolicy}. With `transaction`, each batch of handlers runs in one
 * transaction with the checkpoint it reaches, so a crash never repeats an event.
 */
export const make = <
  const Name extends string,
  const Aggregates extends ReadonlyArray<Aggregate.Any>,
  const H extends Handlers<Aggregate.Event<Aggregates[number]>>,
  E = never,
  R = never,
  FE = never,
  FR = never,
  TR = never
>(options: {
  readonly name: Name
  readonly aggregates: Aggregates
  readonly handlers: H
  readonly dispose: Effect.Effect<void, E, R>
  readonly onForget?: ((subject: string, meta: Aggregate.EventMeta) => Effect.Effect<unknown, FE, FR>) | undefined
  readonly onFailure?: FailurePolicy | undefined
  readonly transaction?: Transaction<TR> | undefined
}): ViewModel<Name, E | FE | HandlerError<H>, R | FR | TR | HandlerContext<H>> => options as any

const isForgetting = (stored: StoredEvent) =>
  stored.aggregateName === PersonalData.streamName && stored.type === PersonalData.subjectForgottenType

/** What a view model reads from the store: its events, and forgotten subjects when it erases them. */
const filterOf = (viewModels: ReadonlyArray<Any>): EventFilter => {
  const forgets = viewModels.some((viewModel) => viewModel.onForget !== undefined)
  return {
    aggregateNames: Array.from(
      new Set([
        ...viewModels.flatMap((viewModel) => viewModel.aggregates.map(({ name }) => name)),
        ...(forgets ? [PersonalData.streamName] : [])
      ])
    ),
    eventTypes: Array.from(
      new Set([
        ...viewModels.flatMap((viewModel) => Object.keys(viewModel.handlers)),
        ...(forgets ? [PersonalData.subjectForgottenType] : [])
      ])
    )
  }
}

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
  /** How many events the view model parked and skips, see {@link park}. */
  readonly parked: number
  /** Whether this process holds the view model's lease, and so runs it. See `Leases`. */
  readonly holdsLease: boolean
}

export interface RetryParkedOptions {
  readonly viewModel: string
  /** The positions of the events to retry. Defaults to every event the view model parked. */
  readonly positions?: ReadonlyArray<number> | undefined
}

export interface RetryParkedResult {
  /** The positions of the events that were handled, and are no longer parked. */
  readonly succeeded: ReadonlyArray<number>
  /** The events that failed again, as they are parked now. */
  readonly failed: ReadonlyArray<ParkedEvents.ParkedEvent>
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
   * View models that another process holds are not waited for.
   */
  caughtUp(position?: number): Effect.Effect<void, EventStoreError>
  readonly status: Effect.Effect<ReadonlyArray<Status>>
  /** The events the view models of this layer parked, or those of one of them. */
  parked(viewModel?: string): Effect.Effect<ReadonlyArray<ParkedEvents.ParkedEvent>, UnknownViewModel | EventStoreError>
  /**
   * Handles parked events again, in order of position. Those that succeed are no
   * longer parked, and the others stay parked with the new failure.
   */
  retryParked(
    options: RetryParkedOptions
  ): Effect.Effect<RetryParkedResult, UnknownViewModel | ViewModelError | EventStoreError>
}>()("effect-cqrs/ViewModels") {}

/**
 * The names of the metrics view models record through Effect's `Metric` module.
 * Each series carries a `view_model` attribute with the view model's name.
 */
export const MetricNames = {
  /** Counter: events a view model parked after its handler kept failing on them. */
  parkedEvents: "cqrs_view_model_parked_events_total"
} as const

// Stalled view models keep their place so events are never processed out of order.
const retryDelay = (attempt: number) => Duration.millis(Math.min(100 * 2 ** attempt, 30_000))

const describeFailure = (cause: unknown) => Cause.pretty(Cause.fail(cause))

const movedElsewhere = (viewModel: string) =>
  new ViewModelError({
    viewModel,
    position: 0,
    cause: new Error(`Another process moved the checkpoint of view model ${viewModel}`)
  })

const notHeld = (viewModel: string) =>
  new ViewModelError({
    viewModel,
    position: 0,
    cause: new Error(`Another process holds the lease on view model ${viewModel}`)
  })

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
  const filter = filterOf(viewModels)
  const decoded = new WeakMap<StoredEvent, unknown>()
  let last:
    | { readonly afterPosition: number; readonly latest: number; readonly events: ReadonlyArray<StoredEvent> }
    | undefined

  return {
    read: (afterPosition: number, latest: number) =>
      Effect.suspend(() =>
        last?.afterPosition === afterPosition && last.latest === latest
          ? Effect.succeed(last.events)
          // A view model behind by `tailSize` positions finds everything it needs in that
          // many events, and one that fell further behind meanwhile reads the rest in pages.
          : Stream.runCollect(store.read({ ...filter, afterPosition }).pipe(Stream.take(tailSize))).pipe(
            Effect.tap((events) => Effect.sync(() => void (last = { afterPosition, latest, events })))
          )
      ),
    decoded
  }
}

type Tail = ReturnType<typeof makeTail>

interface RunnerServices {
  readonly leases: Leases.Leases["Service"]
  readonly parkedEvents: ParkedEvents.ParkedEvents["Service"]
}

const makeRunner = Effect.fnUntraced(function*<V extends Any>(viewModel: V, tail: Tail, services: RunnerServices) {
  const store = yield* EventStore
  const checkpoints = yield* Checkpoints
  const context = withoutAmbient(yield* Effect.context<Context<V>>())
  const provide = <A, E>(effect: Effect.Effect<A, E, any>) =>
    Effect.provideContext(effect, context) as Effect.Effect<A, E>
  const semaphore = yield* Semaphore.make(1)
  const name = viewModel.name
  const policy = viewModel.onFailure ?? stall
  const transaction = viewModel.transaction
  const { leases, parkedEvents } = services
  const parkedCounter = Metric.counter(MetricNames.parkedEvents, {
    description: "Events a view model parked after its handler kept failing on them",
    incremental: true,
    attributes: { view_model: name }
  })

  const fail = (position: number) => (cause: unknown) => new ViewModelError({ viewModel: name, position, cause })

  // The lease comes first, so that the checkpoint read below is the one a previous
  // holder saved when it gave the lease up. Its finalizer, giving it up, runs last.
  const lease = yield* leases.acquire(name)

  const checkpoint = yield* checkpoints.get(name)
  const position = yield* SubscriptionRef.make(checkpoint)
  const saved = yield* Ref.make(checkpoint)
  const lastError = yield* Ref.make(Option.none<ViewModelError>())
  const noStall = { position: -1, failedAt: -1, attempts: 0 }
  const stalled = yield* Ref.make(noStall)

  // The positions of the events this view model parked, which it skips. They are few,
  // and kept in memory so that skipping costs nothing for the events around them.
  const parked = new Set<number>()
  const loadParked = parkedEvents.list(name).pipe(
    Effect.map((events) => {
      parked.clear()
      for (const event of events) parked.add(event.position)
    })
  )
  yield* loadParked
  // The event that failed last, which is the one to park.
  let failed: StoredEvent | undefined

  // Aggregates with personal fields need the keys, and fail here when they're missing.
  const keys = Option.getOrUndefined(yield* Effect.serviceOption(PersonalData.Keys))
  const decoders = new Map(viewModel.aggregates.map((aggregate) => [aggregate.name, makeEventDecoder(aggregate, keys)]))
  const filter = filterOf([viewModel])
  const onForget = viewModel.onForget
  const wants = (stored: StoredEvent) =>
    isForgetting(stored)
      ? onForget !== undefined
      : decoders.has(stored.aggregateName) && Object.hasOwn(viewModel.handlers, stored.type)

  const decode = (stored: StoredEvent) => decoders.get(stored.aggregateName)!(stored)

  // Events from the tail are the same objects for everyone, so the first to decode one
  // does it for the rest.
  const decodeShared = (stored: StoredEvent) =>
    tail.decoded.has(stored)
      ? Effect.succeed(tail.decoded.get(stored))
      : decode(stored).pipe(Effect.tap((event) => Effect.sync(() => void tail.decoded.set(stored, event))))

  // `retryParked` runs beside the view model and must not take the place of the
  // event it is failing on, which is the one to park, so it doesn't record.
  const handle =
    (decodeEvent: (stored: StoredEvent) => Effect.Effect<unknown, unknown>, record = true) => (stored: StoredEvent) => {
      // Remembers which event failed while wrapping the error, rather than in a step of
      // its own: this runs for every event.
      const failHere = (cause: unknown) => {
        if (record) failed = stored
        return fail(stored.position)(cause)
      }
      return Effect.gen(function*() {
        // The filter reads the stream of forgotten subjects only for a view model that erases them.
        if (isForgetting(stored)) {
          yield* onForget!(stored.aggregateId, stored).pipe(Causation.causedBy(stored))
          return
        }
        // The filter crosses names with types, so an event can match without a handler.
        const handler = viewModel.handlers[stored.type]
        if (handler === undefined) return
        const event = yield* decodeEvent(stored)
        // Commands the handler runs record this event as their cause.
        yield* handler(event as never, stored).pipe(Causation.causedBy(stored))
      }).pipe(
        Effect.mapError(failHere),
        Effect.catchDefect((defect) => Effect.fail(failHere(defect)))
      )
    }

  // Runs handlers in the view model's transaction, when it has one.
  // The handlers' services are provided around it, as they are without a transaction.
  const transactional = (effect: Effect.Effect<void, ViewModelError, any>, at: number) =>
    transaction === undefined ? effect : transaction(effect as Effect.Effect<void, ViewModelError>).pipe(
      Effect.mapError((error) => error instanceof ViewModelError ? error : fail(at)(error)),
      Effect.catchDefect((defect) => Effect.fail(fail(at)(defect)))
    )

  // Moves the checkpoint on from where this process last saved it. Another process
  // that moved it meanwhile runs the view model too, and this one must neither move
  // it back nor commit a batch that one may have committed already.
  const advanceCheckpoint = checkpoints.advance
  const writeCheckpoint = (to: number) =>
    advanceCheckpoint === undefined
      ? Effect.mapError(checkpoints.set(name, to), fail(0))
      : Ref.get(saved).pipe(
        Effect.flatMap((from) => advanceCheckpoint(name, from, to)),
        Effect.mapError(fail(0)),
        Effect.flatMap((moved) => {
          if (moved) return Effect.void
          outdated = true
          return Effect.fail(movedElsewhere(name))
        })
      )

  // Whoever moved the checkpoint got further than this process knows, and moving it
  // on from the old place would fail forever. So the next attempt starts from where
  // the checkpoint is now. It is read after the failure, outside the transaction that
  // the failure rolls back.
  let outdated = false
  const reload = Effect.gen(function*() {
    const from = yield* checkpoints.get(name)
    yield* Ref.set(saved, from)
    yield* SubscriptionRef.set(position, from)
    yield* loadParked
    outdated = false
  }).pipe(
    Effect.catch((error) => Effect.logError(`View model ${name} could not load its checkpoint`, error))
  )
  const reloadWhenMoved = <A, R>(effect: Effect.Effect<A, ViewModelError, R>) =>
    Effect.tapError(effect, () => outdated ? reload : Effect.void)

  const save = (to: number) => Effect.andThen(writeCheckpoint(to), Ref.set(saved, to))

  const advance = (to: number) =>
    Ref.get(saved).pipe(
      Effect.flatMap((last) => to < last || to - last >= checkpointInterval ? save(to) : Effect.void),
      Effect.andThen(SubscriptionRef.set(position, to))
    )

  // A crash repeats at most what came after the last saved checkpoint: two batches.
  // In a transaction the checkpoint commits with the batch, so a crash repeats nothing.
  const processBatch = (batch: ReadonlyArray<StoredEvent>, decodeEvent: typeof decode | typeof decodeShared) => {
    const last = batch[batch.length - 1]!.position
    const events = parked.size === 0 ? batch : batch.filter((stored) => !parked.has(stored.position))
    const handleAll = Effect.forEach(events, handle(decodeEvent), { discard: true })
    if (transaction === undefined) return Effect.andThen(handleAll, advance(last))
    // A failed commit belongs to no event in particular, so it is never parked.
    return transactional(
      Effect.andThen(handleAll, writeCheckpoint(last)),
      0
    ).pipe(
      Effect.andThen(Ref.set(saved, last)),
      Effect.andThen(SubscriptionRef.set(position, last))
    )
  }

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
      reloadWhenMoved,
      semaphore.withPermit,
      provide
    )

  const parkFailed = (error: ViewModelError, attempts: number) =>
    Effect.gen(function*() {
      const stored = failed
      if (stored === undefined || stored.position !== error.position) return false
      yield* parkedEvents.park({
        viewModel: name,
        position: stored.position,
        eventId: eventId(stored),
        error: describeFailure(error.cause),
        attempts,
        parkedAt: yield* DateTime.now
      })
      parked.add(stored.position)
      failed = undefined
      yield* Ref.set(stalled, noStall)
      yield* Metric.update(parkedCounter, 1)
      yield* Effect.logWarning(
        `View model ${name} parked the event at ${stored.position} after ${attempts} attempts`,
        error
      )
      return true
    }).pipe(
      Effect.catch((parkError) =>
        Effect.logError(`View model ${name} could not park the event at ${error.position}`, parkError).pipe(
          Effect.as(false)
        )
      )
    )

  // Failing again at the same place doubles the delay. Having moved on means that
  // stall is over, so the next one starts from the shortest delay. Under the `park`
  // policy an event that failed often enough is parked instead, and the view model
  // goes on at once. Returns how long to wait before trying again.
  const onFailure = (error: ViewModelError) =>
    Effect.gen(function*() {
      const current = yield* SubscriptionRef.get(position)
      const attempts = yield* Ref.modify(stalled, (last) => {
        const attempts = last.position === current && last.failedAt === error.position ? last.attempts + 1 : 1
        return [attempts, { position: current, failedAt: error.position, attempts }] as const
      })
      if (policy._tag === "Park" && error.position > 0 && attempts >= policy.attempts) {
        // The view model no longer waits on the event it parked.
        if (yield* parkFailed(error, attempts)) {
          yield* Ref.set(lastError, Option.none())
          return Duration.zero
        }
      }
      yield* Ref.set(lastError, Option.some(error))
      yield* Effect.logError(`View model ${name} failed, retrying`, error)
      return retryDelay(attempts - 1)
    })

  const backOff = (error: ViewModelError) => Effect.flatMap(onFailure(error), Effect.sleep)

  const step = (latest: number) => catchUp(latest).pipe(Effect.andThen(Ref.set(lastError, Option.none())))

  // Heads that arrived while the view model was busy come together, and only the
  // latest of them matters: one step catches up on all of them. A failure ends the
  // subscription, so heads don't pile up while the view model waits to try again,
  // and the new one starts from the current head.
  const run = store.changes.pipe(
    Stream.runForEachArray((heads) => {
      let latest = heads[0]!
      for (const head of heads) if (head > latest) latest = head
      return Effect.flatMap(SubscriptionRef.get(position), (current) =>
        // Nothing to do also means nothing is failing any more.
        latest > current ? step(latest) : Ref.set(lastError, Option.none()))
    }),
    Effect.catch(backOff),
    Effect.forever
  )

  // Another process may have moved the view model on while this one waited, and
  // appended events this process was never told about.
  const resume = Effect.gen(function*() {
    const from = yield* checkpoints.get(name)
    yield* Ref.set(saved, from)
    yield* SubscriptionRef.set(position, from)
    yield* loadParked
    return yield* store.head
  }).pipe(
    Effect.tapError((error) => Effect.logError(`View model ${name} could not load its checkpoint, retrying`, error)),
    Effect.retry(Schedule.spaced("1 second"))
  )

  // Finalizers run last to first: the fibers below stop, then this saves where it got
  // to, then the lease is given up. A process that lost the lease saves nothing, or
  // it would move back the checkpoint of the one that took over.
  yield* Effect.addFinalizer(() =>
    lease.held.pipe(
      Effect.flatMap((held) => held ? SubscriptionRef.get(position).pipe(Effect.flatMap(save)) : Effect.void),
      Effect.ignore
    )
  )

  // Runs the view model while this process holds its lease, and stops it the moment
  // the lease is lost. A lease held from the start runs at once, with what was just
  // loaded: during a burst of commands every extra step waits behind all of them.
  const scope = yield* Effect.scope
  let running: Fiber.Fiber<unknown, unknown> | undefined = (yield* lease.held)
    ? yield* Effect.forkIn(run, scope)
    : undefined
  yield* lease.changes.pipe(
    Stream.changes,
    Stream.runForEach((held) =>
      Effect.suspend(() => {
        if (held && running === undefined) {
          return Effect.forkIn(
            resume.pipe(
              Effect.flatMap((head) => step(head).pipe(Effect.tapError(backOff), Effect.retry(Schedule.forever))),
              Effect.andThen(run)
            ),
            scope
          ).pipe(
            Effect.map((fiber) => void (running = fiber))
          )
        }
        if (!held && running !== undefined) {
          const fiber = running
          running = undefined
          return Fiber.interrupt(fiber)
        }
        return Effect.void
      })
    ),
    Effect.forkScoped
  )

  const requireLease = Effect.flatMap(lease.held, (held) => held ? Effect.void : Effect.fail(notHeld(name)))

  const replay = (range: Pick<ReplayOptions, "startTime" | "finishTime">) =>
    Effect.gen(function*() {
      yield* requireLease
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
    }).pipe(reloadWhenMoved, semaphore.withPermit, provide)

  // Reads the one event at `at`, which the filter selects since the view model handled it.
  const readAt = (at: number) =>
    store.read({ ...filter, afterPosition: at - 1 }).pipe(
      Stream.take(1),
      Stream.runHead,
      Effect.map(Option.filter((stored) => stored.position === at))
    )

  const retryOne = (event: ParkedEvents.ParkedEvent) =>
    readAt(event.position).pipe(
      Effect.mapError(fail(0)),
      Effect.flatMap(Option.match({
        onNone: () => Effect.fail(fail(event.position)(new Error("The event is no longer in the store"))),
        onSome: (stored) =>
          transactional(
            handle(decode, false)(stored).pipe(
              Effect.andThen(parkedEvents.remove(name, event.position).pipe(Effect.mapError(fail(event.position))))
            ),
            event.position
          )
      }))
    )

  const retryParked = (positions: ReadonlyArray<number> | undefined) =>
    Effect.gen(function*() {
      yield* requireLease
      const all = yield* parkedEvents.list(name)
      const selected = positions === undefined ? all : all.filter((event) => positions.includes(event.position))
      const succeeded: Array<number> = []
      const failures: Array<ParkedEvents.ParkedEvent> = []
      for (const event of selected) {
        const result = yield* Effect.result(retryOne(event))
        if (result._tag === "Success") {
          parked.delete(event.position)
          succeeded.push(event.position)
          continue
        }
        const again: ParkedEvents.ParkedEvent = {
          ...event,
          error: describeFailure(result.failure.cause),
          attempts: event.attempts + 1,
          parkedAt: yield* DateTime.now
        }
        yield* parkedEvents.park(again)
        failures.push(again)
      }
      return { succeeded, failed: failures } satisfies RetryParkedResult
    }).pipe(semaphore.withPermit, provide)

  // A view model that another process runs is not waited for, and neither is one
  // that this process stops running while it waits.
  const caughtUp = (target: number) =>
    Effect.race(
      SubscriptionRef.changes(position).pipe(
        Stream.filter((current) => current >= target),
        Stream.runHead,
        Effect.asVoid
      ),
      lease.changes.pipe(
        Stream.filter((held) => !held),
        Stream.runHead,
        // A lease that is never lost ends without a value.
        Effect.flatMap(Option.match({ onNone: () => Effect.never, onSome: () => Effect.void }))
      )
    )

  const status = Effect.gen(function*() {
    return {
      name,
      position: yield* SubscriptionRef.get(position),
      lastError: yield* Ref.get(lastError),
      parked: parked.size,
      holdsLease: yield* lease.held
    } satisfies Status
  })

  return { name, replay, caughtUp, status, retryParked }
})

/**
 * Starts the view models in the background for as long as the layer lives.
 *
 * Each view model runs where its lease is held, see `Leases`: by default in every
 * process, which suits a single one. Events that view models park go to the
 * `ParkedEvents` in context, or to memory without one.
 */
export const layer = <const VMs extends ReadonlyArray<Any>>(
  viewModels: VMs
): Layer.Layer<ViewModels, EventStoreError, EventStore | Checkpoints | Context<VMs[number]>> =>
  Layer.effect(
    ViewModels,
    Effect.gen(function*() {
      const store = yield* EventStore
      const leases = Option.getOrElse(yield* Effect.serviceOption(Leases.Leases), () => Leases.memory)
      const parkedEvents = yield* Option.match(yield* Effect.serviceOption(ParkedEvents.ParkedEvents), {
        onNone: () => ParkedEvents.makeMemory,
        onSome: Effect.succeed
      })
      const tail = makeTail(store, viewModels)
      const runners = yield* Effect.forEach(
        viewModels,
        (viewModel) => makeRunner(viewModel, tail, { leases, parkedEvents })
      )

      const find = (name: string) => {
        const runner = runners.find((runner) => runner.name === name)
        return runner === undefined ? Effect.fail(new UnknownViewModel({ name })) : Effect.succeed(runner)
      }

      const replay = Effect.fn("ViewModels.replay")(function*(options: ReplayOptions) {
        const selected = yield* Effect.forEach(options.viewModels, find)
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

      const parked = Effect.fn("ViewModels.parked")(function*(viewModel?: string) {
        if (viewModel !== undefined) {
          yield* find(viewModel)
          return yield* parkedEvents.list(viewModel)
        }
        const names = new Set(runners.map((runner) => runner.name))
        return (yield* parkedEvents.list()).filter((event) => names.has(event.viewModel))
      })

      const retryParked = Effect.fn("ViewModels.retryParked")(function*(options: RetryParkedOptions) {
        const runner = yield* find(options.viewModel)
        return yield* runner.retryParked(options.positions)
      })

      return ViewModels.of({
        replay,
        caughtUp,
        status: Effect.forEach(runners, (runner) => runner.status),
        parked,
        retryParked
      })
    })
  )
