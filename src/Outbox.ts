/**
 * Publishes events to other systems, such as Kafka, NATS or SQS, as integration
 * events.
 *
 * ```ts
 * const TodoOutbox = Outbox.make({
 *   name: "todo-outbox",
 *   aggregates: [Todo],
 *   messages: {
 *     TodoCreated: ({ title }) => ({ type: "todo.created.v1", payload: { title } })
 *   },
 *   publish: (messages) => producer.send(messages)
 * })
 *
 * const Publishing = Outbox.layer([TodoOutbox])
 * ```
 *
 * An outbox is a view model: it reads the store from its checkpoint, so it
 * publishes what was stored while the process was down, and nothing is lost
 * between storing an event and publishing it. Delivery is at least once. After a
 * crash, events published since the last checkpoint are published again, so
 * consumers deduplicate on {@link Message.id}, which stays the same.
 *
 * Messages are published in batches of up to `batchSize`. A batch goes out when
 * it is full or `linger` after its first message, and the checkpoint never moves
 * past a message that wasn't published. A failed batch is retried, in order,
 * until it succeeds.
 *
 * @module
 */
import {
  Context,
  type DateTime,
  Duration,
  Effect,
  Latch,
  Layer,
  Predicate,
  Schedule,
  Schema,
  type SchemaAST,
  Semaphore
} from "effect"
import type * as Aggregate from "./Aggregate.ts"
import { Checkpoints } from "./Checkpoints.ts"
import { eventId, type EventStore, EventStoreError } from "./EventStore.ts"
import * as ViewModel from "./ViewModel.ts"

/** An integration event, what a publisher sends. */
export interface Message {
  /** The id of the event it comes from, the same however often it is published: `todo/a@1`. */
  readonly id: string
  readonly type: string
  readonly payload: unknown
  readonly aggregateName: string
  readonly aggregateId: string
  /** The version of the event in its stream. */
  readonly version: number
  /** The position of the event in the store. */
  readonly position: number
  readonly timestamp: DateTime.Utc
  readonly correlationId: string | null
  readonly causationId: string | null
}

/** What a mapping makes of an event. `undefined` publishes nothing for it. */
export interface Mapped {
  readonly type: string
  readonly payload: unknown
}

export type Mappings<Event extends Aggregate.Tagged> = {
  readonly [Tag in Event["_tag"]]?: (
    event: Extract<Event, { readonly _tag: Tag }>,
    meta: Aggregate.EventMeta
  ) => Mapped | undefined
}

export interface Outbox<out Name extends string, out E, out R> {
  readonly name: Name
  readonly aggregates: ReadonlyArray<Aggregate.Any>
  readonly mappings: Record<string, (event: never, meta: Aggregate.EventMeta) => Mapped | undefined>
  readonly publish: (messages: ReadonlyArray<Message>) => Effect.Effect<void, E, R>
  readonly batchSize: number
  readonly linger: Duration.Duration
}

export type Any = Outbox<string, any, any>

export type Context<O extends Any> = O extends Outbox<any, any, infer R> ? R : never

/** Publishing failed, which the outbox retries. */
export class OutboxError extends Schema.TaggedError<OutboxError>()("OutboxError", {
  outbox: Schema.String,
  cause: Schema.Defect()
}) {}

const tagsOf = (aggregate: Aggregate.Any): ReadonlyArray<string> => {
  const tags: Array<string> = []
  const visit = (ast: SchemaAST.AST): void => {
    if (ast._tag === "Union") return ast.types.forEach(visit)
    if (ast._tag !== "Objects") return
    const tag = ast.propertySignatures.find(({ name }) => name === "_tag")?.type
    if (tag?._tag === "Literal" && Predicate.isString(tag.literal)) tags.push(tag.literal)
  }
  visit(aggregate.events.ast)
  return tags
}

/**
 * Defines an outbox. `messages` maps events, by tag, to integration events, and
 * only those are published. Without it every event of the aggregates is
 * published, typed `<aggregate>.<tag>`, with its JSON form as payload.
 *
 * Mapping is where domain events become a contract with other teams, and where
 * personal data is left out: events reach it decrypted, see `PersonalData`.
 */
export const make = <
  const Name extends string,
  const Aggregates extends ReadonlyArray<Aggregate.Any>,
  E = never,
  R = never
>(options: {
  readonly name: Name
  readonly aggregates: Aggregates
  readonly messages?: Mappings<Aggregate.Event<Aggregates[number]>> | undefined
  readonly publish: (messages: ReadonlyArray<Message>) => Effect.Effect<void, E, R>
  /** The most messages in one call to `publish`. Defaults to 100. */
  readonly batchSize?: number | undefined
  /** How long a batch waits for more messages before it is published. Defaults to 10 milliseconds. */
  readonly linger?: Duration.Input | undefined
}): Outbox<Name, E, R> => {
  let mappings = options.messages as Record<string, (event: never, meta: Aggregate.EventMeta) => Mapped | undefined>
  if (mappings === undefined) {
    mappings = {}
    for (const aggregate of options.aggregates) {
      const encode = Schema.encodeSync(Schema.toCodecJson(aggregate.events))
      for (const tag of tagsOf(aggregate)) {
        const previous = mappings[tag]
        mappings[tag] = (event: never, meta) =>
          meta.aggregateName === aggregate.name
            ? { type: `${aggregate.name}.${tag}`, payload: encode(event) }
            : previous?.(event, meta)
      }
    }
  }
  const batchSize = options.batchSize ?? 100
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new Error(`Outbox ${options.name}: batchSize must be a positive integer`)
  }
  return {
    name: options.name,
    aggregates: options.aggregates,
    mappings,
    publish: options.publish,
    batchSize,
    linger: Duration.fromInputUnsafe(options.linger ?? "10 millis")
  }
}

export class Outboxes extends Context.Service<Outboxes, {
  /**
   * Waits until every event up to `position`, by default the latest, is
   * published. Mostly for tests.
   */
  published(position?: number): Effect.Effect<void, EventStoreError | OutboxError>
  /** Where each outbox is, and the failure it is retrying, see `ViewModel.Status`. */
  readonly status: Effect.Effect<ReadonlyArray<ViewModel.Status>>
}>()("effect-cqrs/Outboxes") {}

// A batch that failed in the background is tried again with growing pauses.
const retrySchedule = Schedule.exponential("100 millis").pipe(
  Schedule.modifyDelay(({ output }) => Effect.succeed(Duration.min(output, Duration.seconds(30))))
)

const makeRunner = Effect.fnUntraced(function*<O extends Any>(outbox: O) {
  const context = yield* Effect.context<Context<O>>()
  const lock = yield* Semaphore.make(1)
  const pending = yield* Latch.make(false)
  const buffer: Array<Message> = []
  // A handler that failed runs again, and its message must not be buffered twice.
  let accepted = 0

  const publishBatch = lock.withPermit(Effect.suspend(() => {
    const batch = buffer.slice(0, outbox.batchSize)
    if (batch.length === 0) return Effect.void
    return outbox.publish(batch).pipe(
      Effect.mapError((cause) => new OutboxError({ outbox: outbox.name, cause })),
      Effect.catchDefect((cause) => Effect.fail(new OutboxError({ outbox: outbox.name, cause }))),
      Effect.andThen(Effect.sync(() => void buffer.splice(0, batch.length))),
      Effect.provideContext(context as Context.Context<unknown>)
    )
  }))

  const flush: Effect.Effect<void, OutboxError> = Effect.suspend(() =>
    buffer.length === 0 ? Effect.void : Effect.andThen(publishBatch, flush)
  )

  const handlers = Object.fromEntries(
    Object.entries(outbox.mappings).map(([tag, map]) => [
      tag,
      (event: never, meta: Aggregate.EventMeta) =>
        Effect.suspend(() => {
          if (meta.position <= accepted) return buffer.length >= outbox.batchSize ? flush : Effect.void
          const mapped = map(event, meta)
          accepted = meta.position
          if (mapped === undefined) return Effect.void
          buffer.push({
            id: eventId(meta),
            type: mapped.type,
            payload: mapped.payload,
            aggregateName: meta.aggregateName,
            aggregateId: meta.aggregateId,
            version: meta.version,
            position: meta.position,
            timestamp: meta.timestamp,
            correlationId: meta.correlationId,
            causationId: meta.causationId
          })
          return buffer.length >= outbox.batchSize ? flush : pending.open
        })
    ])
  )

  const viewModel = ViewModel.make({
    name: outbox.name,
    aggregates: outbox.aggregates,
    handlers: handlers as unknown as ViewModel.Handlers<any>,
    // Messages already published can't be taken back.
    dispose: Effect.void
  })

  // Publishes what is left of a batch once it has waited `linger`.
  yield* Effect.gen(function*() {
    yield* pending.await
    yield* pending.close
    yield* Effect.sleep(outbox.linger)
    yield* flush.pipe(
      Effect.tapError((error) => Effect.logError(`Outbox ${outbox.name} failed to publish, retrying`, error)),
      Effect.retry(retrySchedule)
    )
  }).pipe(Effect.forever, Effect.forkScoped)

  return { name: outbox.name, viewModel, flush }
})

/**
 * Runs the outboxes in the background for as long as the layer lives. Each one
 * keeps a checkpoint under its name in `Checkpoints`, which should be durable:
 * with an in-memory checkpoint an outbox publishes everything again on restart.
 */
export const layer = <const Os extends ReadonlyArray<Any>>(
  outboxes: Os
): Layer.Layer<Outboxes, EventStoreError, EventStore | Checkpoints | Context<Os[number]>> =>
  Layer.effect(
    Outboxes,
    Effect.gen(function*() {
      const runners = yield* Effect.forEach(outboxes, makeRunner)
      const byName = new Map(runners.map((runner) => [runner.name, runner]))
      const checkpoints = yield* Checkpoints

      // A checkpoint only moves past messages that were published, so a crash
      // publishes again rather than skipping.
      const guarded = Checkpoints.of({
        get: checkpoints.get,
        set: (name, position) => {
          const runner = byName.get(name)
          return runner === undefined
            ? checkpoints.set(name, position)
            : runner.flush.pipe(
              Effect.mapError((cause) => new EventStoreError({ operation: "outbox", cause })),
              Effect.andThen(checkpoints.set(name, position))
            )
        }
      })

      const context = yield* Layer.build(ViewModel.layer(runners.map(({ viewModel }) => viewModel))).pipe(
        Effect.provideService(Checkpoints, guarded)
      )
      const viewModels = Context.get(context, ViewModel.ViewModels)

      return Outboxes.of({
        published: (position) =>
          viewModels.caughtUp(position).pipe(
            Effect.andThen(Effect.forEach(runners, (runner) => runner.flush, { discard: true }))
          ),
        status: viewModels.status
      })
    })
  ) as any
