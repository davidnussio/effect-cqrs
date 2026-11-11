/**
 * Sagas: reactions that span several aggregates and outlive one command, run as
 * durable workflows from `effect/workflow`.
 *
 * A saga has two halves. Its trigger is a view model that follows the log from a
 * checkpoint and starts a workflow for each event it cares about. The workflow is
 * the process itself: each command it runs is an `Activity`, so when the engine
 * replays the workflow after a restart it skips the commands that already
 * finished, and `withCompensation` undoes earlier steps when a later one fails.
 *
 * Triggers see an event at least once, and the workflow's idempotency key turns
 * a second start into a join of the first execution. That only holds while the
 * engine remembers the execution, so pair `WorkflowEngine.layerMemory` with
 * in-memory checkpoints, as for view models, and a durable engine such as
 * `ClusterWorkflowEngine` from `effect/cluster` with durable ones.
 *
 * An activity that is cut off before its result is saved runs again, so the
 * commands a saga runs should be idempotent: give each a `commandId` made from the
 * execution id and the step, so that running it again returns the first result,
 * or record the id of the transfer it belongs to.
 *
 * @module
 */
import { Context, Effect, Layer, Schedule, Schema } from "effect"
import { Activity, Workflow } from "effect/workflow"
import type { WorkflowEngine } from "effect/workflow"
import type * as Aggregate from "./Aggregate.ts"
import type { CommandResult } from "./AggregateService.ts"
import * as Causation from "./Causation.ts"
import type { Checkpoints } from "./Checkpoints.ts"
import type { EventStore, EventStoreError } from "./EventStore.ts"
import * as Ids from "./Ids.ts"
import * as ViewModel from "./ViewModel.ts"

/**
 * The causation a workflow carries in its payload. A workflow body runs in the
 * engine's fibers, not in the trigger's, so this is how commands it runs learn
 * which event caused them.
 */
export const CausationSchema = Schema.Struct({
  correlationId: Schema.String,
  causationId: Schema.String
})

/** A workflow whose payload carries the causation of the event that started it. */
export type SagaWorkflow<
  Tag extends string = string,
  Fields extends Schema.Struct.Fields = Schema.Struct.Fields,
  Success extends Schema.Top = Schema.Top,
  Error extends Schema.Top = Schema.Top
> = Workflow.Workflow<Tag, Schema.Struct<Fields & { readonly causation: typeof CausationSchema }>, Success, Error>

/**
 * Defines the workflow of a saga. It is `Workflow.make` with a `causation` field
 * added to the payload, which {@link start} fills in and {@link toLayer} applies.
 */
export const workflow = <
  const Tag extends string,
  const Fields extends Schema.Struct.Fields,
  Success extends Schema.Top = Schema.Void,
  Error extends Schema.Top = Schema.Never
>(tag: Tag, options: {
  readonly payload: Fields
  /** Identifies one run of the saga, such as a transfer id. Starting it again joins that run. */
  readonly idempotencyKey: (payload: Schema.Struct.Type<Fields>) => string
  readonly success?: Success
  readonly error?: Error
  readonly suspendedRetrySchedule?: Schedule.Schedule<any, unknown> | undefined
}): SagaWorkflow<Tag, Fields, Success, Error> =>
  Workflow.make(tag, {
    ...options,
    payload: { ...options.payload, causation: CausationSchema }
  }) as any

/**
 * Registers the body of a saga's workflow with the engine. Commands the body
 * runs record the event that started the saga as their cause.
 */
export const toLayer = <
  Tag extends string,
  Fields extends Schema.Struct.Fields,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R
>(
  self: SagaWorkflow<Tag, Fields, Success, Error>,
  execute: (
    payload: Schema.Struct.Type<Fields>,
    executionId: string
  ) => Effect.Effect<Success["Type"], Error["Type"], R>
) =>
  self.toLayer((payload, executionId) =>
    Causation.withCausation(
      execute(payload as Schema.Struct.Type<Fields>, executionId),
      (payload as { readonly causation: Causation.Causation }).causation
    )
  )

/**
 * Starts a saga's workflow without waiting for it, and returns its execution id.
 * Inside a trigger the event being handled becomes the saga's cause; elsewhere
 * the current causation does, or a new correlation when there is none.
 */
export const start = <
  Tag extends string,
  Fields extends Schema.Struct.Fields,
  Success extends Schema.Top,
  Error extends Schema.Top
>(
  self: SagaWorkflow<Tag, Fields, Success, Error>,
  payload: Schema.Struct.MakeIn<Fields>
): Effect.Effect<string, never, WorkflowEngine.WorkflowEngine> =>
  Causation.CurrentCausation.use((current) =>
    current !== undefined
      ? self.execute({ ...payload, causation: current } as never, { discard: true })
      : Effect.flatMap(Ids.next, (id) =>
        self.execute({ ...payload, causation: { correlationId: id, causationId: id } } as never, { discard: true }))
  ) as any

/**
 * The trigger of a saga: a view model whose handlers start workflows with
 * {@link start}, so it keeps a checkpoint and retries like view models do. Run it
 * with {@link layer}, or with `ViewModel.layer` next to the other view models,
 * where replaying it starts every saga again and joins the executions the engine
 * still remembers.
 */
export const make = <
  const Name extends string,
  const Aggregates extends ReadonlyArray<Aggregate.Any>,
  const H extends ViewModel.Handlers<Aggregate.Event<Aggregates[number]>>
>(options: {
  readonly name: Name
  readonly aggregates: Aggregates
  readonly handlers: H
  /** What to do with an event that keeps failing, see `ViewModel.park`. */
  readonly onFailure?: ViewModel.FailurePolicy | undefined
}) => ViewModel.make({ ...options, dispose: Effect.void })

/**
 * The triggers that {@link layer} runs, managed like view models: their status,
 * the events they parked and retrying those. It is `ViewModel.ViewModels` under
 * another name, so it doesn't stand in for the view models' own.
 */
export class Sagas extends Context.Service<Sagas, ViewModel.ViewModels["Service"]>()("effect-cqrs/Sagas") {}

/**
 * Runs triggers in the background for as long as the layer lives, apart from
 * the view models, and provides {@link Sagas} to manage them. Workflows usually
 * run commands, so they sit on top of the command side, which view models are
 * often provided to.
 */
export const layer = <const Triggers extends ReadonlyArray<ViewModel.Any>>(
  sagas: Triggers
): Layer.Layer<Sagas, EventStoreError, EventStore | Checkpoints | ViewModel.Context<Triggers[number]>> =>
  Layer.effect(Sagas, ViewModel.ViewModels.use((viewModels) => Effect.succeed(Sagas.of(viewModels)))).pipe(
    Layer.provide(ViewModel.layer(sagas))
  )

/** What a saga keeps of a command it ran: enough to wait for view models to see it. */
export const CommandOutcome = Schema.Struct({
  aggregateId: Schema.String,
  version: Schema.Int,
  position: Schema.Int
})

// Losing a race or a store error usually passes, so try again before giving up.
const transient = (error: unknown) =>
  Schema.is(Schema.Struct({ _tag: Schema.Literals(["ConcurrencyConflict", "EventStoreError"]) }))(error)

const transientRetry = Schedule.exponential("50 millis").pipe(Schedule.jittered)

/**
 * Runs a command as an activity of a saga's workflow, so a replay of the
 * workflow returns the saved outcome instead of running it again.
 *
 * Failures matching `error`, such as a domain error the saga reacts to, are
 * saved like a success. A lost race or a store error is retried a few times.
 * Anything else ends the workflow as a defect, which runs its compensations.
 */
export const command = <Err extends Schema.Top = typeof Schema.Never, E = never, R = never>(
  name: string,
  options: {
    readonly execute: Effect.Effect<CommandResult<unknown>, E, R>
    readonly error?: Err | undefined
  }
): Activity.Activity<typeof CommandOutcome, Err, R> => {
  const expected = options.error === undefined ? () => false : Schema.is(Schema.toType(options.error))
  return Activity.make({
    name,
    success: CommandOutcome,
    error: options.error,
    execute: options.execute.pipe(
      Effect.retry({ while: transient, schedule: transientRetry, times: 5 }),
      Effect.map(({ aggregateId, position, version }) => ({ aggregateId, version, position })),
      Effect.catch((error) => expected(error) ? Effect.fail(error as Err["Type"]) : Effect.die(error))
    )
  }) as any
}
