/**
 * A project that installed effect-cqrs from its tarball, as a user would from npm.
 * Node runs it, and TypeScript checks it, so a broken build or broken declarations
 * fail before anything is published.
 */
import { Effect, Layer, Schema } from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, Fixture, ViewModel } from "effect-cqrs"
import type * as AggregateModule from "effect-cqrs/Aggregate"

const Events = Schema.TaggedUnion({ Incremented: { by: Schema.Int } })

const Counter = Aggregate.make<number>()({
  name: "counter",
  events: Events,
  initial: 0,
  projection: { Incremented: (total, { by }) => total + by }
}).command("increment", {
  payload: Schema.Struct({ by: Schema.Int }),
  handler: (_, { by }) => Effect.succeed(Events.cases.Incremented.make({ by }))
})

// The declarations must carry real types: if they degraded to `any`, this
// expected error would go away and fail the check.
const command: AggregateModule.CommandName<typeof Counter> = "increment"
// @ts-expect-error not a command of Counter
const notACommand: AggregateModule.CommandName<typeof Counter> = "decrement"

let total = 0
const Total = ViewModel.make({
  name: "total",
  aggregates: [Counter],
  dispose: Effect.sync(() => {
    total = 0
  }),
  handlers: {
    Incremented: ({ by }) =>
      Effect.sync(() => {
        total += by
      })
  }
})

const program = Effect.gen(function*() {
  const counters = yield* AggregateService.make(Counter)
  const viewModels = yield* ViewModel.ViewModels
  yield* counters.commands.increment("a", { by: 2 })
  const { position } = yield* counters.commands.increment("a", { by: 3 })
  yield* viewModels.caughtUp(position)
  const state = yield* counters.readModel("a")
  const tested = yield* Fixture.make(Counter).when("increment", { by: 1 }).expectEvent("Incremented")
  return { state, projected: total, testedVersion: tested.version }
}).pipe(
  Effect.provide(
    ViewModel.layer([Total]).pipe(Layer.provideMerge(Layer.merge(EventStore.layerMemory, Checkpoints.layerMemory)))
  )
)

const result = await Effect.runPromise(program)
if (result.state !== 5 || result.projected !== 5 || result.testedVersion !== 1) {
  throw new Error(`effect-cqrs gave ${JSON.stringify(result)}`)
}
console.log(`effect-cqrs works from its package (${command}, ${notACommand} rejected by types)`)
