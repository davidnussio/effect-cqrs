/**
 * The post "The message that blocks the queue": a view model whose handler can't read
 * one author's name. By default it stops on that event and waits. With `park`, it sets
 * the event aside and carries on, and `retryParked` handles it once the handler is
 * fixed. `blocked-queue.test.ts` runs it.
 */
import { Context, Effect, Layer, Option, Ref, Schedule } from "effect"
import { Checkpoints, EventStore, ParkedEvents, ViewModel } from "effect-cqrs"
import { Book, Books, Catalog, CatalogView } from "./library.ts"

// #region shelf-marks
/** The shelf mark printed on each book's spine: the first letters of the author's surname. */
export class ShelfMarks extends Context.Service<ShelfMarks, Ref.Ref<ReadonlyMap<string, string>>>()(
  "library/ShelfMarks"
) {
  static readonly layer = Layer.effect(ShelfMarks, Ref.make<ReadonlyMap<string, string>>(new Map()))
}

// The bug: an author is assumed to have a first name and a surname.
const surnameOf = (author: string) => author.split(" ")[1]!

const shelfMarks = (surname: (author: string) => string, onFailure?: ViewModel.FailurePolicy) =>
  ViewModel.make({
    name: "shelf-marks",
    aggregates: [Book],
    dispose: ShelfMarks.use((marks) => Ref.set(marks, new Map())),
    onFailure,
    handlers: {
      BookAdded: ({ author }, { aggregateId }) =>
        ShelfMarks.use((marks) =>
          Ref.update(marks, (all) => new Map(all).set(aggregateId, surname(author).slice(0, 3).toUpperCase()))
        )
    }
  })

export const ShelfMarksView = shelfMarks(surnameOf)
// #endregion

// #region parking
export const ParkingShelfMarksView = shelfMarks(surnameOf, ViewModel.park({ attempts: 3 }))
// #endregion

// #region fixed
// The fix: the surname is the last word, and a single name is all surname.
const lastWord = (author: string) => author.split(" ").at(-1) ?? author

export const FixedShelfMarksView = shelfMarks(lastWord, ViewModel.park({ attempts: 3 }))
// #endregion

/** What outlives a deploy: the log, the checkpoints, the parked events and both read models. */
export const Storage = Layer.mergeAll(
  EventStore.layerMemory,
  Checkpoints.layerMemory,
  ParkedEvents.layerMemory,
  ShelfMarks.layer,
  Catalog.layer
)

/** One version of the shelf marks, next to the catalog, which has no bug. */
export const deploy = (shelfMarksView: typeof ShelfMarksView) =>
  ViewModel.layer([CatalogView, shelfMarksView]).pipe(Layer.provideMerge(Books.layer))

/** Three books arrive, Homer's in the middle. */
export const threeBooks = Effect.gen(function*() {
  const books = yield* Books
  // #region three-books
  yield* books.commands.add("dune", { title: "Dune", author: "Frank Herbert" })
  yield* books.commands.add("odyssey", { title: "The Odyssey", author: "Homer" })
  yield* books.commands.add("neuromancer", { title: "Neuromancer", author: "William Gibson" })
  // #endregion
})

/** Asks again until the answer satisfies `until`. */
const waitFor = <A, E, R>(effect: Effect.Effect<A, E, R>, until: (a: A) => boolean) =>
  Effect.repeat(effect, { until, schedule: Schedule.spaced("10 millis") })

const statusOf = (name: string) =>
  ViewModel.ViewModels.use((viewModels) => viewModels.status).pipe(
    Effect.map((all) => all.find((status) => status.name === name))
  )

/** The default: the shelf marks stop at Homer. */
export const stalled = Effect.gen(function*() {
  yield* threeBooks

  // #region stalled
  const status = yield* waitFor(statusOf("shelf-marks"), (status) => Option.isSome(status?.lastError ?? Option.none()))
  // { name: "shelf-marks", position: 1, lastError: Some(ViewModelError { position: 2, cause: TypeError }) }

  const marks = yield* ShelfMarks.use(Ref.get)
  // Map { "dune" => "HER" }: nothing for Homer, and nothing for Gibson either
  const catalog = yield* Catalog.use(Ref.get)
  // all three books: the catalog has no bug, and doesn't wait for the shelf marks
  // #endregion

  return { status, marks, catalog }
}).pipe(Effect.provide(deploy(ShelfMarksView)))

/** With parking: the shelf marks skip Homer after three tries. */
export const parked = Effect.gen(function*() {
  const viewModels = yield* ViewModel.ViewModels
  yield* threeBooks

  // #region parked
  yield* waitFor(viewModels.parked("shelf-marks"), (parked) => parked.length > 0)
  yield* viewModels.caughtUp()

  const marks = yield* ShelfMarks.use(Ref.get)
  // Map { "dune" => "HER", "neuromancer" => "GIB" }
  const parked = yield* viewModels.parked("shelf-marks")
  // [{ viewModel: "shelf-marks", position: 2, eventId: "book/odyssey@1", error: "…TypeError…", attempts: 3, … }]
  // #endregion

  return { marks, parked }
}).pipe(Effect.provide(deploy(ParkingShelfMarksView)))

/** The fix is deployed, and the parked event is handled again. */
export const retried = Effect.gen(function*() {
  const viewModels = yield* ViewModel.ViewModels

  // #region retried
  const result = yield* viewModels.retryParked({ viewModel: "shelf-marks" })
  // { succeeded: [2], failed: [] }

  const marks = yield* ShelfMarks.use(Ref.get)
  // Map { "dune" => "HER", "neuromancer" => "GIB", "odyssey" => "HOM" }
  // #endregion

  return { result, marks }
}).pipe(Effect.provide(deploy(FixedShelfMarksView)))
