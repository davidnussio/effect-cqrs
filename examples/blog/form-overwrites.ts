/**
 * The post "The form that overwrites": two librarians edit the same catalog record from
 * what they saw on their screens. Saving the whole form loses an edit, commands that say
 * what changed keep both, and an expected version stops the edits that really collide.
 * `form-overwrites.test.ts` runs it.
 */
import { Context, Effect, Layer, Ref, Schema } from "effect"
import { Aggregate, AggregateService, Checkpoints, EventStore, ViewModel } from "effect-cqrs"

// #region listing
const Fields = { title: Schema.NonEmptyString, author: Schema.NonEmptyString, shelf: Schema.NonEmptyString }

export const ListingEvent = Schema.TaggedUnion({
  ListingCreated: Fields,
  // What a form that saves every field records.
  ListingSaved: Fields,
  // What commands that say what changed record.
  AuthorCorrected: { author: Schema.NonEmptyString },
  MovedToShelf: { shelf: Schema.NonEmptyString },
  Retitled: { title: Schema.NonEmptyString }
})

export interface ListingState {
  readonly title: string
  readonly author: string
  readonly shelf: string
}

/** A book's record in the catalog: what the librarians edit. */
export const Listing = Aggregate.make<ListingState>()({
  name: "listing",
  events: ListingEvent,
  initial: { title: "", author: "", shelf: "" },
  projection: {
    ListingCreated: (_, { author, shelf, title }) => ({ title, author, shelf }),
    ListingSaved: (_, { author, shelf, title }) => ({ title, author, shelf }),
    AuthorCorrected: (state, { author }) => ({ ...state, author }),
    MovedToShelf: (state, { shelf }) => ({ ...state, shelf }),
    Retitled: (state, { title }) => ({ ...state, title })
  }
})
  .command("create", {
    payload: Schema.Struct(Fields),
    handler: (_, fields) => Effect.succeed(ListingEvent.cases.ListingCreated.make(fields))
  })
  .command("saveForm", {
    payload: Schema.Struct(Fields),
    handler: (_, fields) => Effect.succeed(ListingEvent.cases.ListingSaved.make(fields))
  })
  .command("correctAuthor", {
    payload: Schema.Struct({ author: Schema.NonEmptyString }),
    handler: (state, { author }) =>
      Effect.succeed(state.author === author ? [] : ListingEvent.cases.AuthorCorrected.make({ author }))
  })
  .command("moveToShelf", {
    payload: Schema.Struct({ shelf: Schema.NonEmptyString }),
    handler: (state, { shelf }) =>
      Effect.succeed(state.shelf === shelf ? [] : ListingEvent.cases.MovedToShelf.make({ shelf }))
  })
  .command("retitle", {
    payload: Schema.Struct({ title: Schema.NonEmptyString }),
    handler: (state, { title }) =>
      Effect.succeed(state.title === title ? [] : ListingEvent.cases.Retitled.make({ title }))
  })
// #endregion

// #region screen
export interface Screen extends ListingState {
  /** The version of the record this screen shows, which an edit sends back. */
  readonly version: number
}

/** What the librarians' screens show: every record, with the version it was read at. */
export class Screens extends Context.Service<Screens, Ref.Ref<ReadonlyMap<string, Screen>>>()("library/Screens") {
  static readonly layer = Layer.effect(Screens, Ref.make<ReadonlyMap<string, Screen>>(new Map()))
}

const show = (listingId: string, version: number, update: (fields: ListingState) => ListingState) =>
  Screens.use((screens) =>
    Ref.update(screens, (all) => {
      const fields = all.get(listingId) ?? { title: "", author: "", shelf: "" }
      return new Map(all).set(listingId, { ...update(fields), version })
    })
  )

export const ScreensView = ViewModel.make({
  name: "screens",
  aggregates: [Listing],
  dispose: Screens.use((screens) => Ref.set(screens, new Map())),
  handlers: {
    ListingCreated: ({ author, shelf, title }, { aggregateId, version }) =>
      show(aggregateId, version, () => ({ title, author, shelf })),
    ListingSaved: ({ author, shelf, title }, { aggregateId, version }) =>
      show(aggregateId, version, () => ({ title, author, shelf })),
    AuthorCorrected: ({ author }, { aggregateId, version }) => show(aggregateId, version, (f) => ({ ...f, author })),
    MovedToShelf: ({ shelf }, { aggregateId, version }) => show(aggregateId, version, (f) => ({ ...f, shelf })),
    Retitled: ({ title }, { aggregateId, version }) => show(aggregateId, version, (f) => ({ ...f, title }))
  }
})
// #endregion

export class Listings extends Context.Service<Listings, AggregateService.AggregateService<typeof Listing>>()(
  "library/Listings"
) {
  static readonly layer = Layer.effect(Listings, AggregateService.make(Listing))
}

export const Library = ViewModel.layer([ScreensView]).pipe(
  Layer.provideMerge(Listings.layer),
  Layer.provideMerge(Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, Screens.layer))
)

/** Creates Dune with a typo in the author, and returns what a screen opened now shows. */
const openDune = Effect.gen(function*() {
  const listings = yield* Listings
  const viewModels = yield* ViewModel.ViewModels
  const { position } = yield* listings.commands.create("dune", {
    title: "Dune",
    author: "Frank Herbet",
    shelf: "A3"
  })
  yield* viewModels.caughtUp(position)
  const screen = (yield* Screens.use(Ref.get)).get("dune")
  if (screen === undefined) return yield* Effect.die("The screens have not seen Dune")
  return screen
})

/** Both librarians save the whole form. */
export const wholeForm = Effect.gen(function*() {
  const listings = yield* Listings

  // #region whole-form
  // Bianca and Carlo open the same record, author misspelled, on shelf A3.
  const bianca = yield* openDune
  const carlo = bianca

  // Bianca fixes the author. Carlo, a minute later, moves the book.
  yield* listings.commands.saveForm("dune", { title: bianca.title, author: "Frank Herbert", shelf: bianca.shelf })
  yield* listings.commands.saveForm("dune", { title: carlo.title, author: carlo.author, shelf: "B1" })

  const dune = yield* listings.state("dune")
  // { title: "Dune", author: "Frank Herbet", shelf: "B1" }: Carlo's form put the typo back
  // #endregion

  return dune
})

/** Both librarians say what they changed. */
export const whatChanged = Effect.gen(function*() {
  const listings = yield* Listings

  // #region what-changed
  yield* openDune
  yield* listings.commands.correctAuthor("dune", { author: "Frank Herbert" })
  yield* listings.commands.moveToShelf("dune", { shelf: "B1" })

  const dune = yield* listings.state("dune")
  // { title: "Dune", author: "Frank Herbert", shelf: "B1" }: both edits kept
  // #endregion

  return dune
})

/** Both librarians retitle the book, from the same screen. */
export const sameField = Effect.gen(function*() {
  const listings = yield* Listings

  // #region same-field
  const bianca = yield* openDune
  const carlo = bianca
  // Both screens show version 1.

  yield* listings.commands.retitle("dune", { title: "Dune (Deluxe Edition)" }, { expectedVersion: bianca.version })

  const refused = yield* Effect.flip(
    listings.commands.retitle("dune", { title: "Dune: 60th Anniversary" }, { expectedVersion: carlo.version })
  )
  // refused: WrongExpectedVersion { expectedVersion: 1, actualVersion: 2 }
  // Carlo's screen reloads, shows Bianca's title, and he decides what to do.
  // #endregion

  return { refused, dune: yield* listings.state("dune") }
})
