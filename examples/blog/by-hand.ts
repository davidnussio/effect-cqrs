/**
 * Event sourcing by hand, without a framework, for the first post of the blog series:
 * a book's events, the fold that turns them into its state, and a command that decides
 * on new events.
 */

// #region events
type BookEvent =
  | { readonly _tag: "BookAdded"; readonly title: string }
  | { readonly _tag: "BookLent"; readonly memberId: string }
  | { readonly _tag: "BookReturned" }
// #endregion

// #region evolve
interface Book {
  readonly title: string
  readonly lentTo: string | null
}

const initial: Book = { title: "", lentTo: null }

// Applies one event to the state: what happened, folded into what is true now.
const evolve = (state: Book, event: BookEvent): Book => {
  switch (event._tag) {
    case "BookAdded":
      return { ...state, title: event.title }
    case "BookLent":
      return { ...state, lentTo: event.memberId }
    case "BookReturned":
      return { ...state, lentTo: null }
  }
}

export const currentState = (events: ReadonlyArray<BookEvent>): Book => events.reduce(evolve, initial)
// #endregion

// #region decide
// Looks at the state and decides what happens next, or refuses.
export const lend = (state: Book, memberId: string): ReadonlyArray<BookEvent> => {
  if (state.lentTo !== null) throw new Error(`Already lent to ${state.lentTo}`)
  return [{ _tag: "BookLent", memberId }]
}
// #endregion

// #region another-question
// A question nobody asked when the events were written, answered from the same log.
export const timesLent = (events: ReadonlyArray<BookEvent>): number =>
  events.filter((event) => event._tag === "BookLent").length
// #endregion

export const example = () => {
  // #region run
  const log: Array<BookEvent> = [{ _tag: "BookAdded", title: "Dune" }]

  log.push(...lend(currentState(log), "alice"))
  log.push({ _tag: "BookReturned" })
  log.push(...lend(currentState(log), "bob"))

  currentState(log) // { title: "Dune", lentTo: "bob" }
  timesLent(log) // 2
  // #endregion
  return { log, state: currentState(log), timesLent: timesLent(log) }
}
