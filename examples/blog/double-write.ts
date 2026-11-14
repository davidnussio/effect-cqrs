/**
 * The post "The double write": telling another system about a loan, first by storing
 * and sending in one request, in either order, then with an outbox. A broker that is
 * sometimes down shows the difference. `double-write.test.ts` runs it.
 */
import { Context, Effect, Layer, Ref, Schedule, Schema } from "effect"
import { Checkpoints, EventStore, Outbox } from "effect-cqrs"
import { Book, Books } from "./library.ts"

// #region broker
export class BrokerDown extends Schema.TaggedError<BrokerDown>()("BrokerDown", {}) {}

export interface Sent {
  readonly id: string | null
  readonly type: string
  readonly payload: unknown
}

/** A message broker in memory, which can be told to fail the next few sends. */
export class Broker extends Context.Service<Broker, {
  send(messages: ReadonlyArray<Sent>): Effect.Effect<void, BrokerDown>
  readonly received: Effect.Effect<ReadonlyArray<Sent>>
  failNext(times: number): Effect.Effect<void>
}>()("library/Broker") {
  static readonly layer = Layer.effect(
    Broker,
    Effect.gen(function*() {
      const received = yield* Ref.make<ReadonlyArray<Sent>>([])
      const failures = yield* Ref.make(0)
      return Broker.of({
        send: (messages) =>
          Ref.modify(failures, (left) => [left > 0, Math.max(0, left - 1)]).pipe(
            Effect.flatMap((down) =>
              down ? Effect.fail(new BrokerDown()) : Ref.update(received, (all) => [...all, ...messages])
            )
          ),
        received: Ref.get(received),
        failNext: (times) => Ref.set(failures, times)
      })
    })
  )
}
// #endregion

// #region store-then-send
export const storeThenSend = Effect.fnUntraced(function*(bookId: string, loanId: string, memberId: string) {
  const books = yield* Books
  const broker = yield* Broker
  yield* books.commands.lend(bookId, { loanId, memberId })
  yield* broker.send([{ id: null, type: "library.book-lent.v1", payload: { bookId, loanId, memberId } }])
})
// #endregion

// #region send-then-store
export const sendThenStore = Effect.fnUntraced(function*(bookId: string, loanId: string, memberId: string) {
  const books = yield* Books
  const broker = yield* Broker
  yield* broker.send([{ id: null, type: "library.book-lent.v1", payload: { bookId, loanId, memberId } }])
  yield* books.commands.lend(bookId, { loanId, memberId })
})
// #endregion

/** The library without an outbox: the books, the store and the broker. */
export const WithoutOutbox = Books.layer.pipe(Layer.provideMerge(Layer.merge(EventStore.layerMemory, Broker.layer)))

/** Both orders, on a bad day each. */
export const inOneRequest = Effect.gen(function*() {
  const books = yield* Books
  const broker = yield* Broker

  // #region store-then-send-run
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* broker.failNext(1)

  const failed = yield* Effect.flip(storeThenSend("book-1", "loan-1", "alice"))
  // failed._tag: "BrokerDown", so the front desk tells Alice it didn't work

  const book = yield* books.state("book-1")
  // { status: "lent", loan: { loanId: "loan-1", memberId: "alice", … }, … }
  const afterStoring = yield* broker.received
  // []: nobody else will ever hear of loan-1
  // #endregion

  // #region send-then-store-run
  const refused = yield* Effect.flip(sendThenStore("book-1", "loan-2", "bob"))
  // refused._tag: "BookAlreadyLent"

  const afterSending = yield* broker.received
  // [{ type: "library.book-lent.v1", payload: { bookId: "book-1", loanId: "loan-2", memberId: "bob" } }]:
  // everybody heard of a loan that never happened
  // #endregion

  return { failed: failed._tag, book, afterStoring, refused: refused._tag, afterSending }
})

// #region outbox
export const LoansOutbox = Outbox.make({
  name: "loans-outbox",
  aggregates: [Book],
  messages: {
    BookLent: ({ loanId, memberId }, { aggregateId }) => ({
      type: "library.book-lent.v1",
      payload: { bookId: aggregateId, loanId, memberId }
    }),
    BookReturned: ({ late, loanId }, { aggregateId }) => ({
      type: "library.book-returned.v1",
      payload: { bookId: aggregateId, loanId, late }
    })
  },
  publish: (messages) =>
    Broker.use((broker) => broker.send(messages.map(({ id, payload, type }) => ({ id, type, payload }))))
})

export const Library = Outbox.layer([LoansOutbox]).pipe(
  Layer.provideMerge(Books.layer),
  Layer.provideMerge(Layer.mergeAll(EventStore.layerMemory, Checkpoints.layerMemory, Broker.layer))
)
// #endregion

/** The same loan with the outbox, on a day the broker is down for two sends. */
export const withOutbox = Effect.gen(function*() {
  const books = yield* Books
  const broker = yield* Broker
  const outboxes = yield* Outbox.Outboxes

  // #region outbox-run
  yield* books.commands.add("book-1", { title: "Dune", author: "Frank Herbert" })
  yield* broker.failNext(2)

  // The command only stores. The broker isn't on its path, so it can't fail it.
  const { position } = yield* books.commands.lend("book-1", { loanId: "loan-1", memberId: "alice" })

  // The outbox tries, fails, waits a little longer each time, and gets through.
  // `published` fails while the broker is down, so the example asks again.
  yield* outboxes.published(position).pipe(Effect.retry(Schedule.spaced("50 millis")))
  const received = yield* broker.received
  // [{ id: "book/book-1@2", type: "library.book-lent.v1", payload: { bookId: "book-1", loanId: "loan-1", … } }]
  // #endregion

  return { received }
})
