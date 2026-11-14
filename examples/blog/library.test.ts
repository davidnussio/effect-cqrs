import { assert, describe, it } from "@effect/vitest"
import { Arbitrary, DateTime, Effect, Layer, Option, Ref, References, Schema } from "effect"
import { Aggregate, Fixture, PersonalData, ViewModel } from "effect-cqrs"
import { TestClock } from "effect/testing"
import { example } from "./by-hand.ts"
import {
  Application,
  Book,
  BookEvent,
  BooksInMemory,
  borrowing,
  Directory,
  firstSteps,
  Library,
  loanLimit,
  Member,
  MemberEvent,
  Members,
  ReadSide,
  readSide
} from "./library.ts"
import { onSqlite } from "./storage.ts"

const quiet = Effect.provideService(References.MinimumLogLevel, "None")

describe("blog", () => {
  it("1. folds events into state by hand", () => {
    const { log, state, timesLent } = example()
    assert.strictEqual(log.length, 4)
    assert.deepStrictEqual(state, { title: "Dune", lentTo: "bob" })
    assert.strictEqual(timesLent, 2)
  })

  it.effect("2. runs commands and reads state and history", () =>
    Effect.gen(function*() {
      const result = yield* firstSteps.pipe(Effect.provide(BooksInMemory))
      assert.strictEqual(result.lent.version, 2)
      assert.deepStrictEqual(result.lent.events.map(({ _tag }) => _tag), ["BookLent"])
      assert.strictEqual(result.answer, "book-1 is out, try again later")
      assert.strictEqual(result.invalid, "InvalidCommand")
      assert.strictEqual(result.state.status, "lent")
      assert.strictEqual(result.state.author, "Frank Herbert")
      assert.deepStrictEqual(result.history.map(({ type, version }) => [version, type]), [
        [1, "BookAdded"],
        [2, "BookLent"]
      ])
    }))

  it.effect("3. keeps read models and rebuilds them", () =>
    Effect.gen(function*() {
      const result = yield* readSide.pipe(Effect.provide(ReadSide))
      assert.deepStrictEqual(Object.fromEntries(result.catalog), {
        "book-1": { title: "Dune", author: "Frank Herbert", available: false },
        "book-2": { title: "Neuromancer", author: "William Gibson", available: true }
      })
      assert.deepStrictEqual([...result.alice ?? []], ["book-1"])
      assert.strictEqual(result.eventCount, 3)
      assert.deepStrictEqual(result.rebuilt, result.catalog)
    }))

  it.effect("4. arranges loans with a saga", () =>
    Effect.gen(function*() {
      const result = yield* borrowing.pipe(Effect.provide(Library))
      assert.deepStrictEqual(result.forAlice, Option.some("LoanConfirmed"))
      assert.deepStrictEqual(result.forBob, Option.some("LoanCancelled"))
      assert.deepStrictEqual(result.alice, { joined: true, name: "Alice Rossi", loans: {} })

      const [, requested, cancelled] = result.bobsHistory
      assert.deepStrictEqual(result.bobsHistory.map(({ type }) => type), [
        "MemberJoined",
        "LoanRequested",
        "LoanCancelled"
      ])
      assert.strictEqual(cancelled.correlationId, requested.correlationId)
      assert.strictEqual(cancelled.causationId, "member/bob@2")

      const aliceRequested = result.alicesHistory.find(({ type }) => type === "LoanRequested")
      const bookLent = result.bookHistory.find(({ type }) => type === "BookLent")
      assert.isDefined(aliceRequested?.correlationId)
      assert.strictEqual(bookLent?.correlationId, aliceRequested?.correlationId)
    }).pipe(quiet))

  it.effect("5. runs the library on SQLite", () =>
    Effect.gen(function*() {
      const result = yield* borrowing.pipe(
        Effect.provide(
          Application.pipe(
            Layer.provideMerge(onSqlite({ events: ":memory:", keys: ":memory:" })),
            Layer.provide(PersonalData.layerMasterKeyEphemeral)
          )
        )
      )
      assert.deepStrictEqual(result.forBob, Option.some("LoanCancelled"))
    }).pipe(quiet))
})

describe("5. testing the library", () => {
  // #region fixture
  const added = BookEvent.cases.BookAdded.make({ title: "Dune", author: "Frank Herbert" })
  const lent = BookEvent.cases.BookLent.make({
    loanId: "loan-1",
    memberId: "alice",
    dueOn: DateTime.makeUnsafe(0).pipe(DateTime.add({ days: 21 }))
  })

  it.effect("lends a book for three weeks", () =>
    Fixture.make(Book)
      .givenEvents([added])
      .when("lend", { loanId: "loan-1", memberId: "alice" })
      .expectEvent(lent)
      .inspectState((state) => assert.strictEqual(state.status, "lent")))

  it.effect("records nothing when the same loan comes again", () =>
    Fixture.make(Book)
      .givenEvents([added, lent])
      .when("lend", { loanId: "loan-1", memberId: "alice" })
      .expectEvents([]))

  it.effect("refuses a book that is out", () =>
    Effect.gen(function*() {
      const error = yield* Fixture.make(Book)
        .givenEvents([added, lent])
        .whenFails("lend", { loanId: "loan-2", memberId: "bob" })
      assert.strictEqual(error._tag, "BookAlreadyLent")
    }))

  it.effect("notes a book that comes back late", () =>
    Effect.gen(function*() {
      yield* TestClock.adjust("22 days")
      yield* Fixture.make(Book)
        .givenEvents([added, lent])
        .when("returnBook", {})
        .expectEvent(BookEvent.cases.BookReturned.make({ loanId: "loan-1", memberId: "alice", late: true }))
    }))
  // #endregion

  // #region invariants
  // A few loan ids, so that random commands often talk about the same loan.
  const LoanId = Schema.Literals(["loan-1", "loan-2", "loan-3", "loan-4", "loan-5", "loan-6", "loan-7", "loan-8"])

  it.effect("never has more than three books out for a member", () =>
    Fixture.make(Member)
      .givenEvents([MemberEvent.cases.MemberJoined.make({ name: "Alice Rossi" })])
      .checkInvariants({
        invariant: (state) => Object.keys(state.loans).length <= loanLimit,
        commands: {
          join: false,
          requestLoan: Arbitrary.schema(Schema.Struct({ loanId: LoanId, bookId: Schema.String })),
          confirmLoan: Arbitrary.schema(Schema.Struct({ loanId: LoanId })),
          cancelLoan: Arbitrary.schema(Schema.Struct({ loanId: LoanId, reason: Schema.String })),
          closeLoan: Arbitrary.schema(Schema.Struct({ loanId: LoanId }))
        },
        check: { runs: 1000, seed: 1 }
      }))
  // #endregion

  // #region historical
  it.effect("reads books added before authors were recorded", () =>
    Fixture.make(Book)
      .givenEvents([Aggregate.historical(Book, "BookAdded", 1, { title: "Dune" })])
      .inspectState((state) => assert.strictEqual(state.author, "Unknown")))
  // #endregion

  // #region forget
  it.effect("forgets a member", () =>
    Effect.gen(function*() {
      const members = yield* Members
      const viewModels = yield* ViewModel.ViewModels
      yield* members.commands.join("alice", { name: "Alice Rossi" })

      yield* PersonalData.forget("alice")
      yield* viewModels.caughtUp()

      const alice = yield* members.state("alice")
      assert.strictEqual(alice.name, "[forgotten]")
      const directory = yield* Directory.use(Ref.get)
      assert.isFalse(directory.has("alice"))
    }).pipe(Effect.provide(Library), quiet))
  // #endregion
})
