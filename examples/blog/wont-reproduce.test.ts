import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { Fixture } from "effect-cqrs"
import { readFileSync } from "node:fs"
import { join } from "node:path"
import { decodeStream, FixedMember, Member, MemberEvent, production } from "./wont-reproduce.ts"

// #region carla
// Carla's stream, as the support script exported it.
const carla = decodeStream(readFileSync(join(import.meta.dirname, "carla.json"), "utf8"))
// #endregion

describe("the bug that won't reproduce", () => {
  it.effect("refuses Carla in production, and exports her stream as the file holds it", () =>
    Effect.gen(function*() {
      const { exported, refused } = yield* production
      assert.strictEqual(refused._tag, "LoanLimitReached")
      assert.deepStrictEqual(decodeStream(exported), carla)
    }))

  // #region works-for-me
  it.effect("lends to a member who just joined", () =>
    Fixture.make(Member)
      .givenEvents([MemberEvent.cases.MemberJoined.make({})])
      .when("requestLoan", { loanId: "loan-1", bookId: "dune" })
      .expectEvent("LoanRequested"))
  // #endregion

  // #region reproduced
  it.effect("refuses Carla, who has no books", () =>
    Effect.gen(function*() {
      const error = yield* Fixture.make(Member).givenEvents(carla).whenFails("requestLoan", {
        loanId: "loan-4",
        bookId: "solaris"
      })
      assert.strictEqual(error._tag, "LoanLimitReached")
    }))
  // #endregion

  // #region regression
  it.effect("lends to Carla, whose earlier loans were all cancelled", () =>
    Fixture.make(FixedMember)
      .givenEvents(carla)
      .when("requestLoan", { loanId: "loan-4", bookId: "solaris" })
      .expectEvent("LoanRequested"))
  // #endregion
})
