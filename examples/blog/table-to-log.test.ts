import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { Library, migrating } from "./table-to-log.ts"

describe("from a table to a log", () => {
  it.effect("imports each row once, then carries on with commands", () =>
    Effect.gen(function*() {
      const { catalog, history } = yield* migrating
      assert.deepStrictEqual(history.map(({ type }) => type), ["BookImported", "BookReturned"])
      assert.deepStrictEqual(history[0]?.metadata, { importedFrom: "books table", script: "scripts/import-books.ts" })
      assert.deepStrictEqual([...catalog], [
        ["book-1", { title: "Dune", author: "Frank Herbert", available: true }],
        ["book-2", { title: "Emma", author: "Jane Austen", available: false }]
      ])
    }).pipe(Effect.provide(Library)))
})
