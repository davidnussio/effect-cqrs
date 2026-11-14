import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer } from "effect"
import { HttpRouter, HttpServer } from "effect/http"
import { ReadSide } from "./library.ts"
import { inProcess, overHttp, Server } from "./reading.ts"

describe("three ways to read", () => {
  it.effect("reads the aggregate, and the catalog once it has seen the command", () =>
    Effect.gen(function*() {
      const { afterReturn, book } = yield* inProcess.pipe(Effect.provide(ReadSide))
      assert.strictEqual(book.status, "lent")
      assert.strictEqual(book.loan?.memberId, "alice")
      assert.deepStrictEqual(afterReturn, { title: "Dune", author: "Frank Herbert", available: true })
    }))

  it.effect("reads them over HTTP, with Event-Position and Read-After", () =>
    Effect.gen(function*() {
      const { dispose, handler } = HttpRouter.toWebHandler(Server.pipe(Layer.provide(HttpServer.layerServices)), {
        disableLogger: true
      })
      yield* Effect.addFinalizer(() => Effect.promise(dispose))

      const result = yield* Effect.promise(() =>
        overHttp((path, init) => handler(new Request(new URL(path, "http://library"), init)))
      )
      assert.match(result.position, /^\d+$/)
      assert.strictEqual((result.book as { status: string }).status, "lent")
      assert.deepStrictEqual(result.catalog, [
        { bookId: "book-1", title: "Dune", author: "Frank Herbert", available: false }
      ])
    }))
})
