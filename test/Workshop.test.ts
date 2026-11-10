import { assert, describe, it } from "@effect/vitest"
import { Effect, Layer, Option, Redacted, Ref, References, Schedule } from "effect"
import { EventStore, PersonalData } from "effect-cqrs"
import { HttpClientRequest, HttpServer } from "effect/http"
import { HttpApiMiddleware, HttpApiTest } from "effect/http-api"
import { TestClock } from "effect/testing"
import { Api, Authorization } from "../examples/workshop/api.ts"
import { forgotten } from "../examples/workshop/domain/account.ts"
import { Accounts, BankLive } from "../examples/workshop/domain/bank.ts"
import { Orders, OrdersStore, Products, reasons, seedCatalog, ShopLive } from "../examples/workshop/domain/shop.ts"
import * as Auth from "../examples/workshop/server/auth.ts"
import { Handlers, StoreKind } from "../examples/workshop/server/http.ts"

const adminCode = "presenter"

const Tokens = Layer.effect(
  Auth.Tokens,
  Auth.make({ secret: Redacted.make("test-secret"), adminCode: Option.some(Redacted.make(adminCode)) })
)

const Keys = PersonalData.layerMemory().pipe(Layer.provide(PersonalData.layerMasterKeyEphemeral))

const Server = Layer.mergeAll(Handlers, HttpServer.layerServices, Layer.effectDiscard(seedCatalog)).pipe(
  Layer.provide(Tokens),
  Layer.provideMerge(ShopLive),
  Layer.provideMerge(BankLive),
  Layer.provideMerge(Keys),
  Layer.provideMerge(EventStore.layerMemory),
  Layer.provide(Layer.succeed(StoreKind, "sqlite"))
)

/** An in-memory client that sends `token`, or no token at all. */
const clientAs = (token?: string) =>
  HttpApiTest.groups(Api, ["system", "auth", "me", "bank", "shop", "admin"]).pipe(
    Effect.provide(HttpApiMiddleware.layerClient(
      Authorization,
      ({ next, request }) => next(token === undefined ? request : HttpClientRequest.bearerToken(request, token))
    ))
  )

const join = (nickname: string, code?: string) =>
  Effect.flatMap(
    clientAs(),
    (client) => client.auth.join({ payload: code === undefined ? { nickname } : { nickname, adminCode: code } })
  )

const quiet = Effect.provideService(References.MinimumLogLevel, "None")

describe("workshop", () => {
  describe("tokens", () => {
    it.effect("accept what they issued until they expire", () =>
      Effect.gen(function*() {
        const tokens = yield* Auth.make({ secret: Redacted.make("secret"), adminCode: Option.none(), ttl: "1 hour" })
        const user = { id: "u1", nickname: "Alice", role: "participant" as const }
        const token = yield* tokens.issue(user)
        assert.deepStrictEqual(yield* tokens.verify(token), user)

        const [head, body] = token.split(".")
        const forged = yield* Effect.flip(tokens.verify(`${head}.${body}.AAAA`))
        assert.strictEqual(forged.reason, "signature")
        const other = yield* Auth.make({ secret: Redacted.make("another"), adminCode: Option.none() })
        assert.strictEqual((yield* Effect.flip(other.verify(token))).reason, "signature")
        assert.strictEqual((yield* Effect.flip(tokens.verify("not.a.token"))).reason, "malformed")

        yield* TestClock.adjust("2 hours")
        assert.strictEqual((yield* Effect.flip(tokens.verify(token))).reason, "expired")
      }))

    it.effect("refuse every admin code when none is configured", () =>
      Effect.gen(function*() {
        const tokens = yield* Auth.make({ secret: Redacted.make("secret"), adminCode: Option.none() })
        assert.isFalse(tokens.isAdminCode(""))
        const configured = yield* Auth.make({
          secret: Redacted.make("secret"),
          adminCode: Option.some(Redacted.make("x"))
        })
        assert.isTrue(configured.isAdminCode("x"))
        assert.isFalse(configured.isAdminCode("y"))
      }))
  })

  describe("api", () => {
    it.effect("opens an account on join and answers only with a valid token", () =>
      Effect.gen(function*() {
        const session = yield* join("Alice")
        assert.strictEqual(session.user.nickname, "Alice")
        assert.strictEqual(session.user.role, "participant")

        const me = yield* Effect.flatMap(clientAs(session.token), (client) => client.me.get())
        assert.deepStrictEqual(me, { ...session.user, status: "open", balance: 0 })

        const anonymous = yield* Effect.flip(Effect.flatMap(clientAs(), (client) => client.me.get()))
        assert.strictEqual(anonymous._tag, "Unauthorized")
        const tampered = yield* Effect.flip(Effect.flatMap(clientAs(`${session.token}x`), (client) => client.me.get()))
        assert.strictEqual(tampered._tag, "Unauthorized")

        assert.strictEqual((yield* join("Bob", adminCode)).user.role, "admin")
        assert.strictEqual((yield* Effect.flip(join("Eve", "guess")))._tag, "WrongAdminCode")
      }).pipe(Effect.provide(Server), quiet))

    it.effect("moves money between participants", () =>
      Effect.gen(function*() {
        const alice = yield* join("Alice")
        const bob = yield* join("Bob")
        const asAlice = yield* clientAs(alice.token)
        const asBob = yield* clientAs(bob.token)

        assert.strictEqual((yield* asAlice.me.deposit({ payload: { amount: 100 } })).balance, 100)
        const sent = yield* asAlice.me.transfer({ payload: { to: bob.user.id, amount: 30 } })
        assert.strictEqual(sent.balance, 70)

        const broke = yield* Effect.flip(asAlice.me.transfer({ payload: { to: bob.user.id, amount: 500 } }))
        assert.strictEqual(broke._tag, "InsufficientFunds")
        assert.deepStrictEqual(broke._tag === "InsufficientFunds" && [broke.balance, broke.requested], [70, 500])
        const self = yield* Effect.flip(asAlice.me.transfer({ payload: { to: alice.user.id, amount: 1 } }))
        assert.strictEqual(self._tag, "SameAccountTransfer")
        const nobody = yield* Effect.flip(asAlice.me.transfer({ payload: { to: "nobody", amount: 1 } }))
        assert.strictEqual(nobody._tag, "RecipientNotFound")

        // The saga credits Bob after the command returns: wait for the view models to see it.
        const ledger = yield* asAlice.me.ledger({ query: { position: sent.position } })
        assert.deepStrictEqual(ledger.map(({ amount, type }) => [type, amount]), [
          ["TransferSent", -30],
          ["MoneyDeposited", 100]
        ])
        const credited = yield* waitFor(asBob.me.get(), (me) => me.balance === 30)
        assert.strictEqual(credited.balance, 30)
        const bobLedger = yield* asBob.me.ledger({ query: {} })
        assert.deepStrictEqual(bobLedger.map(({ counterparty, type }) => [type, counterparty]), [
          ["TransferReceived", alice.user.id]
        ])

        const participants = yield* asBob.bank.participants({ query: {} })
        assert.deepStrictEqual(participants.map(({ balance, nickname }) => [nickname, balance]), [
          ["Alice", 70],
          ["Bob", 30]
        ])
      }).pipe(Effect.provide(Server), quiet))

    it.effect("pays out, closes and forgets the nickname when an account closes", () =>
      Effect.gen(function*() {
        const alice = yield* join("Alice")
        const bob = yield* join("Bob")
        const asAlice = yield* clientAs(alice.token)
        yield* asAlice.me.deposit({ payload: { amount: 40 } })

        const closed = yield* asAlice.me.close()
        assert.strictEqual(closed.balance, 0)
        const refused = yield* Effect.flip(asAlice.me.get())
        assert.strictEqual(refused._tag, "Unauthorized")
        assert.strictEqual(refused._tag === "Unauthorized" && refused.message, "The account is closed")

        const asBob = yield* clientAs(bob.token)
        const participants = yield* asBob.bank.participants({ query: { position: closed.position } })
        const row = participants.find(({ id }) => id === alice.user.id)
        assert.deepStrictEqual(row && { nickname: row.nickname, status: row.status, balance: row.balance }, {
          nickname: forgotten,
          status: "closed",
          balance: 0
        })
        // The events are still there, with their amounts.
        const accounts = yield* Accounts
        const history = yield* accounts.history(alice.user.id, { payload: true })
        assert.deepStrictEqual(history.map(({ type }) => type), [
          "AccountOpened",
          "MoneyDeposited",
          "MoneyWithdrawn",
          "AccountClosed"
        ])
        assert.strictEqual((yield* accounts.readModel(alice.user.id)).nickname, forgotten)

        const late = yield* Effect.flip(asBob.me.transfer({ payload: { to: alice.user.id, amount: 1 } }))
        assert.strictEqual(late._tag, "RecipientNotFound")
      }).pipe(Effect.provide(Server), quiet))
  })

  describe("transfer saga", () => {
    it.effect("refunds a transfer to an account that closed in the meantime", () =>
      Effect.gen(function*() {
        const accounts = yield* Accounts
        yield* accounts.commands.openAccount("alice", { nickname: "Alice" })
        yield* accounts.commands.openAccount("bob", { nickname: "Bob" })
        yield* accounts.commands.closeAccount("bob", {})
        yield* accounts.commands.deposit("alice", { amount: 50 })
        yield* accounts.commands.sendTransfer("alice", { transferId: "t1", to: "bob", amount: 20 })

        const alice = yield* waitFor(accounts.readModel("alice"), (state) => state.refunded.includes("t1"))
        assert.strictEqual(alice.balance, 50)
        assert.strictEqual((yield* accounts.readModel("bob")).balance, 0)

        // Running the saga's commands again changes nothing.
        yield* accounts.commands.refundTransfer("alice", { transferId: "t1", to: "bob", amount: 20, reason: "again" })
        assert.strictEqual((yield* accounts.readModel("alice")).balance, 50)
      }).pipe(Effect.provide(Server), quiet))
  })
})

/** Joins, adds `amount` and returns the session with a client for it. */
const participant = (nickname: string, amount: number, code?: string) =>
  Effect.gen(function*() {
    const session = yield* join(nickname, code)
    const client = yield* clientAs(session.token)
    if (amount > 0) yield* client.me.deposit({ payload: { amount } })
    return { ...session, client }
  })

/** Waits on the real clock, for tests where many fibers retry with delays. */
const waitLive = <A, E, R>(self: Effect.Effect<A, E, R>, done: (a: A) => boolean) =>
  self.pipe(Effect.repeat({ until: done, schedule: Schedule.spaced("20 millis"), times: 500 }))

const settled = (status: string) => status === "fulfilled" || status === "rejected" || status === "cancelled"

describe("workshop shop", () => {
  it.live("sells the last piece to exactly one of 20 buyers and refunds the others", () =>
    Effect.gen(function*() {
      const buyers = yield* Effect.forEach(Array.from({ length: 20 }, (_, i) => `buyer${i}`), (name) =>
        participant(name, 100))
      // The cap is 25 and there is one in stock.
      yield* Effect.forEach(buyers, ({ client }) =>
        client.shop.order({ payload: { productId: "cappellino", quantity: 1, backorder: false } }), {
        concurrency: "unbounded"
      })

      const store = yield* OrdersStore
      const orders = yield* waitLive(
        Effect.map(Ref.get(store), (map) =>
          Array.from(map.values())),
        (list) =>
          list.length === 20 && list.every((order) =>
            settled(order.status)
          )
      )
      const fulfilled = orders.filter((order) => order.status === "fulfilled")
      assert.strictEqual(fulfilled.length, 1)
      assert.deepStrictEqual(
        new Set(orders.filter((order) => order.status === "rejected").map((order) => order.reason)),
        new Set([reasons.outOfStock])
      )

      const accounts = yield* Accounts
      const balances = yield* Effect.forEach(buyers, ({ user }) =>
        Effect.map(accounts.readModel(user.id), (state) =>
          [user.id, state.balance] as const))
      for (const [id, balance] of balances) {
        assert.strictEqual(balance, id === fulfilled[0]!.customer ? 75 : 100)
      }
      const cap = yield* (yield* Products).readModel("cappellino")
      assert.strictEqual(cap.available, 0)
    }).pipe(Effect.provide(Server), quiet), 30_000)

  it.live("queues backorders, serves them in order and lets the last one cancel", () =>
    Effect.gen(function*() {
      const presenter = yield* participant("Presenter", 0, adminCode)
      const buyers = yield* Effect.forEach(["Anna", "Bruno", "Carla"], (name) => participant(name, 100))

      // The headphones are out of stock: each order waits in the queue, in the order placed.
      const placed = []
      for (const { client } of buyers) {
        const { orderId, position } = yield* client.shop.order({
          payload: { productId: "cuffie", quantity: 1, backorder: true }
        })
        yield* waitLive(
          client.me.orders({ query: { position } }),
          (orders) => orders.some((order) => order.id === orderId && order.status === "backordered")
        )
        placed.push(orderId)
      }
      const [first, second, third] = placed as [string, string, string]
      const queued = yield* buyers[0]!.client.shop.products({ query: {} })
      assert.deepStrictEqual(queued.find((p) => p.id === "cuffie")?.queue.map((w) => w.orderId), placed)

      const refused = yield* Effect.flip(buyers[0]!.client.admin.restock({
        params: { productId: "cuffie" },
        payload: { quantity: 2 }
      }))
      assert.strictEqual(refused._tag, "Forbidden")
      yield* presenter.client.admin.restock({ params: { productId: "cuffie" }, payload: { quantity: 2 } })

      const orders = yield* Orders
      yield* waitLive(
        Effect.forEach([first, second], (id) => orders.readModel(id)),
        (states) => states.every((state) => state.status === "fulfilled")
      )
      assert.strictEqual((yield* orders.readModel(third)).status, "backordered")

      const late = yield* Effect.flip(buyers[0]!.client.me.cancelOrder({ params: { orderId: first } }))
      assert.strictEqual(late._tag, "OrderNotCancellable")
      const notTheirs = yield* Effect.flip(buyers[0]!.client.me.cancelOrder({ params: { orderId: third } }))
      assert.strictEqual(notTheirs._tag, "OrderNotFound")

      yield* buyers[2]!.client.me.cancelOrder({ params: { orderId: third } })
      const accounts = yield* Accounts
      const carla = yield* waitLive(accounts.readModel(buyers[2]!.user.id), (state) => state.balance === 100)
      assert.strictEqual(carla.balance, 100)
      assert.strictEqual((yield* accounts.readModel(buyers[0]!.user.id)).balance, 40)
    }).pipe(Effect.provide(Server), quiet), 30_000)

  it.live("rejects an order the customer can't pay for", () =>
    Effect.gen(function*() {
      const poor = yield* participant("Poor", 0)
      const { orderId, position } = yield* poor.client.shop.order({
        payload: { productId: "caffe", quantity: 1, backorder: false }
      })
      const orders = yield* waitLive(
        poor.client.me.orders({ query: { position } }),
        (list) => list.some((order) => order.id === orderId && settled(order.status))
      )
      assert.deepStrictEqual(orders.map(({ reason, status }) => [status, reason]), [[
        "rejected",
        reasons.insufficientFunds
      ]])
    }).pipe(Effect.provide(Server), quiet), 30_000)

  it.live("gives back waiting orders before paying out a closing account", () =>
    Effect.gen(function*() {
      const leaving = yield* participant("Leaving", 100)
      const { orderId } = yield* leaving.client.shop.order({
        payload: { productId: "cuffie", quantity: 1, backorder: true }
      })
      const orders = yield* Orders
      yield* waitLive(orders.readModel(orderId), (state) => state.status === "backordered")
      // The view the close reads from has to know the order is waiting.
      yield* waitLive(leaving.client.me.orders({ query: {} }), (list) => list[0]?.status === "backordered")

      yield* leaving.client.me.close()
      const accounts = yield* Accounts
      const history = yield* accounts.history(leaving.user.id, { payload: true })
      assert.deepStrictEqual(history.map(({ type }) => type), [
        "AccountOpened",
        "MoneyDeposited",
        "PurchasePaid",
        "PurchaseRefunded",
        "MoneyWithdrawn",
        "AccountClosed"
      ])
      const withdrawn = history.find(({ type }) => type === "MoneyWithdrawn")
      assert.deepStrictEqual(withdrawn?.payload, { _tag: "MoneyWithdrawn", amount: 100 })
      assert.strictEqual((yield* orders.readModel(orderId)).status, "cancelled")
      const cuffie = yield* (yield* Products).readModel("cuffie")
      assert.deepStrictEqual(cuffie.queue, [])
    }).pipe(Effect.provide(Server), quiet), 30_000)
})

/** Repeats `self` until `done` holds, moving the test clock so the saga's fibers run. */
const waitFor = <A, E, R>(self: Effect.Effect<A, E, R>, done: (a: A) => boolean) =>
  self.pipe(
    Effect.tap(() => TestClock.adjust("10 millis")),
    Effect.repeat({ until: done, schedule: Schedule.recurs(500) })
  )
