/**
 * A rehearsal for the workshop: many virtual participants join, open the live
 * stream and read their page every 2 seconds as a phone would, and deposit, buy
 * and send money for a while, through the same typed client the UI uses. Each
 * participant has connections of its own, at most six as a browser keeps to one
 * server, rather than one pool shared by all. At the end it asks the server whether
 * the books balance and the view models caught up, and reports how long each call took.
 *
 *   ADMIN_CODE=prova pnpm workshop:load
 *
 * - `LOAD_URL`, the server, `http://localhost:3000` by default.
 * - `LOAD_PARTICIPANTS`, 50 by default, and `LOAD_SECONDS`, 60 by default.
 * - `ADMIN_CODE`, the server's, to read its metrics at the end.
 *
 * It exits with 1 when a call failed for a reason other than the domain's own
 * refusals, or when the books don't balance.
 */
import { NodeHttpClient, NodeRuntime } from "@effect/platform-node"
import { Clock, Config, Duration, Effect, Random, Ref, Stream } from "effect"
import { Sse } from "effect/encoding"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api"
import { Api, Authorization } from "./api.ts"

/** Refusals that are part of the game, not failures of the server. */
const expected = new Set(["InsufficientFunds", "RecipientNotFound", "TooManyRequests", "ProductNotFound"])

interface Stats {
  readonly timings: ReadonlyMap<string, ReadonlyArray<number>>
  readonly refusals: ReadonlyMap<string, number>
  readonly failures: ReadonlyArray<string>
  readonly streamMessages: number
}

const tagOf = (error: unknown) =>
  typeof error === "object" && error !== null && "_tag" in error ? String(error._tag) : String(error)

/**
 * What went wrong, for the report. An `HttpClientError` alone says nothing: its reason
 * tells a refused connection from a status code, with the URL, and its cause the rest.
 */
const describe = (error: unknown) => {
  const tag = tagOf(error)
  if (tag !== "HttpClientError" || typeof error !== "object" || error === null || !("reason" in error)) return tag
  const reason = error.reason as { readonly message?: string; readonly cause?: unknown }
  // fetch only says "fetch failed": what the socket did is a cause or two further down.
  const causes: Array<string> = []
  for (let cause = reason.cause; cause instanceof Error && causes.length < 3; cause = cause.cause) {
    const code = "code" in cause ? ` ${String(cause.code)}` : ""
    causes.push(`${cause.message}${code}`.trim())
  }
  return `${tag}: ${reason.message ?? "unknown"}${causes.length === 0 ? "" : ` (${causes.join(" ← ")})`}`
}

/**
 * A phone's connections to the server: a pool of its own, built again for each
 * participant, holding at most six as a browser does over HTTP/1.1. The live stream
 * takes one of them, as it does in a browser. One fetch pool shared by every
 * participant had no such limit, and spread no participant's calls over its own sockets.
 */
const phoneConnections = NodeHttpClient.layerUndiciOptions({ connections: 6 })

const percentile = (values: ReadonlyArray<number>, p: number) => {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0
}

const program = Effect.gen(function*() {
  const baseUrl = yield* Config.String("LOAD_URL").pipe(Config.withDefault("http://localhost:3000"))
  const count = yield* Config.Int("LOAD_PARTICIPANTS").pipe(Config.withDefault(50))
  const seconds = yield* Config.Int("LOAD_SECONDS").pipe(Config.withDefault(60))
  const adminCode = yield* Config.option(Config.String("ADMIN_CODE"))
  const stats = yield* Ref.make<Stats>({ timings: new Map(), refusals: new Map(), failures: [], streamMessages: 0 })

  const clientFor = (token?: string) =>
    HttpApiClient.make(Api, { baseUrl }).pipe(
      Effect.provide(HttpApiMiddleware.layerClient(Authorization, ({ next, request }) =>
        next(token === undefined ? request : HttpClientRequest.bearerToken(request, token))))
    )

  /** Runs one call, timing it and sorting its outcome. */
  const measure = <A, E, R>(name: string, call: Effect.Effect<A, E, R>) =>
    Effect.gen(function*() {
      const start = yield* Clock.currentTimeMillis
      const exit = yield* Effect.exit(call)
      const millis = (yield* Clock.currentTimeMillis) - start
      yield* Ref.update(stats, (current) => {
        if (exit._tag === "Success") {
          const timings = new Map(current.timings).set(name, [...(current.timings.get(name) ?? []), millis])
          return { ...current, timings }
        }
        const failure = exit.cause.reasons.find((reason) => reason._tag === "Fail")
        const tag = failure?._tag === "Fail" ? tagOf(failure.error) : "defect"
        if (expected.has(tag)) {
          return { ...current, refusals: new Map(current.refusals).set(tag, (current.refusals.get(tag) ?? 0) + 1) }
        }
        const detail = failure?._tag === "Fail" ? describe(failure.error) : "defect"
        return { ...current, failures: [...current.failures, `${name}: ${detail}`] }
      })
      return exit._tag === "Success" ? exit.value : undefined
    })

  /** Listens to the live stream as a phone would, counting what arrives. */
  const listen = (token: string) =>
    Effect.gen(function*() {
      const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk)
      yield* HttpClientResponse.stream(
        client.execute(HttpClientRequest.get(`${baseUrl}/stream/events`).pipe(HttpClientRequest.bearerToken(token)))
      ).pipe(
        Stream.decodeText,
        Stream.pipeThroughChannel(Sse.decode()),
        Stream.runForEach(() =>
          Ref.update(stats, (current) => ({ ...current, streamMessages: current.streamMessages + 1 }))
        )
      )
    }).pipe(Effect.ignore)

  yield* Effect.logInfo(`${count} participants against ${baseUrl} for ${seconds} s`)
  const anonymous = yield* clientFor()
  const sessions = (yield* Effect.forEach(
    Array.from({ length: count }, (_, i) => i),
    (i) => measure("join", anonymous.auth.join({ payload: { nickname: `load-${i}` } })),
    { concurrency: 10 }
  )).filter((session) => session !== undefined)
  const ids = sessions.map((session) => session.user.id)
  const products = ["caffe", "cornetto", "succo", "cioccolato", "quaderno", "cappellino", "cuffie"] as const
  const deadline = (yield* Clock.currentTimeMillis) + seconds * 1000

  const act = Effect.fnUntraced(
    function*(token: string, id: string) {
      const client = yield* clientFor(token)
      yield* Effect.forkScoped(listen(token))
      // What the page of a phone reads, at most every 2 seconds while the log moves: the
      // bank's section, then the shop's, which takes the balance from the bank's.
      // The tuple's requirements infer as `any`: they are none, as for every call of the client.
      const screen = Effect.all([
        client.me.get(),
        client.me.ledger({ query: {} }),
        client.bank.participants({ query: {} }),
        client.shop.products({ query: {} }),
        client.me.orders({ query: {} }),
        client.shop.bot()
      ], {
        concurrency: "unbounded"
      }) as Effect.Effect<unknown, unknown>
      yield* measure("refresh", screen).pipe(
        Effect.andThen(Effect.sleep("2 seconds")),
        Effect.forever,
        Effect.forkScoped
      )
      let lastDeposit = 0
      while ((yield* Clock.currentTimeMillis) < deadline) {
        yield* Effect.sleep(Duration.millis(yield* Random.nextIntBetween(1000, 3000)))
        const me = yield* measure("me", client.me.get())
        const now = yield* Clock.currentTimeMillis
        const roll = yield* Random.next
        // Under the limit of 10 deposits a minute.
        if ((me === undefined || me.balance < 30) && now - lastDeposit > 7000) {
          lastDeposit = now
          yield* measure("deposit", client.me.deposit({ payload: { amount: 100 } }))
        } else if (roll < 0.55) {
          const productId = yield* Random.choice(products)
          yield* measure(
            "order",
            client.shop.order({ payload: { productId, quantity: 1, backorder: (yield* Random.next) < 0.3 } })
          )
        } else if (roll < 0.8) {
          const to = yield* Random.choice(ids.filter((other) => other !== id))
          yield* measure("transfer", client.me.transfer({ payload: { to, amount: 1 } }))
        } else {
          yield* measure("orders", client.me.orders({ query: {} }))
        }
      }
    },
    Effect.scoped,
    Effect.provide(phoneConnections)
  )

  yield* Effect.forEach(sessions, (session) => act(session.token, session.user.id), { concurrency: "unbounded" })
  yield* Effect.logInfo("Done: waiting for the sagas to settle")
  yield* Effect.sleep("5 seconds")

  const result = yield* Ref.get(stats)
  console.log("\ncall        count     p50     p95     max")
  for (const [name, values] of result.timings) {
    console.log(
      `${name.padEnd(10)} ${String(values.length).padStart(6)} ${String(percentile(values, 50)).padStart(6)}ms ${
        String(percentile(values, 95)).padStart(6)
      }ms ${String(Math.max(...values)).padStart(6)}ms`
    )
  }
  console.log(`\nrefused by the domain: ${JSON.stringify(Object.fromEntries(result.refusals))}`)
  console.log(`stream messages received: ${result.streamMessages}`)
  console.log(`failures: ${result.failures.length}`)
  for (const failure of result.failures.slice(0, 10)) console.log(`  ${failure}`)

  let balanced = true
  if (adminCode._tag === "Some") {
    const admin = yield* anonymous.auth.join({ payload: { nickname: "load-admin", adminCode: adminCode.value } })
    const metrics = yield* (yield* clientFor(admin.token)).admin.metrics()
    balanced = metrics.invariant !== "broken"
    console.log(`\nbooks: ${metrics.invariant} (balances ${metrics.balances}), log at ${metrics.position}`)
    console.log(`view models behind: ${metrics.viewModels.map((vm) => `${vm.name} ${vm.lag}`).join(", ")}`)
    console.log(`orders: ${JSON.stringify(metrics.orders)}, server memory ${metrics.memoryMb} MB`)
  } else {
    console.log("\nSet ADMIN_CODE to check the books at the end.")
  }
  if (result.failures.length > 0 || !balanced) {
    return yield* Effect.fail("The rehearsal found problems")
  }
}).pipe(Effect.provide(FetchHttpClient.layer))

NodeRuntime.runMain(program)
