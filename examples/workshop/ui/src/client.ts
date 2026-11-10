/**
 * The typed client of the workshop server, built from the same `api.ts` the
 * server implements, and the session it sends with every call.
 */
import { type Duration, Effect, Exit, Layer, Predicate } from "effect"
import { FetchHttpClient, HttpClientRequest } from "effect/http"
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api"
import { useSyncExternalStore } from "react"
import { Api, Authorization, type Health, type Session } from "../../api.ts"

export const apiUrl = (import.meta.env.VITE_API_URL ?? "http://localhost:3000").replace(/\/$/, "")

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

const storageKey = "workshop.session"

const readStored = (): Session | undefined => {
  try {
    const raw = localStorage.getItem(storageKey)
    return raw === null ? undefined : JSON.parse(raw) as Session
  } catch {
    return undefined
  }
}

let current = readStored()
const listeners = new Set<() => void>()

export const session = {
  get: () => current,
  set: (next: Session | undefined) => {
    current = next
    try {
      if (next === undefined) localStorage.removeItem(storageKey)
      else localStorage.setItem(storageKey, JSON.stringify(next))
    } catch {
      // Private windows may refuse storage: the session then lasts as long as the page.
    }
    for (const listener of listeners) listener()
  }
}

export const useSession = () =>
  useSyncExternalStore((listener) => {
    listeners.add(listener)
    return () => listeners.delete(listener)
  }, session.get)

// ---------------------------------------------------------------------------
// Calls
// ---------------------------------------------------------------------------

/** Adds the token of the current session to every call that needs one. */
const WithToken = HttpApiMiddleware.layerClient(Authorization, ({ next, request }) => {
  const token = session.get()?.token
  return next(token === undefined ? request : HttpClientRequest.bearerToken(request, token))
})

const ClientLayer = Layer.mergeAll(WithToken, FetchHttpClient.layer)

export type Client = HttpApiClient.ForApi<typeof Api>

const makeClient = () => HttpApiClient.make(Api, { baseUrl: apiUrl })

export type Outcome<A> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly tag: string; readonly message: string }

const messages: Record<string, (error: Record<string, unknown>) => string> = {
  InsufficientFunds: (error) => `Saldo insufficiente: hai ${error.balance} crediti`,
  SameAccountTransfer: () => "Non puoi mandare soldi a te stesso",
  RecipientNotFound: () => "Il destinatario non ha più un conto aperto",
  AccountNotOpen: () => "Il conto non è aperto",
  WrongAdminCode: () => "Codice del presentatore sbagliato",
  Unauthorized: (error) => `Sessione non valida: ${error.message}`,
  HttpClientError: () => "Il server non risponde",
  SchemaError: () => "Valore non valido",
  TimeoutError: () => "Il server non ha risposto in tempo"
}

const describe = (error: unknown): { tag: string; message: string } => {
  const tag = Predicate.hasProperty(error, "_tag") && Predicate.isString(error._tag) ? error._tag : "Error"
  const describeTag = messages[tag]
  return { tag, message: describeTag ? describeTag(error as Record<string, unknown>) : String(error) }
}

/**
 * Calls the server and says how it went. A call refused because the session is no
 * longer valid ends the session, which sends the UI back to the join screen.
 */
export const call = async <A, E>(
  f: (client: Client) => Effect.Effect<A, E>,
  options?: { readonly timeout?: Duration.Input }
): Promise<Outcome<A>> => {
  const exit = await Effect.runPromiseExit(
    Effect.flatMap(makeClient(), f).pipe(
      Effect.timeout(options?.timeout ?? "20 seconds"),
      Effect.provide(ClientLayer)
    )
  )
  if (Exit.isSuccess(exit)) return { ok: true, value: exit.value }
  const failure = exit.cause.reasons.find((reason) => reason._tag === "Fail")
  const { message, tag } = describe(failure?._tag === "Fail" ? failure.error : exit.cause)
  if (tag === "Unauthorized") session.set(undefined)
  return { ok: false, tag, message }
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

export type HealthResult =
  | { readonly _tag: "Ok"; readonly health: Health; readonly millis: number }
  | { readonly _tag: "Failed"; readonly message: string }

/** Asks the server how it is. A free Render instance takes about a minute to wake up. */
export const checkHealth = async (): Promise<HealthResult> => {
  const started = performance.now()
  const outcome = await call((client) => client.system.health(), { timeout: "90 seconds" })
  return outcome.ok
    ? { _tag: "Ok", health: outcome.value, millis: performance.now() - started }
    : { _tag: "Failed", message: outcome.message }
}
