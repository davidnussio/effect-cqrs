/**
 * The HTTP API of the workshop server. It holds no server code, so the UI imports
 * it to call the server with `HttpApiClient`.
 */
import { Context, Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSecurity, OpenApi } from "effect/http-api"
import {
  AccountNotOpen,
  AccountStatus,
  Amount,
  DepositAmount,
  InsufficientFunds,
  Nickname,
  SameAccountTransfer
} from "./domain/account.ts"
import { LedgerEntry, Participant } from "./domain/bank.ts"

// ---------------------------------------------------------------------------
// System
// ---------------------------------------------------------------------------

export const Health = Schema.Struct({
  status: Schema.Literal("ok"),
  /** Where events are stored. */
  store: Schema.Literals(["postgres", "sqlite"]),
  /** The position of the last event in the log, read from the store on every call. */
  position: Schema.Number,
  /** The commit the server was built from, when the host says. */
  commit: Schema.optionalKey(Schema.String),
  startedAt: Schema.DateTimeUtcFromString,
  uptimeSeconds: Schema.Number
})
export type Health = typeof Health.Type

export const System = HttpApiGroup.make("system").add(
  HttpApiEndpoint.get("health", "/health", { success: Health })
    .annotate(OpenApi.Summary, "Says the server is up and can reach the event store")
)

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

export const Role = Schema.Literals(["participant", "admin"])
export type Role = typeof Role.Type

/** Who is calling. Their id is the id of their account. */
export const User = Schema.Struct({ id: Schema.String, nickname: Schema.String, role: Role })
export type User = typeof User.Type

export class CurrentUser extends Context.Service<CurrentUser, User>()("workshop/CurrentUser") {}

export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {
  message: Schema.String
}, { httpApiStatus: 401 }) {}

/**
 * Checks the bearer token the server issued at `/auth/join`, and that the account
 * it names is still open.
 */
export class Authorization extends HttpApiMiddleware.Service<Authorization, {
  provides: CurrentUser
  requires: never
}>()("workshop/Authorization", {
  // Clients provide `HttpApiMiddleware.layerClient(Authorization, ...)` to add the token.
  requiredForClient: true,
  security: { bearer: HttpApiSecurity.bearer },
  error: Unauthorized
}) {}

export class WrongAdminCode extends Schema.TaggedError<WrongAdminCode>()("WrongAdminCode", {}, {
  httpApiStatus: 403
}) {}

export const Session = Schema.Struct({
  /** Send it as `Authorization: Bearer <token>`. */
  token: Schema.String,
  user: User
})
export type Session = typeof Session.Type

export const Auth = HttpApiGroup.make("auth").add(
  HttpApiEndpoint.post("join", "/auth/join", {
    payload: Schema.Struct({
      nickname: Nickname,
      /** The presenter's code, which makes the session an admin one. */
      adminCode: Schema.optionalKey(Schema.String)
    }),
    success: Session,
    error: WrongAdminCode
  }).annotate(OpenApi.Summary, "Opens an account and returns a token for it")
)

// ---------------------------------------------------------------------------
// The caller's account
// ---------------------------------------------------------------------------

/** A view model's position to wait for, from an earlier receipt: read your own writes. */
const positionQuery = { position: Schema.optionalKey(Schema.NumberFromString) }

export const Me = Schema.Struct({
  id: Schema.String,
  nickname: Schema.String,
  role: Role,
  status: AccountStatus,
  balance: Schema.Number
})
export type Me = typeof Me.Type

/** What a command left: the position to wait for and the balance after it. */
export const Receipt = Schema.Struct({
  position: Schema.Number,
  balance: Schema.Number
})
export type Receipt = typeof Receipt.Type

export class RecipientNotFound extends Schema.TaggedError<RecipientNotFound>()("RecipientNotFound", {
  accountId: Schema.String
}, { httpApiStatus: 404 }) {}

export const MyAccount = HttpApiGroup.make("me")
  .add(
    HttpApiEndpoint.get("get", "/me", { success: Me })
      .annotate(OpenApi.Summary, "The caller's account")
  )
  .add(
    HttpApiEndpoint.post("deposit", "/me/deposit", {
      payload: Schema.Struct({ amount: DepositAmount }),
      success: Receipt,
      error: AccountNotOpen
    }).annotate(OpenApi.Summary, "Adds money, up to 100 at a time")
  )
  .add(
    HttpApiEndpoint.post("transfer", "/me/transfers", {
      payload: Schema.Struct({ to: Schema.NonEmptyString, amount: Amount }),
      success: Schema.Struct({ ...Receipt.fields, transferId: Schema.String }),
      error: [AccountNotOpen, InsufficientFunds, SameAccountTransfer, RecipientNotFound]
    }).annotate(OpenApi.Summary, "Sends money to another participant")
  )
  .add(
    HttpApiEndpoint.get("ledger", "/me/ledger", { query: positionQuery, success: Schema.Array(LedgerEntry) })
      .annotate(OpenApi.Summary, "The caller's movements, newest first")
  )
  .add(
    HttpApiEndpoint.post("close", "/me/close", { success: Receipt, error: AccountNotOpen })
      .annotate(
        OpenApi.Summary,
        "Pays out the balance, closes the account and forgets the caller's personal data"
      )
  )
  .middleware(Authorization)

// ---------------------------------------------------------------------------
// The bank
// ---------------------------------------------------------------------------

export const Bank = HttpApiGroup.make("bank")
  .add(
    HttpApiEndpoint.get("participants", "/participants", {
      query: positionQuery,
      success: Schema.Array(Participant)
    }).annotate(OpenApi.Summary, "Everyone who joined, with their balance")
  )
  .middleware(Authorization)

export class Api extends HttpApi.make("workshop-api")
  .add(System)
  .add(Auth)
  .add(MyAccount)
  .add(Bank)
  .annotateMerge(OpenApi.annotations({ title: "effect-cqrs workshop", version: "0.2.0" }))
{}
