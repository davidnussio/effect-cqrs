/**
 * Tokens the server issues itself: a JWT signed with HMAC SHA-256 through
 * WebCrypto, so no identity provider is needed. A token names the account it was
 * issued for, the nickname and the role, and expires after a day.
 */
import { Clock, Config, Context, Duration, Effect, Layer, Option, Redacted, Result, Schema } from "effect"
import { Base64Url } from "effect/encoding"
import { timingSafeEqual } from "node:crypto"
import { Role, type User } from "../api.ts"

export class InvalidToken extends Schema.TaggedError<InvalidToken>()("InvalidToken", {
  reason: Schema.Literals(["malformed", "signature", "expired"])
}) {}

const Claims = Schema.Struct({
  sub: Schema.String,
  name: Schema.String,
  role: Role,
  iat: Schema.Int,
  exp: Schema.Int
})

const decodeClaims = Schema.decodeUnknownOption(Schema.fromJsonString(Claims))

// The only header this server writes, so the only one it accepts: a token can't
// pick another algorithm, or none.
const header = Base64Url.encode(JSON.stringify({ alg: "HS256", typ: "JWT" }))

const encoder = new TextEncoder()

export class Tokens extends Context.Service<Tokens, {
  issue(user: User): Effect.Effect<string>
  verify(token: string): Effect.Effect<User, InvalidToken>
  /** Whether `code` is the presenter's code. False when the server has none. */
  isAdminCode(code: string): boolean
}>()("workshop/Tokens") {}

export const make = Effect.fnUntraced(function*(options: {
  readonly secret: Redacted.Redacted<string>
  readonly adminCode: Option.Option<Redacted.Redacted<string>>
  readonly ttl?: Duration.Input | undefined
}) {
  const ttlSeconds = Math.round(Duration.toSeconds(Duration.fromInputUnsafe(options.ttl ?? "1 day")))
  const key = yield* Effect.promise(() =>
    crypto.subtle.importKey(
      "raw",
      encoder.encode(Redacted.value(options.secret)),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"]
    )
  )

  const issue = Effect.fnUntraced(function*(user: User) {
    const now = Math.floor((yield* Clock.currentTimeMillis) / 1000)
    const claims = { sub: user.id, name: user.nickname, role: user.role, iat: now, exp: now + ttlSeconds }
    const unsigned = `${header}.${Base64Url.encode(JSON.stringify(claims))}`
    const signature = yield* Effect.promise(() => crypto.subtle.sign("HMAC", key, encoder.encode(unsigned)))
    return `${unsigned}.${Base64Url.encode(new Uint8Array(signature))}`
  })

  const verify = Effect.fnUntraced(function*(token: string) {
    const [head, body, signature, ...rest] = token.split(".")
    if (head !== header || body === undefined || signature === undefined || rest.length > 0) {
      return yield* new InvalidToken({ reason: "malformed" })
    }
    const bytes = Base64Url.decode(signature)
    if (Result.isFailure(bytes)) return yield* new InvalidToken({ reason: "malformed" })
    const valid = yield* Effect.promise(() =>
      crypto.subtle.verify("HMAC", key, bytes.success, encoder.encode(`${head}.${body}`))
    )
    if (!valid) return yield* new InvalidToken({ reason: "signature" })
    const json = Base64Url.decodeString(body)
    const claims = Result.isSuccess(json) ? decodeClaims(json.success) : Option.none()
    if (Option.isNone(claims)) return yield* new InvalidToken({ reason: "malformed" })
    if (claims.value.exp * 1000 <= (yield* Clock.currentTimeMillis)) {
      return yield* new InvalidToken({ reason: "expired" })
    }
    return { id: claims.value.sub, nickname: claims.value.name, role: claims.value.role }
  })

  const isAdminCode = (code: string) =>
    Option.match(options.adminCode, {
      onNone: () => false,
      onSome: (expected) => {
        const a = encoder.encode(code)
        const b = encoder.encode(Redacted.value(expected))
        return a.length === b.length && timingSafeEqual(a, b)
      }
    })

  return Tokens.of({ issue, verify, isAdminCode })
})

/** A secret anyone can read in this file: fine on a laptop, refused in production. */
const developmentSecret = "workshop-development-secret-not-for-production"

/**
 * Tokens configured by `AUTH_SECRET` and `ADMIN_CODE`. Without `AUTH_SECRET` the
 * server uses a development secret, unless `NODE_ENV` is `production`.
 */
export const layerConfig = Layer.effect(
  Tokens,
  Effect.gen(function*() {
    const production = (yield* Config.String("NODE_ENV").pipe(Config.withDefault("development"))) === "production"
    const configured = yield* Config.option(Config.Redacted("AUTH_SECRET"))
    const adminCode = yield* Config.option(Config.Redacted("ADMIN_CODE"))
    if (Option.isNone(configured)) {
      if (production) return yield* Effect.die("AUTH_SECRET is required when NODE_ENV is production")
      yield* Effect.logWarning("AUTH_SECRET is not set: tokens are signed with the development secret")
    }
    if (Option.isNone(adminCode)) yield* Effect.logWarning("ADMIN_CODE is not set: nobody can join as admin")
    return yield* make({
      secret: Option.getOrElse(configured, () => Redacted.make(developmentSecret)),
      adminCode
    })
  })
)
