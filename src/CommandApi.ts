/**
 * Serves an aggregate over HTTP with `effect/http-api`: one `POST` endpoint per
 * command, and `GET` endpoints for `history` and, given a schema of the state,
 * `state`.
 *
 * ```ts
 * const TodoApi = CommandApi.group(Todo, {
 *   state: TodoStateSchema,
 *   errors: [TodoAlreadyCreated, TodoNotFound, TodoAlreadyDeleted]
 * })
 *
 * class Api extends HttpApi.make("api").add(TodoApi) {}
 *
 * const TodoHandlers = CommandApi.layer(Api, "todo", AggregateService.make(Todo))
 * ```
 *
 * | Endpoint | Does |
 * | --- | --- |
 * | `POST /todo/:aggregateId/createTodo` | Runs `createTodo` with the body as payload |
 * | `GET /todo/:aggregateId` | `state`, with an optional `finishTime` |
 * | `GET /todo/:aggregateId/history` | `history`, with optional `payload`, `startTime`, `finishTime`, `afterVersion` and `limit` |
 *
 * The group is an ordinary `HttpApiGroup`, so it takes middleware, a different
 * prefix and OpenAPI annotations like any other, and `HttpApiClient` calls it.
 *
 * A command's response carries the store position of its last event, in the body
 * and in an `Event-Position` header. {@link ReadAfter} is middleware for your own
 * query endpoints: a request with a `Read-After` header waits until the view models
 * have handled that position, so a client reads what it just wrote.
 *
 * @module
 */
import { type Duration, Effect, Layer, Option, Predicate, Schema, SchemaAST, type Scope } from "effect"
import { HttpEffect, HttpServerRequest, HttpServerResponse } from "effect/http"
import {
  type HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiError,
  HttpApiGroup,
  HttpApiMiddleware,
  HttpApiSchema,
  OpenApi
} from "effect/http-api"
import type * as Aggregate from "./Aggregate.ts"
import { type AggregateService, type CommandResult, InvalidCommand, WrongExpectedVersion } from "./AggregateService.ts"
import { ConcurrencyConflict } from "./EventStore.ts"
import { Metadata } from "./Metadata.ts"
import { ViewModels } from "./ViewModel.ts"

/**
 * `Idempotency-Key` becomes the command id, so a client retrying after a timeout
 * gets the first result back, and `If-Match` the version the client based its
 * command on, as `3` or `"3"`. `If-Match: *` asks for no version in particular. A
 * malformed `If-Match` fails as an invalid command.
 */
const retryHeaders = (headers: Readonly<Record<string, string | undefined>>) => {
  const commandId = headers["idempotency-key"]
  const ifMatch = headers["if-match"]?.replace(/^W\//, "").replaceAll("\"", "").trim()
  return {
    ...(commandId === undefined || commandId === "" ? {} : { commandId }),
    ...(ifMatch === undefined || ifMatch === "" || ifMatch === "*" ? {} : { expectedVersion: Number(ifMatch) })
  }
}

/**
 * A command whose payload doesn't match its schema, or a body that isn't JSON.
 * The same error as `AggregateService.InvalidCommand`, answered with `400`.
 */
export const InvalidCommandError = InvalidCommand.pipe(HttpApiSchema.status(400))

/** The stream moved on while the command was decided, and retries ran out. Answered with `409`. */
export const ConcurrencyConflictError = ConcurrencyConflict.pipe(HttpApiSchema.status(409))

/** The stream is no longer at the version `If-Match` named. Answered with `409`. */
export const WrongExpectedVersionError = WrongExpectedVersion.pipe(HttpApiSchema.status(409))

/**
 * A command handler failed with an error that `errors` doesn't declare. Its tag
 * and message are kept, and the rest of it is left out of the response.
 */
export class CommandRejected extends Schema.TaggedError<CommandRejected>()("CommandRejected", {
  aggregateName: Schema.String,
  command: Schema.String,
  /** The tag of the handler's error. */
  error: Schema.String,
  message: Schema.String
}, { httpApiStatus: 422 }) {}

/** The body of a successful command, `CommandResult` with the events in their JSON form. */
export const commandResult = <Events extends Aggregate.EventSchema<any>>(events: Events) =>
  Schema.Struct({
    aggregateName: Schema.String,
    aggregateId: Schema.String,
    version: Schema.Int,
    position: Schema.Int,
    events: Schema.Array(events),
    correlationId: Schema.String
  })

/** One entry of `history`. `payload` is there when the request asks for it. */
export const historyEntry = <Events extends Aggregate.EventSchema<any>>(events: Events) =>
  Schema.Struct({
    version: Schema.Int,
    timestamp: Schema.DateTimeUtc,
    type: Schema.String,
    correlationId: Schema.NullOr(Schema.String),
    causationId: Schema.NullOr(Schema.String),
    metadata: Metadata,
    payload: Schema.optionalKey(events)
  })

const params = { aggregateId: Schema.String }

const stateQuery = { finishTime: Schema.optionalKey(Schema.DateTimeUtc) }

const historyQuery = {
  payload: Schema.optionalKey(Schema.Boolean),
  startTime: Schema.optionalKey(Schema.DateTimeUtc),
  finishTime: Schema.optionalKey(Schema.DateTimeUtc),
  /** Lists the events after this version, the last of the previous page. */
  afterVersion: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  /** Lists at most this many events, and never more than the layer's `historyLimit`. */
  limit: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)))
}

type Params = Schema.toCodecStringTree<Schema.Struct<typeof params>>

type CommandEndpoint<A extends Aggregate.Any, K extends string, Errors extends Schema.Top> =
  HttpApiEndpoint.HttpApiEndpoint<
    K,
    "POST",
    string,
    Params,
    never,
    Schema.toCodecJson<A["commands"][K]["payload"]>,
    never,
    Schema.toCodecJson<ReturnType<typeof commandResult<A["events"]>>>,
    Schema.toCodecJson<
      | typeof InvalidCommandError
      | typeof ConcurrencyConflictError
      | typeof WrongExpectedVersionError
      | typeof CommandRejected
      | Errors
    >
  >

type HistoryEndpoint<A extends Aggregate.Any> = HttpApiEndpoint.HttpApiEndpoint<
  "history",
  "GET",
  string,
  Params,
  Schema.toCodecStringTree<Schema.Struct<typeof historyQuery>>,
  never,
  never,
  Schema.toCodecJson<Schema.$Array<ReturnType<typeof historyEntry<A["events"]>>>>
>

type StateEndpoint<State extends Schema.Top> = HttpApiEndpoint.HttpApiEndpoint<
  "state",
  "GET",
  string,
  Params,
  Schema.toCodecStringTree<Schema.Struct<typeof stateQuery>>,
  never,
  never,
  Schema.toCodecJson<State>
>

/** The endpoints of {@link group}. */
export type Endpoints<A extends Aggregate.Any, State extends Schema.Top, Errors extends Schema.Top> =
  | { readonly [K in Aggregate.CommandName<A>]: CommandEndpoint<A, K, Errors> }[Aggregate.CommandName<A>]
  | HistoryEndpoint<A>
  | ([State] extends [never] ? never : StateEndpoint<State>)

export interface Options<
  Name extends string,
  State extends Schema.Top,
  Errors extends ReadonlyArray<Schema.Top>
> {
  /** The identifier of the group, the aggregate's name by default. */
  readonly name?: Name | undefined
  /** Where the endpoints start, `/<aggregate name>` by default. */
  readonly prefix?: `/${string}` | undefined
  /** The schema of the state. Without it there is no `state` endpoint. */
  readonly state?: State | undefined
  /**
   * Errors of the command handlers to answer with as they are. Others become
   * {@link CommandRejected}. An error without an `httpApiStatus` annotation is
   * answered with `422`.
   */
  readonly errors?: Errors | undefined
}

const reserved = ["history", "state"]

const hasStatus = (schema: Schema.Top): boolean =>
  SchemaAST.resolve(schema.ast)?.httpApiStatus !== undefined ||
  schema.ast.context?.annotations?.httpApiStatus !== undefined

/**
 * Builds the endpoints of an aggregate. Add the group to an `HttpApi` and
 * implement it with {@link layer}.
 */
export const group = <
  A extends Aggregate.Any,
  const Name extends string = A["name"],
  State extends Schema.Top = never,
  const Errors extends ReadonlyArray<Schema.Top> = []
>(
  aggregate: A,
  options?: Options<Name, State, Errors>
): HttpApiGroup.HttpApiGroup<Name, Endpoints<A, State, Errors[number]>> => {
  const errors = [
    InvalidCommandError,
    ConcurrencyConflictError,
    WrongExpectedVersionError,
    CommandRejected,
    ...(options?.errors ?? []).map((error) => hasStatus(error) ? error : error.pipe(HttpApiSchema.status(422)))
  ]
  const endpoints: Array<HttpApiEndpoint.Constraint> = []
  for (const [name, command] of Object.entries(aggregate.commands)) {
    if (reserved.includes(name)) {
      throw new Error(`CommandApi: ${aggregate.name} has a command named ${name}, which is the name of an endpoint`)
    }
    endpoints.push(
      HttpApiEndpoint.post(name, `/:aggregateId/${name}`, {
        params,
        payload: command.payload,
        success: commandResult(aggregate.events),
        error: errors
      }).annotate(OpenApi.Summary, `Runs ${name}`)
    )
  }
  endpoints.push(
    HttpApiEndpoint.get("history", "/:aggregateId/history", {
      params,
      query: historyQuery,
      success: Schema.Array(historyEntry(aggregate.events))
    }).annotate(OpenApi.Summary, "Lists the events of one aggregate")
  )
  if (options?.state !== undefined) {
    endpoints.push(
      HttpApiEndpoint.get("state", "/:aggregateId", {
        params,
        query: stateQuery,
        success: options.state
      }).annotate(OpenApi.Summary, "Materializes the state of one aggregate")
    )
  }
  return (HttpApiGroup.make(options?.name ?? aggregate.name) as unknown as HttpApiGroup.HttpApiGroup<string, any>)
    .add(...(endpoints as [HttpApiEndpoint.Constraint]))
    .prefix(options?.prefix ?? `/${aggregate.name}`) as any
}

const tagOf = (error: unknown): string =>
  Predicate.hasProperty(error, "_tag") && Predicate.isString(error._tag) ? error._tag : "UnknownError"

const messageOf = (error: unknown): string =>
  Predicate.hasProperty(error, "message") && Predicate.isString(error.message) ? error.message : ""

// Failures of the store, of decoding or of keys are the server's, not the request's.
const serverErrors = ["EventStoreError", "EventDecodeError", "PersonalDataError", "UnknownCommand"]

// A request without a body runs a command whose payload has no required fields.
const readBody = (request: HttpServerRequest.HttpServerRequest, aggregateName: string, type: string) =>
  request.text.pipe(
    Effect.flatMap((text) =>
      text.trim() === ""
        ? Effect.succeed({})
        : Effect.try({ try: () => JSON.parse(text) as unknown, catch: (cause) => cause })
    ),
    Effect.mapError(() => new InvalidCommand({ aggregateName, type, message: "The body is not JSON" }))
  )

export interface LayerOptions {
  /**
   * The most events one `history` response lists, so that a long stream can't make
   * one response as large as the stream. A client reads further with
   * `afterVersion`. Defaults to 1,000.
   */
  readonly historyLimit?: number | undefined
}

/**
 * Implements a group built by {@link group} with an aggregate service, or an
 * effect that builds one when the layer starts.
 *
 * Unexpected failures, such as a store that can't be reached, are defects and
 * answered with `500`.
 */
export const layer = <
  ApiId extends string,
  Groups extends HttpApiGroup.Constraint,
  const Name extends HttpApiGroup.Identifier<Groups>,
  A extends Aggregate.Any,
  E = never,
  R = never
>(
  api: HttpApi.HttpApi<ApiId, Groups>,
  name: Name,
  service: AggregateService<A> | Effect.Effect<AggregateService<A>, E, R>,
  options?: LayerOptions
): Layer.Layer<HttpApiGroup.Service<ApiId, Name>, E, Exclude<R, Scope.Scope>> =>
  HttpApiBuilder.group(api, name, (handlers: any) =>
    Effect.gen(function*() {
      const aggregates = Effect.isEffect(service) ? yield* service : service
      const { aggregate } = aggregates
      const aggregateName: string = aggregate.name
      const endpoints = (api.groups as unknown as Record<string, HttpApiGroup.Top>)[name]!.endpoints

      let result = handlers
      for (const [identifier, endpoint] of Object.entries(endpoints)) {
        if (identifier === "history") {
          const historyLimit = options?.historyLimit ?? 1_000
          result = result.handle(identifier, ({ params, query }: any) =>
            aggregates.history(params.aggregateId, {
              ...query,
              limit: Math.min(query.limit ?? historyLimit, historyLimit)
            }).pipe(Effect.orDie))
          continue
        }
        if (identifier === "state") {
          result = result.handle(identifier, ({ params, query }: any) =>
            aggregates.state(params.aggregateId, query).pipe(Effect.orDie))
          continue
        }
        if (!Object.hasOwn(aggregate.commands, identifier)) {
          continue
        }
        const expected = Array.from(endpoint.error, (schema) =>
          Schema.is(Schema.toType(schema)))
        const reject = (error: unknown) => {
          const tag = tagOf(error)
          if (tag === "InvalidCommand" || tag === "ConcurrencyConflict" || tag === "WrongExpectedVersion") {
            return Effect.fail(error)
          }
          if (serverErrors.includes(tag)) {
            return Effect.die(error)
          }
          if (
            expected.some((is) => is(error))
          ) return Effect.fail(error)
          return Effect.fail(
            new CommandRejected({ aggregateName, command: identifier, error: tag, message: messageOf(error) })
          )
        }
        result = result.handleRaw(identifier, ({ params, request }: any) =>
          readBody(request, aggregateName, identifier).pipe(
            Effect.flatMap((payload): Effect.Effect<CommandResult<unknown>, unknown> =>
              aggregates.dispatch({
                aggregateId: params.aggregateId,
                type: identifier,
                payload,
                ...retryHeaders(request.headers)
              })
            ),
            Effect.tap((result) =>
              HttpEffect.appendPreResponseHandler((_, response) =>
                Effect.succeed(HttpServerResponse.setHeader(response, "event-position", String(result.position)))
              )
            ),
            Effect.catch(reject)
          ))
      }
      return result
    })) as any

/**
 * The view models didn't reach the position a `Read-After` header asked for in
 * time, because they are behind or stalled on a failing handler. Answered with
 * `503`: the client can try again, or read without the header and accept an older
 * answer.
 */
export class NotCaughtUp extends Schema.TaggedError<NotCaughtUp>()("NotCaughtUp", {
  /** The position the request asked for. */
  position: Schema.Int,
  message: Schema.String
}, { httpApiStatus: 503 }) {}

/**
 * Middleware for query endpoints that read view models. A request with a
 * `Read-After: <position>` header waits until every view model of this process has
 * handled that position, the one a command answered with, before the endpoint
 * runs. A request without the header runs at once. A header that isn't a position
 * is answered with `400`.
 *
 * ```ts
 * const Catalog = HttpApiGroup.make("catalog")
 *   .add(HttpApiEndpoint.get("available", "/books/available", { success: Books }))
 *   .middleware(CommandApi.ReadAfter)
 *
 * const ReadAfterLive = CommandApi.layerReadAfter({ timeout: "2 seconds" })
 * ```
 *
 * Implement it with {@link layerReadAfter}.
 */
export class ReadAfter extends HttpApiMiddleware.Service<ReadAfter>()("effect-cqrs/CommandApi/ReadAfter", {
  error: [NotCaughtUp, HttpApiError.BadRequest]
}) {}

export interface ReadAfterOptions {
  /** How long a request waits before it is answered with {@link NotCaughtUp}. Defaults to 5 seconds. */
  readonly timeout?: Duration.Input | undefined
}

const decodePosition = Schema.decodeUnknownOption(
  Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))
)

/**
 * Implements {@link ReadAfter} with the `ViewModels` of this process. View models
 * that another process holds are not waited for, as with `ViewModels.caughtUp`.
 */
export const layerReadAfter = (options?: ReadAfterOptions): Layer.Layer<ReadAfter, never, ViewModels> =>
  Layer.effect(
    ReadAfter,
    Effect.gen(function*() {
      const viewModels = yield* ViewModels
      const timeout = options?.timeout ?? "5 seconds"
      return (httpEffect) =>
        Effect.gen(function*() {
          const request = yield* HttpServerRequest.HttpServerRequest
          const header = request.headers["read-after"]?.trim()
          if (header === undefined || header === "") {
            return yield* httpEffect
          }
          const position = decodePosition(header)
          if (Option.isNone(position)) {
            return yield* Effect.fail(new HttpApiError.BadRequest({}))
          }
          yield* viewModels.caughtUp(position.value).pipe(
            Effect.orDie,
            Effect.timeoutOrElse({
              duration: timeout,
              orElse: () =>
                Effect.fail(
                  new NotCaughtUp({
                    position: position.value,
                    message: `The view models have not handled position ${position.value} yet`
                  })
                )
            })
          )
          return yield* httpEffect
        })
    })
  )
