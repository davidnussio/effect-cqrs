/**
 * What the live streams send: the events as they are stored, who is in the room,
 * the metrics and the spans. One hub follows the log for every connection.
 */
import {
  Cause,
  Context,
  DateTime,
  Effect,
  Exit,
  JsonSchema,
  Layer,
  Metric,
  Option,
  PubSub,
  Ref,
  Schema,
  type Scope,
  Stream,
  Tracer
} from "effect"
import { AggregateService, EventCatalog, EventStore, PersonalData, ViewModel } from "effect-cqrs"
import { Sse } from "effect/encoding"
import { HttpServerResponse } from "effect/http"
import { type CommandTiming, LiveEvent, Metrics, Presence, SpanEntry, type User } from "../api.ts"
import { Account, forgotten } from "../domain/account.ts"
import { BooksStore, expectedBalances, ParticipantsStore } from "../domain/bank.ts"
import { Order, OrdersStore, Product } from "../domain/shop.ts"

// ---------------------------------------------------------------------------
// Spans
// ---------------------------------------------------------------------------

type Span = typeof SpanEntry.Type

/** The spans that ended last, numbered so a stream can ask for the ones it hasn't sent. */
export class Spans extends Context.Service<Spans, {
  push(span: Span): void
  since(seq: number): { readonly seq: number; readonly spans: ReadonlyArray<Span> }
}>()("workshop/Spans") {}

export const makeSpans = (max = 500): Spans["Service"] => {
  let seq = 0
  let items: Array<{ readonly seq: number; readonly span: Span }> = []
  return {
    push(span) {
      items.push({ seq: ++seq, span })
      if (items.length > max) items = items.slice(-max)
    },
    since(after) {
      return { seq, spans: items.filter((item) => item.seq > after).map((item) => item.span) }
    }
  }
}

// The streams, the health checks, CORS preflights and every single SQL statement
// would bury the commands, sagas and view models the control room is there to show.
const quiet = (attributes: ReadonlyMap<string, unknown> | undefined) => {
  if (attributes === undefined) return false
  if (attributes.has("db.system.name") || attributes.get("http.request.method") === "OPTIONS") return true
  const url = attributes.get("url.path")
  return typeof url === "string" && (url.startsWith("/stream") || url === "/health" || url.startsWith("/shop/bot"))
}

const millis = (nanos: bigint) => Number(nanos / 1000n) / 1000

const failureOf = (exit: Exit.Exit<unknown, unknown>): string => {
  const error = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none()
  if (Option.isSome(error)) {
    const value = error.value as { readonly _tag?: unknown }
    return typeof value._tag === "string" ? value._tag : "error"
  }
  return Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause) ? "interrupted" : "defect"
}

/**
 * A tracer that keeps the spans that end, for the control room. It does nothing
 * while a span runs, so a server on a tenth of a CPU pays little for it.
 */
export const tracerFor = (spans: Spans["Service"]) => {
  class EndedSpan extends Tracer.NativeSpan {
    override end(endTime: bigint, exit: Exit.Exit<unknown, unknown>) {
      super.end(endTime, exit)
      if (quiet(this._attributes)) return
      spans.push({
        spanId: this.spanId,
        traceId: this.traceId,
        parentId: Option.getOrUndefined(this.parent)?.spanId,
        name: this.name,
        start: millis(this.startTime),
        end: millis(endTime),
        attributes: this._attributes === undefined ? {} : Object.fromEntries(this._attributes),
        status: Exit.isSuccess(exit) ? "ok" : "error",
        ...(Exit.isSuccess(exit) ? {} : { error: failureOf(exit) })
      })
    }
  }
  return Tracer.make({ span: (options) => new EndedSpan(options) })
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

const aggregates = [Account, Product, Order] as const

const withoutTag = (value: unknown): Schema.Json => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return value as Schema.Json
  const { _tag, ...rest } = value as Record<string, Schema.Json>
  return rest
}

const redact = (payload: Schema.Json, fields: ReadonlyArray<PersonalData.Field>): Schema.Json => {
  if (fields.length === 0) return payload
  const copy = structuredClone(payload) as Record<string, unknown>
  for (const { path } of fields) {
    let target: Record<string, unknown> | undefined = copy
    for (const key of path.slice(0, -1)) target = target?.[key] as Record<string, unknown> | undefined
    const last = path.at(-1)
    if (target !== undefined && last !== undefined && last in target) target[last] = "[personal]"
  }
  return copy as Schema.Json
}

/** One event, as each stream sends it, serialized once for every connection. */
export interface Variants {
  readonly public: LiveEvent
  readonly control: LiveEvent
  readonly publicJson: string
  readonly controlJson: string
}

const withJson = (variants: { readonly public: LiveEvent; readonly control: LiveEvent }): Variants => ({
  ...variants,
  publicJson: JSON.stringify(variants.public),
  controlJson: JSON.stringify(variants.control)
})

const makeVariants = (keys: PersonalData.Keys["Service"]) => {
  const entries = aggregates.map((aggregate) => {
    const decode = AggregateService.makeEventDecoder(aggregate as typeof Account, keys)
    const encode = Schema.encodeSync(Schema.toCodecJson(aggregate.events as typeof Account.events))
    const personal = PersonalData.fieldsByTag(aggregate.events, aggregate.name)
    return [aggregate.name as string, { decode, encode, personal }] as const
  })
  const codecs = new Map<string, (typeof entries)[number][1]>(entries)

  return Effect.fnUntraced(function*(stored: EventStore.StoredEvent) {
    const base = {
      position: stored.position,
      aggregate: stored.aggregateName,
      aggregateId: stored.aggregateId,
      version: stored.version,
      type: stored.type,
      timestamp: DateTime.formatIso(stored.timestamp),
      ...(typeof stored.metadata.actor === "string" ? { actor: stored.metadata.actor } : {}),
      correlationId: stored.correlationId,
      causationId: stored.causationId
    }
    const codec = codecs.get(stored.aggregateName)
    if (codec === undefined) {
      const payload = withoutTag(stored.payload)
      return withJson({ public: { ...base, payload }, control: { ...base, payload } })
    }
    const decoded = yield* codec.decode(stored).pipe(
      Effect.map((event) => withoutTag(codec.encode(event as never))),
      // An event the control room can't read still shows, as stored.
      Effect.orElseSucceed(() => stored.payload)
    )
    return withJson({
      public: { ...base, payload: redact(withoutTag(stored.payload), codec.personal.get(stored.type) ?? []) },
      control: { ...base, payload: decoded, stored: withoutTag(stored.payload) }
    })
  })
}

// ---------------------------------------------------------------------------
// Hub
// ---------------------------------------------------------------------------

export class Live extends Context.Service<Live, {
  /** The last events, oldest first, for a stream that just opened. */
  readonly recent: Effect.Effect<ReadonlyArray<Variants>>
  readonly events: PubSub.PubSub<Variants>
  readonly presence: Effect.Effect<Presence>
  readonly presenceChanges: PubSub.PubSub<Presence>
  /** Counts the caller as in the room until the scope closes. */
  join(user: User): Effect.Effect<void, never, Scope.Scope>
  readonly connections: Effect.Effect<number>
}>()("workshop/Live") {}

const recentLength = 100
const changesLength = 30

interface Online {
  readonly nickname: string
  readonly since: string
  readonly connections: number
}

export const LiveLive = Layer.effect(
  Live,
  Effect.gen(function*() {
    const store = yield* EventStore.EventStore
    const toVariants = makeVariants(yield* PersonalData.Keys)
    const events = yield* PubSub.sliding<Variants>(1024)
    const presenceChanges = yield* PubSub.sliding<Presence>(64)
    const recent = yield* Ref.make<ReadonlyArray<Variants>>([])
    const online = yield* Ref.make<ReadonlyMap<string, Online>>(new Map())
    const changes = yield* Ref.make<Presence["changes"]>([])

    const presence = Effect.map(
      Effect.all([Ref.get(online), Ref.get(changes)]),
      ([people, latest]): Presence => ({
        online: Array.from(people.entries()).map(([id, person]) => ({ id, ...person })),
        changes: latest
      })
    )
    const announce = Effect.flatMap(presence, (snapshot) => PubSub.publish(presenceChanges, snapshot))

    const change = (kind: "joined" | "left", user: User) =>
      Effect.gen(function*() {
        const at = DateTime.formatIso(yield* DateTime.now)
        yield* Ref.update(changes, (list) =>
          [{ kind, id: user.id, nickname: user.nickname, at }, ...list].slice(0, changesLength))
      })

    // Follows the log from a little before its head, so the control room opens on the latest events.
    const head = yield* Effect.orDie(store.head)
    yield* store.subscribe({ afterPosition: Math.max(0, head - recentLength) }).pipe(
      Stream.runForEach((stored) =>
        Effect.gen(function*() {
          const variants = yield* toVariants(stored)
          yield* Ref.update(recent, (list) => [...list, variants].slice(-recentLength))
          yield* PubSub.publish(events, variants)
          // The room forgets a nickname as soon as the log does.
          if (stored.type === PersonalData.subjectForgottenType) {
            yield* Ref.update(changes, (list) =>
              list.map((entry) => entry.id === stored.aggregateId ? { ...entry, nickname: forgotten } : entry))
            yield* announce
          }
        })
      ),
      Effect.catchCause((cause) => Effect.logError("The live hub stopped following the log", cause)),
      Effect.forkScoped
    )

    const join = (user: User) =>
      Effect.acquireRelease(
        Effect.gen(function*() {
          const since = DateTime.formatIso(yield* DateTime.now)
          const before = (yield* Ref.get(online)).get(user.id)
          yield* Ref.update(online, (people) =>
            new Map(people).set(user.id, {
              nickname: user.nickname,
              since: before?.since ?? since,
              connections: (before?.connections ?? 0) + 1
            }))
          if (before === undefined) yield* change("joined", user)
          yield* announce
        }),
        () =>
          Effect.gen(function*() {
            const before = (yield* Ref.get(online)).get(user.id)
            if (before === undefined) return
            if (before.connections > 1) {
              yield* Ref.update(
                online,
                (people) => new Map(people).set(user.id, { ...before, connections: before.connections - 1 })
              )
            } else {
              yield* Ref.update(online, (people) => {
                const next = new Map(people)
                next.delete(user.id)
                return next
              })
              yield* change("left", user)
            }
            yield* announce
          })
      ).pipe(Effect.asVoid)

    return Live.of({
      recent: Ref.get(recent),
      events,
      presence,
      presenceChanges,
      join,
      connections: Effect.map(
        Ref.get(online),
        (people) => Array.from(people.values()).reduce((sum, person) => sum + person.connections, 0)
      )
    })
  })
)

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

const timingsOf = (snapshots: ReadonlyArray<Metric.Metric.Snapshot>): ReadonlyArray<CommandTiming> =>
  snapshots
    .filter((snapshot) => snapshot.id === AggregateService.MetricNames.commandDuration)
    .map((snapshot) => {
      const state = snapshot.state as Metric.HistogramState
      return {
        aggregate: String(snapshot.attributes?.aggregate ?? ""),
        command: String(snapshot.attributes?.command ?? ""),
        outcome: String(snapshot.attributes?.outcome ?? ""),
        count: state.count,
        totalMs: state.sum,
        maxMs: state.count > 0 ? state.max : 0
      }
    })

/** Everything the control room measures, read once. */
export const metrics = Effect.gen(function*() {
  const store = yield* EventStore.EventStore
  const viewModels = yield* ViewModel.ViewModels
  const live = yield* Live
  const position = yield* Effect.orDie(store.head)
  const statuses = yield* viewModels.status
  const participants = yield* ParticipantsStore.use(Ref.get)
  const books = yield* BooksStore.use(Ref.get)
  const orders = yield* OrdersStore.use(Ref.get)

  const balances = Array.from(participants.values()).reduce((sum, participant) => sum + participant.balance, 0)
  const positionOf = (name: string) => statuses.find((status) => status.name === name)?.position
  const invariant = positionOf("participants") !== positionOf("books")
    ? "catching-up" as const
    : balances === expectedBalances(books)
    ? "ok" as const
    : "broken" as const

  const byStatus: Record<string, number> = {}
  for (const order of orders.values()) byStatus[order.status] = (byStatus[order.status] ?? 0) + 1

  return {
    at: DateTime.formatIso(yield* DateTime.now),
    position,
    connections: yield* live.connections,
    commands: timingsOf(yield* Metric.snapshot),
    viewModels: statuses.map((status) => ({
      name: status.name,
      position: status.position,
      lag: Math.max(0, position - status.position),
      ...(status.lastError._tag === "Some" ? { error: String(status.lastError.value.cause) } : {})
    })),
    orders: byStatus,
    books,
    balances,
    invariant,
    memoryMb: Math.round(process.memoryUsage().rss / 1_000_000)
  } satisfies Metrics
})

// ---------------------------------------------------------------------------
// Server-Sent Events
// ---------------------------------------------------------------------------

interface Message {
  readonly event: "event" | "presence" | "metrics" | "spans"
  /** The data, already serialized: an event is serialized once for every stream. */
  readonly json: string
}

const message = (event: Message["event"], data: unknown): Message => ({ event, json: JSON.stringify(data) })

/**
 * Answers with a stream of Server-Sent Events. A comment every 15 seconds keeps
 * proxies, and a sleeping free instance, from closing it.
 */
export const sseResponse = <E>(messages: Stream.Stream<Message, E>) => {
  const encoded = messages.pipe(
    Stream.map(({ event, json }) => Sse.encoder.write({ _tag: "Event", event, id: undefined, data: json }))
  )
  const heartbeat = Stream.tick("15 seconds").pipe(Stream.map(() => ": ping\n\n"))
  return HttpServerResponse.stream(
    // No `retry:` line: the client reconnects with its own backoff, and Effect's
    // `Sse.decode` surfaces that line as a failure of the stream.
    Stream.merge(encoded, heartbeat).pipe(Stream.encodeText),
    {
      contentType: "text/event-stream",
      headers: { "cache-control": "no-cache", "x-accel-buffering": "no" }
    }
  )
}

/** The participants' stream: events with personal fields hidden, and who is in the room. */
export const publicStream = (user: User) =>
  Stream.unwrap(Effect.gen(function*() {
    const live = yield* Live
    yield* live.join(user)
    const presence = yield* live.presence
    return Stream.concat(
      Stream.succeed(message("presence", presence)),
      Stream.merge(
        Stream.fromPubSub(live.events).pipe(
          Stream.map((variants): Message => ({ event: "event", json: variants.publicJson }))
        ),
        Stream.fromPubSub(live.presenceChanges).pipe(Stream.map((data) => message("presence", data)))
      )
    )
  }))

/** The control room's stream: everything, every second. */
export const controlStream = Stream.unwrap(Effect.gen(function*() {
  const live = yield* Live
  const spans = yield* Spans
  const context = yield* Effect.context<
    EventStore.EventStore | ViewModel.ViewModels | Live | ParticipantsStore | BooksStore | OrdersStore
  >()
  const recent = yield* live.recent
  let sent = spans.since(Number.MAX_SAFE_INTEGER).seq - 200
  const initial: Array<Message> = [
    ...recent.map((variants): Message => ({ event: "event", json: variants.controlJson })),
    message("presence", yield* live.presence)
  ]
  const everySecond = Stream.tick("1 second").pipe(
    Stream.mapEffect(() =>
      Effect.gen(function*() {
        const latest = spans.since(sent)
        sent = latest.seq
        const out: Array<Message> = [message("metrics", yield* Effect.provideContext(metrics, context))]
        if (latest.spans.length > 0) out.push(message("spans", latest.spans))
        return out
      })
    ),
    Stream.flatMap((messages) => Stream.fromIterable(messages))
  )
  return Stream.concat(
    Stream.fromIterable(initial),
    Stream.mergeAll([
      Stream.fromPubSub(live.events).pipe(
        Stream.map((variants): Message => ({ event: "event", json: variants.controlJson }))
      ),
      Stream.fromPubSub(live.presenceChanges).pipe(Stream.map((data) => message("presence", data))),
      everySecond
    ], { concurrency: "unbounded" })
  )
}))

// ---------------------------------------------------------------------------
// AsyncAPI
// ---------------------------------------------------------------------------

const draft07 = (schema: Schema.Top) => JsonSchema.toDocumentDraft07(Schema.toJsonSchemaDocument(schema)).schema

/**
 * The AsyncAPI document of the framework's events and commands, plus the two
 * live streams this server sends them on.
 */
export const asyncApiDocument = (): Schema.Json => {
  const base = EventCatalog.document([...aggregates], {
    title: "effect-cqrs workshop",
    version: "0.5.0",
    description: "The events the bank and the shop store, and the Server-Sent Events streams that carry them."
  })
  const live = {
    "live.event": { name: "event", title: "An event stored", payload: draft07(LiveEvent) },
    "live.presence": { name: "presence", title: "Who is in the room", payload: draft07(Presence) },
    "live.metrics": { name: "metrics", title: "The control room's measures, every second", payload: draft07(Metrics) },
    "live.spans": { name: "spans", title: "Spans that ended", payload: draft07(Schema.Array(SpanEntry)) }
  }
  const ref = (key: string) => ({ $ref: `#/components/messages/${key}` })
  const channels = {
    "live.events": {
      address: "/stream/events",
      title: "Live events and presence",
      description: "Server-Sent Events. Needs `Authorization: Bearer <token>`. Personal fields read `[personal]`.",
      messages: { event: ref("live.event"), presence: ref("live.presence") }
    },
    "live.control": {
      address: "/stream/control",
      title: "The control room",
      description: "Server-Sent Events, admin only. Personal fields decrypted, with the payload as stored.",
      messages: {
        event: ref("live.event"),
        presence: ref("live.presence"),
        metrics: ref("live.metrics"),
        spans: ref("live.spans")
      }
    }
  }
  const operations = Object.fromEntries(
    Object.entries(channels).map(([key, channel]) => [key, {
      action: "send",
      channel: { $ref: `#/channels/${key}` },
      messages: Object.keys(channel.messages).map((name) => ({ $ref: `#/channels/${key}/messages/${name}` }))
    }])
  )
  return JSON.parse(JSON.stringify({
    ...base,
    channels: { ...base.channels, ...channels },
    operations: { ...base.operations, ...operations },
    components: { ...base.components, messages: { ...base.components.messages, ...live } }
  })) as Schema.Json
}
