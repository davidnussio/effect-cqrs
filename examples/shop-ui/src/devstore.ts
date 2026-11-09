/**
 * What the screen shows, kept outside React and Effect so both can reach it: the
 * engine and the tracer write into it, and components subscribe with
 * `useSyncExternalStore`.
 */

/** A span the tracer saw, while it runs and after it ends. */
export interface SpanEntry {
  readonly spanId: string
  readonly traceId: string
  readonly parentId: string | undefined
  readonly name: string
  /** Milliseconds, on the clock the spans were timed with. */
  readonly start: number
  readonly end: number | undefined
  readonly attributes: Readonly<Record<string, unknown>>
  readonly status: "running" | "ok" | "error"
  /** The tag of the error, `defect` or `interrupted`, when the span failed. */
  readonly error?: string
}

/** A row of the event store, as the store keeps it and as the aggregate reads it. */
export interface EventRow {
  readonly position: number
  readonly aggregateName: string
  readonly aggregateId: string
  readonly version: number
  readonly type: string
  readonly schemaVersion: number
  readonly payload: unknown
  readonly decoded: unknown
  readonly metadata: Readonly<Record<string, unknown>>
  readonly correlationId: string | null
  readonly causationId: string | null
  readonly timestamp: number
}

export interface OrderRow {
  readonly id: string
  readonly sku: string
  readonly quantity: number
  readonly status: "placed" | "confirmed" | "rejected"
  readonly reason?: string
  /** From the event's metadata. */
  readonly userId?: string
  readonly appVersion?: string
  /** Stored in schema version 1, before orders had a quantity. */
  readonly legacy: boolean
  readonly position: number
  readonly executionId: string
}

export interface StockRow {
  readonly sku: string
  readonly available: number
  readonly reserved: number
}

/** What `WorkflowEngine.poll` says about a saga's execution. */
export type EngineStatus = "running" | "suspended" | "succeeded" | "failed"

export interface Checkpoint {
  readonly name: string
  readonly position: number
  readonly error?: string
}

export interface CommandTiming {
  readonly aggregate: string
  readonly command: string
  readonly outcome: string
  readonly count: number
  readonly totalMs: number
  readonly maxMs: number
}

export interface Metrics {
  readonly commands: ReadonlyArray<CommandTiming>
  readonly loads: { readonly memory: number; readonly snapshot: number; readonly replay: number }
  readonly conflicts: number
  readonly cacheSize: number
}

export const emptyMetrics: Metrics = {
  commands: [],
  loads: { memory: 0, snapshot: 0, replay: 0 },
  conflicts: 0,
  cacheSize: 0
}

export interface Settings {
  /** Milliseconds every command waits before it runs, so the saga's steps can be watched. */
  readonly latency: number
  /** The customer who places orders, recorded in the metadata. */
  readonly customer: string
}

export interface Snapshot {
  readonly spans: ReadonlyArray<SpanEntry>
  readonly events: ReadonlyArray<EventRow>
  readonly orders: Readonly<Record<string, OrderRow>>
  readonly stock: Readonly<Record<string, StockRow>>
  readonly engine: Readonly<Record<string, EngineStatus>>
  readonly checkpoints: ReadonlyArray<Checkpoint>
  readonly metrics: Metrics
  readonly settings: Settings
  readonly epoch: number
}

const maxSpans = 400

const initial = (settings: Settings, epoch: number): Snapshot => ({
  spans: [],
  events: [],
  orders: {},
  stock: {},
  engine: {},
  checkpoints: [],
  metrics: emptyMetrics,
  settings,
  epoch
})

let state = initial({ latency: 350, customer: "alice" }, 0)
const listeners = new Set<() => void>()
let scheduled = false

const notify = () => {
  if (scheduled) return
  scheduled = true
  queueMicrotask(() => {
    scheduled = false
    for (const listener of listeners) listener()
  })
}

const set = (f: (snapshot: Snapshot) => Snapshot) => {
  state = f(state)
  notify()
}

/** Only a change is worth a render: the polls run several times a second. */
const setIfChanged = <K extends keyof Snapshot>(key: K, value: Snapshot[K]) => {
  if (JSON.stringify(value) === JSON.stringify(state[key])) return
  set((s) => ({ ...s, [key]: value }))
}

const upsertSpan = (span: SpanEntry) =>
  set((s) => {
    const index = s.spans.findIndex((entry) => entry.spanId === span.spanId)
    if (index === -1) return { ...s, spans: [...s.spans, span].slice(-maxSpans) }
    const spans = s.spans.slice()
    spans[index] = span
    return { ...s, spans }
  })

export const devstore = {
  subscribe: (listener: () => void) => {
    listeners.add(listener)
    return () => void listeners.delete(listener)
  },
  get: () => state,

  reset: () => {
    state = initial(state.settings, state.epoch + 1)
    notify()
  },

  settings: (patch: Partial<Settings>) => set((s) => ({ ...s, settings: { ...s.settings, ...patch } })),

  span: upsertSpan,

  addEvent: (row: EventRow) => set((s) => ({ ...s, events: [...s.events, row] })),

  order: (id: string, f: (order: OrderRow | undefined) => OrderRow | undefined) =>
    set((s) => {
      const next = f(s.orders[id])
      if (next === undefined) return s
      return { ...s, orders: { ...s.orders, [id]: next } }
    }),

  stock: (sku: string, f: (row: StockRow) => StockRow) =>
    set((s) => ({ ...s, stock: { ...s.stock, [sku]: f(s.stock[sku] ?? { sku, available: 0, reserved: 0 }) } })),

  clearOrders: () => set((s) => ({ ...s, orders: {} })),
  clearStock: () => set((s) => ({ ...s, stock: {} })),

  engine: (engine: Snapshot["engine"]) => setIfChanged("engine", engine),
  checkpoints: (checkpoints: ReadonlyArray<Checkpoint>) => setIfChanged("checkpoints", checkpoints),
  metrics: (metrics: Metrics) => setIfChanged("metrics", metrics)
}
