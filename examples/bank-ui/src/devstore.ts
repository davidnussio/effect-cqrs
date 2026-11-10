/**
 * What the screen shows, kept outside React and Effect so both can reach it: the
 * engine writes into it and components subscribe with `useSyncExternalStore`.
 */
import type { StoredEvent } from "effect-cqrs/EventStore"
import { type SpanEntry, upsertSpan } from "../../devtools/tracing.ts"
import type { AccountEvent } from "./domain/account.ts"

export interface EventEntry {
  readonly position: number
  readonly aggregateId: string
  readonly version: number
  readonly type: AccountEvent["_tag"]
  readonly timestamp: number
  readonly payload: unknown
  readonly event: AccountEvent
  readonly stored: StoredEvent
}

export type Stage = "queued" | "validating" | "loading" | "deciding" | "appending" | "done" | "rejected"
export const stages: ReadonlyArray<Stage> = ["queued", "validating", "loading", "deciding", "appending"]

export type Origin = "app" | "saga" | "autopilot" | "console" | "seed"

export type Outcome =
  | { readonly ok: true; readonly version: number; readonly position: number; readonly events: ReadonlyArray<string> }
  | { readonly ok: false; readonly tag: string; readonly message: string }

export interface CommandEntry {
  readonly id: number
  readonly origin: Origin
  readonly aggregateId: string
  readonly type: string
  readonly payload: unknown
  readonly stage: Stage
  readonly enqueuedAt: number
  readonly outcome?: Outcome
}

export interface AccountRow {
  readonly id: string
  readonly owner: string
  readonly color: string
  readonly balance: number
  readonly status: "open" | "closed"
  readonly txCount: number
  readonly lastPosition: number
  readonly lastDelta: number
}

export interface Totals {
  /** Money held by accounts: what went in minus what went out, without transfers still in flight. */
  readonly moneyInBank: number
  /** Sent but not yet received or refunded: the saga between two aggregates at work. */
  readonly inFlight: number
  readonly deposited: number
  readonly withdrawn: number
  readonly transferred: number
  readonly refunded: number
  readonly openAccounts: number
  readonly byType: Readonly<Record<string, number>>
}

export interface FeedItem {
  readonly position: number
  readonly aggregateId: string
  readonly type: string
  readonly text: string
  readonly delta: number
}

export interface ViewData {
  readonly accounts: Readonly<Record<string, AccountRow>>
  readonly totals: Totals
  readonly feed: ReadonlyArray<FeedItem>
}

export type ViewName = keyof ViewData

export const viewModelNames: Record<ViewName, string> = {
  accounts: "accounts",
  totals: "bank-totals",
  feed: "activity-feed"
}

export interface ViewState<Data> {
  readonly data: Data
  /** From `ViewModels.status`: the last position the view model has processed. */
  readonly position: number
  readonly lastApplied: number
  readonly replaying: boolean
  readonly lastReplay?: { readonly upTo: number | undefined; readonly eventCount: number }
  readonly error?: string
}

export interface Settings {
  /** Milliseconds a command spends in each stage of the bus. */
  readonly commandDelay: number
  /** Milliseconds a view model spends on each live event. */
  readonly projectionDelay: number
  /** Milliseconds per event while replaying. */
  readonly replayDelay: number
  readonly paused: boolean
  readonly autopilot: boolean
  readonly autopilotEvery: number
  readonly breakTotals: boolean
}

export interface Pulse {
  readonly id: number
  readonly lane: "command" | "event" | "projection" | "rejected"
}

export interface CommandTiming {
  readonly command: string
  /** `success`, or the tag of the error that refused the command. */
  readonly outcome: string
  readonly count: number
  readonly totalMs: number
  readonly maxMs: number
}

/** What the account service recorded through `Metric`, read with `Metric.snapshot`. */
export interface AggregateMetrics {
  /** Where each load found a state to start from. */
  readonly loads: { readonly memory: number; readonly snapshot: number; readonly replay: number }
  /** Events read from the store on top of that state, over all loads. */
  readonly eventsRead: number
  readonly maxEventsRead: number
  readonly cacheSize: number
  readonly evictions: number
  readonly snapshotsSaved: number
  readonly snapshotsFailed: number
  readonly snapshotReadFailures: number
  readonly conflicts: number
  readonly commands: ReadonlyArray<CommandTiming>
}

export const emptyMetrics: AggregateMetrics = {
  loads: { memory: 0, snapshot: 0, replay: 0 },
  eventsRead: 0,
  maxEventsRead: 0,
  cacheSize: 0,
  evictions: 0,
  snapshotsSaved: 0,
  snapshotsFailed: 0,
  snapshotReadFailures: 0,
  conflicts: 0,
  commands: []
}

export interface Snapshot {
  readonly events: ReadonlyArray<EventEntry>
  readonly commands: ReadonlyArray<CommandEntry>
  readonly views: { readonly [K in ViewName]: ViewState<ViewData[K]> }
  readonly settings: Settings
  readonly pulses: ReadonlyArray<Pulse>
  readonly metrics: AggregateMetrics
  /** Every span the tracer saw, for the trace tab. */
  readonly spans: ReadonlyArray<SpanEntry>
  readonly epoch: number
}

export const emptyTotals: Totals = {
  moneyInBank: 0,
  inFlight: 0,
  deposited: 0,
  withdrawn: 0,
  transferred: 0,
  refunded: 0,
  openAccounts: 0,
  byType: {}
}

const emptyData: ViewData = { accounts: {}, totals: emptyTotals, feed: [] }

const emptyView = <K extends ViewName>(name: K): ViewState<ViewData[K]> => ({
  data: emptyData[name],
  position: 0,
  lastApplied: 0,
  replaying: false
})

const defaultSettings: Settings = {
  commandDelay: 450,
  projectionDelay: 350,
  replayDelay: 60,
  paused: false,
  autopilot: false,
  autopilotEvery: 1400,
  breakTotals: false
}

const initial = (settings: Settings, epoch: number): Snapshot => ({
  events: [],
  commands: [],
  views: { accounts: emptyView("accounts"), totals: emptyView("totals"), feed: emptyView("feed") },
  settings,
  pulses: [],
  metrics: emptyMetrics,
  spans: [],
  epoch
})

const maxCommands = 60
const maxSpans = 600

let state = initial(defaultSettings, 0)
const listeners = new Set<() => void>()
let scheduled = false
let nextId = 1

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

const pulse = (lane: Pulse["lane"]) => {
  const id = nextId++
  set((s) => ({ ...s, pulses: [...s.pulses, { id, lane }] }))
  setTimeout(() => set((s) => ({ ...s, pulses: s.pulses.filter((p) => p.id !== id) })), 900)
}

const updateCommand = (id: number, f: (entry: CommandEntry) => CommandEntry) =>
  set((s) => ({ ...s, commands: s.commands.map((entry) => entry.id === id ? f(entry) : entry) }))

const updateView = <K extends ViewName>(name: K, f: (view: ViewState<ViewData[K]>) => ViewState<ViewData[K]>) =>
  set((s) => ({ ...s, views: { ...s.views, [name]: f(s.views[name]) } }))

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

  span: (span: SpanEntry) => set((s) => ({ ...s, spans: upsertSpan(s.spans, span, maxSpans) })),

  addEvent: (entry: EventEntry) => {
    set((s) => ({ ...s, events: [...s.events, entry] }))
    pulse("event")
  },
  /** Replaces the decoded events at these positions, as after a subject is forgotten. */
  redecoded: (events: ReadonlyMap<number, AccountEvent>) =>
    set((s) => ({
      ...s,
      events: s.events.map((entry) => {
        const event = events.get(entry.position)
        return event === undefined ? entry : { ...entry, event }
      })
    })),

  addCommand: (command: Omit<CommandEntry, "id" | "stage" | "enqueuedAt">) => {
    const id = nextId++
    set((s) => ({
      ...s,
      commands: [{ ...command, id, stage: "queued" as const, enqueuedAt: Date.now() }, ...s.commands].slice(
        0,
        maxCommands
      )
    }))
    pulse("command")
    return id
  },
  stage: (id: number, stage: Stage) => updateCommand(id, (entry) => ({ ...entry, stage })),
  finish: (id: number, outcome: Outcome) => {
    updateCommand(id, (entry) => ({ ...entry, stage: outcome.ok ? "done" : "rejected", outcome }))
    if (!outcome.ok) pulse("rejected")
  },

  apply: <K extends ViewName>(name: K, position: number, f: (data: ViewData[K]) => ViewData[K]) => {
    updateView(name, (view) => ({ ...view, data: f(view.data), lastApplied: position }))
    pulse("projection")
  },
  dispose: (name: ViewName) => updateView(name, (view) => ({ ...view, data: emptyData[name], lastApplied: 0 })),
  replaying: (name: ViewName, replaying: boolean) => updateView(name, (view) => ({ ...view, replaying })),
  replayed: (name: ViewName, lastReplay: { upTo: number | undefined; eventCount: number }) =>
    updateView(name, (view) => ({ ...view, lastReplay })),
  metrics: (metrics: AggregateMetrics) => {
    // Polled several times a second, so only a change is worth a render.
    if (JSON.stringify(metrics) === JSON.stringify(state.metrics)) return
    set((s) => ({ ...s, metrics }))
  },
  status: (name: ViewName, position: number, error: string | undefined) => {
    const view = state.views[name]
    if (view.position === position && view.error === error) return
    updateView(name, ({ error: _, ...rest }) => ({ ...rest, position, ...(error === undefined ? {} : { error }) }))
  }
}
