/**
 * The control room, for the projector: who is in the room, every event as it is
 * stored, the measures, the stock, personal data before and after it is forgotten,
 * the trace, and the presenter's buttons. It reads `/stream/control`.
 */
import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react"
import { TraceView } from "../../../devtools/TraceView.tsx"
import { type SpanEntry, upsertSpan } from "../../../devtools/tracing.ts"
import type { LiveEvent, Metrics, Presence } from "../../api.ts"
import { expectedBalances } from "../../domain/bank.ts"
import type { CatalogProduct } from "../../domain/shop.ts"
import { call, read } from "./client.ts"
import { ControlScreen } from "./ControlScreen.tsx"
import { follow, type LiveStatus } from "./live.ts"
import { Avatar, Button, colorOf } from "./ui.tsx"

const maxEvents = 80
const maxSpans = 400
const historyLength = 60

interface Sample {
  readonly position: number
  readonly commands: number
  readonly totalMs: number
}

const sampleOf = (metrics: Metrics): Sample => ({
  position: metrics.position,
  commands: metrics.commands.reduce((sum, timing) => sum + timing.count, 0),
  totalMs: metrics.commands.reduce((sum, timing) => sum + timing.totalMs, 0)
})

const time = (iso: string) => iso.slice(11, 19)

/** A colour per correlation, so the events that follow from one request line up. */
const correlationColor = (id: string | null) => id === null ? "transparent" : colorOf(id)

export const ControlRoom = () => {
  const [status, setStatus] = useState<LiveStatus>("connecting")
  const [events, setEvents] = useState<ReadonlyArray<LiveEvent>>([])
  const [presence, setPresence] = useState<Presence>()
  const [metrics, setMetrics] = useState<Metrics>()
  const [history, setHistory] = useState<ReadonlyArray<Sample>>([])
  const [spans, setSpans] = useState<ReadonlyArray<SpanEntry>>([])
  const [selected, setSelected] = useState<LiveEvent>()
  // Nicknames as the events told them, and the subjects whose key was destroyed.
  const nicknames = useRef(new Map<string, string>())
  const [forgotten, setForgotten] = useState<ReadonlySet<string>>(new Set())

  useEffect(() =>
    follow("/stream/control", {
      onStatus: setStatus,
      onMessage: (kind, data) => {
        if (kind === "event") {
          const event = data as LiveEvent
          if (event.type === "AccountOpened") {
            nicknames.current.set(event.aggregateId, (event.payload as { nickname: string }).nickname)
          }
          if (event.type === "SubjectForgotten") setForgotten((set) => new Set(set).add(event.aggregateId))
          setEvents((list) =>
            list.some((e) => e.position === event.position) ? list : [event, ...list].slice(0, maxEvents)
          )
        }
        if (kind === "presence") setPresence(data as Presence)
        if (kind === "metrics") {
          const next = data as Metrics
          setMetrics(next)
          setHistory((list) => [...list, sampleOf(next)].slice(-historyLength))
        }
        if (kind === "spans") {
          setSpans((list) =>
            (data as ReadonlyArray<SpanEntry>).reduce((acc, span) => upsertSpan(acc, span, maxSpans), list)
          )
        }
      }
    }), [])

  const who = useCallback((event: LiveEvent) => {
    if (event.actor === "bot") return "🤖 bot"
    if (event.actor === "simulator") return "🛒 simulatore"
    if (event.actor === "presenter") return "🎤 regia"
    if (event.actor !== undefined) return nicknames.current.get(event.actor) ?? event.actor.slice(0, 8)
    return event.causationId?.includes("/") ? "⚙️ saga" : "—"
  }, [])

  return (
    <div className="min-h-full px-4 py-5 lg:px-6">
      <header className="mb-5 flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="font-mono text-xs tracking-widest text-violet-300 uppercase">effect-cqrs · sala di controllo</p>
          <h1 className="mt-1 text-2xl font-semibold">Cosa succede, mentre succede</h1>
        </div>
        <div className="flex flex-wrap items-center gap-4 text-sm text-zinc-400">
          <span className="flex items-center gap-2">
            <span
              className={`size-2 rounded-full ${
                status === "live"
                  ? "bg-emerald-400"
                  : status === "connecting"
                  ? "animate-pulse bg-amber-400"
                  : "bg-rose-400"
              }`}
            />
            {status === "live" ? "in diretta" : status === "connecting" ? "collego…" : "offline"}
          </span>
          <a className="hover:text-zinc-200" href="#" onClick={() => (window.location.hash = "")}>← torna all'app</a>
        </div>
      </header>

      <div className="grid gap-4 lg:grid-cols-12">
        <div className="flex flex-col gap-4 lg:col-span-3">
          <PresencePanel presence={presence} />
          <PersonalDataPanel events={events} forgotten={forgotten} />
        </div>
        <div className="lg:col-span-5">
          <EventFeed events={events} who={who} selected={selected} onSelect={setSelected} />
        </div>
        <div className="flex flex-col gap-4 lg:col-span-4">
          <MetricsPanel metrics={metrics} history={history} />
          <StockPanel position={metrics?.position} />
        </div>
        <div className="lg:col-span-7">
          <Panel title="Trace" aside={<span className="font-mono text-xs text-zinc-500">{spans.length} span</span>}>
            <div className="max-h-[36rem] overflow-y-auto pr-1">
              <TraceView
                spans={spans}
                intro="Ogni richiesta è una trace: il comando, l'append nel log, i passi delle saghe. Le query SQL sono nascoste."
              />
            </div>
          </Panel>
        </div>
        <div className="flex flex-col gap-4 lg:col-span-5">
          <ChaosPanel />
          <ControlScreen />
        </div>
      </div>
    </div>
  )
}

const Panel = ({ aside, children, title }: {
  readonly title: string
  readonly aside?: ReactNode
  readonly children: ReactNode
}) => (
  <section className="rounded-2xl border border-line bg-panel p-4">
    <div className="mb-3 flex items-center justify-between gap-3">
      <h2 className="text-sm font-medium tracking-wide text-zinc-300 uppercase">{title}</h2>
      {aside}
    </div>
    {children}
  </section>
)

// ---------------------------------------------------------------------------
// Presence
// ---------------------------------------------------------------------------

const PresencePanel = ({ presence }: { readonly presence: Presence | undefined }) => (
  <Panel
    title="In sala"
    aside={<span className="text-2xl font-semibold tabular-nums">{presence?.online.length ?? 0}</span>}
  >
    <ul className="flex flex-wrap gap-2">
      {presence?.online.map((person) => (
        <li key={person.id} className="flex items-center gap-2 rounded-full border border-line py-1 pr-3 pl-1 text-sm">
          <Avatar id={person.id} nickname={person.nickname} />
          {person.nickname}
          {person.connections > 1 && <span className="text-xs text-zinc-500">×{person.connections}</span>}
        </li>
      ))}
    </ul>
    <ul className="mt-3 flex max-h-40 flex-col gap-1 overflow-y-auto font-mono text-xs">
      {presence?.changes.map((change) => (
        <li key={`${change.at}-${change.id}-${change.kind}`} className="flex gap-2">
          <span className="text-zinc-500">{time(change.at)}</span>
          <span className={change.kind === "joined" ? "text-emerald-300" : "text-zinc-400"}>
            {change.kind === "joined" ? "→ entra" : "← esce"}
          </span>
          <span className="truncate">{change.nickname}</span>
        </li>
      ))}
    </ul>
  </Panel>
)

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

const aggregateColors: Record<string, string> = {
  account: "text-sky-300 bg-sky-400/10",
  product: "text-amber-300 bg-amber-400/10",
  order: "text-violet-300 bg-violet-400/10",
  "effect-cqrs/personal-data": "text-rose-300 bg-rose-400/10"
}

const summary = (event: LiveEvent): string => {
  const p = event.payload as Record<string, unknown>
  const parts: Array<string> = []
  if (typeof p.nickname === "string") parts.push(p.nickname)
  if (typeof p.amount === "number") parts.push(`${p.amount} crediti`)
  if (typeof p.productId === "string") parts.push(String(p.productId))
  if (typeof p.quantity === "number") parts.push(`× ${p.quantity}`)
  if (typeof p.total === "number") parts.push(`${p.total} crediti`)
  if (typeof p.reason === "string") parts.push(`«${p.reason}»`)
  if (typeof p.name === "string") parts.push(`${p.emoji ?? ""} ${p.name}`)
  return parts.join(" · ")
}

const EventFeed = ({ events, onSelect, selected, who }: {
  readonly events: ReadonlyArray<LiveEvent>
  readonly who: (event: LiveEvent) => string
  readonly selected: LiveEvent | undefined
  readonly onSelect: (event: LiveEvent | undefined) => void
}) => (
  <Panel
    title="Il log, in diretta"
    aside={<span className="font-mono text-xs text-zinc-500">#{events[0]?.position ?? 0}</span>}
  >
    {selected !== undefined && <EventDetail event={selected} onClose={() => onSelect(undefined)} />}
    <ul className="flex max-h-[44rem] flex-col overflow-y-auto">
      {events.map((event) => (
        <li key={event.position}>
          <button
            type="button"
            onClick={() => onSelect(event)}
            className="flex w-full items-center gap-2 border-t border-line py-1.5 text-left text-sm first:border-0 hover:bg-white/5"
          >
            <span
              className="h-6 w-1 shrink-0 rounded-full"
              style={{ background: correlationColor(event.correlationId) }}
            />
            <span className="w-10 shrink-0 text-right font-mono text-xs text-zinc-500">{event.position}</span>
            <span
              className={`shrink-0 rounded px-1.5 py-0.5 font-mono text-[10px] ${
                aggregateColors[event.aggregate] ?? ""
              }`}
            >
              {event.aggregate === "effect-cqrs/personal-data" ? "oblio" : event.aggregate}
            </span>
            <span className="shrink-0 font-medium">{event.type}</span>
            <span className="min-w-0 flex-1 truncate text-zinc-400">{summary(event)}</span>
            <span className="shrink-0 truncate text-xs text-zinc-500">{who(event)}</span>
          </button>
        </li>
      ))}
    </ul>
    <p className="mt-2 text-xs text-zinc-500">
      La barra colorata è la correlazione: gli eventi causati dalla stessa richiesta, saghe comprese, hanno lo stesso
      colore. Tocca un evento per vederlo com'è nel log.
    </p>
  </Panel>
)

const EventDetail = ({ event, onClose }: { readonly event: LiveEvent; readonly onClose: () => void }) => (
  <div className="mb-3 rounded-xl border border-violet-400/30 bg-violet-400/5 p-3 text-xs">
    <div className="mb-2 flex items-center justify-between">
      <span className="font-medium">
        {event.aggregate} · {event.aggregateId.slice(0, 12)} · v{event.version} · {event.type}
      </span>
      <button type="button" className="text-zinc-400 hover:text-zinc-200" onClick={onClose}>chiudi</button>
    </div>
    <div className="grid gap-2 sm:grid-cols-2">
      <div>
        <p className="mb-1 text-zinc-400">letto con le chiavi</p>
        <pre className="overflow-x-auto rounded-lg bg-ink p-2 font-mono">{JSON.stringify(event.payload, null, 2)}</pre>
      </div>
      <div>
        <p className="mb-1 text-zinc-400">come sta nel log</p>
        <pre className="overflow-x-auto rounded-lg bg-ink p-2 font-mono">{JSON.stringify(event.stored ?? event.payload, null, 2)}</pre>
      </div>
    </div>
    <p className="mt-2 font-mono text-zinc-500">
      correlazione {event.correlationId ?? "—"} · causa {event.causationId ?? "—"}
    </p>
  </div>
)

// ---------------------------------------------------------------------------
// Personal data
// ---------------------------------------------------------------------------

const cipherOf = (stored: unknown): string | undefined => {
  const nickname = (stored as { nickname?: { $personal?: { data?: string } } } | undefined)?.nickname
  return nickname?.$personal?.data
}

const PersonalDataPanel = ({ events, forgotten }: {
  readonly events: ReadonlyArray<LiveEvent>
  readonly forgotten: ReadonlySet<string>
}) => {
  const opened = events.filter((event) => event.type === "AccountOpened").slice(0, 6)
  return (
    <Panel title="Dati personali">
      <p className="mb-3 text-xs text-zinc-400">
        Il nickname è cifrato con una chiave per persona. Chiudere il conto distrugge la chiave: l'evento resta, il
        nickname diventa illeggibile.
      </p>
      <ul className="flex flex-col gap-2 text-xs">
        {opened.map((event) => {
          const gone = forgotten.has(event.aggregateId)
          return (
            <li key={event.position} className="rounded-lg border border-line p-2">
              <div className="flex items-center justify-between gap-2">
                <span className={gone ? "text-zinc-500 line-through" : "font-medium"}>
                  {(event.payload as { nickname: string }).nickname}
                </span>
                {gone
                  ? <span className="text-rose-300">🔥 chiave distrutta → [forgotten]</span>
                  : <span className="text-emerald-300">🔑 chiave viva</span>}
              </div>
              <p className="mt-1 truncate font-mono text-zinc-500">nel log: {cipherOf(event.stored) ?? "…"}</p>
            </li>
          )
        })}
      </ul>
    </Panel>
  )
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

const Sparkline = ({ values, color }: { readonly values: ReadonlyArray<number>; readonly color: string }) => {
  const max = Math.max(1, ...values)
  const points = values.map((value, i) => `${(i / Math.max(1, historyLength - 1)) * 100},${30 - (value / max) * 28}`)
  return (
    <svg viewBox="0 0 100 30" preserveAspectRatio="none" className="h-8 w-full">
      <polyline
        points={points.join(" ")}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  )
}

const Tile = ({ label, value, children }: {
  readonly label: string
  readonly value: string
  readonly children?: ReactNode
}) => (
  <div className="rounded-xl border border-line p-3">
    <p className="text-xs text-zinc-400">{label}</p>
    <p className="text-xl font-semibold tabular-nums">{value}</p>
    {children}
  </div>
)

const orderLabels: Record<string, string> = {
  placed: "in pagamento",
  paid: "pagati",
  backordered: "in coda",
  fulfilled: "consegnati",
  cancelled: "annullati",
  rejected: "rifiutati"
}

const MetricsPanel = ({ history, metrics }: {
  readonly metrics: Metrics | undefined
  readonly history: ReadonlyArray<Sample>
}) => {
  const rates = useMemo(() =>
    history.slice(1).map((sample, i) => ({
      events: sample.position - history[i]!.position,
      commands: sample.commands - history[i]!.commands,
      ms: sample.commands - history[i]!.commands > 0
        ? (sample.totalMs - history[i]!.totalMs) / (sample.commands - history[i]!.commands)
        : 0
    })), [history])
  const last = rates.at(-1)
  if (metrics === undefined) {
    return (
      <Panel title="Metriche">
        <p className="text-sm text-zinc-500">…</p>
      </Panel>
    )
  }
  const { books } = metrics
  const inTransit = books.transfersSent - books.transfersReceived - books.transfersRefunded
  const failures = metrics.commands.filter((timing) => timing.outcome !== "success")
    .reduce((sum, timing) => sum + timing.count, 0)
  return (
    <Panel title="Metriche" aside={<span className="font-mono text-xs text-zinc-500">{metrics.memoryMb} MB</span>}>
      <div className="grid grid-cols-2 gap-2">
        <Tile label="Eventi al secondo" value={String(last?.events ?? 0)}>
          <Sparkline values={rates.map((rate) => rate.events)} color="#a78bfa" />
        </Tile>
        <Tile label="Comandi al secondo" value={String(last?.commands ?? 0)}>
          <Sparkline values={rates.map((rate) => rate.commands)} color="#34d399" />
        </Tile>
        <Tile label="Latenza media di un comando" value={`${(last?.ms ?? 0).toFixed(1)} ms`} />
        <Tile label="Eventi nel log · rifiutati" value={`${metrics.position} · ${failures}`} />
      </div>

      <div
        className={`mt-3 rounded-xl border p-3 text-sm ${
          metrics.invariant === "ok"
            ? "border-emerald-400/30 bg-emerald-400/5"
            : metrics.invariant === "catching-up"
            ? "border-amber-400/30 bg-amber-400/5"
            : "border-rose-400/40 bg-rose-400/10"
        }`}
      >
        <p className="font-medium">
          {metrics.invariant === "ok"
            ? "✓ I conti tornano"
            : metrics.invariant === "catching-up"
            ? "… view model in aggiornamento"
            : "✗ I conti non tornano"}
        </p>
        <p className="mt-1 font-mono text-xs text-zinc-400">
          ricaricati {books.deposited} − prelevati {books.withdrawn} − incassi shop{" "}
          {books.purchases - books.purchaseRefunds} − in transito {inTransit} = {expectedBalances(books)}{" "}
          · somma dei saldi {metrics.balances}
        </p>
      </div>

      <div className="mt-3 flex flex-wrap gap-2 text-xs">
        {Object.entries(metrics.orders).map(([status, count]) => (
          <span key={status} className="rounded-full border border-line px-2 py-0.5">
            {orderLabels[status] ?? status} <span className="font-mono">{count}</span>
          </span>
        ))}
      </div>

      <table className="mt-3 w-full font-mono text-xs">
        <thead className="text-zinc-500">
          <tr>
            <th className="text-left font-normal">view model</th>
            <th className="text-right font-normal">posizione</th>
            <th className="text-right font-normal">indietro di</th>
          </tr>
        </thead>
        <tbody>
          {metrics.viewModels.map((vm) => (
            <tr key={vm.name} className={vm.error !== undefined ? "text-rose-300" : ""}>
              <td>{vm.name}</td>
              <td className="text-right">{vm.position}</td>
              <td className={`text-right ${vm.lag > 0 ? "text-amber-300" : "text-zinc-500"}`}>{vm.lag}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  )
}

// ---------------------------------------------------------------------------
// Stock
// ---------------------------------------------------------------------------

const StockPanel = ({ position }: { readonly position: number | undefined }) => {
  const [products, setProducts] = useState<ReadonlyArray<CatalogProduct>>([])
  useEffect(() => {
    const timer = setTimeout(
      () =>
        void read((client, headers) => client.shop.products({ headers }), position).then(
          (outcome) => {
            if (outcome.ok) setProducts(outcome.value)
          }
        ),
      200
    )
    return () => clearTimeout(timer)
  }, [position])
  const max = Math.max(5, ...products.map((product) => product.available))
  return (
    <Panel title="Scaffali e code">
      <ul className="flex flex-col gap-2 text-sm">
        {products.map((product) => {
          const waiting = product.queue.reduce((sum, entry) => sum + entry.quantity, 0)
          return (
            <li key={product.id} className="grid grid-cols-[1.5rem_6rem_1fr_auto] items-center gap-2">
              <span>{product.emoji}</span>
              <span className="truncate">{product.name}</span>
              <span className="h-2 overflow-hidden rounded-full bg-white/5">
                <span
                  className={`block h-full rounded-full ${product.available === 0 ? "bg-rose-400" : "bg-emerald-400"}`}
                  style={{ width: `${(product.available / max) * 100}%` }}
                />
              </span>
              <span className="font-mono text-xs">
                {product.available}
                {waiting > 0 && <span className="text-amber-300">· {product.queue.length} in coda</span>}
                <span className="text-zinc-500">· {product.sold} venduti</span>
              </span>
            </li>
          )
        })}
      </ul>
    </Panel>
  )
}

// ---------------------------------------------------------------------------
// Chaos and replay
// ---------------------------------------------------------------------------

const viewModelNames = ["participants", "ledger", "books", "catalog", "orders"] as const

const ChaosPanel = () => {
  const [note, setNote] = useState<string>()
  const [viewModel, setViewModel] = useState<(typeof viewModelNames)[number]>("catalog")

  const breakNext = async () => {
    const outcome = await call((client) => client.admin.breakNextOrder())
    setNote(
      outcome.ok
        ? `Il prossimo ordine si guasta dopo il pagamento (${outcome.value.pending} in attesa): guarda la saga rimborsarlo.`
        : outcome.message
    )
  }

  const replay = async () => {
    setNote(`Ricostruisco ${viewModel}…`)
    const outcome = await call((client) => client.admin.replay({ payload: { viewModel } }))
    setNote(
      outcome.ok
        ? `${viewModel} ricostruito da zero rileggendo ${outcome.value.eventCount} eventi.`
        : outcome.message
    )
  }

  return (
    <Panel title="Regia">
      <div className="flex flex-col gap-3 text-sm">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="text-zinc-300">Guasta il prossimo ordine dopo il pagamento</span>
          <Button tone="danger" onClick={() => void breakNext()}>💥 Guasta</Button>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="flex items-center gap-2 text-zinc-300">
            Ricostruisci
            <select
              className="rounded-lg border border-line bg-ink px-2 py-1"
              value={viewModel}
              onChange={(event) => setViewModel(event.target.value as typeof viewModel)}
            >
              {viewModelNames.map((name) => <option key={name} value={name}>{name}</option>)}
            </select>
            dal log
          </span>
          <Button tone="ghost" onClick={() => void replay()}>↻ Ricostruisci</Button>
        </div>
        {note !== undefined && <p className="text-xs text-zinc-400">{note}</p>}
      </div>
    </Panel>
  )
}
