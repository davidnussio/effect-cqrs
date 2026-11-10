import { AnimatePresence, motion } from "motion/react"
import { useEffect, useMemo, useState } from "react"
import type { Snapshot, ViewName } from "../devstore.ts"
import { balanceDelta, initialAccount } from "../domain/account.ts"
import { engine } from "../engine.ts"
import { eventColor, foldAt, head, money, signed, useSnapshot } from "../hooks.ts"
import { AnimatedNumber, Button, Code, SectionTitle } from "./ui.tsx"

export const TimeTravelTab = (
  { travel, onTravel }: { travel: number | undefined; onTravel: (position: number | undefined) => void }
) => {
  const events = useSnapshot((s) => s.events)
  const views = useSnapshot((s) => s.views)
  const storeHead = head(events)
  const cursor = travel ?? storeHead
  const [playing, setPlaying] = useState(false)

  // Replays the history into the app, one event at a time.
  useEffect(() => {
    if (!playing) return
    const timer = setInterval(() => {
      const next = (travel ?? 0) + 1
      if (next >= storeHead) {
        setPlaying(false)
        onTravel(undefined)
      } else {
        onTravel(next)
      }
    }, 280)
    return () => clearInterval(timer)
  }, [playing, travel, storeHead, onTravel])

  const accounts = useMemo(() => Array.from(new Set(events.map((event) => event.aggregateId))), [events])
  const [selected, setSelected] = useState<string | undefined>(undefined)
  const account = selected ?? accounts[0]

  const [targets, setTargets] = useState<ReadonlyArray<ViewName>>(["accounts", "totals", "feed"])
  const replaying = targets.some((name) => views[name].replaying)

  return (
    <div className="space-y-5">
      <div className="glass rounded-2xl p-4">
        <SectionTitle
          right={
            <div className="flex gap-1.5">
              <Button
                onClick={() => {
                  if (!playing && (travel === undefined || travel >= storeHead)) onTravel(0)
                  setPlaying(!playing)
                }}
                disabled={storeHead === 0}
              >
                {playing ? "❚❚" : "▶ Play history"}
              </Button>
              <Button
                tone={travel === undefined ? "ghost" : "primary"}
                onClick={() => {
                  setPlaying(false)
                  onTravel(undefined)
                }}
              >
                Live
              </Button>
            </div>
          }
        >
          Scrub the log
        </SectionTitle>
        <div className="mb-1 flex items-baseline justify-between">
          <span className="text-3xl font-semibold tabular-nums">
            #<AnimatedNumber value={cursor} format={(n) => Math.round(n).toString()} />
          </span>
          <span className="font-mono text-[10px] text-white/40">of {storeHead} events</span>
        </div>
        <div className="relative pt-3">
          <div className="pointer-events-none absolute inset-x-0 top-0 flex h-3 items-end gap-px">
            {events.map((event) => (
              <motion.div
                key={event.position}
                initial={{ scaleY: 0 }}
                animate={{ scaleY: 1, opacity: event.position <= cursor ? 1 : 0.2 }}
                className="flex-1 origin-bottom rounded-t-sm"
                style={{ background: eventColor[event.type], height: Math.abs(balanceDelta(event.event)) > 0 ? 12 : 7 }}
              />
            ))}
          </div>
          <input
            type="range"
            className="w-full"
            min={0}
            max={storeHead}
            value={cursor}
            style={{
              "--pct": `${storeHead === 0 ? 0 : (cursor / storeHead) * 100}%`,
              "--fill": "#38bdf8"
            } as React.CSSProperties}
            onChange={(event) => {
              setPlaying(false)
              const value = Number(event.target.value)
              onTravel(value >= storeHead ? undefined : value)
            }}
          />
        </div>
        <p className="mt-2 text-[11px] leading-relaxed text-white/45">
          Moving the cursor turns the customer app into a preview: each card is{" "}
          <Code>events.filter(≤ #{cursor}).reduce(evolve, initial)</Code>. The read models are untouched.
        </p>
      </div>

      <div className="glass rounded-2xl p-4">
        <SectionTitle>State is the sum of its events</SectionTitle>
        <div className="mb-3 flex flex-wrap gap-1">
          {accounts.map((id) => (
            <button
              key={id}
              onClick={() => setSelected(id)}
              className={`rounded-full px-2.5 py-0.5 font-mono text-[10.5px] transition ${
                id === account ? "bg-white text-black" : "bg-white/[0.06] text-white/60 hover:bg-white/10"
              }`}
            >
              {id}
            </button>
          ))}
        </div>
        {account !== undefined && <Ledger events={events} account={account} cursor={cursor} onTravel={onTravel} />}
      </div>

      <div className="glass rounded-2xl p-4">
        <SectionTitle>Reprocess the projections</SectionTitle>
        <p className="mb-3 text-[11px] leading-relaxed text-white/45">
          <Code>viewModels.replay</Code>{" "}
          disposes the chosen view models and feeds them the log again, at replay speed. With a window, events after
          #{cursor} stay out of them while new events keep arriving on top.
        </p>
        <div className="mb-3 flex gap-1.5">
          {(["accounts", "totals", "feed"] as const).map((name) => {
            const on = targets.includes(name)
            return (
              <button
                key={name}
                onClick={() => setTargets(on ? targets.filter((t) => t !== name) : [...targets, name])}
                className={`flex items-center gap-1.5 rounded-lg px-2.5 py-1 font-mono text-[10.5px] ring-1 transition ${
                  on ? "bg-sky-400/15 text-sky-200 ring-sky-400/40" : "text-white/40 ring-white/10"
                }`}
              >
                <span className={`h-2 w-2 rounded-sm ${on ? "bg-sky-300" : "bg-white/15"}`} />
                {name}
                {views[name].replaying && (
                  <motion.span animate={{ rotate: 360 }} transition={{ duration: 1, repeat: Infinity, ease: "linear" }}>
                    ↻
                  </motion.span>
                )}
              </button>
            )
          })}
        </div>
        <div className="flex gap-2">
          <Button
            tone="primary"
            disabled={targets.length === 0 || replaying || cursor === 0}
            onClick={() => {
              // Leave the preview so the app shows the read model being rebuilt.
              setPlaying(false)
              onTravel(undefined)
              void engine.replay(targets, cursor)
            }}
          >
            ⏮ Replay up to #{cursor}
          </Button>
          <Button
            disabled={targets.length === 0 || replaying}
            onClick={() => {
              setPlaying(false)
              onTravel(undefined)
              void engine.replay(targets)
            }}
          >
            ↻ Replay everything
          </Button>
        </div>
        <AnimatePresence>
          {targets.some((name) => views[name].lastReplay) && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="mt-3 space-y-0.5 font-mono text-[10px]"
            >
              {targets.map((name) => {
                const last = views[name].lastReplay
                return last && (
                  <div key={name} className="text-sky-300/70">
                    {name}: {last.eventCount} events replayed
                    {last.upTo === undefined ? " (whole log)" : ` (window ends at #${last.upTo})`}
                  </div>
                )
              })}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  )
}

const Ledger = (
  { events, account, cursor, onTravel }: {
    events: ReturnType<typeof useSnapshot<Snapshot["events"]>>
    account: string
    cursor: number
    onTravel: (position: number | undefined) => void
  }
) => {
  const own = events.filter((event) => event.aggregateId === account)
  const state = foldAt(own, cursor)[account] ?? initialAccount
  let running = 0

  return (
    <div>
      <div className="mb-2 rounded-lg bg-black/30 px-3 py-2 font-mono text-[10.5px]">
        <span className="text-white/40">state(#{cursor}) =</span>
        <span className="text-white/85">
          {"{ "}status: <span className="text-violet-300">"{state.status}"</span>, balance:{" "}
          <span className="text-emerald-300">
            <AnimatedNumber value={state.balance} format={(n) => Math.round(n).toString()} />
          </span>
          {" }"}
        </span>
      </div>
      <div className="space-y-px">
        <div className="flex px-2 font-mono text-[9px] uppercase tracking-wider text-white/25">
          <span className="w-8">v</span>
          <span className="flex-1">event</span>
          <span className="w-16 text-right">Δ</span>
          <span className="w-20 text-right">balance</span>
        </div>
        <Row v="0" label="initial" delta="" balance={0} dim={false} color="#64748b" />
        {own.map((event) => {
          const delta = balanceDelta(event.event)
          running += delta
          const dim = event.position > cursor
          return (
            <div key={event.position} onClick={() => onTravel(event.position)} className="cursor-pointer">
              <Row
                v={String(event.version)}
                label={`${event.type}  #${event.position}`}
                delta={delta === 0 ? "·" : signed(delta)}
                balance={running}
                dim={dim}
                color={eventColor[event.type]!}
                current={event.position === cursor}
              />
            </div>
          )
        })}
      </div>
    </div>
  )
}

const Row = (
  { v, label, delta, balance, dim, color, current = false }: {
    v: string
    label: string
    delta: string
    balance: number
    dim: boolean
    color: string
    current?: boolean
  }
) => (
  <motion.div
    animate={{ opacity: dim ? 0.25 : 1, backgroundColor: current ? "rgba(56,189,248,0.15)" : "rgba(255,255,255,0)" }}
    className="flex items-center rounded-md px-2 py-0.5 font-mono text-[10.5px] hover:bg-white/[0.04]"
  >
    <span className="w-8 text-white/35">{v}</span>
    <span className="flex flex-1 items-center gap-1.5 text-white/75">
      <span className="h-1.5 w-1.5 rounded-full" style={{ background: color }} />
      {label}
    </span>
    <span
      className={`w-16 text-right ${
        delta.startsWith("+") ? "text-emerald-300" : delta.startsWith("−") ? "text-rose-300" : "text-white/30"
      }`}
    >
      {delta}
    </span>
    <span className={`w-20 text-right ${dim ? "line-through" : "text-white"}`}>{money(balance)}</span>
  </motion.div>
)
