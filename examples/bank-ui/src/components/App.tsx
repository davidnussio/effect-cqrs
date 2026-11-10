import { AnimatePresence, motion } from "motion/react"
import { type ReactNode, useEffect, useState } from "react"
import { engine } from "../engine.ts"
import { head, useSnapshot } from "../hooks.ts"
import { BankApp } from "./BankApp.tsx"
import { Console, Speed } from "./Commands.tsx"
import { EventStore } from "./EventStore.tsx"
import { Metrics } from "./Metrics.tsx"
import { Pipeline } from "./Pipeline.tsx"
import { Projections } from "./Projections.tsx"
import { TimeTravel } from "./TimeTravel.tsx"
import { Trace } from "./Trace.tsx"
import { AnimatedNumber, Button, Code } from "./ui.tsx"

// The documentation site serves the demo under its own /demo/.
const docsUrl = import.meta.env.BASE_URL.endsWith("/demo/") ? `${import.meta.env.BASE_URL}../` : undefined

// The page follows a command: the app that sends it, the write side, the log, the read
// side, the log again as history, and what Effect recorded on the way.
const sections = [
  { id: "bank", label: "Bank" },
  { id: "commands", label: "Commands" },
  { id: "events", label: "Event store" },
  { id: "projections", label: "Projections" },
  { id: "time-travel", label: "Time travel" },
  { id: "observability", label: "Trace & metrics" }
] as const

type SectionId = (typeof sections)[number]["id"]

export const App = () => {
  // A position while time travelling: the app then shows a fold of the events up
  // to there instead of the read model.
  const [travel, setTravel] = useState<number | undefined>(undefined)
  const events = useSnapshot((s) => s.events)
  const epoch = useSnapshot((s) => s.epoch)
  const active = useActiveSection()

  return (
    <div className="relative min-h-full">
      <div className="bg-aurora pointer-events-none fixed inset-0" />
      <div className="bg-grid pointer-events-none fixed inset-0" />

      <div className="sticky top-0 z-40">
        <header className="flex items-center gap-4 border-b border-white/[0.06] bg-[#07070d]/75 px-4 py-3 backdrop-blur-xl sm:px-6">
          <a href="#bank" className="flex items-center gap-3">
            <Logo />
            <div className="hidden sm:block">
              <div className="text-[15px] font-semibold tracking-tight">
                Event Bank <span className="text-white/40">Lab</span>
              </div>
              <div className="font-mono text-[10px] text-white/40">effect-cqrs in your browser · in-memory store</div>
            </div>
          </a>
          <SectionIndex active={active} travel={travel} />
          <div className="ml-auto flex items-center gap-4 font-mono text-[11px] text-white/50">
            <Stat label="events" value={head(events)} />
            <Stat label="streams" value={new Set(events.map((e) => e.aggregateId)).size} />
            <Button
              tone="ghost"
              title="Empty the event store and start again with the three accounts"
              onClick={() => {
                setTravel(undefined)
                void engine.reset()
              }}
            >
              ↺ Reset
            </Button>
            {docsUrl !== undefined && (
              <a
                href={docsUrl}
                className="rounded-lg px-2.5 py-1.5 font-sans text-xs font-medium text-white/60 transition-colors hover:bg-white/[0.06] hover:text-white"
              >
                Docs ↗
              </a>
            )}
          </div>
        </header>
        <AnimatePresence>
          {travel !== undefined && <TravelBar travel={travel} head={head(events)} onTravel={setTravel} />}
        </AnimatePresence>
      </div>

      <main key={epoch} className="relative z-10 mx-auto max-w-[1600px] px-4 pb-24 sm:px-6">
        <section
          id="bank"
          className="grid scroll-mt-20 items-start gap-6 pt-6 xl:grid-cols-[minmax(0,1fr)_minmax(440px,40%)]"
        >
          <BankApp travel={travel} />
          <Pipeline />
        </section>

        <Section
          id="commands"
          step="01"
          side="write side"
          title="Commands"
          lead={
            <>
              Each click is a command. The bus validates it, loads the account's state, lets the aggregate decide, and
              appends what it decided. Slow the stages down to watch them in the pipeline above, or write a command by
              hand and send it through <Code>dispatch</Code>.
            </>
          }
        >
          <div className="grid items-start gap-5 lg:grid-cols-2">
            <Speed />
            <Console />
          </div>
        </Section>

        <Section
          id="events"
          step="02"
          side="source of truth"
          title="Event store"
          lead="An append-only log with a stream per account. Nothing in it changes: a correction is another event. Click an event to see what is stored."
        >
          <EventStore travel={travel} />
        </Section>

        <Section
          id="projections"
          step="03"
          side="read side"
          title="Projections"
          lead="Three view models read the same events, each from its own checkpoint, and fold them into the shape a screen needs. They are disposable: a rebuild wipes one and feeds it the log again."
        >
          <Projections />
        </Section>

        <Section
          id="time-travel"
          step="04"
          side="history"
          title="Time travel"
          lead="Any past state is a fold of the log. Scrub to an earlier event and the bank above becomes a preview of that moment, or rebuild the view models up to it."
        >
          <TimeTravel travel={travel} onTravel={setTravel} />
        </Section>

        <Section
          id="observability"
          step="05"
          side="observability"
          title="Trace & metrics"
          lead="What Effect recorded along the way: a span for every stage a command waited in, and the metrics of AggregateService."
        >
          <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
            <Trace />
            <Metrics />
          </div>
        </Section>
      </main>
    </div>
  )
}

const Section = (
  { id, step, side, title, lead, children }: {
    id: SectionId
    step: string
    side: string
    title: string
    lead: ReactNode
    children: ReactNode
  }
) => (
  <section id={id} className="scroll-mt-24 border-t border-white/[0.06] pt-10 mt-14">
    <div className="mb-6 max-w-3xl">
      <div className="font-mono text-[10px] uppercase tracking-[0.2em] text-white/40">
        <span className="text-violet-300/80">{step}</span> · {side}
      </div>
      <h2 className="mt-1 text-2xl font-semibold tracking-tight">{title}</h2>
      <p className="mt-2 text-sm leading-relaxed text-white/50">{lead}</p>
    </div>
    {children}
  </section>
)

/**
 * The last section whose top has passed a third of the window, or the last one at
 * the bottom of the page, where a short section never gets that high.
 */
const useActiveSection = (): SectionId => {
  const [active, setActive] = useState<SectionId>("bank")
  useEffect(() => {
    // Scroll events come at most once a frame, so this runs no more often than that.
    const update = () => {
      const line = window.innerHeight / 3
      let current: SectionId = sections[0].id
      for (const { id } of sections) {
        const top = document.getElementById(id)?.getBoundingClientRect().top
        if (top !== undefined && top <= line) current = id
      }
      const bottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 4
      setActive(bottom ? sections[sections.length - 1].id : current)
    }
    update()
    window.addEventListener("scroll", update, { passive: true })
    window.addEventListener("resize", update)
    return () => {
      window.removeEventListener("scroll", update)
      window.removeEventListener("resize", update)
    }
  }, [])
  return active
}

const SectionIndex = ({ active, travel }: { active: SectionId; travel: number | undefined }) => {
  const pending = useSnapshot((s) => s.commands.filter((c) => c.outcome === undefined).length)
  const stalled = useSnapshot((s) => Object.values(s.views).some((view) => view.error !== undefined))
  const badges: Partial<Record<SectionId, { color: string; text: string } | undefined>> = {
    commands: pending > 0 ? { color: "#fbbf24", text: String(pending) } : undefined,
    projections: stalled ? { color: "#fb7185", text: "!" } : undefined,
    "time-travel": travel !== undefined ? { color: "#38bdf8", text: `#${travel}` } : undefined
  }

  return (
    <nav aria-label="Sections" className="ml-4 hidden items-center gap-0.5 lg:flex">
      {sections.map(({ id, label }) => {
        const badge = badges[id]
        return (
          <a
            key={id}
            href={`#${id}`}
            aria-current={active === id ? "location" : undefined}
            className={`relative flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-medium transition-colors ${
              active === id ? "text-white" : "text-white/45 hover:text-white/80"
            }`}
          >
            {active === id && (
              <motion.span
                layoutId="section-highlight"
                className="absolute inset-0 rounded-lg bg-white/[0.07] ring-1 ring-white/[0.08]"
                transition={{ type: "spring", stiffness: 500, damping: 40 }}
              />
            )}
            <span className="relative">{label}</span>
            {badge && (
              <motion.span
                key={badge.text}
                initial={{ scale: 1.5 }}
                animate={{ scale: 1 }}
                className="relative rounded-full px-1.5 py-px font-mono text-[9px] font-semibold"
                style={{ background: `${badge.color}26`, color: badge.color }}
              >
                {badge.text}
              </motion.span>
            )}
          </a>
        )
      })}
    </nav>
  )
}

/** Shown on every section while time travelling, so the way back is always in sight. */
const TravelBar = (
  { travel, head, onTravel }: { travel: number; head: number; onTravel: (position: number | undefined) => void }
) => (
  <motion.div
    initial={{ opacity: 0, height: 0 }}
    animate={{ opacity: 1, height: "auto" }}
    exit={{ opacity: 0, height: 0 }}
    className="overflow-hidden border-b border-sky-400/25 bg-sky-500/10 backdrop-blur-xl"
  >
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 px-4 py-2 text-xs text-sky-100 sm:px-6">
      <motion.span animate={{ rotate: -360 }} transition={{ duration: 3, repeat: Infinity, ease: "linear" }}>
        ⏪
      </motion.span>
      <span>
        <b>Time travel at #{travel}</b>
        <span className="text-sky-100/60">
          {" "}· the bank shows events 1…{travel} folded with the aggregate's projection. No read model changed.
        </span>
      </span>
      <input
        type="range"
        aria-label="Position in the log"
        className="min-w-40 flex-1"
        min={0}
        max={head}
        value={travel}
        style={{ "--pct": `${head === 0 ? 0 : (travel / head) * 100}%`, "--fill": "#38bdf8" } as React.CSSProperties}
        onChange={(event) => {
          const value = Number(event.target.value)
          onTravel(value >= head ? undefined : value)
        }}
      />
      <Button tone="primary" onClick={() => onTravel(undefined)}>Back to live</Button>
    </div>
  </motion.div>
)

const Stat = ({ label, value }: { label: string; value: number }) => (
  <div className="hidden items-baseline gap-1.5 md:flex">
    <span className="text-[15px] font-semibold text-white">
      <AnimatedNumber value={value} format={(n) => Math.round(n).toString()} />
    </span>
    {label}
  </div>
)

const Logo = () => (
  <motion.div
    className="relative grid h-9 w-9 shrink-0 place-items-center rounded-xl bg-gradient-to-br from-violet-500 to-fuchsia-500 shadow-[0_0_30px_-6px_rgba(167,139,250,0.9)]"
    animate={{ rotate: [0, 6, -6, 0] }}
    transition={{ duration: 6, repeat: Infinity, ease: "easeInOut" }}
  >
    <svg viewBox="0 0 24 24" className="h-5 w-5 text-white" fill="none" stroke="currentColor" strokeWidth={2.2}>
      <path d="M4 7h10M4 12h16M4 17h7" strokeLinecap="round" />
      <circle cx="18" cy="7" r="2" fill="currentColor" />
      <circle cx="15" cy="17" r="2" fill="currentColor" />
    </svg>
  </motion.div>
)
