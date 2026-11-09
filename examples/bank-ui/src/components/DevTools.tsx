import { AnimatePresence, motion } from "motion/react"
import { useState } from "react"
import type { Pulse, Stage } from "../devstore.ts"
import { useSnapshot } from "../hooks.ts"
import { CommandsTab } from "./CommandsTab.tsx"
import { EventsTab } from "./EventsTab.tsx"
import { MetricsTab } from "./MetricsTab.tsx"
import { ProjectionsTab } from "./ProjectionsTab.tsx"
import { TimeTravelTab } from "./TimeTravelTab.tsx"

const tabs = ["Commands", "Event store", "Projections", "Time travel", "Metrics"] as const
type Tab = (typeof tabs)[number]

export const DevTools = (
  { travel, onTravel }: { travel: number | undefined; onTravel: (position: number | undefined) => void }
) => {
  const [tab, setTab] = useState<Tab>("Commands")
  const pending = useSnapshot((s) => s.commands.filter((c) => c.outcome === undefined).length)
  const eventCount = useSnapshot((s) => s.events.length)
  const stalled = useSnapshot((s) => Object.values(s.views).some((view) => view.error !== undefined))

  return (
    <aside className="flex min-h-0 flex-col border-l border-white/[0.07] bg-[#0a0a12]/80 backdrop-blur-xl">
      <div className="flex items-center gap-2 border-b border-white/[0.06] px-4 pt-3 pb-2">
        <span className="font-mono text-[10px] font-semibold uppercase tracking-[0.22em] text-violet-300/80">
          ◆ CQRS devtools
        </span>
      </div>
      <FlowStrip />
      <nav className="flex gap-1 border-b border-white/[0.06] px-3">
        {tabs.map((name) => (
          <button
            key={name}
            onClick={() => setTab(name)}
            className={`relative px-3 py-2.5 text-xs font-medium transition-colors ${
              tab === name ? "text-white" : "text-white/45 hover:text-white/80"
            }`}
          >
            {name}
            {name === "Commands" && pending > 0 && <Badge color="#fbbf24">{pending}</Badge>}
            {name === "Event store" && <Badge color="#a78bfa">{eventCount}</Badge>}
            {name === "Projections" && stalled && <Badge color="#fb7185">!</Badge>}
            {name === "Time travel" && travel !== undefined && <Badge color="#38bdf8">#{travel}</Badge>}
            {tab === name && (
              <motion.span
                layoutId="tab-underline"
                className="absolute inset-x-2 -bottom-px h-[2px] rounded-full bg-gradient-to-r from-violet-400 to-fuchsia-400"
              />
            )}
          </button>
        ))}
      </nav>
      <div className="scroll-thin relative min-h-0 flex-1 overflow-y-auto">
        <AnimatePresence mode="wait">
          <motion.div
            key={tab}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.18 }}
            className="p-4"
          >
            {tab === "Commands" && <CommandsTab />}
            {tab === "Event store" && <EventsTab travel={travel} />}
            {tab === "Projections" && <ProjectionsTab />}
            {tab === "Time travel" && <TimeTravelTab travel={travel} onTravel={onTravel} />}
            {tab === "Metrics" && <MetricsTab />}
          </motion.div>
        </AnimatePresence>
      </div>
    </aside>
  )
}

const Badge = ({ color, children }: { color: string; children: React.ReactNode }) => (
  <motion.span
    key={String(children)}
    initial={{ scale: 1.5 }}
    animate={{ scale: 1 }}
    className="ml-1.5 rounded-full px-1.5 py-px font-mono text-[9px] font-semibold"
    style={{ background: `${color}26`, color }}
  >
    {children}
  </motion.span>
)

// ---------------------------------------------------------------------------
// Flow strip: where each command and event is right now.

const nodes = [
  { id: "app", x: 52, label: "UI", sub: "intent" },
  { id: "bus", x: 176, label: "Command", sub: "validate" },
  { id: "aggregate", x: 300, label: "Aggregate", sub: "rehydrate · decide" },
  { id: "store", x: 424, label: "Event store", sub: "append-only" },
  { id: "views", x: 548, label: "View models", sub: "project" }
] as const

type NodeId = (typeof nodes)[number]["id"]

const stageNode: Partial<Record<Stage, NodeId>> = {
  queued: "bus",
  validating: "bus",
  loading: "aggregate",
  deciding: "aggregate",
  appending: "store"
}

const y = 44
const xOf = (id: NodeId) => nodes.find((node) => node.id === id)!.x

// The read side flows back to the UI under the strip.
const returnArc = (t: number) => {
  const [x0, y0, x1, y1, x2, y2, x3, y3] = [548, y + 22, 470, 112, 130, 112, 52, y + 22]
  const u = 1 - t
  return {
    x: u * u * u * x0 + 3 * u * u * t * x1 + 3 * u * t * t * x2 + t * t * t * x3,
    y: u * u * u * y0 + 3 * u * u * t * y1 + 3 * u * t * t * y2 + t * t * t * y3
  }
}
const arcPoints = Array.from({ length: 16 }, (_, i) => returnArc(i / 15))

const pulsePath = (lane: Pulse["lane"]) => {
  switch (lane) {
    case "command":
      return { cx: [xOf("app"), xOf("bus")], cy: [y, y], color: "#a78bfa" }
    case "event":
      return { cx: [xOf("aggregate"), xOf("store"), xOf("views")], cy: [y, y, y], color: "#34d399" }
    case "projection":
      return { cx: arcPoints.map((p) => p.x), cy: arcPoints.map((p) => p.y), color: "#38bdf8" }
    case "rejected":
      return { cx: [xOf("aggregate"), xOf("bus"), xOf("app")], cy: [y, y - 26, y], color: "#fb7185" }
  }
}

const FlowStrip = () => {
  const pulses = useSnapshot((s) => s.pulses)
  const commands = useSnapshot((s) => s.commands)
  const settings = useSnapshot((s) => s.settings)
  const active = commands.findLast((c) => c.outcome === undefined && c.stage !== "queued")
  const activeNode = active === undefined ? undefined : stageNode[active.stage]
  const queued = commands.filter((c) => c.stage === "queued").length

  return (
    <div className="relative border-b border-white/[0.06] px-3 pt-1">
      <svg viewBox="0 0 600 124" className="w-full">
        <defs>
          <linearGradient id="wire" x1="0" x2="1">
            <stop offset="0" stopColor="#a78bfa" stopOpacity="0.5" />
            <stop offset="1" stopColor="#38bdf8" stopOpacity="0.5" />
          </linearGradient>
          <filter id="glow">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>
        <line x1={52} x2={548} y1={y} y2={y} stroke="url(#wire)" strokeWidth={1.5} strokeDasharray="3 5" />
        <path
          d={`M548,${y + 22} C470,112 130,112 52,${y + 22}`}
          fill="none"
          stroke="#38bdf8"
          strokeOpacity={0.25}
          strokeWidth={1.5}
          strokeDasharray="3 5"
        />
        <text x={300} y={118} textAnchor="middle" className="fill-sky-300/50 font-mono" fontSize={8.5}>
          query side · eventually consistent
        </text>
        {nodes.map((node) => {
          const on = node.id === activeNode
          return (
            <g key={node.id}>
              <motion.rect
                x={node.x - 50}
                y={y - 21}
                width={100}
                height={42}
                rx={11}
                animate={{
                  fill: on ? "rgba(167,139,250,0.22)" : "rgba(255,255,255,0.035)",
                  stroke: on ? "#a78bfa" : "rgba(255,255,255,0.10)"
                }}
                strokeWidth={1.2}
                filter={on ? "url(#glow)" : undefined}
              />
              <text x={node.x} y={y - 2} textAnchor="middle" fontSize={11.5} fontWeight={600} className="fill-white/90">
                {node.label}
              </text>
              <text x={node.x} y={y + 12} textAnchor="middle" fontSize={8} className="fill-white/40 font-mono">
                {node.sub}
              </text>
            </g>
          )
        })}
        <AnimatePresence>
          {pulses.map((pulse) => {
            const path = pulsePath(pulse.lane)
            return (
              <motion.circle
                key={pulse.id}
                r={4.5}
                fill={path.color}
                filter="url(#glow)"
                initial={{ cx: path.cx[0], cy: path.cy[0], opacity: 0 }}
                animate={{ cx: path.cx, cy: path.cy, opacity: [0, 1, 1, 0] }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.85, ease: "easeInOut" }}
              />
            )
          })}
        </AnimatePresence>
      </svg>
      <div className="absolute top-1.5 right-4 flex gap-2 font-mono text-[9.5px]">
        {settings.paused && (
          <motion.span
            animate={{ opacity: [1, 0.4, 1] }}
            transition={{ duration: 1.2, repeat: Infinity }}
            className="text-amber-300"
          >
            ❚❚ paused
          </motion.span>
        )}
        {queued > 0 && <span className="text-white/45">{queued} queued</span>}
        {active && (
          <span className="text-violet-300">
            {active.type} → {active.stage}
          </span>
        )}
      </div>
    </div>
  )
}
