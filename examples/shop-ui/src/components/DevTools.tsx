import { AnimatePresence, motion } from "motion/react"
import { useState } from "react"
import { Slider } from "../../../bank-ui/src/components/ui.tsx"
import { devstore } from "../devstore.ts"
import { useSnapshot } from "../hooks.ts"
import { DatabaseTab } from "./DatabaseTab.tsx"
import { MetricsTab } from "./MetricsTab.tsx"
import { SagaTab } from "./SagaTab.tsx"
import { TraceTab } from "./TraceTab.tsx"

const tabs = ["Trace", "Saga", "Database", "Metrics"] as const
type Tab = (typeof tabs)[number]

export const DevTools = () => {
  const [tab, setTab] = useState<Tab>("Trace")
  const traces = useSnapshot((s) => new Set(s.spans.map((span) => span.traceId)).size)
  const running = useSnapshot((s) => Object.values(s.engine).filter((status) => status === "running").length)
  const rows = useSnapshot((s) => s.events.length)
  const latency = useSnapshot((s) => s.settings.latency)

  return (
    <aside className="flex min-h-0 flex-col border-white/[0.07] bg-[#0a0a12]/80 backdrop-blur-xl lg:border-l">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-white/[0.06] px-4 py-3">
        <span className="font-mono text-[10px] font-semibold uppercase tracking-[0.22em] text-sky-300/80">
          ◆ Effect devtools
        </span>
        <div className="ml-auto w-full sm:w-56">
          <Slider
            label="Latency before each command"
            value={latency}
            min={0}
            max={1500}
            step={50}
            color="#38bdf8"
            onChange={(value) => devstore.settings({ latency: value })}
          />
        </div>
      </div>
      <nav className="scroll-thin flex gap-1 overflow-x-auto border-b border-white/[0.06] px-3">
        {tabs.map((name) => (
          <button
            key={name}
            onClick={() => setTab(name)}
            className={`relative shrink-0 whitespace-nowrap px-3 py-2.5 text-xs font-medium transition-colors ${
              tab === name ? "text-white" : "text-white/45 hover:text-white/80"
            }`}
          >
            {name}
            {name === "Trace" && <Badge color="#a78bfa">{traces}</Badge>}
            {name === "Saga" && <Badge color="#38bdf8" hidden={running === 0}>{running}</Badge>}
            {name === "Database" && <Badge color="#34d399">{rows}</Badge>}
            {tab === name && (
              <motion.span
                layoutId="tab-underline"
                className="absolute inset-x-2 -bottom-px h-[2px] rounded-full bg-gradient-to-r from-sky-400 to-violet-400"
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
            {tab === "Trace" && <TraceTab />}
            {tab === "Saga" && <SagaTab />}
            {tab === "Database" && <DatabaseTab />}
            {tab === "Metrics" && <MetricsTab />}
          </motion.div>
        </AnimatePresence>
      </div>
    </aside>
  )
}

// A hidden badge still takes its space, so the tabs don't move when it comes and goes.
const Badge = ({ color, hidden = false, children }: { color: string; hidden?: boolean; children: React.ReactNode }) => (
  <motion.span
    key={hidden ? "hidden" : String(children)}
    initial={{ scale: hidden ? 1 : 1.5 }}
    animate={{ scale: 1 }}
    aria-hidden={hidden}
    className={`ml-1.5 rounded-full px-1.5 py-px font-mono text-[9px] font-semibold ${hidden ? "invisible" : ""}`}
    style={{ background: `${color}26`, color }}
  >
    {children}
  </motion.span>
)
