import { motion } from "motion/react"
import { useState } from "react"
import { engine } from "../engine.ts"
import { head, useSnapshot } from "../hooks.ts"
import { BankApp } from "./BankApp.tsx"
import { DevTools } from "./DevTools.tsx"
import { AnimatedNumber, Button } from "./ui.tsx"

// The documentation site serves the demo under its own /demo/.
const docsUrl = import.meta.env.BASE_URL.endsWith("/demo/") ? `${import.meta.env.BASE_URL}../` : undefined

export const App = () => {
  // A position while time travelling: the app then shows a fold of the events up
  // to there instead of the read model.
  const [travel, setTravel] = useState<number | undefined>(undefined)
  const events = useSnapshot((s) => s.events)
  const epoch = useSnapshot((s) => s.epoch)

  return (
    <div className="bg-aurora relative flex h-full flex-col">
      <div className="bg-grid pointer-events-none absolute inset-0" />
      <header className="relative z-10 flex items-center gap-4 border-b border-white/[0.06] px-5 py-3">
        <Logo />
        <div>
          <div className="text-[15px] font-semibold tracking-tight">
            Event Bank <span className="text-white/40">Lab</span>
          </div>
          <div className="font-mono text-[10px] text-white/40">
            effect-cqrs running in your browser · in-memory event store
          </div>
        </div>
        <div className="ml-auto flex items-center gap-5 font-mono text-[11px] text-white/50">
          {docsUrl !== undefined && (
            <a
              href={docsUrl}
              className="rounded-lg px-2.5 py-1.5 text-xs font-medium text-white/60 transition-colors hover:bg-white/[0.06] hover:text-white"
            >
              Docs ↗
            </a>
          )}
          <Stat label="events" value={head(events)} />
          <Stat label="streams" value={new Set(events.map((e) => e.aggregateId)).size} />
          <Button
            tone="ghost"
            onClick={() => {
              setTravel(undefined)
              void engine.reset()
            }}
          >
            ↺ Reset store
          </Button>
        </div>
      </header>
      <main key={epoch} className="relative z-10 grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_minmax(460px,44%)]">
        <BankApp travel={travel} onExitTravel={() => setTravel(undefined)} />
        <DevTools travel={travel} onTravel={setTravel} />
      </main>
    </div>
  )
}

const Stat = ({ label, value }: { label: string; value: number }) => (
  <div className="flex items-baseline gap-1.5">
    <span className="text-[15px] font-semibold text-white">
      <AnimatedNumber value={value} format={(n) => Math.round(n).toString()} />
    </span>
    {label}
  </div>
)

const Logo = () => (
  <motion.div
    className="relative grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-violet-500 to-fuchsia-500 shadow-[0_0_30px_-6px_rgba(167,139,250,0.9)]"
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
