import { motion } from "motion/react"
import { AnimatedNumber, Button } from "../../../bank-ui/src/components/ui.tsx"
import { engine } from "../engine.ts"
import { useSnapshot } from "../hooks.ts"
import { DevTools } from "./DevTools.tsx"
import { ShopApp } from "./ShopApp.tsx"

// The documentation site serves the demo under its own path.
const docsUrl = import.meta.env.BASE_URL === "/" ? undefined : `${import.meta.env.BASE_URL}../`

export const App = () => {
  const events = useSnapshot((s) => s.events.length)
  const orders = useSnapshot((s) => Object.keys(s.orders).length)
  const running = useSnapshot((s) => Object.values(s.engine).filter((status) => status === "running").length)
  const epoch = useSnapshot((s) => s.epoch)

  return (
    <div className="bg-aurora relative flex h-full flex-col">
      <div className="bg-grid pointer-events-none absolute inset-0" />
      <header className="relative z-10 flex items-center gap-4 border-b border-white/[0.06] px-5 py-3">
        <Logo />
        <div>
          <div className="text-[15px] font-semibold tracking-tight">
            Shop <span className="text-white/40">Lab</span>
          </div>
          <div className="font-mono text-[10px] text-white/40">
            the tutorial's shop · traced, measured and stored in your browser
          </div>
        </div>
        <div className="ml-auto flex items-center gap-5 font-mono text-[11px] text-white/50">
          {docsUrl !== undefined && (
            <a
              href={`${docsUrl}tutorial/shop/`}
              className="rounded-lg px-2.5 py-1.5 text-xs font-medium text-white/60 transition-colors hover:bg-white/[0.06] hover:text-white"
            >
              Tutorial ↗
            </a>
          )}
          <Stat label="events" value={events} />
          <Stat label="orders" value={orders} />
          <Stat label="sagas running" value={running} />
          <Button tone="ghost" onClick={() => void engine.reset()}>↺ Reset</Button>
        </div>
      </header>
      <main key={epoch} className="relative z-10 grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_minmax(500px,48%)]">
        <ShopApp />
        <DevTools />
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
    className="relative grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-sky-400 to-violet-500 shadow-[0_0_30px_-6px_rgba(56,189,248,0.8)]"
    animate={{ rotate: [0, 6, -6, 0] }}
    transition={{ duration: 6, repeat: Infinity, ease: "easeInOut" }}
  >
    <svg viewBox="0 0 24 24" className="h-5 w-5 text-white" fill="none" stroke="currentColor" strokeWidth={2.2}>
      <path d="M5 8h14l-1.5 10h-11z" strokeLinejoin="round" />
      <path d="M9 8V6a3 3 0 0 1 6 0v2" strokeLinecap="round" />
    </svg>
  </motion.div>
)
