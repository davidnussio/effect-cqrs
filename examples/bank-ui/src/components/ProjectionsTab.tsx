import { AnimatePresence, motion } from "motion/react"
import type { ReactNode } from "react"
import { devstore, viewModelNames, type ViewName, type ViewState } from "../devstore.ts"
import { lagFactor } from "../domain/views.ts"
import { engine } from "../engine.ts"
import { eventColor, head, money, signed, useSnapshot } from "../hooks.ts"
import { AnimatedNumber, Button, Code, Pill, Toggle } from "./ui.tsx"

export const ProjectionsTab = () => {
  const views = useSnapshot((s) => s.views)
  const breakTotals = useSnapshot((s) => s.settings.breakTotals)

  return (
    <div className="space-y-4">
      <p className="text-xs leading-relaxed text-white/50">
        Three view models read the same events, each from its own checkpoint, and fold them into the shape a screen
        needs. They are disposable: a replay wipes one and rebuilds it from the log.
      </p>

      <ViewCard name="accounts" view={views.accounts}>
        <div className="space-y-1">
          <AnimatePresence initial={false}>
            {Object.values(views.accounts.data).map((row) => (
              <motion.div
                key={row.id}
                layout
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0 }}
                className="flex items-center gap-2 font-mono text-[11px]"
              >
                <span className="h-2 w-2 rounded-full" style={{ background: row.color }} />
                <span className="w-16 truncate text-white/80">{row.owner}</span>
                <span className="w-20 text-right text-white">
                  <AnimatedNumber value={row.balance} format={money} />
                </span>
                <span className="text-white/35">{row.txCount} tx</span>
                {row.status === "closed" && <Pill color="#94a3b8">closed</Pill>}
                <span className="ml-auto text-white/25">@#{row.lastPosition}</span>
              </motion.div>
            ))}
          </AnimatePresence>
          {Object.keys(views.accounts.data).length === 0 && <Empty />}
        </div>
      </ViewCard>

      <ViewCard
        name="totals"
        view={views.totals}
        extra={
          <Toggle
            on={breakTotals}
            onChange={(on) => devstore.settings({ breakTotals: on })}
            label={<span className="text-rose-300/80">Crash handler</span>}
            color="#fb7185"
          />
        }
      >
        <div className="mb-3 grid grid-cols-3 gap-2">
          {[
            ["in bank", views.totals.data.moneyInBank, "#e9e7ff"],
            ["in flight", views.totals.data.inFlight, "#f472b6"],
            ["refunded", views.totals.data.refunded, "#fb923c"]
          ].map(([label, value, color]) => (
            <div key={label as string} className="rounded-lg bg-white/[0.03] px-2.5 py-1.5">
              <div className="text-[9px] uppercase tracking-wider text-white/35">{label}</div>
              <div className="font-mono text-sm font-semibold" style={{ color: color as string }}>
                <AnimatedNumber value={value as number} format={money} />
              </div>
            </div>
          ))}
        </div>
        <TypeBars byType={views.totals.data.byType} />
      </ViewCard>

      <ViewCard name="feed" view={views.feed}>
        <div className="space-y-0.5">
          <AnimatePresence initial={false}>
            {views.feed.data.slice(0, 6).map((item) => (
              <motion.div
                key={item.position}
                layout
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 1, height: "auto" }}
                exit={{ opacity: 0, height: 0 }}
                className="flex gap-2 font-mono text-[10.5px]"
              >
                <span className="text-white/30">#{item.position}</span>
                <span className="text-white/75">{item.aggregateId}</span>
                <span className="truncate text-white/45">{item.text}</span>
                {item.delta !== 0 && <span className="ml-auto text-white/60">{signed(item.delta)}</span>}
              </motion.div>
            ))}
          </AnimatePresence>
          {views.feed.data.length === 0 && <Empty />}
        </div>
      </ViewCard>
    </div>
  )
}

const Empty = () => <div className="py-2 text-center text-[11px] text-white/25">disposed · empty</div>

export const ViewCard = (
  { name, view, children, extra }: { name: ViewName; view: ViewState<unknown>; children: ReactNode; extra?: ReactNode }
) => {
  const storeHead = useSnapshot((s) => head(s.events))
  const behind = Math.max(0, storeHead - view.position)
  const status = view.error !== undefined
    ? { label: "stalled", color: "#fb7185" }
    : view.replaying
    ? { label: "replaying", color: "#38bdf8" }
    : behind > 0
    ? { label: `${behind} behind`, color: "#fbbf24" }
    : { label: "caught up", color: "#34d399" }

  return (
    <motion.div
      layout
      className="glass rounded-2xl p-4"
      animate={{ boxShadow: view.error ? "0 0 0 1px rgba(251,113,133,0.5)" : "0 0 0 0px rgba(0,0,0,0)" }}
    >
      <div className="flex items-center gap-2">
        <Code>{viewModelNames[name]}</Code>
        <Pill color={status.color}>{status.label}</Pill>
        <span className="font-mono text-[9.5px] text-white/30">latency ×{lagFactor[name]}</span>
        <div className="ml-auto flex items-center gap-2">
          {extra}
          <Button onClick={() => void engine.replay([name])} disabled={view.replaying}>↻ Rebuild</Button>
        </div>
      </div>
      <div className="mt-3 flex items-center gap-2 font-mono text-[10px] text-white/40">
        <span className="w-24 whitespace-nowrap">checkpoint #{view.position}</span>
        <div className="relative h-1.5 flex-1 overflow-hidden rounded-full bg-white/[0.06]">
          <motion.div
            className="absolute inset-y-0 left-0 rounded-full"
            animate={{ width: `${storeHead === 0 ? 0 : (view.position / storeHead) * 100}%`, background: status.color }}
            transition={{ type: "spring", stiffness: 120, damping: 20 }}
          />
        </div>
        <span className="w-16 whitespace-nowrap text-right">head #{storeHead}</span>
      </div>
      <AnimatePresence>
        {view.error && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="mt-2 overflow-hidden rounded-lg bg-rose-500/10 px-2.5 py-1.5 font-mono text-[10px] text-rose-200"
          >
            {view.error}
            <div className="text-rose-200/50">
              The view model stops on this event and retries with backoff. Nothing is skipped or reordered.
            </div>
          </motion.div>
        )}
      </AnimatePresence>
      {view.lastReplay && (
        <div className="mt-2 font-mono text-[10px] text-sky-300/70">
          last replay: {view.lastReplay.eventCount} events
          {view.lastReplay.upTo !== undefined ? ` up to #${view.lastReplay.upTo}, later events left out` : ", full log"}
        </div>
      )}
      <div className="mt-3 border-t border-white/[0.05] pt-3">{children}</div>
    </motion.div>
  )
}

const TypeBars = ({ byType }: { byType: Readonly<Record<string, number>> }) => {
  const max = Math.max(1, ...Object.values(byType))
  return (
    <div className="space-y-1">
      {Object.keys(eventColor).map((type) => (
        <div key={type} className="flex items-center gap-2 font-mono text-[9.5px]">
          <span className="w-28 truncate text-white/45">{type}</span>
          <div className="h-2 flex-1 overflow-hidden rounded-full bg-white/[0.04]">
            <motion.div
              className="h-full rounded-full"
              style={{ background: eventColor[type] }}
              animate={{ width: `${((byType[type] ?? 0) / max) * 100}%` }}
              transition={{ type: "spring", stiffness: 140, damping: 20 }}
            />
          </div>
          <span className="w-6 text-right text-white/60">{byType[type] ?? 0}</span>
        </div>
      ))}
    </div>
  )
}
