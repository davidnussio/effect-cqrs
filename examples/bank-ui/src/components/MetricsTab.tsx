import { motion } from "motion/react"
import type { ReactNode } from "react"
import type { CommandTiming } from "../devstore.ts"
import { useSnapshot } from "../hooks.ts"
import { Code, Pill, SectionTitle } from "./ui.tsx"

const sources = [
  { key: "memory", label: "memory", color: "#34d399", hint: "remembered from the last command" },
  { key: "snapshot", label: "snapshot", color: "#38bdf8", hint: "saved every 5 events, then the rest" },
  { key: "replay", label: "replay", color: "#fbbf24", hint: "every event from the first" }
] as const

const cacheCapacity = 4

const ms = (value: number) => (value < 1 ? `${Math.round(value * 1000)}µs` : `${value.toFixed(1)}ms`)

export const MetricsTab = () => {
  const metrics = useSnapshot((s) => s.metrics)
  const loads = metrics.loads.memory + metrics.loads.snapshot + metrics.loads.replay

  return (
    <div className="space-y-4">
      <p className="text-xs leading-relaxed text-white/50">
        <Code>AggregateService</Code> records these through Effect's <Code>Metric</Code>, and the devtools read them
        with <Code>Metric.snapshot</Code>. In production an exporter sends the same series on. Only {cacheCapacity}{" "}
        accounts stay in memory here, so with more of them open a command has to start from a snapshot or a replay.
      </p>

      <div className="glass rounded-2xl p-4">
        <SectionTitle right={<span className="font-mono text-[10px] text-white/35">{loads} loads</span>}>
          Where the state came from
        </SectionTitle>
        <div className="flex h-3 overflow-hidden rounded-full bg-white/[0.05]">
          {sources.map(({ color, key }) => (
            <motion.div
              key={key}
              className="h-full"
              style={{ background: color }}
              animate={{ width: `${loads === 0 ? 0 : (metrics.loads[key] / loads) * 100}%` }}
              transition={{ type: "spring", stiffness: 140, damping: 20 }}
            />
          ))}
        </div>
        <div className="mt-3 space-y-1">
          {sources.map(({ color, hint, key, label }) => (
            <div key={key} className="flex items-center gap-2 font-mono text-[10.5px]">
              <span className="h-2 w-2 rounded-full" style={{ background: color }} />
              <span className="w-16 text-white/80">{label}</span>
              <span className="w-10 text-right text-white">{metrics.loads[key]}</span>
              <span className="w-10 text-right text-white/35">
                {loads === 0 ? "" : `${Math.round((metrics.loads[key] / loads) * 100)}%`}
              </span>
              <span className="truncate text-white/35">{hint}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <Tile
          label="events read per load"
          value={loads === 0 ? "0" : (metrics.eventsRead / loads).toFixed(1)}
          detail={`max ${metrics.maxEventsRead}, ${metrics.eventsRead} in all`}
          color="#a78bfa"
        />
        <Tile
          label="states in memory"
          value={`${metrics.cacheSize} / ${cacheCapacity}`}
          detail={`${metrics.evictions} dropped to make room`}
          color="#34d399"
        />
        <Tile
          label="snapshots saved"
          value={String(metrics.snapshotsSaved)}
          detail={metrics.snapshotsFailed + metrics.snapshotReadFailures === 0
            ? "none failed"
            : `${metrics.snapshotsFailed} saves and ${metrics.snapshotReadFailures} reads failed`}
          color="#38bdf8"
        />
        <Tile
          label="conflicts"
          value={String(metrics.conflicts)}
          detail="appends that lost a race, then retried"
          color={metrics.conflicts > 0 ? "#fb7185" : "#94a3b8"}
        />
      </div>

      <div className="glass rounded-2xl p-4">
        <SectionTitle>Commands</SectionTitle>
        {metrics.commands.length === 0
          ? <div className="py-2 text-center text-[11px] text-white/25">no commands yet</div>
          : (
            <table className="w-full font-mono text-[10.5px]">
              <thead>
                <tr className="text-left text-[9px] uppercase tracking-wider text-white/35">
                  <th className="pb-1.5 font-medium">command</th>
                  <th className="pb-1.5 font-medium">outcome</th>
                  <th className="pb-1.5 text-right font-medium">count</th>
                  <th className="pb-1.5 text-right font-medium">mean</th>
                  <th className="pb-1.5 text-right font-medium">max</th>
                </tr>
              </thead>
              <tbody>
                {metrics.commands.map((timing) => <CommandRow key={`${timing.command}/${timing.outcome}`} timing={timing} />)}
              </tbody>
            </table>
          )}
        <div className="mt-2 text-[10px] text-white/30">
          Timed from when a command asks for its turn on the stream, so the bus's own delays aren't included.
        </div>
      </div>

      <div className="text-[10px] leading-relaxed text-white/30">
        Series: <Code>cqrs_state_loads_total</Code>, <Code>cqrs_events_replayed</Code>,{" "}
        <Code>cqrs_state_cache_size</Code>, <Code>cqrs_state_cache_evictions_total</Code>,{" "}
        <Code>cqrs_snapshot_saves_total</Code>, <Code>cqrs_snapshot_read_failures_total</Code>,{" "}
        <Code>cqrs_concurrency_conflicts_total</Code> and <Code>cqrs_command_duration</Code>, each with an{" "}
        <Code>aggregate</Code> attribute.
      </div>
    </div>
  )
}

const Tile = ({ label, value, detail, color }: { label: string; value: ReactNode; detail: string; color: string }) => (
  <div className="rounded-lg bg-white/[0.03] px-3 py-2">
    <div className="text-[9px] uppercase tracking-wider text-white/35">{label}</div>
    <div className="font-mono text-lg font-semibold tabular-nums" style={{ color }}>{value}</div>
    <div className="truncate text-[10px] text-white/35">{detail}</div>
  </div>
)

const CommandRow = ({ timing }: { timing: CommandTiming }) => (
  <tr className="border-t border-white/[0.04]">
    <td className="py-1 text-white/80">{timing.command}</td>
    <td className="py-1">
      <Pill color={timing.outcome === "success" ? "#34d399" : "#fb7185"}>{timing.outcome}</Pill>
    </td>
    <td className="py-1 text-right text-white">{timing.count}</td>
    <td className="py-1 text-right text-white/60">{ms(timing.totalMs / timing.count)}</td>
    <td className="py-1 text-right text-white/60">{ms(timing.maxMs)}</td>
  </tr>
)
