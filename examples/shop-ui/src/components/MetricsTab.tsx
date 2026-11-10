import { motion } from "motion/react"
import { Code, Pill, SectionTitle } from "../../../bank-ui/src/components/ui.tsx"
import type { CommandTiming } from "../devstore.ts"
import { ms, useSnapshot } from "../hooks.ts"

const sources = [
  { key: "memory", label: "memory", color: "#34d399" },
  { key: "snapshot", label: "snapshot", color: "#38bdf8" },
  { key: "replay", label: "replay", color: "#fbbf24" }
] as const

export const MetricsTab = () => {
  const metrics = useSnapshot((s) => s.metrics)
  const loads = metrics.loads.memory + metrics.loads.snapshot + metrics.loads.replay
  const commands = metrics.commands.reduce((sum, timing) => sum + timing.count, 0)
  const maxMean = Math.max(...metrics.commands.map((timing) => timing.totalMs / timing.count), 0.001)

  return (
    <div className="space-y-4">
      <p className="text-xs leading-relaxed text-white/50">
        Both aggregate services record these with Effect's <Code>Metric</Code>, read here with{" "}
        <Code>Metric.snapshot</Code>. The demo's latency comes before a command asks for its turn, so it isn't timed.
      </p>

      <div className="grid grid-cols-3 gap-2">
        <Tile label="commands" value={commands} color="#a78bfa" />
        <Tile label="states in memory" value={metrics.cacheSize} color="#34d399" />
        <Tile label="conflicts" value={metrics.conflicts} color={metrics.conflicts > 0 ? "#fb7185" : "#94a3b8"} />
      </div>

      <div className="glass rounded-2xl p-4">
        <SectionTitle right={<span className="font-mono text-[10px] text-white/35">{loads} loads</span>}>
          Where each command found its state
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
        <div className="mt-2 flex gap-4 font-mono text-[10.5px]">
          {sources.map(({ color, key, label }) => (
            <span key={key} className="flex items-center gap-1.5 text-white/60">
              <span className="h-2 w-2 rounded-full" style={{ background: color }} />
              {label} <span className="text-white">{metrics.loads[key]}</span>
            </span>
          ))}
        </div>
      </div>

      <div className="glass rounded-2xl p-4">
        <SectionTitle>cqrs_command_duration</SectionTitle>
        {metrics.commands.length === 0
          ? <div className="py-2 text-center text-[11px] text-white/25">no commands yet</div>
          : (
            <table className="w-full font-mono text-[10.5px]">
              <thead>
                <tr className="text-left text-[9px] uppercase tracking-wider text-white/35">
                  <th className="pb-1.5 font-medium">command</th>
                  <th className="pb-1.5 font-medium">outcome</th>
                  <th className="pb-1.5 text-right font-medium">count</th>
                  <th className="pb-1.5 pl-3 font-medium">mean</th>
                  <th className="pb-1.5 text-right font-medium">max</th>
                </tr>
              </thead>
              <tbody>
                {metrics.commands.map((timing) => (
                  <CommandRow
                    key={`${timing.aggregate}.${timing.command}/${timing.outcome}`}
                    timing={timing}
                    max={maxMean}
                  />
                ))}
              </tbody>
            </table>
          )}
      </div>
    </div>
  )
}

const Tile = ({ label, value, color }: { label: string; value: number; color: string }) => (
  <div className="rounded-lg bg-white/[0.03] px-3 py-2">
    <div className="text-[9px] uppercase tracking-wider text-white/35">{label}</div>
    <motion.div
      key={value}
      initial={{ scale: 1.15 }}
      animate={{ scale: 1 }}
      className="origin-left font-mono text-lg font-semibold tabular-nums"
      style={{ color }}
    >
      {value}
    </motion.div>
  </div>
)

const CommandRow = ({ max, timing }: { timing: CommandTiming; max: number }) => {
  const mean = timing.totalMs / timing.count
  return (
    <tr className="border-t border-white/[0.04]">
      <td className="py-1 text-white/80">
        <span className="text-white/40">{timing.aggregate}.</span>
        {timing.command}
      </td>
      <td className="py-1">
        <Pill color={timing.outcome === "success" ? "#34d399" : "#fb7185"}>{timing.outcome}</Pill>
      </td>
      <td className="py-1 text-right text-white">{timing.count}</td>
      <td className="py-1 pl-3">
        <div className="flex items-center gap-2">
          <div className="h-1.5 w-16 overflow-hidden rounded-full bg-white/[0.05]">
            <motion.div
              className="h-full rounded-full bg-violet-400"
              animate={{ width: `${(mean / max) * 100}%` }}
              transition={{ type: "spring", stiffness: 140, damping: 20 }}
            />
          </div>
          <span className="text-white/60">{ms(mean)}</span>
        </div>
      </td>
      <td className="py-1 text-right text-white/60">{ms(timing.maxMs)}</td>
    </tr>
  )
}
