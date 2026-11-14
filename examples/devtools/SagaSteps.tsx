/**
 * The steps of one saga execution as a row of nodes joined by wires, shared by
 * the demos' saga tabs. Each demo works out its steps from its own spans and events.
 */
import { motion } from "motion/react"
import { ms, type SpanEntry } from "./tracing.ts"

export type StepState = "pending" | "running" | "ok" | "failed" | "skipped"

export interface Step {
  readonly label: string
  readonly sub: string
  readonly state: StepState
  readonly detail?: string
}

/** What `WorkflowEngine.poll` says about a saga's execution. */
export type EngineStatus = "running" | "suspended" | "succeeded" | "failed"

export const engineColor: Record<EngineStatus, string> = {
  running: "#38bdf8",
  suspended: "#fbbf24",
  succeeded: "#34d399",
  failed: "#fb7185"
}

const stepColor: Record<StepState, string> = {
  pending: "rgba(255,255,255,0.18)",
  running: "#38bdf8",
  ok: "#34d399",
  failed: "#fb7185",
  skipped: "rgba(255,255,255,0.08)"
}

export const step = (label: string, sub: string, state: StepState, detail?: string | undefined): Step => ({
  label,
  sub,
  state,
  ...(detail === undefined ? {} : { detail })
})

/** The last span of an activity in this execution: a replay would open another. */
export const activity = (spans: ReadonlyArray<SpanEntry>, executionId: string, name: string) =>
  spans.findLast((span) => span.name === name && span.attributes.executionId === executionId)

export const stateOf = (span: SpanEntry | undefined): StepState =>
  span === undefined ? "pending" : span.status === "running" ? "running" : span.status === "ok" ? "ok" : "failed"

export const duration = (span: SpanEntry | undefined) => span?.end === undefined ? undefined : ms(span.end - span.start)

export const StepRow = ({ steps }: { steps: ReadonlyArray<Step> }) => (
  <div className="mt-3 flex items-center">
    {steps.map((step, index) => (
      <div key={index} className="flex flex-1 items-center last:flex-none">
        <StepNode step={step} />
        {index < steps.length - 1 && <Connector from={step} to={steps[index + 1]!} />}
      </div>
    ))}
  </div>
)

const StepNode = ({ step }: { step: Step }) => {
  const color = stepColor[step.state]
  return (
    <div className="flex w-[86px] shrink-0 flex-col items-center text-center">
      <motion.div
        className="grid h-8 w-8 place-items-center rounded-full font-mono text-[11px] font-bold"
        animate={{
          background: step.state === "skipped" ? "rgba(255,255,255,0.02)" : `${color}26`,
          boxShadow: step.state === "running"
            ? [`0 0 0 0 ${color}80`, `0 0 0 8px ${color}00`]
            : `inset 0 0 0 1.5px ${color}`
        }}
        transition={step.state === "running" ? { duration: 1, repeat: Infinity } : { duration: 0.3 }}
        style={{ color }}
      >
        {step.state === "ok" ? "✓" : step.state === "failed" ? "✕" : step.state === "running" ? "…" : "·"}
      </motion.div>
      <div className={`mt-1 text-[11px] font-medium ${step.state === "skipped" ? "text-white/20" : "text-white/85"}`}>
        {step.label}
      </div>
      <div className="font-mono text-[9px] text-white/30">{step.sub}</div>
      <div className="h-3 font-mono text-[9px]" style={{ color: step.state === "failed" ? "#fb7185" : "#ffffff59" }}>
        {step.detail ?? ""}
      </div>
    </div>
  )
}

const Connector = ({ from, to }: { from: Step; to: Step }) => {
  const done = from.state === "ok" || from.state === "failed"
  const live = done && to.state !== "skipped"
  return (
    <div className="relative mx-1 -mt-9 h-[2px] flex-1 overflow-hidden rounded-full bg-white/[0.06]">
      <motion.div
        className="absolute inset-y-0 left-0 rounded-full"
        style={{ background: live ? stepColor[to.state === "pending" ? "running" : to.state] : "transparent" }}
        initial={false}
        animate={{ width: live ? "100%" : "0%" }}
        transition={{ duration: 0.4 }}
      />
    </div>
  )
}
