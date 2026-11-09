import { AnimatePresence, motion } from "motion/react"
import { Code, Pill } from "../../../bank-ui/src/components/ui.tsx"
import type { OrderRow, SpanEntry } from "../devstore.ts"
import { engineColor, ms, short, statusColor, useSnapshot } from "../hooks.ts"

type StepState = "pending" | "running" | "ok" | "failed" | "skipped"

interface Step {
  readonly label: string
  readonly sub: string
  readonly state: StepState
  readonly detail?: string
}

const stepColor: Record<StepState, string> = {
  pending: "rgba(255,255,255,0.18)",
  running: "#38bdf8",
  ok: "#34d399",
  failed: "#fb7185",
  skipped: "rgba(255,255,255,0.08)"
}

/** The last span of an activity in this execution: a replay would open another. */
const activity = (spans: ReadonlyArray<SpanEntry>, executionId: string, name: string) =>
  spans.findLast((span) => span.name === name && span.attributes.executionId === executionId)

const stateOf = (span: SpanEntry | undefined): StepState =>
  span === undefined ? "pending" : span.status === "running" ? "running" : span.status === "ok" ? "ok" : "failed"

const duration = (span: SpanEntry | undefined) =>
  span?.end === undefined ? undefined : ms(span.end - span.start)

const stepsOf = (order: OrderRow, spans: ReadonlyArray<SpanEntry>): ReadonlyArray<Step> => {
  const started = spans.find((span) => span.name === "Fulfil.execute" && span.attributes.executionId === order.executionId)
  const reserve = activity(spans, order.executionId, "reserve")
  const confirm = activity(spans, order.executionId, "confirm")
  const reject = activity(spans, order.executionId, "reject")
  const reserveState = stateOf(reserve)
  const step = (label: string, sub: string, state: StepState, detail: string | undefined): Step => ({
    label,
    sub,
    state,
    ...(detail === undefined ? {} : { detail })
  })
  // The last step depends on how the reservation went: confirm, or reject on `OutOfStock`.
  const last = reserveState === "ok"
    ? step("confirm", "order.confirm", stateOf(confirm), duration(confirm))
    : reserveState === "failed"
    ? step("reject", "order.reject", stateOf(reject), duration(reject))
    : step("confirm | reject", "after reserve", "pending", undefined)
  return [
    step("trigger", "OrderPlaced → start", started === undefined ? "pending" : "ok", undefined),
    step("reserve", "stock.reserve", reserveState, reserve?.error ?? duration(reserve)),
    last
  ]
}

export const SagaTab = () => {
  const orders = useSnapshot((s) => s.orders)
  const spans = useSnapshot((s) => s.spans)
  const engine = useSnapshot((s) => s.engine)
  const list = Object.values(orders).sort((a, b) => b.position - a.position)

  return (
    <div className="space-y-3">
      <p className="text-xs leading-relaxed text-white/50">
        Each <Code>OrderPlaced</Code> starts a <Code>Fulfil</Code> workflow. Its steps are activities: the engine keeps
        each result, so a replay after a crash skips the steps that finished. Step states come from the activities'
        spans, the execution's from <Code>Fulfil.poll</Code>.
      </p>
      {list.length === 0 && <div className="py-6 text-center text-xs text-white/25">no sagas yet</div>}
      <AnimatePresence initial={false}>
        {list.map((order) => (
          <SagaCard key={order.id} order={order} steps={stepsOf(order, spans)} status={engine[order.executionId]} />
        ))}
      </AnimatePresence>
    </div>
  )
}

const SagaCard = (
  { order, status, steps }: { order: OrderRow; steps: ReadonlyArray<Step>; status: keyof typeof engineColor | undefined }
) => (
  <motion.div
    layout
    initial={{ opacity: 0, y: -10 }}
    animate={{ opacity: 1, y: 0 }}
    exit={{ opacity: 0 }}
    className="glass rounded-xl px-3.5 py-3"
  >
    <div className="flex items-center gap-2">
      <span className="font-mono text-[11.5px] font-semibold">Fulfil</span>
      <span className="font-mono text-[10px] text-white/40">
        {order.id} · {order.quantity}× {order.sku}
      </span>
      <span className="font-mono text-[10px] text-white/25" title={order.executionId}>
        exec {short(order.executionId, 10)}
      </span>
      <div className="ml-auto flex items-center gap-1.5">
        <Pill color={statusColor[order.status]}>{order.status}</Pill>
        <Pill color={status === undefined ? "#64748b" : engineColor[status]} solid={status === "succeeded"}>
          {status ?? "not started"}
        </Pill>
      </div>
    </div>
    <div className="mt-3 flex items-center">
      {steps.map((step, index) => (
        <div key={index} className="flex flex-1 items-center last:flex-none">
          <StepNode step={step} />
          {index < steps.length - 1 && <Connector from={step} to={steps[index + 1]!} />}
        </div>
      ))}
    </div>
  </motion.div>
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
