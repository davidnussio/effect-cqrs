import { AnimatePresence, motion } from "motion/react"
import { Code, Pill } from "../../../bank-ui/src/components/ui.tsx"
import { activity, duration, stateOf, type Step, step, StepRow } from "../../../devtools/SagaSteps.tsx"
import type { OrderRow, SpanEntry } from "../devstore.ts"
import { engineColor, short, statusColor, useSnapshot } from "../hooks.ts"

const stepsOf = (order: OrderRow, spans: ReadonlyArray<SpanEntry>): ReadonlyArray<Step> => {
  const started = spans.find((span) =>
    span.name === "Fulfil.execute" && span.attributes.executionId === order.executionId
  )
  const span = (name: string) => activity(spans, order.executionId, name)
  const reserve = span("reserve")
  const reserveState = stateOf(reserve)
  const head = [
    step("trigger", "OrderPlaced → start", started === undefined ? "pending" : "ok", undefined),
    step("reserve", "stock.reserve", reserveState, reserve?.error ?? duration(reserve))
  ]
  // Out of stock: the saga rejects the order and ends.
  if (reserveState === "failed") {
    const reject = span("reject")
    return [...head, step("reject", "order.reject", stateOf(reject), duration(reject))]
  }

  const request = span("request-payment")
  const confirm = span("confirm")
  const expire = span("expire")
  const release = span("release")
  // The race between the payment and the clock, read from the order's state.
  const payment = order.status === "awaiting-payment"
    ? step("payment", "pay or 15 s", "running", "waiting")
    : order.status === "paid" || order.status === "confirmed"
    ? step("payment", "pay or 15 s", "ok", "paid")
    : order.status === "rejected" && request !== undefined
    ? step("payment", "pay or 15 s", "failed", "timed out")
    : step("payment", "pay or 15 s", "pending", undefined)
  // Then confirm, or expire the order and release its stock.
  const last = confirm !== undefined || payment.state === "ok"
    ? step("confirm", "order.confirm", stateOf(confirm), duration(confirm))
    : expire !== undefined
    ? step(
      "expire",
      "reject + release",
      release === undefined ? "running" : stateOf(release),
      duration(release)
    )
    : step("confirm | expire", "after payment", "pending", undefined)
  return [
    ...head,
    step("request", "order.requestPayment", stateOf(request), duration(request)),
    payment,
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
        Each <Code>OrderPlaced</Code> starts a <Code>Fulfil</Code>{" "}
        workflow. Its steps are activities: the engine keeps each result, so a replay after a crash skips the steps that
        finished. The payment step races a <Code>DurableDeferred</Code>, which <Code>OrderPaid</Code>{" "}
        completes, against a 15-second{" "}
        <Code>DurableClock</Code>. Step states come from the activities' spans, the execution's from{" "}
        <Code>Fulfil.poll</Code>.
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
  { order, status, steps }: {
    order: OrderRow
    steps: ReadonlyArray<Step>
    status: keyof typeof engineColor | undefined
  }
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
        <Pill color={statusColor[order.status]}>{order.status.replace("-", " ")}</Pill>
        <Pill color={status === undefined ? "#64748b" : engineColor[status]} solid={status === "succeeded"}>
          {status ?? "not started"}
        </Pill>
      </div>
    </div>
    <StepRow steps={steps} />
  </motion.div>
)
