import { AnimatePresence, motion } from "motion/react"
import {
  activity,
  duration,
  engineColor,
  type EngineStatus,
  stateOf,
  type Step,
  step,
  StepRow
} from "../../../devtools/SagaSteps.tsx"
import type { SpanEntry } from "../../../devtools/tracing.ts"
import type { EventEntry, TransferRun } from "../devstore.ts"
import { eventColor, head, money, useSnapshot } from "../hooks.ts"
import { Code, Pill } from "./ui.tsx"

/** Transfers shown, newest first: the autopilot sends plenty. */
const shown = 30

interface Transfer {
  readonly transferId: string
  readonly from: string
  readonly to: string
  readonly amount: number
  readonly position: number
  readonly received?: EventEntry
  readonly refunded?: EventEntry
}

/** Each `TransferSent`, with the event that settled it, if any. */
const transfersOf = (events: ReadonlyArray<EventEntry>): ReadonlyArray<Transfer> => {
  const settled = new Map<string, Pick<Transfer, "received" | "refunded">>()
  const sent: Array<Transfer> = []
  for (const entry of events) {
    const event = entry.event
    if (event._tag === "TransferSent") {
      sent.push({
        transferId: event.transferId,
        from: entry.aggregateId,
        to: event.to,
        amount: event.amount,
        position: entry.position
      })
    } else if (event._tag === "TransferReceived") {
      settled.set(event.transferId, { ...settled.get(event.transferId), received: entry })
    } else if (event._tag === "TransferRefunded") {
      settled.set(event.transferId, { ...settled.get(event.transferId), refunded: entry })
    }
  }
  return sent.slice(-shown).reverse().map((transfer) => ({ ...transfer, ...settled.get(transfer.transferId) }))
}

/**
 * The steps, from the activities' spans while they run and from the events once
 * they are stored: the trace tab keeps only the latest spans, the log keeps everything.
 */
const stepsOf = (
  transfer: Transfer,
  run: TransferRun | undefined,
  spans: ReadonlyArray<SpanEntry>,
  triggerAt: number
): ReadonlyArray<Step> => {
  const span = (name: string) => run === undefined ? undefined : activity(spans, run.executionId, name)
  const credit = span("credit")
  const refund = span("refund")
  const started = run?.status !== undefined || transfer.position <= triggerAt
  const trigger = step("trigger", "TransferSent → start", started ? "ok" : "pending", started ? undefined : "waiting")

  const creditStep = transfer.received !== undefined
    ? step("credit", `${transfer.to}.receiveTransfer`, "ok", duration(credit) ?? `#${transfer.received.position}`)
    : transfer.refunded !== undefined
    // The refund records why the credit was refused.
    ? step("credit", `${transfer.to}.receiveTransfer`, "failed", reasonOf(transfer.refunded))
    : step("credit", `${transfer.to}.receiveTransfer`, stateOf(credit), credit?.error ?? duration(credit))

  const refundStep = transfer.refunded !== undefined
    ? step("refund", `${transfer.from}.refundTransfer`, "ok", duration(refund) ?? `#${transfer.refunded.position}`)
    : transfer.received !== undefined
    ? step("refund", "not needed", "skipped")
    : creditStep.state === "failed"
    ? step("refund", `${transfer.from}.refundTransfer`, refund === undefined ? "running" : stateOf(refund))
    : step("refund", "if refused", "pending")

  return [trigger, creditStep, refundStep]
}

const reasonOf = (entry: EventEntry) => entry.event._tag === "TransferRefunded" ? entry.event.reason : undefined

export const SagaTab = () => {
  const events = useSnapshot((s) => s.events)
  const spans = useSnapshot((s) => s.spans)
  const runs = useSnapshot((s) => s.transfers)
  const trigger = useSnapshot((s) => s.trigger)
  const transfers = transfersOf(events)
  const behind = Math.max(0, head(events) - trigger.position)

  return (
    <div className="space-y-3">
      <p className="text-xs leading-relaxed text-white/50">
        Each <Code>TransferSent</Code> starts a <Code>Transfer</Code>{" "}
        workflow, named after the transfer id, so a second start joins the first. It credits the other account with{" "}
        <Code>receiveTransfer</Code>, and when that account isn't open it refunds the sender with{" "}
        <Code>refundTransfer</Code>. Each command is an activity: the engine keeps its outcome, so a replay skips it.
        Step states come from the activities' spans and the events, the execution's from <Code>Transfer.poll</Code>.
      </p>
      <div className="flex flex-wrap items-center gap-2 rounded-lg bg-white/[0.03] px-3 py-2 font-mono text-[10px] ring-1 ring-white/[0.06]">
        <span className="text-white/45">trigger</span>
        <span className="text-white/85">transfers</span>
        <span className={behind === 0 ? "text-emerald-300" : "text-amber-300"}>
          {behind === 0 ? `in sync #${trigger.position}` : `${behind} behind · #${trigger.position}`}
        </span>
        {trigger.error !== undefined && <span className="text-rose-300">{trigger.error}</span>}
        <span className="ml-auto text-white/30">a view model with a checkpoint, from Saga.make</span>
      </div>
      {transfers.length === 0 && (
        <div className="py-6 text-center text-xs text-white/25">
          no transfers yet: drag a card onto another one
        </div>
      )}
      <AnimatePresence initial={false}>
        {transfers.map((transfer) => (
          <TransferCard
            key={transfer.transferId}
            transfer={transfer}
            status={runs[transfer.transferId]?.status}
            executionId={runs[transfer.transferId]?.executionId}
            steps={stepsOf(transfer, runs[transfer.transferId], spans, trigger.position)}
          />
        ))}
      </AnimatePresence>
    </div>
  )
}

const TransferCard = (
  { executionId, status, steps, transfer }: {
    transfer: Transfer
    steps: ReadonlyArray<Step>
    status: EngineStatus | undefined
    executionId: string | undefined
  }
) => {
  const outcome = transfer.received !== undefined
    ? { label: "credited", color: eventColor.TransferReceived! }
    : transfer.refunded !== undefined
    ? { label: "refunded", color: eventColor.TransferRefunded! }
    : { label: "in flight", color: eventColor.TransferSent! }
  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0 }}
      className="glass rounded-xl px-3.5 py-3"
    >
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="font-mono text-[11.5px] font-semibold">Transfer</span>
        <span className="font-mono text-[10px] text-white/40">
          {transfer.transferId} · {transfer.from} → {transfer.to} · {money(transfer.amount)}
        </span>
        {executionId !== undefined && (
          <span className="font-mono text-[10px] text-white/25" title={executionId}>
            exec {executionId.length > 10 ? `${executionId.slice(0, 10)}…` : executionId}
          </span>
        )}
        <div className="ml-auto flex items-center gap-1.5">
          <Pill color={outcome.color}>{outcome.label}</Pill>
          <Pill color={status === undefined ? "#64748b" : engineColor[status]} solid={status === "succeeded"}>
            {status ?? "not started"}
          </Pill>
        </div>
      </div>
      <StepRow steps={steps} />
    </motion.div>
  )
}
