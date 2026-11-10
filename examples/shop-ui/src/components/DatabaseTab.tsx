import { AnimatePresence, motion } from "motion/react"
import { type ReactNode, useState } from "react"
import { Code, Pill } from "../../../bank-ui/src/components/ui.tsx"
import type { EventRow } from "../devstore.ts"
import { engineColor, ms, short, useSnapshot } from "../hooks.ts"

const tables = ["cqrs_events", "cqrs_checkpoints", "workflow_executions", "workflow_activities"] as const
type Table = (typeof tables)[number]

const activities = new Set([
  "reserve",
  "reject",
  "request-payment",
  "DurableClock/payment-timeout",
  "confirm",
  "expire",
  "release"
])

export const DatabaseTab = () => {
  const [table, setTable] = useState<Table>("cqrs_events")
  // One number per selector: an object would be new on every read and render forever.
  const counts: Record<Table, number> = {
    cqrs_events: useSnapshot((s) => s.events.length),
    cqrs_checkpoints: useSnapshot((s) => s.checkpoints.length),
    workflow_executions: useSnapshot((s) => Object.keys(s.orders).length),
    workflow_activities: useSnapshot((s) => s.spans.filter((span) => activities.has(span.name)).length)
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-1.5">
        {tables.map((name) => (
          <button
            key={name}
            onClick={() => setTable(name)}
            className={`relative rounded-lg px-2.5 py-1 font-mono text-[10.5px] transition-colors ${
              table === name ? "text-white" : "text-white/45 hover:text-white/80"
            }`}
          >
            {table === name && (
              <motion.span
                layoutId="table"
                className="absolute inset-0 rounded-lg bg-white/[0.07] ring-1 ring-white/10"
              />
            )}
            <span className="relative">
              {name} <span className="text-white/30">{counts[name]}</span>
            </span>
          </button>
        ))}
      </div>
      {table === "cqrs_events" && <EventsTable />}
      {table === "cqrs_checkpoints" && <CheckpointsTable />}
      {table === "workflow_executions" && <ExecutionsTable />}
      {table === "workflow_activities" && <ActivitiesTable />}
    </div>
  )
}

const Note = ({ children }: { children: ReactNode }) => (
  <p className="text-[11px] leading-relaxed text-white/45">{children}</p>
)

const Grid = ({ head, children }: { head: ReadonlyArray<string>; children: ReactNode }) => (
  <div className="glass scroll-thin overflow-x-auto rounded-xl">
    <table className="w-full font-mono text-[10.5px]">
      <thead>
        <tr className="border-b border-white/[0.06] text-left text-[9px] uppercase tracking-wider text-white/35">
          {head.map((name) => <th key={name} className="px-2.5 py-2 font-medium whitespace-nowrap">{name}</th>)}
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  </div>
)

const Row = ({ children, onClick }: { children: ReactNode; onClick?: () => void }) => (
  <motion.tr
    layout="position"
    initial={{ backgroundColor: "rgba(124,92,255,0.22)" }}
    animate={{ backgroundColor: "rgba(124,92,255,0)" }}
    transition={{ duration: 1.4 }}
    onClick={onClick}
    className={`border-t border-white/[0.04] align-top ${
      onClick === undefined ? "" : "cursor-pointer hover:bg-white/[0.03]"
    }`}
  >
    {children}
  </motion.tr>
)

const Cell = ({ children, dim = false }: { children: ReactNode; dim?: boolean }) => (
  <td className={`px-2.5 py-1.5 whitespace-nowrap ${dim ? "text-white/40" : "text-white/80"}`}>{children}</td>
)

const json = (value: unknown) => JSON.stringify(value)

const EventsTable = () => {
  const events = useSnapshot((s) => s.events)
  const [open, setOpen] = useState<number | undefined>(undefined)
  const rows = events.slice(-100).reverse()

  return (
    <>
      <Note>
        The event log as a SQL store keeps it. Rows are only ever appended. An imported v1 order keeps its old payload
        here; the aggregate reads it upcast. Click a row to compare.
      </Note>
      <Grid
        head={["position", "aggregate", "id", "v", "type", "schema", "payload", "metadata", "correlation", "causation"]}
      >
        <AnimatePresence initial={false}>
          {rows.map((event) => (
            <EventLine
              key={event.position}
              event={event}
              open={open === event.position}
              onToggle={() => setOpen(open === event.position ? undefined : event.position)}
            />
          ))}
        </AnimatePresence>
      </Grid>
    </>
  )
}

const EventLine = ({ event, onToggle, open }: { event: EventRow; open: boolean; onToggle: () => void }) => (
  <>
    <Row onClick={onToggle}>
      <Cell dim>#{event.position}</Cell>
      <Cell>{event.aggregateName}</Cell>
      <Cell>{event.aggregateId}</Cell>
      <Cell dim>{event.version}</Cell>
      <Cell>{event.type}</Cell>
      <Cell>
        {event.schemaVersion === 1 && event.type === "OrderPlaced"
          ? <Pill color="#fbbf24">1</Pill>
          : <span className="text-white/40">{event.schemaVersion}</span>}
      </Cell>
      <Cell>{json(event.payload)}</Cell>
      <Cell>
        <span className="text-sky-200/80">{json(event.metadata)}</span>
      </Cell>
      <Cell dim>{short(event.correlationId)}</Cell>
      <Cell dim>{short(event.causationId, 14)}</Cell>
    </Row>
    {open && (
      <tr>
        <td colSpan={10} className="px-2.5 pb-2">
          {/* Sticky, so the comparison stays in view however far the table is scrolled. */}
          <div className="sticky left-2.5 grid w-[440px] max-w-full grid-cols-2 gap-2">
            <Block title={`stored · schema version ${event.schemaVersion}`} value={event.payload} />
            <Block title="as the aggregate reads it" value={event.decoded} />
          </div>
        </td>
      </tr>
    )}
  </>
)

const Block = ({ title, value }: { title: string; value: unknown }) => (
  <div className="rounded-md bg-black/40 p-2">
    <div className="mb-1 text-[9px] uppercase tracking-wider text-white/35">{title}</div>
    <pre className="text-[10px] leading-relaxed text-emerald-100/80">{JSON.stringify(value, null, 2)}</pre>
  </div>
)

const CheckpointsTable = () => {
  const checkpoints = useSnapshot((s) => s.checkpoints)
  const head = useSnapshot((s) => s.events.at(-1)?.position ?? 0)
  return (
    <>
      <Note>
        How far each view model has read, from <Code>ViewModels.status</Code>. <Code>fulfilment</Code>{" "}
        is the saga's trigger: it starts a workflow for each order it reads.
      </Note>
      <Grid head={["view_model", "position", "behind", "error"]}>
        {checkpoints.map((checkpoint) => (
          <Row key={checkpoint.name}>
            <Cell>{checkpoint.name}</Cell>
            <Cell>
              <motion.span key={checkpoint.position} initial={{ color: "#a78bfa" }} animate={{ color: "#e7e7f0" }}>
                {checkpoint.position}
              </motion.span>
            </Cell>
            <Cell dim>{head - checkpoint.position}</Cell>
            <Cell dim>{checkpoint.error ?? "—"}</Cell>
          </Row>
        ))}
      </Grid>
    </>
  )
}

const ExecutionsTable = () => {
  const orders = useSnapshot((s) => s.orders)
  const engine = useSnapshot((s) => s.engine)
  const spans = useSnapshot((s) => s.spans)
  const rows = Object.values(orders).sort((a, b) => b.position - a.position)
  return (
    <>
      <Note>
        <Code>WorkflowEngine.layerMemory</Code> keeps executions in memory and answers <Code>Fulfil.poll</Code>.{" "}
        <Code>ClusterWorkflowEngine</Code> keeps the same rows in SQL, so they survive a restart.
      </Note>
      <Grid head={["execution_id", "workflow", "order", "payload", "status", "activities"]}>
        <AnimatePresence initial={false}>
          {rows.map((order) => {
            const status = engine[order.executionId]
            const done = spans.filter((span) =>
              activities.has(span.name) && span.attributes.executionId === order.executionId && span.end !== undefined
            ).length
            return (
              <Row key={order.id}>
                <Cell dim>{short(order.executionId, 12)}</Cell>
                <Cell>Fulfil</Cell>
                <Cell>{order.id}</Cell>
                <Cell>{json({ orderId: order.id, sku: order.sku, quantity: order.quantity })}</Cell>
                <Cell>
                  <Pill color={status === undefined ? "#64748b" : engineColor[status]}>{status ?? "pending"}</Pill>
                </Cell>
                <Cell dim>{done}</Cell>
              </Row>
            )
          })}
        </AnimatePresence>
      </Grid>
    </>
  )
}

const ActivitiesTable = () => {
  const spans = useSnapshot((s) => s.spans)
  const rows = spans.filter((span) => activities.has(span.name)).reverse()
  return (
    <>
      <Note>
        One row per activity run, from its span. The engine saves each result, a failure the saga expects such as{" "}
        <Code>OutOfStock</Code> included, so a replay takes the same branch.
      </Note>
      <Grid head={["execution_id", "activity", "status", "result", "took"]}>
        <AnimatePresence initial={false}>
          {rows.map((span) => (
            <Row key={span.spanId}>
              <Cell dim>{short(String(span.attributes.executionId ?? ""), 12)}</Cell>
              <Cell>{span.name}</Cell>
              <Cell>
                <Pill color={span.status === "running" ? "#38bdf8" : span.status === "ok" ? "#34d399" : "#fb7185"}>
                  {span.status}
                </Pill>
              </Cell>
              <Cell dim>{span.status === "running" ? "…" : span.error ?? "success"}</Cell>
              <Cell dim>{span.end === undefined ? "…" : ms(span.end - span.start)}</Cell>
            </Row>
          ))}
        </AnimatePresence>
      </Grid>
    </>
  )
}
