import { AnimatePresence, motion, useAnimate } from "motion/react"
import { useMemo, useState } from "react"
import type { AccountRow, CommandEntry, EventEntry, Outcome, Totals, ViewData } from "../devstore.ts"
import { balanceDelta } from "../domain/account.ts"
import { engine, newTransferId, palette } from "../engine.ts"
import { eventColor, foldAt, head, money, signed, useSnapshot } from "../hooks.ts"
import { AnimatedNumber, Button, Code, Pill } from "./ui.tsx"

/** Accounts as a fold of the events up to `position`, shaped like the read model's rows. */
const rowsAt = (events: ReadonlyArray<EventEntry>, position: number): Record<string, AccountRow> => {
  const states = foldAt(events, position)
  const rows: Record<string, AccountRow> = {}
  for (const [id, state] of Object.entries(states)) {
    if (state.status === "none") continue
    const own = events.filter((event) => event.aggregateId === id && event.position <= position)
    const last = own.at(-1)
    rows[id] = {
      id,
      owner: state.owner,
      color: state.color,
      balance: state.balance,
      status: state.status,
      txCount: own.filter((event) => balanceDelta(event.event) !== 0).length,
      lastPosition: last?.position ?? 0,
      lastDelta: last === undefined ? 0 : balanceDelta(last.event)
    }
  }
  return rows
}

export const BankApp = ({ travel, onExitTravel }: { travel: number | undefined; onExitTravel: () => void }) => {
  const events = useSnapshot((s) => s.events)
  const accountsView = useSnapshot((s) => s.views.accounts)
  const totalsView = useSnapshot((s) => s.views.totals)
  const feedView = useSnapshot((s) => s.views.feed)
  const commands = useSnapshot((s) => s.commands)
  const [transfer, setTransfer] = useState<{ from: string; to?: string } | undefined>(undefined)
  const [dragging, setDragging] = useState<string | undefined>(undefined)

  const live = travel === undefined
  const rows = useMemo(
    () => Object.values(live ? accountsView.data : rowsAt(events, travel)),
    [live, accountsView.data, events, travel]
  )
  const storeHead = head(events)

  return (
    <section className="scroll-thin relative min-h-0 overflow-y-auto px-6 py-5">
      <AnimatePresence>
        {!live && (
          <motion.div
            initial={{ opacity: 0, y: -12 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -12 }}
            className="sticky top-0 z-30 mb-4 flex items-center gap-3 rounded-xl border border-sky-400/30 bg-sky-500/10 px-4 py-2.5 text-xs text-sky-100 backdrop-blur-xl"
          >
            <motion.span animate={{ rotate: -360 }} transition={{ duration: 3, repeat: Infinity, ease: "linear" }}>
              ⏪
            </motion.span>
            <div>
              <b>Time travel preview at #{travel}.</b> These cards are a pure fold of events 1…{travel}{" "}
              with the aggregate's projection. Nothing was replayed and no read model changed.
            </div>
            <Button tone="primary" onClick={onExitTravel}>Back to live</Button>
          </motion.div>
        )}
      </AnimatePresence>

      <div className="mb-5 flex items-end justify-between gap-4">
        <div>
          <div className="font-mono text-[10px] uppercase tracking-[0.2em] text-white/40">Customer app</div>
          <h1 className="text-2xl font-semibold tracking-tight">Accounts</h1>
          <p className="mt-1 max-w-md text-xs text-white/45">
            {live
              ? (
                <>
                  Rendered from the <Code>accounts</Code>{" "}
                  view model, never from the aggregate. Buttons send commands; drag a card onto another to transfer.
                </>
              )
              : "Read only while time travelling."}
          </p>
        </div>
        {live && (
          <Freshness
            label="accounts"
            position={accountsView.position}
            head={storeHead}
            replaying={accountsView.replaying}
          />
        )}
      </div>

      {live && <Vault totals={totalsView.data} position={totalsView.position} head={storeHead} />}

      <motion.div layout className="grid grid-cols-[repeat(auto-fill,minmax(250px,1fr))] gap-4">
        <AnimatePresence>
          {rows.map((row) => (
            <AccountCard
              key={row.id}
              row={row}
              live={live}
              events={events}
              upTo={travel ?? storeHead}
              pending={commands.filter((c) => c.aggregateId === row.id && c.outcome === undefined)}
              dragging={dragging}
              onDragStart={() => setDragging(row.id)}
              onDrop={(to) => {
                setDragging(undefined)
                if (to !== undefined && to !== row.id) setTransfer({ from: row.id, to })
              }}
              onTransfer={() => setTransfer({ from: row.id })}
            />
          ))}
          {live && <OpenAccountCard key="__open" existing={rows} />}
        </AnimatePresence>
      </motion.div>

      {live && <Activity feed={feedView.data} position={feedView.position} head={storeHead} />}

      <AnimatePresence>
        {transfer && (
          <TransferDialog
            from={transfer.from}
            initialTo={transfer.to}
            accounts={rows}
            onClose={() => setTransfer(undefined)}
          />
        )}
      </AnimatePresence>
    </section>
  )
}

const Freshness = (
  { label, position, head, replaying }: { label: string; position: number; head: number; replaying?: boolean }
) => {
  const behind = Math.max(0, head - position)
  const color = replaying ? "#38bdf8" : behind === 0 ? "#34d399" : "#fbbf24"
  return (
    <div className="flex items-center gap-2 rounded-full bg-white/[0.04] py-1 pr-3 pl-1.5 font-mono text-[10px] ring-1 ring-white/[0.06]">
      <span className="relative flex h-2 w-2">
        {behind > 0 && (
          <span
            className="absolute inline-flex h-full w-full animate-ping rounded-full opacity-75"
            style={{ background: color }}
          />
        )}
        <span className="relative inline-flex h-2 w-2 rounded-full" style={{ background: color }} />
      </span>
      <span className="text-white/50">{label}</span>
      <span style={{ color }}>
        {replaying ? "replaying…" : behind === 0 ? `in sync #${position}` : `${behind} behind · #${position}/${head}`}
      </span>
    </div>
  )
}

const Vault = (
  { totals, position, head }: { totals: Totals; position: number; head: number }
) => (
  <div className="mb-5">
    <div className="mb-2 flex items-center justify-between">
      <div className="font-mono text-[10px] uppercase tracking-[0.2em] text-white/40">Bank vault</div>
      <Freshness label="bank-totals" position={position} head={head} />
    </div>
    <motion.div
      layout
      className="glass relative grid grid-cols-4 gap-px overflow-hidden rounded-2xl"
    >
      {[
        { label: "Money in bank", value: totals.moneyInBank, color: "#e9e7ff" },
        { label: "In flight", value: totals.inFlight, color: "#f472b6" },
        { label: "Deposited", value: totals.deposited, color: "#34d399" },
        { label: "Withdrawn", value: totals.withdrawn, color: "#fbbf24" }
      ].map((item) => (
        <div key={item.label} className="bg-panel/40 px-4 py-3">
          <div className="text-[10px] uppercase tracking-wider text-white/40">{item.label}</div>
          <div className="mt-0.5 text-xl font-semibold" style={{ color: item.color }}>
            <AnimatedNumber value={item.value} format={money} />
          </div>
        </div>
      ))}
    </motion.div>
  </div>
)

const quick = [
  { label: "+25", type: "deposit", amount: 25 },
  { label: "+100", type: "deposit", amount: 100 },
  { label: "−50", type: "withdraw", amount: 50 },
  { label: "−200", type: "withdraw", amount: 200 }
] as const

const AccountCard = (
  { row, live, events, upTo, pending, dragging, onDragStart, onDrop, onTransfer }: {
    row: AccountRow
    live: boolean
    events: ReadonlyArray<EventEntry>
    upTo: number
    pending: ReadonlyArray<CommandEntry>
    dragging: string | undefined
    onDragStart: () => void
    onDrop: (to: string | undefined) => void
    onTransfer: () => void
  }
) => {
  const [error, setError] = useState<Outcome | undefined>(undefined)
  const [scope, animate] = useAnimate<HTMLDivElement>()
  const closed = row.status === "closed"
  const isTarget = dragging !== undefined && dragging !== row.id && !closed

  const send = async (type: string, payload: unknown) => {
    const outcome = await engine.send({ aggregateId: row.id, type, payload })
    if (!outcome.ok) {
      setError(outcome)
      void animate(scope.current, { x: [0, -10, 9, -6, 4, 0] }, { duration: 0.45 })
      setTimeout(() => setError((current) => (current === outcome ? undefined : current)), 3200)
    }
  }

  const series = useMemo(() => {
    let balance = 0
    const points = [0]
    for (const event of events) {
      if (event.position > upTo) break
      if (event.aggregateId !== row.id) continue
      balance += balanceDelta(event.event)
      points.push(balance)
    }
    return points
  }, [events, upTo, row.id])

  return (
    <motion.div
      layout
      initial={{ opacity: 0, scale: 0.85, y: 20 }}
      animate={{ opacity: closed ? 0.55 : 1, scale: 1, y: 0 }}
      exit={{ opacity: 0, scale: 0.8, filter: "blur(6px)" }}
      transition={{ type: "spring", stiffness: 260, damping: 24 }}
      ref={scope}
      data-drop-id={row.id}
      className="relative"
    >
      <motion.div
        drag={live && !closed}
        dragSnapToOrigin
        dragElastic={0.6}
        whileDrag={{ scale: 1.04, rotate: -3, zIndex: 50, cursor: "grabbing" }}
        onDragStart={onDragStart}
        onDragEnd={(event) => {
          const { clientX, clientY } = event as PointerEvent
          const target = document.elementsFromPoint(clientX, clientY)
            .map((element) => element.closest<HTMLElement>("[data-drop-id]")?.dataset.dropId)
            .find((id) => id !== undefined && id !== row.id)
          onDrop(target)
        }}
        animate={isTarget
          ? { scale: 1.03, boxShadow: `0 0 0 2px ${row.color}, 0 20px 60px -10px ${row.color}` }
          : { scale: 1, boxShadow: `0 18px 50px -24px ${row.color}` }}
        className={`relative overflow-hidden rounded-2xl p-4 ${live && !closed ? "cursor-grab" : ""}`}
        style={{
          background: `linear-gradient(135deg, ${row.color} 0%, ${row.color}cc 35%, #15121f 100%)`,
          filter: closed ? "grayscale(0.9)" : undefined
        }}
      >
        <div className="card-sheen pointer-events-none absolute inset-0" />
        <div className="relative flex items-start justify-between">
          <div>
            <div className="text-[15px] font-semibold text-white drop-shadow">{row.owner}</div>
            <div className="font-mono text-[10px] text-white/70">account/{row.id}</div>
          </div>
          <div className="flex items-center gap-1.5">
            {closed && <Pill color="#0b0b12" solid>closed</Pill>}
            <Chip />
          </div>
        </div>
        <div className="relative mt-5 text-[28px] leading-none font-semibold text-white drop-shadow-[0_2px_10px_rgba(0,0,0,0.35)]">
          <AnimatedNumber value={row.balance} format={money} />
        </div>
        <div className="relative mt-3 flex items-end justify-between">
          <Sparkline points={series} />
          <div className="text-right font-mono text-[10px] text-white/70">
            {row.txCount} tx · v{events.filter((e) => e.aggregateId === row.id && e.position <= upTo).length}
          </div>
        </div>
        <AnimatePresence>
          {live && row.lastDelta !== 0 && (
            <motion.div
              key={row.lastPosition}
              initial={{ opacity: 0, y: 10, scale: 0.8 }}
              animate={{ opacity: [0, 1, 1, 0], y: -26, scale: 1 }}
              transition={{ duration: 1.6, times: [0, 0.15, 0.7, 1] }}
              className="pointer-events-none absolute top-14 right-4 rounded-full bg-black/40 px-2 py-0.5 font-mono text-sm font-semibold backdrop-blur"
              style={{ color: row.lastDelta > 0 ? "#6ee7b7" : "#fda4af" }}
            >
              {signed(row.lastDelta)}
            </motion.div>
          )}
        </AnimatePresence>
        <AnimatePresence>
          {pending.length > 0 && (
            <motion.div
              initial={{ opacity: 0, scale: 0.6 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.6 }}
              className="absolute top-3 right-12 flex items-center gap-1 rounded-full bg-black/35 px-2 py-0.5 font-mono text-[10px] text-white backdrop-blur"
            >
              <motion.span
                className="h-1.5 w-1.5 rounded-full bg-white"
                animate={{ opacity: [1, 0.2, 1] }}
                transition={{ duration: 0.9, repeat: Infinity }}
              />
              {pending.length} pending
            </motion.div>
          )}
        </AnimatePresence>
      </motion.div>

      {live && (
        <div className="mt-2 flex flex-wrap items-center gap-1">
          {!closed && (
            <>
              {quick.map((q) => (
                <Button key={q.label} onClick={() => void send(q.type, { amount: q.amount })}>
                  <span className={q.type === "deposit" ? "text-emerald-300" : "text-amber-300"}>{q.label}</span>
                </Button>
              ))}
              <Button onClick={onTransfer} title="Or drag the card onto another one">⇄</Button>
              <Button
                tone="ghost"
                onClick={() => void send("closeAccount", {})}
                title="Only an empty account can close"
              >
                ✕
              </Button>
            </>
          )}
          {row.owner !== "[forgotten]" && (
            <Button
              tone="danger"
              onClick={() => void engine.forget(row.id)}
              title="Forget the owner: destroys the key that encrypts their name, the events stay"
            >
              forget
            </Button>
          )}
        </div>
      )}

      <AnimatePresence>
        {error && (
          <motion.div
            initial={{ opacity: 0, y: -6, height: 0 }}
            animate={{ opacity: 1, y: 0, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="mt-1.5 overflow-hidden rounded-lg border border-rose-400/30 bg-rose-500/10 px-2.5 py-1.5 font-mono text-[10px] text-rose-200"
          >
            <b>{error.ok ? "" : error.tag}</b> {error.ok ? "" : error.message}
            <div className="text-rose-200/50">Rejected by the aggregate, so no event was stored.</div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  )
}

const Chip = () => (
  <div className="h-6 w-8 rounded-md bg-gradient-to-br from-amber-200 to-amber-500 opacity-90 ring-1 ring-black/20">
    <div className="mx-auto mt-1 h-4 w-5 rounded-sm border border-black/25" />
  </div>
)

const Sparkline = ({ points }: { points: ReadonlyArray<number> }) => {
  const width = 120
  const height = 28
  const max = Math.max(1, ...points)
  const min = Math.min(0, ...points)
  const step = points.length > 1 ? width / (points.length - 1) : width
  const d = points
    .map((p, i) =>
      `${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${(height - ((p - min) / (max - min)) * height).toFixed(1)}`
    )
    .join(" ")
  return (
    <svg width={width} height={height} className="overflow-visible">
      <motion.path
        d={d}
        fill="none"
        stroke="white"
        strokeOpacity={0.85}
        strokeWidth={1.6}
        strokeLinejoin="round"
        initial={false}
        animate={{ d }}
        transition={{ duration: 0.5 }}
      />
    </svg>
  )
}

const OpenAccountCard = ({ existing }: { existing: ReadonlyArray<AccountRow> }) => {
  const [open, setOpen] = useState(false)
  const [owner, setOwner] = useState("")
  const [color, setColor] = useState(palette[3]!)
  const [error, setError] = useState<string | undefined>(undefined)

  const submit = async () => {
    const id = owner.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-") || "anonymous"
    const outcome = await engine.send({ aggregateId: id, type: "openAccount", payload: { owner: owner.trim(), color } })
    if (outcome.ok) {
      setOpen(false)
      setOwner("")
      setError(undefined)
    } else {
      setError(`${outcome.tag}: ${outcome.message}`)
    }
  }

  return (
    <motion.div
      layout
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="flex min-h-[170px] flex-col rounded-2xl border border-dashed border-white/15 p-4"
    >
      {!open
        ? (
          <button
            onClick={() => setOpen(true)}
            className="group m-auto flex flex-col items-center gap-2 text-white/50 hover:text-white"
          >
            <motion.span
              whileHover={{ rotate: 90, scale: 1.1 }}
              className="grid h-11 w-11 place-items-center rounded-full bg-white/[0.06] text-2xl ring-1 ring-white/10"
            >
              +
            </motion.span>
            <span className="text-xs">Open an account</span>
            <span className="font-mono text-[10px] text-white/30">{existing.length} open so far</span>
          </button>
        )
        : (
          <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className="flex flex-col gap-3">
            <div className="text-xs font-medium text-white/70">New account</div>
            <input
              autoFocus
              value={owner}
              onChange={(event) => setOwner(event.target.value)}
              onKeyDown={(event) => event.key === "Enter" && void submit()}
              placeholder="Owner name (empty fails validation)"
              className="rounded-lg bg-white/[0.06] px-3 py-2 text-sm ring-1 ring-white/10 outline-none placeholder:text-white/30 focus:ring-violet-400/60"
            />
            <div className="flex gap-1.5">
              {palette.map((swatch) => (
                <motion.button
                  key={swatch}
                  whileHover={{ scale: 1.15 }}
                  onClick={() => setColor(swatch)}
                  className="h-5 w-5 rounded-full"
                  style={{
                    background: swatch,
                    boxShadow: swatch === color ? `0 0 0 2px #07070d, 0 0 0 4px ${swatch}` : undefined
                  }}
                />
              ))}
            </div>
            {error && <div className="font-mono text-[10px] text-rose-300">{error}</div>}
            <div className="flex gap-2">
              <Button tone="primary" onClick={() => void submit()}>openAccount</Button>
              <Button tone="ghost" onClick={() => setOpen(false)}>Cancel</Button>
            </div>
          </motion.div>
        )}
    </motion.div>
  )
}

const TransferDialog = (
  { from, initialTo, accounts, onClose }: {
    from: string
    initialTo: string | undefined
    accounts: ReadonlyArray<AccountRow>
    onClose: () => void
  }
) => {
  const source = accounts.find((account) => account.id === from)
  const [to, setTo] = useState(initialTo ?? accounts.find((account) => account.id !== from)?.id ?? "")
  const [amount, setAmount] = useState(Math.min(50, Math.max(5, source?.balance ?? 50)))
  const send = () => {
    void engine.send({ aggregateId: from, type: "sendTransfer", payload: { transferId: newTransferId(), to, amount } })
    onClose()
  }
  return (
    <motion.div
      className="fixed inset-0 z-50 grid place-items-center bg-black/50 backdrop-blur-sm"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.9, y: 20 }}
        animate={{ scale: 1, y: 0 }}
        exit={{ scale: 0.9, y: 20 }}
        onClick={(event) => event.stopPropagation()}
        className="glass w-[420px] rounded-2xl p-5 shadow-2xl"
      >
        <div className="font-mono text-[10px] uppercase tracking-[0.2em] text-white/40">sendTransfer</div>
        <div className="mt-1 text-lg font-semibold">
          {source?.owner} → {accounts.find((account) => account.id === to)?.owner ?? "?"}
        </div>
        <p className="mt-1 text-xs text-white/50">
          The <Code>account/{from}</Code> aggregate records <Code>TransferSent</Code>. A saga then sends{" "}
          <Code>receiveTransfer</Code> to the other account, and refunds if that is refused.
        </p>
        <div className="mt-4 flex flex-wrap gap-1.5">
          {accounts.filter((account) => account.id !== from).map((account) => (
            <button
              key={account.id}
              onClick={() => setTo(account.id)}
              className="rounded-full px-3 py-1 text-xs transition"
              style={{
                background: account.id === to ? account.color : `${account.color}22`,
                color: account.id === to ? "#0b0b12" : account.color
              }}
            >
              {account.owner}
              {account.status === "closed" && " (closed)"}
            </button>
          ))}
        </div>
        <div className="mt-4 flex items-center gap-3">
          <input
            type="number"
            value={amount}
            onChange={(event) => setAmount(Number(event.target.value))}
            className="w-28 rounded-lg bg-white/[0.06] px-3 py-2 font-mono text-sm ring-1 ring-white/10 outline-none"
          />
          <span className="text-xs text-white/40">balance {money(source?.balance ?? 0)}</span>
        </div>
        <div className="mt-5 flex justify-end gap-2">
          <Button tone="ghost" onClick={onClose}>Cancel</Button>
          <Button tone="primary" onClick={send} disabled={to === ""}>Send transfer</Button>
        </div>
      </motion.div>
    </motion.div>
  )
}

const Activity = (
  { feed, position, head }: { feed: ViewData["feed"]; position: number; head: number }
) => (
  <div className="mt-7">
    <div className="mb-2 flex items-center justify-between">
      <div className="font-mono text-[10px] uppercase tracking-[0.2em] text-white/40">Recent activity</div>
      <Freshness label="activity-feed" position={position} head={head} />
    </div>
    <div className="glass overflow-hidden rounded-2xl">
      <AnimatePresence initial={false}>
        {feed.slice(0, 8).map((item) => (
          <motion.div
            key={item.position}
            layout
            initial={{ opacity: 0, x: -20, backgroundColor: "rgba(167,139,250,0.18)" }}
            animate={{ opacity: 1, x: 0, backgroundColor: "rgba(0,0,0,0)" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.45 }}
            className="flex items-center gap-3 border-b border-white/[0.04] px-4 py-2 text-xs last:border-0"
          >
            <span className="h-2 w-2 rounded-full" style={{ background: eventColor[item.type] }} />
            <span className="font-mono text-white/35">#{item.position}</span>
            <span className="font-medium text-white/85">{item.aggregateId}</span>
            <span className="text-white/55">{item.text}</span>
            {item.delta !== 0 && (
              <span className={`ml-auto font-mono ${item.delta > 0 ? "text-emerald-300" : "text-rose-300"}`}>
                {signed(item.delta)}
              </span>
            )}
          </motion.div>
        ))}
      </AnimatePresence>
      {feed.length === 0 && <div className="px-4 py-6 text-center text-xs text-white/30">Nothing projected yet</div>}
    </div>
  </div>
)
