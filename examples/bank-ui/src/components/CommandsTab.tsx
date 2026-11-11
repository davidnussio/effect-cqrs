import { AnimatePresence, motion } from "motion/react"
import { useState } from "react"
import { type CommandEntry, devstore, type Outcome, stages } from "../devstore.ts"
import { engine } from "../engine.ts"
import { originColor, useSnapshot } from "../hooks.ts"
import { Button, Code, Pill, SectionTitle, Slider, Toggle } from "./ui.tsx"

const templates: ReadonlyArray<{ label: string; command: unknown }> = [
  { label: "deposit", command: { aggregateId: "alice", type: "deposit", payload: { amount: 75 } } },
  { label: "withdraw", command: { aggregateId: "bob", type: "withdraw", payload: { amount: 30 } } },
  {
    label: "sendTransfer",
    command: {
      aggregateId: "alice",
      type: "sendTransfer",
      payload: { transferId: "tx-console", to: "carol", amount: 20 }
    }
  },
  { label: "closeAccount", command: { aggregateId: "carol", type: "closeAccount", payload: {} } },
  { label: "✗ bad payload", command: { aggregateId: "alice", type: "deposit", payload: { amount: -5 } } },
  { label: "✗ unknown type", command: { aggregateId: "alice", type: "printMoney", payload: {} } },
  { label: "✗ no account", command: { aggregateId: "mallory", type: "deposit", payload: { amount: 10 } } }
]

export const CommandsTab = () => {
  const settings = useSnapshot((s) => s.settings)
  const commands = useSnapshot((s) => s.commands)

  return (
    <div className="space-y-5">
      <div className="glass space-y-4 rounded-2xl p-4">
        <SectionTitle
          right={
            <div className="flex gap-1.5">
              <Button
                tone={settings.paused ? "primary" : "default"}
                onClick={() => engine.setPaused(!settings.paused)}
              >
                {settings.paused ? "▶ Resume" : "❚❚ Pause"}
              </Button>
              <Button onClick={engine.step} disabled={!settings.paused} title="Advance one stage">
                Step ▸
              </Button>
            </div>
          }
        >
          Speed
        </SectionTitle>
        <div className="grid grid-cols-1 gap-x-5 gap-y-4 sm:grid-cols-2">
          <Slider
            label="Command latency per stage"
            value={settings.commandDelay}
            min={0}
            max={2000}
            step={50}
            onChange={(commandDelay) => devstore.settings({ commandDelay })}
            hint="Slow motion through validate → load → decide → append"
          />
          <Slider
            label="Projection latency per event"
            value={settings.projectionDelay}
            min={0}
            max={2000}
            step={50}
            color="#38bdf8"
            onChange={(projectionDelay) => devstore.settings({ projectionDelay })}
            hint="How far read models trail the event store"
          />
          <Slider
            label="Replay speed per event"
            value={settings.replayDelay}
            min={0}
            max={400}
            step={10}
            color="#34d399"
            onChange={(replayDelay) => devstore.settings({ replayDelay })}
          />
          <div className="space-y-2.5">
            <Toggle
              on={settings.autopilot}
              onChange={(autopilot) => devstore.settings({ autopilot })}
              label={
                <span>
                  Autopilot <span className="text-white/35">random customers</span>
                </span>
              }
              color="#fbbf24"
            />
            <Slider
              label="Every"
              value={settings.autopilotEvery}
              min={200}
              max={4000}
              step={100}
              color="#fbbf24"
              onChange={(autopilotEvery) => devstore.settings({ autopilotEvery })}
            />
          </div>
        </div>
      </div>

      <Console />

      <div>
        <SectionTitle right={<span className="font-mono text-[10px] text-white/35">newest first</span>}>
          Command bus
        </SectionTitle>
        <div className="space-y-1.5">
          <AnimatePresence initial={false}>
            {commands.map((command) => <CommandRow key={command.id} command={command} />)}
          </AnimatePresence>
        </div>
      </div>
    </div>
  )
}

const compact = (value: unknown) => JSON.stringify(value).replace(/"(\w+)":/g, "$1:")

const CommandRow = ({ command }: { command: CommandEntry }) => {
  const { outcome } = command
  const index = stages.indexOf(command.stage)
  const done = outcome !== undefined
  const border = outcome === undefined
    ? "rgba(167,139,250,0.35)"
    : outcome.ok
    ? "rgba(52,211,153,0.18)"
    : "rgba(251,113,133,0.35)"
  return (
    <motion.div
      layout
      initial={{ opacity: 0, x: 30, scale: 0.97 }}
      animate={{ opacity: 1, x: 0, scale: 1 }}
      exit={{ opacity: 0, height: 0 }}
      transition={{ type: "spring", stiffness: 380, damping: 32 }}
      className="rounded-xl bg-white/[0.025] px-3 py-2"
      style={{ boxShadow: `inset 0 0 0 1px ${border}` }}
    >
      <div className="flex items-center gap-2 text-xs">
        <Pill color={originColor[command.origin]!}>{command.origin}</Pill>
        <span className="font-mono font-semibold text-white/90">{command.type}</span>
        <span className="font-mono text-white/40">→ {command.aggregateId}</span>
        <span className="ml-auto truncate font-mono text-[10px] text-white/35">{compact(command.payload)}</span>
      </div>
      <div className="mt-2 flex items-center gap-1">
        {stages.map((stage, i) => {
          const reached = done || i <= index
          const current = !done && i === index
          return (
            <div key={stage} className="flex-1">
              <div className="h-1 overflow-hidden rounded-full bg-white/[0.06]">
                <motion.div
                  className="h-full rounded-full"
                  initial={false}
                  animate={{
                    width: reached ? "100%" : "0%",
                    background: outcome === undefined
                      ? "#a78bfa"
                      : outcome.ok
                      ? "#34d399"
                      : i === stages.length - 1
                      ? "#fb7185"
                      : "#5b5b77"
                  }}
                  transition={{ duration: 0.3 }}
                />
              </div>
              <div className={`mt-1 font-mono text-[8.5px] ${current ? "text-violet-300" : "text-white/25"}`}>
                {current
                  ? (
                    <motion.span animate={{ opacity: [1, 0.4, 1] }} transition={{ duration: 0.8, repeat: Infinity }}>
                      {stage}
                    </motion.span>
                  )
                  : stage}
              </div>
            </div>
          )
        })}
      </div>
      {outcome && <OutcomeLine outcome={outcome} />}
    </motion.div>
  )
}

const OutcomeLine = ({ outcome }: { outcome: Outcome }) => (
  <motion.div
    initial={{ opacity: 0, y: -4 }}
    animate={{ opacity: 1, y: 0 }}
    className={`mt-1.5 font-mono text-[10px] ${outcome.ok ? "text-emerald-300/90" : "text-rose-300"}`}
  >
    {outcome.ok
      ? outcome.events.length === 0
        ? "✓ accepted, no event (idempotent)"
        : `✓ ${outcome.events.join(", ")} · stream v${outcome.version} · store #${outcome.position}`
      : (
        <>
          ✗ {outcome.tag} <span className="text-rose-300/60">{outcome.message}</span>
        </>
      )}
  </motion.div>
)

const Console = () => {
  const [text, setText] = useState(JSON.stringify(templates[0]!.command, null, 2))
  const [result, setResult] = useState<Outcome | { parse: string } | undefined>(undefined)
  const [busy, setBusy] = useState(false)

  const dispatch = async () => {
    let input: unknown
    try {
      input = JSON.parse(text)
    } catch (error) {
      setResult({ parse: (error as Error).message })
      return
    }
    setBusy(true)
    const { aggregateId, type, payload } = (input ?? {}) as Record<string, unknown>
    setResult(
      await engine.send({ aggregateId: String(aggregateId ?? ""), type: String(type ?? ""), payload }, "console")
    )
    setBusy(false)
  }

  return (
    <div className="glass rounded-2xl p-4">
      <SectionTitle>
        Console · <span className="normal-case tracking-normal">raw JSON through</span>
        <Code>dispatch</Code>
      </SectionTitle>
      <div className="mb-2 flex flex-wrap gap-1">
        {templates.map((template) => (
          <button
            key={template.label}
            onClick={() => {
              setText(JSON.stringify(template.command, null, 2))
              setResult(undefined)
            }}
            className={`rounded-md px-2 py-0.5 font-mono text-[10px] ring-1 ring-white/10 hover:bg-white/10 ${
              template.label.startsWith("✗") ? "text-rose-300/80" : "text-white/60"
            }`}
          >
            {template.label}
          </button>
        ))}
      </div>
      <textarea
        value={text}
        onChange={(event) => setText(event.target.value)}
        spellCheck={false}
        rows={6}
        className="w-full resize-none rounded-lg bg-black/40 p-3 font-mono text-[11px] leading-relaxed text-emerald-100/90 ring-1 ring-white/10 outline-none focus:ring-violet-400/50"
      />
      <div className="mt-2 flex items-center gap-3">
        <Button tone="primary" onClick={() => void dispatch()} disabled={busy}>
          {busy ? "Dispatching…" : "Dispatch ⏎"}
        </Button>
        <AnimatePresence mode="wait">
          {result && (
            <motion.div
              key={JSON.stringify(result)}
              initial={{ opacity: 0, x: -6 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0 }}
              className="min-w-0 flex-1 truncate font-mono text-[10px]"
            >
              {"parse" in result
                ? <span className="text-rose-300">JSON: {result.parse}</span>
                : <OutcomeLine outcome={result} />}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  )
}
