import { DateTime } from "effect"
import { useCallback, useEffect, useState } from "react"
import type { BotSettings, BotStatus, SimulatorSettings, SimulatorStatus } from "../../domain/automation.ts"
import { call } from "./client.ts"
import { Button, Card } from "./ui.tsx"

/** The presenter's controls: the restock bot and the simulated shoppers. */
export const ControlScreen = () => {
  const [bot, setBot] = useState<BotStatus>()
  const [simulator, setSimulator] = useState<SimulatorStatus>()
  const [error, setError] = useState<string>()

  const refresh = useCallback(async () => {
    const [botOutcome, simulatorOutcome] = await Promise.all([
      call((client) => client.shop.bot()),
      call((client) => client.admin.simulator())
    ])
    if (botOutcome.ok) setBot(botOutcome.value)
    if (simulatorOutcome.ok) setSimulator(simulatorOutcome.value)
    else setError(simulatorOutcome.message)
  }, [])

  useEffect(() => {
    void refresh()
    const timer = setInterval(() => void refresh(), 1500)
    return () => clearInterval(timer)
  }, [refresh])

  const configureBot = async (payload: Partial<BotSettings>) => {
    const outcome = await call((client) => client.admin.configureBot({ payload }))
    if (outcome.ok) setBot(outcome.value)
    else setError(outcome.message)
  }

  const runBot = async () => {
    const outcome = await call((client) => client.admin.runBot())
    if (outcome.ok) setBot(outcome.value)
    else setError(outcome.message)
  }

  const configureSimulator = async (payload: Partial<SimulatorSettings>) => {
    const outcome = await call((client) => client.admin.configureSimulator({ payload }))
    if (outcome.ok) setSimulator(outcome.value)
    else setError(outcome.message)
  }

  return (
    <div className="flex flex-col gap-5">
      {error !== undefined && <p className="text-sm text-rose-300">{error}</p>}

      <Card
        title="🤖 Bot di rifornimento"
        aside={bot && <Toggle on={bot.settings.enabled} onChange={(enabled) => void configureBot({ enabled })} />}
      >
        {bot === undefined ? <p className="text-sm text-zinc-500">…</p> : (
          <div className="flex flex-col gap-4">
            <Choice
              label="Ogni"
              unit="s"
              value={bot.settings.intervalSeconds}
              options={[5, 10, 30, 60, 120]}
              onChange={(intervalSeconds) => void configureBot({ intervalSeconds })}
            />
            <Choice
              label="Sotto i"
              unit="pezzi"
              value={bot.settings.threshold}
              options={[0, 1, 2, 5, 10]}
              onChange={(threshold) => void configureBot({ threshold })}
            />
            <Choice
              label="Aggiunge"
              unit="pezzi"
              value={bot.settings.batch}
              options={[1, 3, 5, 10, 20]}
              onChange={(batch) => void configureBot({ batch })}
            />
            <div className="flex flex-wrap items-center justify-between gap-3 border-t border-line pt-4 text-sm">
              <span className="text-zinc-400">
                {bot.runs} giri · {bot.restocked} pezzi aggiunti
                {bot.lastRunAt !== undefined && ` · ultimo alle ${DateTime.formatIso(bot.lastRunAt).slice(11, 19)}`}
              </span>
              <Button tone="ghost" onClick={() => void runBot()}>Rifornisci ora</Button>
            </div>
          </div>
        )}
      </Card>

      <Card
        title="🛒 Clienti simulati"
        aside={simulator && (
          <Toggle
            on={simulator.settings.enabled}
            onChange={(enabled) => void configureSimulator({ enabled })}
          />
        )}
      >
        {simulator === undefined ? <p className="text-sm text-zinc-500">…</p> : (
          <div className="flex flex-col gap-4">
            <Choice
              label="Quanti"
              unit="clienti"
              value={simulator.settings.shoppers}
              options={[1, 3, 5, 10, 20]}
              onChange={(shoppers) => void configureSimulator({ shoppers })}
            />
            <Choice
              label="Ritmo"
              unit="azioni al minuto"
              value={simulator.settings.actionsPerMinute}
              options={[12, 30, 60, 120, 300]}
              onChange={(actionsPerMinute) => void configureSimulator({ actionsPerMinute })}
            />
            <div className="border-t border-line pt-4">
              <p className="text-sm text-zinc-400">{simulator.actions} azioni finora</p>
              <ul className="mt-2 flex flex-col gap-1 font-mono text-xs text-zinc-500">
                {simulator.recent.map((line, index) => <li key={`${index}-${line}`}>{line}</li>)}
              </ul>
            </div>
          </div>
        )}
      </Card>
    </div>
  )
}

const Toggle = ({ on, onChange }: { readonly on: boolean; readonly onChange: (on: boolean) => void }) => (
  <button
    type="button"
    role="switch"
    aria-checked={on}
    onClick={() => onChange(!on)}
    className={`rounded-full px-3 py-1 text-sm font-medium ring-1 ${
      on ? "bg-emerald-400/15 text-emerald-300 ring-emerald-400/30" : "bg-white/5 text-zinc-400 ring-white/10"
    }`}
  >
    {on ? "acceso" : "spento"}
  </button>
)

const Choice = ({ label, onChange, options, unit, value }: {
  readonly label: string
  readonly unit: string
  readonly value: number
  readonly options: ReadonlyArray<number>
  readonly onChange: (value: number) => void
}) => (
  <div className="flex flex-wrap items-center gap-2 text-sm">
    <span className="w-20 text-zinc-400">{label}</span>
    {(options.includes(value) ? options : [...options, value].sort((a, b) => a - b)).map((option) => (
      <button
        key={option}
        type="button"
        onClick={() => onChange(option)}
        className={`rounded-lg px-2.5 py-1 font-mono ${
          option === value ? "bg-violet-500 text-white" : "border border-line text-zinc-300 hover:bg-white/5"
        }`}
      >
        {option}
      </button>
    ))}
    <span className="text-zinc-500">{unit}</span>
  </div>
)
