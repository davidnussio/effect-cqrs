import { DateTime } from "effect"
import { type FormEvent, useCallback, useEffect, useState } from "react"
import type { Me, Session } from "../../api.ts"
import { maxDeposit } from "../../domain/account.ts"
import type { LedgerEntry, Participant } from "../../domain/bank.ts"
import { call, type Outcome, read, session } from "./client.ts"
import { useLiveRefresh, useRoom } from "./live.ts"
import { Avatar, Button, Card, credits, inputClass } from "./ui.tsx"

interface Data {
  readonly me: Me
  readonly ledger: ReadonlyArray<LedgerEntry>
  readonly participants: ReadonlyArray<Participant>
}

/** Reads the account, its movements and the participants, waiting for `position` when given. */
const load = async (position?: number): Promise<Outcome<Data>> => {
  const [me, ledger, participants] = await Promise.all([
    call((client) => client.me.get()),
    read((client, headers) => client.me.ledger({ headers }), position),
    read((client, headers) => client.bank.participants({ headers }), position)
  ])
  if (!me.ok) return me
  if (!ledger.ok) return ledger
  if (!participants.ok) return participants
  return { ok: true, value: { me: me.value, ledger: ledger.value, participants: participants.value } }
}

const labels: Record<string, string> = {
  MoneyDeposited: "Ricarica",
  MoneyWithdrawn: "Saldo pagato alla chiusura",
  PurchasePaid: "Acquisto nello shop",
  PurchaseRefunded: "Rimborso di un acquisto",
  TransferSent: "Bonifico a",
  TransferReceived: "Bonifico da",
  TransferRefunded: "Rimborso del bonifico a"
}

export const BankScreen = ({ onBalance, session: current }: {
  readonly session: Session
  /** Tells the page the balance, which it shows in the header. */
  readonly onBalance: (balance: number) => void
}) => {
  const [data, setData] = useState<Data>()
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState<{ readonly tone: "ok" | "error"; readonly text: string }>()

  const refresh = useCallback(async (position?: number) => {
    const outcome = await load(position)
    if (outcome.ok) setData(outcome.value)
    else setNotice({ tone: "error", text: outcome.message })
  }, [])

  // Reads again when the log moves, as the live stream reports it.
  useLiveRefresh(refresh)
  const balance = data?.me.balance
  useEffect(() => {
    if (balance !== undefined) onBalance(balance)
  }, [balance, onBalance])
  const { presence } = useRoom()
  const online = new Set(presence?.online.map((person) => person.id))

  const run = async <A extends { readonly position: number }>(
    command: () => Promise<Outcome<A>>,
    success: (value: A) => string
  ) => {
    setBusy(true)
    const outcome = await command()
    setBusy(false)
    if (outcome.ok) {
      setNotice({ tone: "ok", text: success(outcome.value) })
      await refresh(outcome.value.position)
    } else {
      setNotice({ tone: "error", text: outcome.message })
    }
    return outcome.ok
  }

  const deposit = (amount: number) =>
    run(() => call((client) => client.me.deposit({ payload: { amount } })), () => `Caricati ${credits(amount)}`)

  const nicknameOf = (id: string | undefined) =>
    data?.participants.find((participant) => participant.id === id)?.nickname ?? "?"

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <div className="flex items-center gap-3">
          <Avatar id={current.user.id} nickname={current.user.nickname} />
          <div className="min-w-0">
            <p className="truncate font-medium">
              {data?.me.nickname ?? current.user.nickname}
              {current.user.role === "admin" && (
                <span className="ml-2 rounded-full bg-violet-400/15 px-2 py-0.5 text-xs text-violet-300">
                  presentatore
                </span>
              )}
            </p>
            <p className="font-mono text-xs text-zinc-500">{current.user.id.slice(0, 8)}</p>
          </div>
        </div>
        <p className="mt-5 text-sm text-zinc-400">Saldo</p>
        <p className="text-4xl font-semibold tabular-nums">{data === undefined ? "…" : credits(data.me.balance)}</p>
        <div className="mt-5 flex flex-wrap gap-2">
          {[10, 50, maxDeposit].map((amount) => (
            <Button
              key={amount}
              tone="ghost"
              disabled={busy}
              onClick={() => void deposit(amount)}
            >
              + {amount}
            </Button>
          ))}
        </div>
        {notice !== undefined && (
          <p className={`mt-4 text-sm ${notice.tone === "ok" ? "text-emerald-300" : "text-rose-300"}`}>
            {notice.text}
          </p>
        )}
      </Card>

      {data !== undefined && (
        <TransferForm
          me={data.me}
          participants={data.participants}
          busy={busy}
          onSend={(to, amount) =>
            run(
              () => call((client) => client.me.transfer({ payload: { to, amount } })),
              () => `Inviati ${credits(amount)} a ${nicknameOf(to)}`
            )}
        />
      )}

      <Card title="Movimenti">
        {data === undefined || data.ledger.length === 0
          ? <p className="text-sm text-zinc-500">Nessun movimento: carica dei soldi per iniziare.</p>
          : (
            <ul className="flex flex-col">
              {data.ledger.map((entry) => (
                <li key={entry.position} className="flex items-baseline gap-3 border-t border-line py-2 first:border-0">
                  <span className="w-12 shrink-0 font-mono text-xs text-zinc-500">
                    {DateTime.formatIso(entry.timestamp).slice(11, 16)}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-sm">
                    {labels[entry.type] ?? entry.type}
                    {entry.counterparty !== undefined && ` ${nicknameOf(entry.counterparty)}`}
                  </span>
                  <span
                    className={`font-mono text-sm tabular-nums ${
                      entry.amount >= 0 ? "text-emerald-300" : "text-rose-300"
                    }`}
                  >
                    {entry.amount >= 0 ? "+" : ""}
                    {entry.amount}
                  </span>
                </li>
              ))}
            </ul>
          )}
      </Card>

      <Card
        title="Partecipanti"
        aside={<span className="text-xs text-zinc-500">{data?.participants.length ?? 0}</span>}
      >
        <ul className="flex flex-col gap-2">
          {data?.participants.map((participant) => (
            <li
              key={participant.id}
              className={`flex items-center gap-3 ${participant.status === "closed" ? "opacity-40" : ""}`}
            >
              <span className="relative">
                <Avatar id={participant.id} nickname={participant.nickname} />
                {online.has(participant.id) && (
                  <span
                    title="in sala"
                    className="absolute -right-0.5 -bottom-0.5 size-3 rounded-full border-2 border-panel bg-emerald-400"
                  />
                )}
              </span>
              <span className="min-w-0 flex-1 truncate text-sm">
                {participant.nickname}
                {participant.id === current.user.id && <span className="text-zinc-500">(tu)</span>}
              </span>
              <span className="font-mono text-sm tabular-nums text-zinc-400">
                {participant.status === "closed" ? "chiuso" : participant.balance}
              </span>
            </li>
          ))}
        </ul>
      </Card>

      <CloseAccount balance={data?.me.balance ?? 0} busy={busy} onClose={() => closeAccount(setBusy, setNotice)} />
    </div>
  )
}

const TransferForm = ({ busy, me, onSend, participants }: {
  readonly me: Me
  readonly participants: ReadonlyArray<Participant>
  readonly busy: boolean
  readonly onSend: (to: string, amount: number) => Promise<boolean>
}) => {
  const others = participants.filter((participant) => participant.status === "open" && participant.id !== me.id)
  const [to, setTo] = useState("")
  const [amount, setAmount] = useState("")
  const recipient = others.some((participant) => participant.id === to) ? to : ""
  const value = Number(amount)
  const valid = recipient !== "" && Number.isInteger(value) && value > 0

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (valid && await onSend(recipient, value)) setAmount("")
  }

  return (
    <Card title="Bonifico">
      {others.length === 0
        ? <p className="text-sm text-zinc-500">Nessun altro partecipante, per ora.</p>
        : (
          <form onSubmit={submit} className="flex flex-col gap-3 sm:flex-row">
            <select className={inputClass} value={recipient} onChange={(event) => setTo(event.target.value)}>
              <option value="">A chi?</option>
              {others.map((participant) => (
                <option key={participant.id} value={participant.id}>{participant.nickname}</option>
              ))}
            </select>
            <input
              className={`${inputClass} sm:w-32`}
              inputMode="numeric"
              placeholder="Quanto"
              value={amount}
              onChange={(event) => setAmount(event.target.value.replace(/\D/g, ""))}
            />
            <Button type="submit" disabled={busy || !valid}>Invia</Button>
          </form>
        )}
    </Card>
  )
}

const closeAccount = async (
  setBusy: (busy: boolean) => void,
  setNotice: (notice: { readonly tone: "ok" | "error"; readonly text: string }) => void
) => {
  setBusy(true)
  const outcome = await call((client) => client.me.close())
  setBusy(false)
  if (outcome.ok) session.set(undefined)
  else setNotice({ tone: "error", text: outcome.message })
}

const CloseAccount = ({ balance, busy, onClose }: {
  readonly balance: number
  readonly busy: boolean
  readonly onClose: () => void
}) => {
  const [confirming, setConfirming] = useState(false)
  return (
    <Card>
      {confirming
        ? (
          <div className="flex flex-col gap-4">
            <p className="text-sm text-zinc-300">
              Ti paghiamo il saldo di {credits(balance)}{" "}
              e chiudiamo il conto. Poi distruggiamo la chiave del tuo nickname: gli eventi restano nel log, con importi
              e date, ma il nickname si leggerà <span className="font-mono">[forgotten]</span>. Non si torna indietro.
            </p>
            <div className="flex gap-2">
              <Button tone="danger" disabled={busy} onClick={onClose}>Chiudi e dimentica</Button>
              <Button tone="ghost" onClick={() => setConfirming(false)}>Annulla</Button>
            </div>
          </div>
        )
        : (
          // No "log out": the token lives only in this browser, so leaving would lose the account.
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-sm text-zinc-500">Il conto resta legato a questo browser.</p>
            <Button tone="ghost" onClick={() => setConfirming(true)}>Chiudi il conto</Button>
          </div>
        )}
    </Card>
  )
}
