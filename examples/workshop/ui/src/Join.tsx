import { type FormEvent, useState } from "react"
import { call, session } from "./client.ts"
import { Button, Card, inputClass } from "./ui.tsx"

/** Choose a nickname and get an account. The presenter adds their code. */
export const Join = () => {
  const [nickname, setNickname] = useState("")
  const [adminCode, setAdminCode] = useState("")
  const [presenter, setPresenter] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    setBusy(true)
    setError(undefined)
    const trimmed = nickname.trim()
    const outcome = await call((client) =>
      client.auth.join({
        payload: presenter && adminCode !== "" ? { nickname: trimmed, adminCode } : { nickname: trimmed }
      })
    )
    setBusy(false)
    if (outcome.ok) session.set(outcome.value)
    else setError(outcome.message)
  }

  return (
    <Card>
      <form onSubmit={submit} className="flex flex-col gap-4">
        <div>
          <h2 className="text-lg font-medium">Entra</h2>
          <p className="mt-1 text-sm text-zinc-400">
            Scegli un nickname: ti apriamo un conto. Il nickname è un dato personale, e sparisce dal sistema quando
            chiudi il conto.
          </p>
        </div>
        <input
          className={inputClass}
          value={nickname}
          onChange={(event) => setNickname(event.target.value)}
          placeholder="Il tuo nickname"
          maxLength={24}
          autoFocus
        />
        {presenter && (
          <input
            className={inputClass}
            value={adminCode}
            onChange={(event) => setAdminCode(event.target.value)}
            placeholder="Codice del presentatore"
            type="password"
          />
        )}
        {error !== undefined && <p className="text-sm text-rose-300">{error}</p>}
        <div className="flex items-center justify-between gap-4">
          <button
            type="button"
            className="text-xs text-zinc-500 hover:text-zinc-300"
            onClick={() => setPresenter(!presenter)}
          >
            {presenter ? "Sono un partecipante" : "Sono il presentatore"}
          </button>
          <Button type="submit" disabled={busy || nickname.trim() === ""}>{busy ? "Apro il conto…" : "Entra"}</Button>
        </div>
      </form>
    </Card>
  )
}
