import { DateTime } from "effect"
import { useEffect, useState } from "react"
import { apiUrl, checkHealth, type HealthResult } from "./client.ts"

type Status = { readonly _tag: "Waiting"; readonly slow: boolean } | HealthResult

const useHealth = (intervalMillis: number): Status => {
  const [status, setStatus] = useState<Status>({ _tag: "Waiting", slow: false })
  useEffect(() => {
    let active = true
    let timer: ReturnType<typeof setTimeout> | undefined
    const poll = () => {
      // Say the server is waking up if it hasn't answered after a few seconds.
      const slow = setTimeout(() => {
        if (active) setStatus((current) => current._tag === "Ok" ? current : { _tag: "Waiting", slow: true })
      }, 3000)
      void checkHealth().then((result) => {
        clearTimeout(slow)
        if (!active) return
        setStatus(result)
        timer = setTimeout(poll, intervalMillis)
      })
    }
    poll()
    return () => {
      active = false
      clearTimeout(timer)
    }
  }, [intervalMillis])
  return status
}

const tone = (status: Status) =>
  status._tag === "Ok"
    ? ["online", "bg-emerald-400"]
    : status._tag === "Failed"
    ? ["irraggiungibile", "bg-rose-400"]
    : status.slow
    ? ["si sta svegliando…", "bg-amber-400 animate-pulse"]
    : ["controllo…", "bg-zinc-500"]

/** A line at the bottom of the page: whether the server answers, and links to its docs. */
export const ServerStatus = () => {
  const status = useHealth(10_000)
  const [label, dot] = tone(status)
  return (
    <footer className="flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-line pt-4 text-xs text-zinc-500">
      <span className="flex items-center gap-2">
        <span className={`size-2 rounded-full ${dot}`} />
        server {label}
      </span>
      {status._tag === "Ok" && (
        <span className="font-mono">
          {status.health.store} · posizione {status.health.position} · da{" "}
          {DateTime.formatIso(status.health.startedAt).slice(11, 16)} · {Math.round(status.millis)} ms
          {status.health.commit !== undefined && ` · ${status.health.commit.slice(0, 7)}`}
        </span>
      )}
      {status._tag === "Failed" && <span className="font-mono">{status.message}</span>}
      <span className="ml-auto flex gap-3">
        <a className="hover:text-zinc-300" href={`${apiUrl}/docs`} target="_blank" rel="noreferrer">API</a>
        <a className="hover:text-zinc-300" href={`${apiUrl}/openapi.json`} target="_blank" rel="noreferrer">
          openapi.json
        </a>
      </span>
    </footer>
  )
}
