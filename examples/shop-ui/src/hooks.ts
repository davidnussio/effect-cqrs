import { useEffect, useState, useSyncExternalStore } from "react"
import { devstore, type SpanEntry, type Snapshot } from "./devstore.ts"

export const useSnapshot = <A>(select: (snapshot: Snapshot) => A): A =>
  useSyncExternalStore(devstore.subscribe, () => select(devstore.get()))

/** The current time, every frame while `active`, so running spans keep growing. */
export const useNow = (active: boolean): number => {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!active) return
    let frame = requestAnimationFrame(function tick() {
      setNow(Date.now())
      frame = requestAnimationFrame(tick)
    })
    return () => cancelAnimationFrame(frame)
  }, [active])
  return now
}

export const statusColor = {
  placed: "#fbbf24",
  confirmed: "#34d399",
  rejected: "#fb7185"
} as const

export const engineColor = {
  running: "#38bdf8",
  suspended: "#fbbf24",
  succeeded: "#34d399",
  failed: "#fb7185"
} as const

export const spanColor = (span: SpanEntry): string => {
  if (span.status === "error") return "#fb7185"
  if (span.name.startsWith("ui.") || span.name.startsWith("seed.")) return "#a78bfa"
  if (span.name.startsWith("Fulfil.")) return "#38bdf8"
  if (span.name === "demo.latency") return "#475569"
  if (span.name.startsWith("EventStore.")) return "#34d399"
  if (span.name.includes(".")) return "#f472b6"
  // Activities have the plain names their saga gave them.
  return "#fbbf24"
}

export const short = (id: string | null | undefined, length = 8) =>
  id === null || id === undefined ? "—" : id.length > length ? `${id.slice(0, length)}…` : id

export const ms = (value: number) =>
  value < 1 ? `${Math.round(value * 1000)}µs` : value < 1000 ? `${value.toFixed(1)}ms` : `${(value / 1000).toFixed(2)}s`
