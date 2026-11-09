import { useSyncExternalStore } from "react"
import { devstore, type Snapshot } from "./devstore.ts"

export { ms } from "../../devtools/tracing.ts"

export const useSnapshot = <A>(select: (snapshot: Snapshot) => A): A =>
  useSyncExternalStore(devstore.subscribe, () => select(devstore.get()))

export const statusColor = {
  placed: "#fbbf24",
  "awaiting-payment": "#38bdf8",
  paid: "#2dd4bf",
  confirmed: "#34d399",
  rejected: "#fb7185"
} as const

export const engineColor = {
  running: "#38bdf8",
  suspended: "#fbbf24",
  succeeded: "#34d399",
  failed: "#fb7185"
} as const

export const short = (id: string | null | undefined, length = 8) =>
  id === null || id === undefined ? "—" : id.length > length ? `${id.slice(0, length)}…` : id
