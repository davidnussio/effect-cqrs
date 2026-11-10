import type { ReactNode } from "react"

export const Card = ({ children, title, aside }: {
  readonly children: ReactNode
  readonly title?: string
  readonly aside?: ReactNode
}) => (
  <section className="rounded-2xl border border-line bg-panel p-5">
    {title !== undefined && (
      <div className="mb-4 flex items-center justify-between gap-4">
        <h2 className="text-base font-medium">{title}</h2>
        {aside}
      </div>
    )}
    {children}
  </section>
)

export const Button = ({ children, onClick, disabled, tone = "primary", type = "button" }: {
  readonly children: ReactNode
  readonly onClick?: () => void
  readonly disabled?: boolean
  readonly tone?: "primary" | "ghost" | "danger"
  readonly type?: "button" | "submit"
}) => {
  const tones = {
    primary: "bg-violet-500 text-white hover:bg-violet-400",
    ghost: "border border-line hover:bg-white/5",
    danger: "bg-rose-500/90 text-white hover:bg-rose-500"
  }
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`rounded-xl px-4 py-2 font-medium transition disabled:cursor-not-allowed disabled:opacity-40 ${
        tones[tone]
      }`}
    >
      {children}
    </button>
  )
}

export const inputClass =
  "w-full rounded-xl border border-line bg-ink px-3 py-2 outline-none focus:border-violet-400 placeholder:text-zinc-600"

/** A colour of its own for each participant, from their id. */
export const colorOf = (id: string) => {
  let hash = 0
  for (const char of id) hash = (hash * 31 + char.charCodeAt(0)) | 0
  return `hsl(${Math.abs(hash) % 360} 70% 65%)`
}

export const Avatar = ({ id, nickname }: { readonly id: string; readonly nickname: string }) => (
  <span
    className="grid size-8 shrink-0 place-items-center rounded-full text-sm font-semibold text-ink"
    style={{ background: colorOf(id) }}
  >
    {nickname.startsWith("[") ? "?" : nickname.slice(0, 1).toUpperCase()}
  </span>
)

export const credits = (amount: number) => `${amount.toLocaleString("it-IT")} crediti`
