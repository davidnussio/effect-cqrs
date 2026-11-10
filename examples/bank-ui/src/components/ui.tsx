import { motion, useSpring, useTransform } from "motion/react"
import { type ReactNode, useEffect } from "react"

export const AnimatedNumber = ({ value, format }: { value: number; format: (n: number) => string }) => {
  const spring = useSpring(value, { stiffness: 140, damping: 22, mass: 0.6 })
  useEffect(() => spring.set(value), [spring, value])
  const text = useTransform(spring, (n) => format(n))
  return <motion.span className="tabular-nums">{text}</motion.span>
}

export const Pill = ({ color, children, solid = false }: { color: string; children: ReactNode; solid?: boolean }) => (
  <span
    className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 font-mono text-[10px] font-medium uppercase tracking-wider whitespace-nowrap"
    style={solid
      ? { background: color, color: "#0b0b12" }
      : { background: `${color}1f`, color, boxShadow: `inset 0 0 0 1px ${color}40` }}
  >
    {children}
  </span>
)

export const Button = (
  { children, onClick, tone = "default", disabled = false, title }: {
    children: ReactNode
    onClick?: () => void
    tone?: "default" | "primary" | "danger" | "ghost"
    disabled?: boolean
    title?: string
  }
) => {
  const tones = {
    default: "bg-white/[0.06] hover:bg-white/[0.11] text-white/90 ring-1 ring-white/10",
    primary: "bg-violet-500 hover:bg-violet-400 text-white shadow-[0_0_24px_-4px_rgba(139,92,246,0.7)]",
    danger: "bg-rose-500/15 hover:bg-rose-500/25 text-rose-300 ring-1 ring-rose-400/30",
    ghost: "hover:bg-white/[0.06] text-white/60 hover:text-white"
  }
  return (
    <motion.button
      whileTap={{ scale: 0.94 }}
      whileHover={{ y: -1 }}
      disabled={disabled}
      title={title}
      onClick={onClick}
      className={`inline-flex items-center justify-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-medium transition-colors disabled:opacity-40 disabled:pointer-events-none ${
        tones[tone]
      }`}
    >
      {children}
    </motion.button>
  )
}

export const Slider = (
  { label, value, min, max, step = 10, unit = "ms", onChange, color = "#7c5cff", hint }: {
    label: string
    value: number
    min: number
    max: number
    step?: number
    unit?: string
    onChange: (value: number) => void
    color?: string
    hint?: string
  }
) => {
  const pct = ((value - min) / (max - min)) * 100
  return (
    <label className="block">
      <div className="mb-1.5 flex items-baseline justify-between">
        <span className="text-[11px] font-medium text-white/70">{label}</span>
        <span className="font-mono text-[11px] tabular-nums" style={{ color }}>
          {value}
          <span className="text-white/40">{unit}</span>
        </span>
      </div>
      <input
        type="range"
        className="w-full"
        min={min}
        max={max}
        step={step}
        value={value}
        style={{ "--pct": `${pct}%`, "--fill": color } as React.CSSProperties}
        onChange={(event) => onChange(Number(event.target.value))}
      />
      {hint && <div className="mt-1 text-[10px] text-white/35">{hint}</div>}
    </label>
  )
}

export const Toggle = (
  { on, onChange, label, color = "#7c5cff" }: {
    on: boolean
    onChange: (on: boolean) => void
    label: ReactNode
    color?: string
  }
) => (
  <button onClick={() => onChange(!on)} className="group inline-flex items-center gap-2 text-[11px] text-white/75">
    <span
      className="relative h-[18px] w-8 rounded-full transition-colors"
      style={{ background: on ? color : "#26263a" }}
    >
      <motion.span
        layout
        transition={{ type: "spring", stiffness: 600, damping: 32 }}
        className="absolute top-[2px] h-[14px] w-[14px] rounded-full bg-white shadow"
        style={{ left: on ? 16 : 2 }}
      />
    </span>
    {label}
  </button>
)

export const SectionTitle = ({ children, right }: { children: ReactNode; right?: ReactNode }) => (
  <div className="mb-2.5 flex items-center justify-between">
    <h3 className="font-mono text-[10px] font-semibold uppercase tracking-[0.18em] text-white/45">{children}</h3>
    {right}
  </div>
)

export const Code = ({ children }: { children: ReactNode }) => (
  <code className="rounded bg-white/[0.06] px-1 py-px font-mono text-[0.92em] text-violet-200">{children}</code>
)
