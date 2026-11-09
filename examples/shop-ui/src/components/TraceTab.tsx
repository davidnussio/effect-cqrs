import { AnimatePresence, motion } from "motion/react"
import { useState } from "react"
import { Code, Pill } from "../../../bank-ui/src/components/ui.tsx"
import type { SpanEntry } from "../devstore.ts"
import { ms, spanColor, useNow, useSnapshot } from "../hooks.ts"

interface Trace {
  readonly traceId: string
  readonly root: SpanEntry
  /** Depth-first, each with its depth under the root. */
  readonly rows: ReadonlyArray<{ readonly span: SpanEntry; readonly depth: number }>
  readonly start: number
  readonly end: number | undefined
  readonly failed: boolean
}

const maxTraces = 30

const toTraces = (spans: ReadonlyArray<SpanEntry>): ReadonlyArray<Trace> => {
  const byTrace = new Map<string, Array<SpanEntry>>()
  for (const span of spans) {
    const group = byTrace.get(span.traceId)
    if (group === undefined) byTrace.set(span.traceId, [span])
    else group.push(span)
  }
  const traces: Array<Trace> = []
  for (const [traceId, group] of byTrace) {
    const ids = new Set(group.map((span) => span.spanId))
    // A span whose parent isn't recorded, such as a workflow run in the engine's fiber, starts a tree.
    const roots = group.filter((span) => span.parentId === undefined || !ids.has(span.parentId))
    const children = new Map<string, Array<SpanEntry>>()
    for (const span of group) {
      if (span.parentId === undefined || !ids.has(span.parentId)) continue
      const list = children.get(span.parentId)
      if (list === undefined) children.set(span.parentId, [span])
      else list.push(span)
    }
    const rows: Array<{ span: SpanEntry; depth: number }> = []
    const walk = (span: SpanEntry, depth: number) => {
      rows.push({ span, depth })
      for (const child of (children.get(span.spanId) ?? []).sort((a, b) => a.start - b.start)) walk(child, depth + 1)
    }
    for (const root of roots.sort((a, b) => a.start - b.start)) walk(root, 0)
    const running = group.some((span) => span.end === undefined)
    traces.push({
      traceId,
      root: roots[0]!,
      rows,
      start: Math.min(...group.map((span) => span.start)),
      end: running ? undefined : Math.max(...group.map((span) => span.end!)),
      failed: group.some((span) => span.status === "error")
    })
  }
  return traces.sort((a, b) => b.start - a.start).slice(0, maxTraces)
}

export const TraceTab = () => {
  const spans = useSnapshot((s) => s.spans)
  const traces = toTraces(spans)
  const now = useNow(spans.some((span) => span.end === undefined))

  return (
    <div className="space-y-3">
      <p className="text-xs leading-relaxed text-white/50">
        Every span Effect opens, from <Code>Tracer.make</Code>: the commands' own spans, <Code>EventStore.append</Code>,
        the workflow's <Code>Fulfil.execute</Code> and one span per activity. In production an OTLP exporter sends the
        same spans on. Grey bars are the demo's latency.
      </p>
      {traces.length === 0 && <div className="py-6 text-center text-xs text-white/25">no spans yet</div>}
      <AnimatePresence initial={false}>
        {traces.map((trace) => <TraceCard key={trace.traceId} trace={trace} now={now} />)}
      </AnimatePresence>
    </div>
  )
}

const TraceCard = ({ trace, now }: { trace: Trace; now: number }) => {
  const [open, setOpen] = useState<string | undefined>(undefined)
  const end = trace.end ?? Math.max(now, trace.start + 1)
  const total = Math.max(end - trace.start, 0.001)

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0 }}
      className="glass overflow-hidden rounded-xl"
    >
      <div className="flex items-center gap-2 border-b border-white/[0.05] px-3 py-2">
        <span className="h-2 w-2 rounded-full" style={{ background: trace.failed ? "#fb7185" : spanColor(trace.root) }} />
        <span className="font-mono text-[11.5px] font-semibold text-white/90">{trace.root.name}</span>
        <span className="font-mono text-[10px] text-white/30">{trace.rows.length} spans</span>
        <span className="ml-auto font-mono text-[10.5px] tabular-nums text-white/60">
          {trace.end === undefined
            ? (
              <motion.span animate={{ opacity: [1, 0.4, 1] }} transition={{ duration: 1, repeat: Infinity }}>
                {ms(total)} running
              </motion.span>
            )
            : ms(total)}
        </span>
      </div>
      <div className="px-3 py-1.5">
        {trace.rows.map(({ depth, span }) => {
          const spanEnd = span.end ?? end
          const left = ((span.start - trace.start) / total) * 100
          const width = Math.max(((spanEnd - span.start) / total) * 100, 0.6)
          const color = spanColor(span)
          return (
            <div key={span.spanId}>
              <button
                onClick={() => setOpen(open === span.spanId ? undefined : span.spanId)}
                className="grid w-full grid-cols-[minmax(0,42%)_1fr_56px] items-center gap-2 rounded px-1 py-[3px] text-left hover:bg-white/[0.03]"
              >
                <span className="truncate font-mono text-[10.5px]" style={{ paddingLeft: depth * 12 }}>
                  <span className="text-white/25">{depth > 0 ? "└ " : ""}</span>
                  <span className={span.name === "demo.latency" ? "text-white/35" : "text-white/80"}>{span.name}</span>
                  {span.error !== undefined && <span className="ml-1.5 text-rose-300">{span.error}</span>}
                </span>
                <span className="relative h-2.5 rounded-full bg-white/[0.03]">
                  <motion.span
                    className="absolute top-0 h-full rounded-full"
                    style={{ background: color, opacity: span.end === undefined ? 0.6 : 0.9 }}
                    initial={false}
                    animate={{ left: `${left}%`, width: `${Math.min(width, 100 - left)}%` }}
                    transition={{ type: "spring", stiffness: 260, damping: 30 }}
                  />
                </span>
                <span className="text-right font-mono text-[10px] tabular-nums text-white/45">
                  {ms(spanEnd - span.start)}
                </span>
              </button>
              <AnimatePresence>
                {open === span.spanId && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    className="overflow-hidden"
                  >
                    <div className="mb-1.5 ml-3 flex flex-wrap gap-1.5 rounded-md bg-black/30 p-2">
                      <Pill color={color}>{span.status}</Pill>
                      {Object.entries(span.attributes).map(([key, value]) => (
                        <span key={key} className="font-mono text-[10px] text-white/55">
                          <span className="text-sky-300/70">{key}</span>={JSON.stringify(value)}
                        </span>
                      ))}
                      {Object.keys(span.attributes).length === 0 && (
                        <span className="font-mono text-[10px] text-white/30">no attributes</span>
                      )}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          )
        })}
      </div>
    </motion.div>
  )
}
