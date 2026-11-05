import { AnimatePresence, motion } from "motion/react"
import { useMemo, useState } from "react"
import type { EventEntry } from "../devstore.ts"
import { eventColor, useSnapshot } from "../hooks.ts"
import { Pill, SectionTitle } from "./ui.tsx"

const time = (millis: number) => {
  const date = new Date(millis)
  return `${date.toLocaleTimeString("en-GB")}.${String(date.getMilliseconds()).padStart(3, "0")}`
}

export const EventsTab = ({ travel }: { travel: number | undefined }) => {
  const events = useSnapshot((s) => s.events)
  const [stream, setStream] = useState<string | undefined>(undefined)
  const [open, setOpen] = useState<number | undefined>(undefined)

  const streams = useMemo(() => {
    const byStream = new Map<string, Array<EventEntry>>()
    for (const event of events) byStream.set(event.aggregateId, [...(byStream.get(event.aggregateId) ?? []), event])
    return Array.from(byStream)
  }, [events])

  const visible = useMemo(
    () => events.filter((event) => stream === undefined || event.aggregateId === stream).slice(-150).reverse(),
    [events, stream]
  )

  return (
    <div className="space-y-5">
      <div>
        <SectionTitle right={<span className="font-mono text-[10px] text-white/35">one stream per aggregate id</span>}>
          Streams
        </SectionTitle>
        <div className="glass space-y-1 rounded-2xl p-3">
          {streams.map(([id, own]) => (
            <button
              key={id}
              onClick={() => setStream(stream === id ? undefined : id)}
              className={`flex w-full items-center gap-3 rounded-lg px-2 py-1 text-left transition ${
                stream === id ? "bg-white/[0.07]" : "hover:bg-white/[0.04]"
              }`}
            >
              <span className="w-24 shrink-0 truncate font-mono text-[10px] text-white/60">account/{id}</span>
              <div className="flex flex-1 flex-wrap items-center gap-[3px]">
                {own.map((event) => (
                  <motion.span
                    key={event.position}
                    title={`v${event.version} ${event.type} (#${event.position})`}
                    initial={{ scale: 0, opacity: 0 }}
                    animate={{
                      scale: 1,
                      opacity: travel !== undefined && event.position > travel ? 0.15 : 1
                    }}
                    transition={{ type: "spring", stiffness: 500, damping: 20 }}
                    className="h-3 w-3 rounded-[3px]"
                    style={{ background: eventColor[event.type] }}
                  />
                ))}
              </div>
              <span className="font-mono text-[10px] text-white/35">v{own.length}</span>
            </button>
          ))}
          {streams.length === 0 && <div className="py-3 text-center text-xs text-white/30">Empty store</div>}
        </div>
        <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1">
          {Object.entries(eventColor).map(([type, color]) => (
            <span key={type} className="flex items-center gap-1 font-mono text-[9.5px] text-white/45">
              <span className="h-2 w-2 rounded-sm" style={{ background: color }} />
              {type}
            </span>
          ))}
        </div>
      </div>

      <div>
        <SectionTitle
          right={stream && (
            <button onClick={() => setStream(undefined)} className="font-mono text-[10px] text-violet-300">
              account/{stream} ✕
            </button>
          )}
        >
          Log · append only
        </SectionTitle>
        <div className="space-y-1">
          <AnimatePresence initial={false}>
            {visible.map((event) => (
              <motion.div
                key={event.position}
                layout="position"
                initial={{ opacity: 0, y: -14, backgroundColor: `${eventColor[event.type]}33` }}
                animate={{
                  opacity: travel !== undefined && event.position > travel ? 0.3 : 1,
                  y: 0,
                  backgroundColor: travel === event.position ? "rgba(56,189,248,0.14)" : "rgba(255,255,255,0.02)"
                }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.35 }}
                onClick={() => setOpen(open === event.position ? undefined : event.position)}
                className="cursor-pointer rounded-lg px-3 py-1.5"
                style={{ boxShadow: `inset 2px 0 0 ${eventColor[event.type]}` }}
              >
                <div className="flex items-center gap-2 font-mono text-[10.5px]">
                  <span className="w-8 text-white/35">#{event.position}</span>
                  <Pill color={eventColor[event.type]!}>{event.type}</Pill>
                  <span className="text-white/70">{event.aggregateId}</span>
                  <span className="text-white/30">v{event.version}</span>
                  <span className="ml-auto text-white/25">{time(event.timestamp)}</span>
                </div>
                <AnimatePresence>
                  {open === event.position && (
                    <motion.pre
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: "auto", opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      className="mt-1.5 overflow-hidden rounded-md bg-black/40 p-2 font-mono text-[10px] leading-relaxed text-emerald-100/80"
                    >
                      {JSON.stringify(
                        {
                          position: event.position,
                          aggregateName: "account",
                          aggregateId: event.aggregateId,
                          version: event.version,
                          type: event.type,
                          timestamp: new Date(event.timestamp).toISOString(),
                          payload: event.payload
                        },
                        null,
                        2
                      )}
                    </motion.pre>
                  )}
                </AnimatePresence>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      </div>
    </div>
  )
}
