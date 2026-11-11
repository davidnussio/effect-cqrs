/**
 * The demos on a narrow screen, such as a phone. A wide screen shows the app and its
 * devtools side by side; a narrow one shows one of them at a time, under a switch,
 * and says that the full demo needs more room.
 */
import { AnimatePresence, motion } from "motion/react"
import { useState } from "react"

export type View = "app" | "devtools"

/** The classes that show a pane when it is the chosen view, and always on a wide screen. */
export const pane = (view: View, own: View): string => view === own ? "contents" : "hidden lg:contents"

// The notice stays closed once closed. Storage can be missing or throw, in a private
// window for instance, and then it simply comes back.
const noticeKey = "effect-cqrs-demo:narrow-notice"

const noticeClosed = (): boolean => {
  try {
    return localStorage.getItem(noticeKey) === "closed"
  } catch {
    return false
  }
}

const closeNotice = () => {
  try {
    localStorage.setItem(noticeKey, "closed")
  } catch {
    // Not remembered, then.
  }
}

/** The notice and the switch between the app and the devtools, on narrow screens only. */
export const NarrowBar = (
  { view, onView, app, accent }: {
    view: View
    onView: (view: View) => void
    /** What the app pane is called, such as "Bank". */
    app: string
    /** The color of the chosen view. */
    accent: string
  }
) => {
  const [notice, setNotice] = useState(() => !noticeClosed())

  return (
    <div className="relative z-10 border-b border-white/[0.06] px-3 py-2.5 lg:hidden">
      <AnimatePresence initial={false}>
        {notice && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="overflow-hidden"
          >
            <div className="glass mb-2.5 flex items-start gap-3 rounded-xl px-3 py-2.5">
              <p className="text-[11px] leading-relaxed text-white/60">
                <span className="font-semibold text-white/85">A simplified view.</span>{" "}
                On a wider screen the app and its devtools sit side by side, so you can watch each command go
                through the system while you use it.
              </p>
              <button
                onClick={() => {
                  closeNotice()
                  setNotice(false)
                }}
                aria-label="Close"
                className="-mr-1 shrink-0 rounded-md px-1.5 text-sm leading-none text-white/40 hover:bg-white/[0.06] hover:text-white"
              >
                ×
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
      <div role="tablist" className="grid grid-cols-2 gap-1 rounded-xl bg-white/[0.04] p-1 ring-1 ring-white/[0.06]">
        {([["app", app], ["devtools", "Devtools"]] as const).map(([id, label]) => (
          <button
            key={id}
            role="tab"
            aria-selected={view === id}
            onClick={() => onView(id)}
            className={`relative rounded-lg py-1.5 text-xs font-medium transition-colors ${
              view === id ? "text-white" : "text-white/45"
            }`}
          >
            {view === id && (
              <motion.span
                layoutId="narrow-view"
                className="absolute inset-0 rounded-lg"
                style={{ background: `${accent}33`, boxShadow: `inset 0 0 0 1px ${accent}66` }}
              />
            )}
            <span className="relative">{label}</span>
          </button>
        ))}
      </div>
    </div>
  )
}
