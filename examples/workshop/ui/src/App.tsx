import { type ReactNode, useEffect, useState } from "react"
import { BankScreen } from "./BankScreen.tsx"
import { useSession } from "./client.ts"
import { ControlRoom } from "./ControlRoom.tsx"
import { ControlScreen } from "./ControlScreen.tsx"
import { Join } from "./Join.tsx"
import { joinRoom, useRoom } from "./live.ts"
import { ServerStatus } from "./ServerStatus.tsx"
import { ShopScreen } from "./ShopScreen.tsx"
import { credits } from "./ui.tsx"

/** Whether the page was opened as the control room, for the projector. */
const useHash = () => {
  const [hash, setHash] = useState(() => window.location.hash)
  useEffect(() => {
    const update = () => setHash(window.location.hash)
    window.addEventListener("hashchange", update)
    return () => window.removeEventListener("hashchange", update)
  }, [])
  return hash
}

export const App = () => {
  const current = useSession()
  const hash = useHash()
  const token = current?.token
  // The bank reads the balance; the header keeps it in sight while the shop spends it.
  const [balance, setBalance] = useState<number>()
  const userId = current?.user.id
  useEffect(() => setBalance(undefined), [userId])

  // Being on the page means being in the room: the stream counts who is.
  useEffect(() => token === undefined ? undefined : joinRoom(), [token])

  if (hash === "#sala" && current?.user.role === "admin") return <ControlRoom />

  // The controls are only for the presenter: the server refuses them to anyone else.
  const isAdmin = current?.user.role === "admin"
  const sections = [
    ["banca", "Banca"],
    ["shop", "Shop"],
    ...(isAdmin ? [["regia", "Regia"] as const] : [])
  ] as const

  return (
    <div className="min-h-full">
      <header className="sticky top-0 z-20 border-b border-line bg-ink/85 backdrop-blur-lg">
        <div className="mx-auto flex max-w-xl items-center gap-4 px-4 py-3 lg:max-w-5xl">
          <div className="min-w-0">
            <p className="flex items-center gap-2 font-mono text-xs tracking-widest whitespace-nowrap text-violet-300 uppercase">
              <span>
                <span className="hidden sm:inline">effect-cqrs ·{" "}</span>workshop
              </span>
              {current !== undefined && <LiveBadge />}
            </p>
            {current === undefined
              ? <h1 className="mt-1 text-2xl font-semibold">Benvenuto</h1>
              : (
                <nav aria-label="Sezioni" className="mt-1.5 flex gap-1 text-sm">
                  {sections.map(([id, label]) => (
                    <a
                      key={id}
                      href={`#${id}`}
                      className="rounded-lg px-2.5 py-1 text-zinc-300 ring-1 ring-line hover:bg-white/5 hover:text-white"
                    >
                      {label}
                    </a>
                  ))}
                </nav>
              )}
          </div>
          {current !== undefined && (
            <div className="ml-auto shrink-0 text-right">
              <p className="text-xs text-zinc-500">Saldo</p>
              <p className="font-semibold tabular-nums">{balance === undefined ? "…" : credits(balance)}</p>
            </div>
          )}
        </div>
      </header>

      <main className="mx-auto flex max-w-xl flex-col gap-10 px-4 py-6 lg:max-w-5xl">
        {current === undefined ? <Join /> : (
          <>
            <div className="grid items-start gap-10 lg:grid-cols-2 lg:gap-6">
              <Section id="banca" title="Banca" lead="Il tuo conto: ricariche, bonifici e movimenti.">
                <BankScreen key={current.user.id} session={current} onBalance={setBalance} />
              </Section>
              <Section
                id="shop"
                title="Shop"
                lead="Compra o prenota: l'ordine avanza da solo, un evento alla volta, e lo vedi qui sotto."
              >
                <ShopScreen key={current.user.id} session={current} />
              </Section>
            </div>
            {isAdmin && (
              <Section
                id="regia"
                title="Regia"
                lead="Solo per te che presenti: il bot che rifornisce lo shop e i clienti simulati."
              >
                <ControlScreen />
              </Section>
            )}
          </>
        )}
        <ServerStatus />
      </main>
    </div>
  )
}

const Section = ({ id, title, lead, children }: {
  readonly id: string
  readonly title: string
  readonly lead: string
  readonly children: ReactNode
}) => (
  <section id={id} className="scroll-mt-28">
    <h2 className="text-2xl font-semibold">{title}</h2>
    <p className="mt-1 mb-4 text-sm text-zinc-400">{lead}</p>
    {children}
  </section>
)

const LiveBadge = () => {
  const { presence, status } = useRoom()
  const [label, dot] = status === "live"
    ? [`in diretta · ${presence?.online.length ?? 0} in sala`, "bg-emerald-400"]
    : status === "connecting"
    ? ["collego…", "bg-amber-400 animate-pulse"]
    : ["offline", "bg-rose-400"]
  return (
    <span className="flex items-center gap-1.5 normal-case tracking-normal text-zinc-400">
      <span className={`size-1.5 rounded-full ${dot}`} />
      {label}
    </span>
  )
}
