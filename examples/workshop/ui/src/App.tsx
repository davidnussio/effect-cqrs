import { useState } from "react"
import { BankScreen } from "./BankScreen.tsx"
import { useSession } from "./client.ts"
import { ControlScreen } from "./ControlScreen.tsx"
import { Join } from "./Join.tsx"
import { ServerStatus } from "./ServerStatus.tsx"
import { ShopScreen } from "./ShopScreen.tsx"

const allTabs = [["bank", "Banca"], ["shop", "Shop"], ["control", "Regia"]] as const
type Tab = typeof allTabs[number][0]

export const App = () => {
  const current = useSession()
  const [tab, setTab] = useState<Tab>("bank")
  // The controls are only for the presenter: the server refuses them to anyone else.
  const tabs = allTabs.filter(([id]) => id !== "control" || current?.user.role === "admin")
  return (
    <main className="mx-auto flex min-h-full max-w-xl flex-col gap-5 px-4 py-8">
      <header className="flex items-end justify-between gap-4">
        <div>
          <p className="font-mono text-xs tracking-widest text-violet-300 uppercase">effect-cqrs · workshop</p>
          <h1 className="mt-1 text-2xl font-semibold">
            {current === undefined ? "Benvenuto" : tabs.find(([id]) => id === tab)?.[1]}
          </h1>
        </div>
        {current !== undefined && (
          <nav className="flex rounded-xl border border-line p-1 text-sm">
            {tabs.map(([id, label]) => (
              <button
                key={id}
                type="button"
                onClick={() => setTab(id)}
                className={`rounded-lg px-3 py-1.5 ${
                  tab === id ? "bg-violet-500 text-white" : "text-zinc-400 hover:text-zinc-200"
                }`}
              >
                {label}
              </button>
            ))}
          </nav>
        )}
      </header>
      {current === undefined
        ? <Join />
        : tab === "bank"
        ? <BankScreen key={current.user.id} session={current} />
        : tab === "shop"
        ? <ShopScreen key={current.user.id} session={current} />
        : <ControlScreen />}
      <ServerStatus />
    </main>
  )
}
