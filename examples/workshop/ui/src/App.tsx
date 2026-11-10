import { BankScreen } from "./BankScreen.tsx"
import { useSession } from "./client.ts"
import { Join } from "./Join.tsx"
import { ServerStatus } from "./ServerStatus.tsx"

export const App = () => {
  const current = useSession()
  return (
    <main className="mx-auto flex min-h-full max-w-xl flex-col gap-5 px-4 py-8">
      <header>
        <p className="font-mono text-xs tracking-widest text-violet-300 uppercase">effect-cqrs · workshop</p>
        <h1 className="mt-1 text-2xl font-semibold">Banca</h1>
      </header>
      {current === undefined ? <Join /> : <BankScreen key={current.user.id} session={current} />}
      <ServerStatus />
    </main>
  )
}
