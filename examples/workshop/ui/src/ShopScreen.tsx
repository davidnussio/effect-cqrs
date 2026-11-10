import { DateTime } from "effect"
import { useCallback, useEffect, useState } from "react"
import type { Me, Session } from "../../api.ts"
import { type CatalogProduct, maxQuantity, type ShopOrder } from "../../domain/shop.ts"
import { call, type Outcome } from "./client.ts"
import { Button, Card, credits } from "./ui.tsx"

interface Data {
  readonly me: Me
  readonly products: ReadonlyArray<CatalogProduct>
  readonly orders: ReadonlyArray<ShopOrder>
}

const load = async (position?: number): Promise<Outcome<Data>> => {
  const query = position === undefined ? {} : { position }
  const [me, products, orders] = await Promise.all([
    call((client) => client.me.get()),
    call((client) => client.shop.products({ query })),
    call((client) => client.me.orders({ query }))
  ])
  if (!me.ok) return me
  if (!products.ok) return products
  if (!orders.ok) return orders
  return { ok: true, value: { me: me.value, products: products.value, orders: orders.value } }
}

const statusLabels: Record<ShopOrder["status"], [string, string]> = {
  none: ["—", "text-zinc-400"],
  placed: ["in pagamento", "bg-zinc-500/15 text-zinc-300"],
  paid: ["pagato", "bg-sky-400/15 text-sky-300"],
  backordered: ["in coda", "bg-amber-400/15 text-amber-300"],
  fulfilled: ["consegnato", "bg-emerald-400/15 text-emerald-300"],
  cancelled: ["annullato", "bg-zinc-500/15 text-zinc-400"],
  rejected: ["rifiutato", "bg-rose-400/15 text-rose-300"]
}

export const ShopScreen = ({ session: current }: { readonly session: Session }) => {
  const [data, setData] = useState<Data>()
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState<{ readonly tone: "ok" | "error"; readonly text: string }>()
  const [quantities, setQuantities] = useState<Readonly<Record<string, number>>>({})
  const isAdmin = current.user.role === "admin"

  const refresh = useCallback(async (position?: number) => {
    const outcome = await load(position)
    if (outcome.ok) setData(outcome.value)
    else setNotice({ tone: "error", text: outcome.message })
  }, [])

  // Orders move on their own, through the saga: poll often enough to watch them.
  useEffect(() => {
    void refresh()
    const timer = setInterval(() => void refresh(), 1500)
    return () => clearInterval(timer)
  }, [refresh])

  const run = async (command: () => Promise<Outcome<{ readonly position: number }>>, success: string) => {
    setBusy(true)
    const outcome = await command()
    setBusy(false)
    setNotice(outcome.ok ? { tone: "ok", text: success } : { tone: "error", text: outcome.message })
    if (outcome.ok) await refresh(outcome.value.position)
  }

  const productOf = (id: string) => data?.products.find((product) => product.id === id)

  return (
    <div className="flex flex-col gap-5">
      <Card
        title="Catalogo"
        aside={
          <span className="text-sm text-zinc-400">
            Saldo <span className="font-mono text-zinc-200">{data === undefined ? "…" : credits(data.me.balance)}</span>
          </span>
        }
      >
        <ul className="flex flex-col">
          {data?.products.map((product) => {
            const quantity = quantities[product.id] ?? 1
            const inStock = product.queue.length === 0 && product.available >= quantity
            const waiting = product.queue.reduce((sum, entry) => sum + entry.quantity, 0)
            return (
              <li
                key={product.id}
                className="flex flex-wrap items-center gap-3 border-t border-line py-3 first:border-0"
              >
                <span className="text-2xl">{product.emoji}</span>
                <div className="min-w-0 flex-1">
                  <p className="font-medium">{product.name}</p>
                  <p className="text-xs text-zinc-500">
                    {credits(product.price)} · {product.available > 0
                      ? <span className="text-emerald-300">{product.available} disponibili</span>
                      : <span className="text-rose-300">esaurito</span>}
                    {product.queue.length > 0 && (
                      <span className="text-amber-300">
                        {" "}· {product.queue.length} in coda ({waiting} pezzi)
                      </span>
                    )}
                  </p>
                </div>
                <select
                  aria-label={`Quanti ${product.name}`}
                  className="rounded-lg border border-line bg-ink px-2 py-1.5 text-sm"
                  value={quantity}
                  onChange={(event) => setQuantities({ ...quantities, [product.id]: Number(event.target.value) })}
                >
                  {Array.from({ length: maxQuantity }, (_, i) => i + 1).map((n) => (
                    <option key={n} value={n}>{n}</option>
                  ))}
                </select>
                <Button
                  tone={inStock ? "primary" : "ghost"}
                  disabled={busy}
                  onClick={() =>
                    void run(
                      () =>
                        call((client) =>
                          client.shop.order({ payload: { productId: product.id, quantity, backorder: !inStock } })
                        ),
                      inStock
                        ? `Ordine di ${quantity} ${product.name} inviato`
                        : `Ordine di ${quantity} ${product.name} inviato: aspetterai il rifornimento`
                    )}
                >
                  {inStock ? "Compra" : "Prenota"}
                </Button>
                {isAdmin && (
                  <Button
                    tone="ghost"
                    disabled={busy}
                    onClick={() =>
                      void run(
                        () =>
                          call((client) =>
                            client.admin.restock({ params: { productId: product.id }, payload: { quantity: 5 } })
                          ),
                        `Riforniti 5 ${product.name}`
                      )}
                  >
                    +5
                  </Button>
                )}
              </li>
            )
          })}
        </ul>
        {notice !== undefined && (
          <p className={`mt-3 text-sm ${notice.tone === "ok" ? "text-emerald-300" : "text-rose-300"}`}>{notice.text}</p>
        )}
        <p className="mt-3 text-xs text-zinc-500">
          "Prenota" ti addebita subito e ti mette in coda: quando arriva merce la ricevi in ordine di arrivo. Finché sei
          in coda puoi annullare e riavere i soldi.
        </p>
      </Card>

      <Card title="I miei ordini">
        {data === undefined || data.orders.length === 0
          ? <p className="text-sm text-zinc-500">Nessun ordine, per ora.</p>
          : (
            <ul className="flex flex-col">
              {data.orders.map((order) => {
                const product = productOf(order.productId)
                const [label, tone] = statusLabels[order.status]
                const place = product?.queue.findIndex((entry) => entry.orderId === order.id) ?? -1
                return (
                  <li
                    key={order.id}
                    className="flex flex-wrap items-center gap-3 border-t border-line py-2 first:border-0"
                  >
                    <span className="w-12 shrink-0 font-mono text-xs text-zinc-500">
                      {DateTime.formatIso(order.placedAt).slice(11, 16)}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-sm">
                      {order.quantity} × {product?.emoji} {product?.name ?? order.productId}
                      <span className="text-zinc-500">· {credits(order.total)}</span>
                    </span>
                    <span className={`rounded-full px-2 py-0.5 text-xs ${tone}`}>
                      {label}
                      {order.status === "backordered" && place >= 0 && ` · ${place + 1}°`}
                      {order.reason !== undefined && `: ${order.reason}`}
                    </span>
                    {order.status === "backordered" && (
                      <Button
                        tone="ghost"
                        disabled={busy}
                        onClick={() =>
                          void run(
                            () => call((client) => client.me.cancelOrder({ params: { orderId: order.id } })),
                            "Ordine annullato: il rimborso arriva tra un attimo"
                          )}
                      >
                        Annulla
                      </Button>
                    )}
                  </li>
                )
              })}
            </ul>
          )}
      </Card>
    </div>
  )
}
