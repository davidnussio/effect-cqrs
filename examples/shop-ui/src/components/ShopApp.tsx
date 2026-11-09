import { AnimatePresence, motion } from "motion/react"
import { useEffect, useState } from "react"
import { AnimatedNumber, Button, Code, Pill, SectionTitle } from "../../../bank-ui/src/components/ui.tsx"
import { devstore, type OrderRow } from "../devstore.ts"
import { catalog, customers, engine } from "../engine.ts"
import { engineColor, statusColor, useSnapshot } from "../hooks.ts"

export const ShopApp = () => {
  const orders = useSnapshot((s) => s.orders)
  const list = Object.values(orders).sort((a, b) => b.position - a.position)

  return (
    <section className="scroll-thin min-h-0 overflow-y-auto px-6 py-5">
      <div className="mb-1 font-mono text-[10px] uppercase tracking-[0.2em] text-sky-300/70">Customer app</div>
      <h1 className="text-2xl font-semibold tracking-tight">Fruit shop</h1>
      <p className="mt-1 max-w-xl text-[13px] leading-relaxed text-white/50">
        Shelves and orders are read from the <Code>inventory</Code> and <Code>orders</Code> view models. Placing an
        order records <Code>OrderPlaced</Code>, and the <Code>Fulfil</Code> saga reserves the stock and asks for the
        payment. Pay within 15 seconds and the saga confirms the order. Otherwise it rejects the order and releases the
        stock.
      </p>

      <Customer />

      <div className="mt-5 grid grid-cols-3 gap-3">
        {catalog.map((product) => <ProductCard key={product.sku} {...product} />)}
      </div>

      <Legacy />

      <div className="mt-6">
        <SectionTitle right={<span className="font-mono text-[10px] text-white/35">newest first</span>}>
          Orders
        </SectionTitle>
        {list.length === 0 && (
          <div className="glass rounded-2xl px-4 py-8 text-center text-xs text-white/30">
            No orders yet. Order more than a shelf holds, or let 15 seconds pass without paying, to see the saga reject
            one.
          </div>
        )}
        <div className="space-y-2">
          <AnimatePresence initial={false}>
            {list.map((order) => <OrderCard key={order.id} order={order} />)}
          </AnimatePresence>
        </div>
      </div>
    </section>
  )
}

const Customer = () => {
  const customer = useSnapshot((s) => s.settings.customer)
  return (
    <div className="mt-4 flex items-center gap-2">
      <span className="text-[11px] text-white/45">Ordering as</span>
      {customers.map((name) => (
        <button
          key={name}
          onClick={() => devstore.settings({ customer: name })}
          className={`relative rounded-full px-3 py-1 text-xs font-medium transition-colors ${
            customer === name ? "text-white" : "text-white/45 hover:text-white/80"
          }`}
        >
          {customer === name && (
            <motion.span
              layoutId="customer"
              className="absolute inset-0 rounded-full bg-violet-500/25 ring-1 ring-violet-400/40"
            />
          )}
          <span className="relative">{name}</span>
        </button>
      ))}
      <span className="text-[10px] text-white/30">
        → <Code>Metadata.withMetadata({"{ userId }"})</Code>
      </span>
    </div>
  )
}

const ProductCard = ({ color, label, sku }: { sku: string; label: string; color: string }) => {
  const stock = useSnapshot((s) => s.stock[sku])
  const [quantity, setQuantity] = useState(1)
  const available = stock?.available ?? 0

  return (
    <motion.div
      layout
      className="glass relative overflow-hidden rounded-2xl p-4"
      style={{ boxShadow: `inset 0 1px 0 ${color}30, 0 18px 40px -30px ${color}` }}
    >
      <div
        className="pointer-events-none absolute -top-10 -right-10 h-28 w-28 rounded-full blur-2xl"
        style={{ background: `${color}33` }}
      />
      <div className="flex items-center justify-between">
        <span className="text-sm font-semibold">{label}</span>
        <span className="font-mono text-[10px] text-white/35">stock/{sku}</span>
      </div>
      <div className="mt-2 flex items-baseline gap-2">
        <span className="text-3xl font-semibold" style={{ color }}>
          <AnimatedNumber value={available} format={(n) => Math.round(n).toString()} />
        </span>
        <span className="text-[11px] text-white/40">available</span>
      </div>
      <div className="font-mono text-[10px] text-white/35">{stock?.reserved ?? 0} reserved</div>
      <div className="mt-3 flex items-center gap-1.5">
        <Button tone="ghost" onClick={() => setQuantity((q) => Math.max(1, q - 1))}>−</Button>
        <span className="w-5 text-center font-mono text-sm tabular-nums">{quantity}</span>
        <Button tone="ghost" onClick={() => setQuantity((q) => Math.min(9, q + 1))}>+</Button>
        <div className="ml-auto">
          <Button tone="primary" onClick={() => void engine.placeOrder(sku, quantity)}>Order</Button>
        </div>
      </div>
      <div className="mt-2">
        <Button onClick={() => void engine.restock(sku, 5)}>Restock +5</Button>
      </div>
    </motion.div>
  )
}

const Legacy = () => (
  <div className="glass mt-3 flex items-center gap-3 rounded-2xl px-4 py-3">
    <Pill color="#fbbf24">v1</Pill>
    <div className="min-w-0 flex-1 text-[11.5px] leading-relaxed text-white/55">
      Import an order the shop's first version stored, as <Code>{"{ sku }"}</Code> with no quantity.{" "}
      <Code>EventVersions</Code> upcasts it to one item.
    </div>
    {catalog.map(({ sku }) => (
      <Button key={sku} onClick={() => void engine.importLegacy(sku)}>{sku}</Button>
    ))}
  </div>
)

const OrderCard = ({ order }: { order: OrderRow }) => {
  const product = catalog.find((p) => p.sku === order.sku)
  const engineStatus = useSnapshot((s) => s.engine[order.executionId])
  const legacy = useSnapshot((s) =>
    s.events.some((e) => e.aggregateId === order.id && e.type === "OrderPlaced" && e.schemaVersion === 1)
  )
  const color = statusColor[order.status]

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: -12, scale: 0.98 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      exit={{ opacity: 0 }}
      transition={{ type: "spring", stiffness: 380, damping: 30 }}
      className="glass flex items-center gap-3 rounded-xl px-3.5 py-2.5"
      style={{ boxShadow: `inset 3px 0 0 ${color}` }}
    >
      <div className="w-20 shrink-0 font-mono text-[11px] text-white/70">{order.id}</div>
      <div className="flex items-center gap-1.5 text-[13px]">
        <span className="font-semibold tabular-nums">{order.quantity}×</span>
        <span style={{ color: product?.color }}>{product?.label ?? order.sku}</span>
      </div>
      {legacy && <Pill color="#fbbf24">v1 → qty 1</Pill>}
      <div className="font-mono text-[10px] text-white/35">
        {order.userId === undefined ? "imported" : `by ${order.userId}`}
      </div>
      <div className="ml-auto flex items-center gap-2">
        {engineStatus === "running" && (
          <motion.span
            animate={{ opacity: [1, 0.35, 1] }}
            transition={{ duration: 1, repeat: Infinity }}
            className="font-mono text-[10px]"
            style={{ color: engineColor.running }}
          >
            saga running
          </motion.span>
        )}
        {order.reason !== undefined && <span className="text-[10.5px] text-rose-300/70">{order.reason}</span>}
        {order.status === "awaiting-payment" && order.deadline !== undefined && (
          <>
            <Countdown deadline={order.deadline} />
            <Button tone="primary" onClick={() => void engine.pay(order.id)}>Pay</Button>
          </>
        )}
        <motion.span
          key={order.status}
          initial={{ scale: 0.6, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ type: "spring", stiffness: 500, damping: 22 }}
        >
          <Pill color={color} solid={order.status !== "placed" && order.status !== "awaiting-payment"}>
            {order.status.replace("-", " ")}
          </Pill>
        </motion.span>
      </div>
    </motion.div>
  )
}

/** The time left to pay. When it runs out, the saga's clock fires and the order is rejected. */
const Countdown = ({ deadline }: { deadline: number }) => {
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 250)
    return () => clearInterval(timer)
  }, [])
  const left = Math.max(0, Math.ceil((deadline - now) / 1000))
  return (
    <span
      className={`whitespace-nowrap font-mono text-[10.5px] tabular-nums ${left <= 10 ? "text-rose-300" : "text-sky-300/80"}`}
      title="DurableClock.sleep in the Fulfil saga"
    >
      {Math.floor(left / 60)}:{String(left % 60).padStart(2, "0")} to pay
    </span>
  )
}
