/**
 * The bank and the shop together: their aggregate services, every view model in
 * one layer, and the sagas. One layer of view models means one `ViewModels`
 * service, so `caughtUp` waits for all of them and `status` reports all of them.
 *
 * View models and sagas keep their checkpoints in memory, so they rebuild from the
 * log on every start, which takes moments for a workshop's worth of events. It
 * needs an event store, the keys of personal data and the `SharedState`.
 *
 * Several replicas can run it on one database. The view models keep their data in
 * memory, so every replica runs all of them, with leases that are always held, and
 * follows the log on its own. The sagas run commands, so they take their leases
 * from the `Leases` in context: with `Leases.layerPostgres` one replica runs them,
 * and when it stops another takes over and starts them again from the log, which
 * joins nothing in its new in-memory engine and so runs each step again. Every step
 * is idempotent, which makes that safe: it is what a restart does too.
 */
import { Layer } from "effect"
import * as Checkpoints from "effect-cqrs/Checkpoints"
import * as Leases from "effect-cqrs/Leases"
import * as ViewModel from "effect-cqrs/ViewModel"
import { AccountsLive, BankStores, bankViews, TransferSaga } from "./bank.ts"
import { Chaos, ShopSagas, ShopServices, ShopStores, shopViews } from "./shop.ts"

export const WorkshopLive = Layer.mergeAll(
  ViewModel.layer([...bankViews, ...shopViews]).pipe(Layer.provide(Leases.layerMemory)),
  TransferSaga,
  ShopSagas
).pipe(
  Layer.provideMerge(ShopServices),
  Layer.provideMerge(AccountsLive),
  Layer.provideMerge(Layer.mergeAll(BankStores, ShopStores, Chaos.layer, Checkpoints.layerMemory))
)
