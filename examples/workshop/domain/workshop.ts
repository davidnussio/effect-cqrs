/**
 * The bank and the shop together: their aggregate services, every view model in
 * one layer, and the sagas. One layer of view models means one `ViewModels`
 * service, so `caughtUp` waits for all of them and `status` reports all of them.
 *
 * View models and sagas keep their checkpoints in memory, so they rebuild from the
 * log on every start, which takes moments for a workshop's worth of events. It
 * needs an event store and the keys of personal data.
 */
import { Layer } from "effect"
import * as Checkpoints from "effect-cqrs/Checkpoints"
import * as ViewModel from "effect-cqrs/ViewModel"
import { AccountsLive, BankStores, bankViews, TransferSaga } from "./bank.ts"
import { Chaos, ShopSagas, ShopServices, ShopStores, shopViews } from "./shop.ts"

export const WorkshopLive = Layer.mergeAll(ViewModel.layer([...bankViews, ...shopViews]), TransferSaga, ShopSagas).pipe(
  Layer.provideMerge(ShopServices),
  Layer.provideMerge(AccountsLive),
  Layer.provideMerge(Layer.mergeAll(BankStores, ShopStores, Chaos.layer, Checkpoints.layerMemory))
)
