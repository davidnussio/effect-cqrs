---
"effect-cqrs": patch
---

The tutorial's saga now waits for a payment, for 15 seconds at most. After reserving the stock it asks for the payment, then races a `DurableDeferred`, which the trigger completes on `OrderPaid`, against a 15-second `DurableClock`. A paid order is confirmed. An unpaid one is rejected and its stock released, unless the payment landed just before the deadline. The shop demo has a Pay button with a countdown and shows the new steps in its Saga tab.
