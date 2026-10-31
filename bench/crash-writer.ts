/**
 * Writes to a database until it is killed, printing each position only after its
 * command returned. The `crash` stress scenario does the killing, then checks that
 * everything printed survived.
 */
import { NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { AggregateService } from "effect-cqrs"
import { Account, crashEventsPerCommand, type DurableBackend, openStorage } from "./common.ts"

const program = Effect.gen(function*() {
  const accounts = yield* AggregateService.make(Account)
  for (let index = 0;; index++) {
    // Other writers may be on the same stream, and losing to them isn't a failure.
    const result = yield* accounts.commands.depositMany(`crash-${index % 50}`, {
      amount: 1,
      reference: `r-${process.pid}-${index}`,
      count: crashEventsPerCommand
    }).pipe(Effect.catchTag("ConcurrencyConflict", () => Effect.succeed(undefined)))
    if (result !== undefined) process.stdout.write(`${result.position}\n`)
  }
}).pipe(Effect.provide(openStorage(process.argv[2] as DurableBackend, process.argv[3]!)))

NodeRuntime.runMain(program)
