/**
 * Writes to a database until it is killed, printing each position only after its
 * command returned. The `crash` stress scenario does the killing, then checks that
 * everything printed survived.
 */
import { NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { AggregateService } from "effect-cqrs"
import { Account, crashEventsPerCommand, sqliteStorage } from "./common.ts"

const program = Effect.gen(function*() {
  const accounts = yield* AggregateService.make(Account)
  for (let index = 0;; index++) {
    const { position } = yield* accounts.commands.depositMany(`crash-${index % 50}`, {
      amount: 1,
      reference: `r-${process.pid}-${index}`,
      count: crashEventsPerCommand
    })
    process.stdout.write(`${position}\n`)
  }
}).pipe(Effect.provide(sqliteStorage(process.argv[2]!)))

NodeRuntime.runMain(program)
