/**
 * Runs every stress scenario at full size and fails if an invariant broke. Run
 * with `pnpm stress`, or `STRESS_ONLY=crash,burst pnpm stress` for some of them, and
 * `STRESS_DATABASE=postgres` for the database of `docker-compose.yml`. A full run
 * writes the raw numbers to `bench/results/`, summarized in `bench/REPORT.md`.
 */
import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect } from "effect"
import { environment, timed, writeResults } from "./common.ts"
import { fullSizes, type Report, scenarios } from "./stress-scenarios.ts"

const only = process.env.STRESS_ONLY?.split(",")
const database = process.env.STRESS_DATABASE === "postgres" ? "postgres" : "sqlite"

const program = Effect.gen(function*() {
  const env = yield* environment
  yield* Console.log(JSON.stringify(env))
  const reports: Array<Report & { readonly seconds: number }> = []
  for (const { name, run } of scenarios(fullSizes, database)) {
    if (only !== undefined && !only.includes(name)) continue
    yield* Console.log(`--- ${name}`)
    const { ms, value } = yield* timed(run)
    const report = { ...value, seconds: ms / 1000 }
    reports.push(report)
    yield* Console.log(JSON.stringify(report, null, 2))
  }
  if (only === undefined) {
    yield* writeResults(database === "postgres" ? "stress-postgres" : "stress", { environment: env, reports })
  }

  const violations = reports.flatMap(({ name, violations }) => violations.map((violation) => `${name}: ${violation}`))
  if (violations.length > 0) {
    return yield* Effect.die(new Error(`Invariants broke:\n${violations.join("\n")}`))
  }
  yield* Console.log(`${reports.length} scenarios held their invariants`)
})

NodeRuntime.runMain(program)
