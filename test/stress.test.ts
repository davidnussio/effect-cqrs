import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { scenarios, smokeSizes } from "../bench/stress-scenarios.ts"

// The scenarios of `pnpm stress`, small enough to keep them working.
describe("stress scenarios", () => {
  for (const { name, run } of scenarios(smokeSizes)) {
    it.live(name, () => Effect.map(run, ({ violations }) => assert.deepStrictEqual(violations, [])), { timeout: 30_000 })
  }
})
