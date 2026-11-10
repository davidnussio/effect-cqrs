import { assert, describe, it } from "@effect/vitest"
import { Effect } from "effect"
import { postgresUrl } from "../bench/common.ts"
import { scenarios, smokeSizes } from "../bench/stress-scenarios.ts"

// The scenarios of `pnpm stress`, small enough to keep them working.
for (const database of ["sqlite", ...(postgresUrl === undefined ? [] : ["postgres" as const])] as const) {
  describe(`stress scenarios on ${database}`, () => {
    for (const { name, run } of scenarios(smokeSizes, database)) {
      it.live(name, () => Effect.map(run, ({ violations }) => assert.deepStrictEqual(violations, [])), {
        timeout: 30_000
      })
    }
  })
}
