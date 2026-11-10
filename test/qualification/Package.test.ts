import { assert, describe, expect, it } from "@effect/vitest"
import * as Root from "effect-cqrs"
import * as PersonalDataNode from "effect-cqrs/PersonalDataNode"
import { readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"
import { anomalies, requirementIds, requirements } from "./requirements.ts"
import { verifies } from "./support.ts"

const here = import.meta.dirname
const manifest = JSON.parse(readFileSync(join(here, "../../package.json"), "utf8")) as {
  readonly dependencies?: Record<string, string>
  readonly optionalDependencies?: Record<string, string>
  readonly peerDependencies?: Record<string, string>
  readonly exports: Record<string, string>
  readonly publishConfig: { readonly exports: Record<string, unknown> }
}

describe("package", () => {
  /**
   * The names each module exports at run time. Types leave no trace here: they
   * are what `pnpm check` and `scripts/check-package.ts` cover.
   *
   * When this fails after an intended change, run `pnpm test -u` and review the
   * difference in `api-surface.json` as part of the release.
   */
  it(verifies("PKG-001", "exports the recorded modules and names"), async () => {
    const modules: Record<string, object> = { ...Root, PersonalDataNode }
    const surface = Object.fromEntries(
      Object.keys(modules).sort().map((name) => [name, Object.keys(modules[name]!).sort()])
    )

    await expect(`${JSON.stringify(surface, null, 2)}\n`).toMatchFileSnapshot("./api-surface.json")
    assert.deepStrictEqual(Object.keys(manifest.exports), [".", "./*"])
    assert.deepStrictEqual(Object.keys(manifest.publishConfig.exports), [".", "./*"])
  })

  it(verifies("PKG-002", "depends on nothing at run time but its peer, effect"), () => {
    assert.deepStrictEqual(Object.keys(manifest.dependencies ?? {}), [])
    assert.deepStrictEqual(Object.keys(manifest.optionalDependencies ?? {}), [])
    assert.deepStrictEqual(Object.keys(manifest.peerDependencies ?? {}), ["effect"])
  })
})

/**
 * Not a requirement of the package but of this suite: `pnpm test` fails when a
 * requirement has lost its tests, without waiting for `pnpm qualify` to say so.
 */
describe("traceability", () => {
  const sources = readdirSync(here)
    .filter((file) => file.endsWith(".test.ts"))
    .map((file) => readFileSync(join(here, file), "utf8"))
    .join("\n")
  const claimed = new Set(Array.from(sources.matchAll(/"([A-Z]+-\d{3})"/g), ([, id]) => id!))

  it("has a test for every requirement", () => {
    const known = new Set<string>([...requirementIds, ...Object.keys(anomalies)])
    // A requirement an anomaly breaks is claimed through the anomaly.
    for (const [id, { requirement }] of Object.entries(anomalies)) if (claimed.has(id)) claimed.add(requirement)

    assert.deepStrictEqual(requirementIds.filter((id) => !claimed.has(id)), [])
    assert.deepStrictEqual(Array.from(claimed).filter((id) => !known.has(id)), [])
  })

  it("states every requirement in a sentence, with the harm it prevents", () => {
    for (const id of requirementIds) {
      const { area, harm, statement } = requirements[id]
      assert.match(id, /^[A-Z]+-\d{3}$/)
      assert.isAbove(statement.length, 40, id)
      assert.isTrue(statement.endsWith("."), id)
      assert.isAbove(area.length, 0, id)
      assert.isAbove(harm.length, 0, id)
    }
    for (const [id, anomaly] of Object.entries(anomalies)) {
      assert.match(id, /^ANOM-\d{3}$/)
      assert.isTrue(requirementIds.includes(anomaly.requirement), id)
      assert.isAbove(anomaly.workaround.length, 40, id)
    }
  })
})
