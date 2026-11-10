/**
 * Turns the results of the qualification tests into the evidence a manufacturer
 * files: what was tested, on what, and which requirement each test verified.
 * `pnpm qualify` runs the tests and then this.
 *
 * It exits with an error when a test failed or a requirement was left without a
 * passing test, so a pipeline can't file a report that doesn't hold.
 */
import { NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { SqlClient } from "effect/sql"
import { execSync } from "node:child_process"
import { createHash } from "node:crypto"
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { arch } from "node:os"
import { join } from "node:path"
import { environment, postgresClient, postgresUrl } from "../../bench/common.ts"
import { anomalies, type AnomalyId, type RequirementId, requirementIds, requirements } from "./requirements.ts"

const directory = "qualification-report"
const resultsFile = join(directory, "results.json")

/** What the JSON reporter of Vitest writes, as far as this reads it. */
interface Results {
  readonly success: boolean
  readonly startTime: number
  readonly testResults: ReadonlyArray<{
    readonly name: string
    readonly message: string
    readonly assertionResults: ReadonlyArray<{
      readonly ancestorTitles: ReadonlyArray<string>
      readonly fullName: string
      readonly title: string
      readonly status: string
      readonly duration?: number | null
    }>
  }>
}

type Status = "passed" | "failed" | "not run"

interface Test {
  readonly name: string
  readonly file: string
  readonly status: Status
  readonly configuration: string | undefined
  readonly requirements: ReadonlyArray<RequirementId>
  readonly anomalies: ReadonlyArray<AnomalyId>
  readonly ms: number
}

const configurationNames = ["memory", "sqlite-memory", "sqlite-file", "postgres"] as const

const statusOf = (status: string): Status => status === "passed" ? "passed" : status === "failed" ? "failed" : "not run"

const readTests = (results: Results): ReadonlyArray<Test> =>
  results.testResults.flatMap((file) =>
    file.assertionResults.map((test): Test => {
      const ids = Array.from(test.title.matchAll(/\[([A-Z]+-\d{3})\]/g), ([, id]) => id!)
      const configuration = test.ancestorTitles
        .map((title) => /\bon (\S+)$/.exec(title)?.[1])
        .find((name) => name !== undefined && (configurationNames as ReadonlyArray<string>).includes(name))
      return {
        name: test.fullName,
        file: file.name.slice(file.name.lastIndexOf("/test/") + 1),
        status: statusOf(test.status),
        configuration,
        requirements: ids.filter((id): id is RequirementId => id in requirements),
        anomalies: ids.filter((id): id is AnomalyId => id in anomalies),
        ms: Math.round(test.duration ?? 0)
      }
    })
  )

type Verdict = "verified" | "not met" | "failed" | "not verified"

interface Line {
  readonly id: RequirementId
  readonly verdict: Verdict
  readonly tests: ReadonlyArray<Test>
  /** The anomalies recorded against it, with whether their test ran here. */
  readonly anomalies: ReadonlyArray<{ readonly id: AnomalyId; readonly shown: boolean }>
}

/**
 * A requirement is verified when at least one of its tests passed and none
 * failed. A test of a known anomaly passes by failing as recorded, so it counts
 * against the requirement rather than for it.
 */
const judge = (id: RequirementId, tests: ReadonlyArray<Test>): Line => {
  const own = tests.filter((test) => test.requirements.includes(id))
  const proving = own.filter((test) => test.anomalies.length === 0)
  const recorded = (Object.keys(anomalies) as ReadonlyArray<AnomalyId>)
    .filter((anomaly) => anomalies[anomaly].requirement === id)
    .map((anomaly) => ({
      id: anomaly,
      shown: own.some((test) => test.anomalies.includes(anomaly) && test.status === "passed")
    }))
  const verdict: Verdict = own.some((test) => test.status === "failed")
    ? "failed"
    : recorded.length > 0
    ? "not met"
    : proving.some((test) => test.status === "passed")
    ? "verified"
    : "not verified"
  return { id, verdict, tests: own, anomalies: recorded }
}

const version = (name: string): string =>
  (JSON.parse(readFileSync(`node_modules/${name}/package.json`, "utf8")) as { readonly version: string }).version

const sha256 = (path: string) => createHash("sha256").update(readFileSync(path)).digest("hex")

const git = (command: string) => execSync(`git ${command}`, { encoding: "utf8" }).trim()

const postgresVersion = postgresUrl === undefined
  ? Effect.succeed("not configured, its tests were not run")
  : SqlClient.SqlClient.use((sql) => sql.unsafe<{ readonly server_version: string }>("SHOW server_version")).pipe(
    Effect.map(([row]) => row?.server_version ?? "unknown"),
    Effect.provide(postgresClient()),
    Effect.catch(() => Effect.succeed("configured, but it could not be reached"))
  )

const cell = (text: string) => text.replaceAll("|", "\\|").replaceAll("\n", " ")

const table = (header: ReadonlyArray<string>, rows: ReadonlyArray<ReadonlyArray<string | number>>) =>
  [
    `| ${header.join(" | ")} |`,
    `| ${header.map(() => "---").join(" | ")} |`,
    ...rows.map((row) => `| ${row.map((value) => cell(String(value))).join(" | ")} |`)
  ].join("\n")

const count = (tests: ReadonlyArray<Test>, status: Status) => tests.filter((test) => test.status === status).length

const program = Effect.gen(function*() {
  if (!existsSync(resultsFile)) {
    return yield* Effect.fail(`${resultsFile} is missing: run \`pnpm qualify\`, which runs the tests first`)
  }
  const results = JSON.parse(readFileSync(resultsFile, "utf8")) as Results
  const tests = readTests(results)
  const lines = requirementIds.map((id) => judge(id, tests))
  const untraced = tests.filter((test) => test.requirements.length === 0 && !test.name.startsWith("traceability"))
  const broken = results.testResults.filter((file) => file.assertionResults.length === 0)

  const manifest = JSON.parse(readFileSync("package.json", "utf8")) as {
    readonly name: string
    readonly version: string
    readonly license: string
    readonly repository: { readonly url: string }
  }
  const { cores, cpu, date, memoryGb, node, platform, sqlite } = yield* environment
  const changed = git("status --porcelain -- src test bench package.json pnpm-lock.yaml")
  const commit = git("rev-parse HEAD")
  const postgres = yield* postgresVersion

  const verdicts = (verdict: Verdict) => lines.filter((line) => line.verdict === verdict)
  const failed = verdicts("failed").length > 0 || count(tests, "failed") > 0 || broken.length > 0 || !results.success
  const incomplete = verdicts("not verified").length > 0
  const result = failed ? "FAILED" : incomplete ? "INCOMPLETE" : "PASSED"
  const summary = [
    `${verdicts("verified").length} of ${lines.length} requirements verified`,
    `${verdicts("not met").length} not met (known anomalies)`,
    `${verdicts("failed").length} failed`,
    `${verdicts("not verified").length} not verified`
  ].join(", ")

  const configurations = configurationNames.map((name) => {
    const own = tests.filter((test) => test.configuration === name)
    const shown = own.filter((test) => test.anomalies.length > 0 && test.status === "passed").length
    return {
      name,
      passed: count(own, "passed") - shown,
      failed: count(own, "failed"),
      notRun: count(own, "not run"),
      anomalies: shown,
      state: count(own, "passed") === 0 ? "not run" : count(own, "failed") > 0 ? "failed" : "run"
    }
  })

  const identification = {
    package: `${manifest.name} ${manifest.version}`,
    license: manifest.license,
    source: manifest.repository.url.replace(/^git\+/, ""),
    commit: changed === "" ? commit : `${commit}, with uncommitted changes`,
    date,
    lockfileSha256: sha256("pnpm-lock.yaml"),
    resultsSha256: sha256(resultsFile)
  }
  const system = {
    node,
    platform: `${platform} ${arch()}`,
    cpu: `${cpu}, ${cores} cores, ${memoryGb} GB`,
    effect: version("effect"),
    "@effect/sql-sqlite-node": version("@effect/sql-sqlite-node"),
    sqlite: String(sqlite),
    "@effect/sql-pg": version("@effect/sql-pg"),
    postgresql: postgres,
    vitest: version("vitest"),
    typescript: version("typescript")
  }

  const resultOf = (line: Line) =>
    line.verdict === "not met"
      ? `not met: ${line.anomalies.map(({ id }) => id).join(", ")}`
      : line.verdict
  const where = (line: Line) => {
    const passed = (name: string) =>
      line.tests.some((test) => test.configuration === name && test.status === "passed" && test.anomalies.length === 0)
    const names = configurationNames.filter(passed)
    return names.length > 0 ? names.join(", ") : line.tests.some((test) => test.status === "passed") ? "any" : "none"
  }
  const areas = Array.from(new Set(lines.map((line) => requirements[line.id].area)))

  const markdown = [
    `# Qualification report: ${identification.package}`,
    "",
    `**${result}.** ${summary}. ${count(tests, "passed")} tests passed, ${count(tests, "failed")} failed, ${
      count(tests, "not run")
    } were not run.`,
    "",
    ...(configurations.some(({ state }) => state === "not run")
      ? [
        `**Not run here: ${
          configurations.filter(({ state }) => state === "not run").map(({ name }) => name).join(", ")
        }.** This report says nothing about those storage configurations.`,
        ""
      ]
      : []),
    "This report is evidence that the package behaves as the requirements below say, on the system described here. " +
    "It is not a certificate, and it says nothing about requirements it doesn't list: see `test/qualification/README.md`.",
    "",
    "## What was tested",
    "",
    table(["", ""], Object.entries(identification)),
    "",
    "## Where",
    "",
    table(["", ""], Object.entries(system)),
    "",
    "## Storage configurations",
    "",
    "A requirement about storage holds for a configuration only if its tests ran there.",
    "",
    table(
      ["Configuration", "State", "Passed", "Failed", "Not run", "Known anomalies shown"],
      configurations.map(({ anomalies, failed, name, notRun, passed, state }) => [
        name,
        state,
        passed,
        failed,
        notRun,
        anomalies
      ])
    ),
    "",
    "## Known anomalies",
    "",
    ...(Object.keys(anomalies).length === 0 ? ["None recorded."] : (Object.keys(anomalies) as Array<AnomalyId>).flatMap(
      (id) => {
        const { configuration, requirement, summary, workaround } = anomalies[id]
        const shown = lines.find((line) => line.id === requirement)?.anomalies.find((each) => each.id === id)?.shown
        return [
          `### ${id}: ${requirement} is not met on ${configuration}`,
          "",
          summary,
          "",
          `**Workaround.** ${workaround}`,
          "",
          shown === true
            ? "The test that shows it ran here and failed as recorded."
            : "The test that shows it was not run here.",
          ""
        ]
      }
    )),
    "Anomalies found after this release are listed with the project's issues and in `CHANGELOG.md`.",
    "",
    "## Traceability",
    "",
    ...areas.flatMap((area) => [
      `### ${area}`,
      "",
      table(
        ["Id", "Requirement", "Harm if it fails", "Result", "Passed on", "Tests"],
        lines.filter((line) => requirements[line.id].area === area).map((line) => [
          line.id,
          requirements[line.id].statement,
          requirements[line.id].harm,
          resultOf(line),
          where(line),
          `${count(line.tests, "passed")} passed, ${count(line.tests, "failed")} failed, ${
            count(line.tests, "not run")
          } not run`
        ])
      ),
      ""
    ]),
    "## Every test",
    "",
    "A test of a known anomaly is written to fail, and is listed as `failed as recorded` when it does.",
    "",
    table(
      ["Test", "File", "Result", "ms"],
      tests.map((test) => [
        test.name,
        test.file,
        test.anomalies.length > 0 && test.status === "passed" ? "failed as recorded" : test.status,
        test.ms
      ])
    ),
    "",
    ...(untraced.length === 0
      ? []
      : [
        "## Tests without a requirement",
        "",
        ...untraced.map((test) => `- ${test.name}`),
        ""
      ]),
    ...(broken.length === 0
      ? []
      : ["## Files that could not be run", "", ...broken.map((file) => `- ${file.name}: ${file.message}`), ""])
  ].join("\n")

  mkdirSync(directory, { recursive: true })
  writeFileSync(join(directory, "REPORT.md"), markdown)
  writeFileSync(
    join(directory, "report.json"),
    `${
      JSON.stringify(
        {
          result,
          identification,
          system,
          configurations,
          anomalies,
          requirements: lines.map((line) => ({
            ...requirements[line.id],
            id: line.id,
            verdict: line.verdict,
            anomalies: line.anomalies,
            tests: line.tests.map(({ configuration, name, status }) => ({ name, configuration, status }))
          }))
        },
        null,
        2
      )
    }\n`
  )

  yield* Effect.sync(() => {
    console.log(`\nQualification of ${identification.package}: ${result}`)
    console.log(summary)
    for (const { name, state } of configurations) console.log(`  ${name}: ${state}`)
    for (const line of lines) {
      if (line.verdict !== "verified") console.log(`  ${line.id}: ${resultOf(line)}`)
    }
    console.log(`Written to ${join(directory, "REPORT.md")} and ${join(directory, "report.json")}`)
  })
  if (failed || incomplete) process.exitCode = 1
})

NodeRuntime.runMain(program)
