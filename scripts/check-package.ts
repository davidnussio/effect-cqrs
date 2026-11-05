/**
 * Packs effect-cqrs as `pnpm publish` would, installs the tarball in a new project
 * next to the `effect` this repository uses, and uses it there: Node runs
 * `scripts/consumer/main.ts`, and TypeScript 5.9 and this repository's TypeScript
 * check it. CI runs this, and the release runs it before publishing.
 */
import { execFileSync } from "node:child_process"
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"

const root = resolve(import.meta.dirname, "..")
const work = mkdtempSync(join(tmpdir(), "effect-cqrs-package-"))

const run = (command: string, args: ReadonlyArray<string>, cwd: string) => {
  console.log(`$ ${command} ${args.join(" ")}`)
  execFileSync(command, args, { cwd, stdio: "inherit" })
}

const fail = (message: string): never => {
  console.error(`::error::${message}`)
  process.exit(1)
}

try {
  run("pnpm", ["pack", "--pack-destination", work], root)
  const tarball = readdirSync(work).find((name) => name.endsWith(".tgz")) ?? fail("pnpm pack made no tarball")
  run("tar", ["-xzf", tarball], work)

  // What npm will serve: the build, with exports that point at it.
  const packed = JSON.parse(readFileSync(join(work, "package", "package.json"), "utf8"))
  if (packed.exports?.["."]?.default !== "./dist/index.js") fail("the packed exports don't point at dist")
  if (packed.private === true) fail("the package is private")
  for (const file of ["dist/index.js", "dist/index.d.ts", "dist/ViewModel.js", "LICENSE", "README.md"]) {
    if (!existsSync(join(work, "package", file))) fail(`the tarball has no ${file}`)
  }
  for (const folder of ["test", "bench", "examples", "docs"]) {
    if (existsSync(join(work, "package", folder))) fail(`the tarball includes ${folder}/`)
  }

  const consumer = join(work, "consumer")
  mkdirSync(consumer)
  cpSync(join(root, "scripts", "consumer"), consumer, { recursive: true })
  writeFileSync(join(consumer, "package.json"), `${JSON.stringify({ name: "consumer", private: true, type: "module" })}\n`)
  const effect = JSON.parse(readFileSync(join(root, "node_modules", "effect", "package.json"), "utf8")).version
  run(
    "npm",
    ["install", "--no-audit", "--no-fund", join(work, tarball), `effect@${effect}`, "typescript@5.9"],
    consumer
  )

  run(process.execPath, ["main.ts"], consumer)
  run(join(consumer, "node_modules", ".bin", "tsc"), ["-p", "."], consumer)
  run(join(root, "node_modules", ".bin", "tsc"), ["-p", "."], consumer)
  console.log(`${tarball} installs, runs and type checks`)
} finally {
  rmSync(work, { recursive: true, force: true })
}
