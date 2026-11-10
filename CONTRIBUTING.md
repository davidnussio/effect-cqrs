# Contributing

effect-cqrs is a trial port of [moleculer-cqrs](https://github.com/davidnussio/moleculer-cqrs)
to Effect 4. Issues and pull requests are welcome. For anything larger than a fix, open an
issue first so we can agree on the shape of the change.

## Setup

You need Node 24 or later, pnpm (the version in `packageManager` in `package.json`) and,
for the PostgreSQL tests, Docker.

```bash
git clone https://github.com/davidnussio/effect-cqrs.git
cd effect-cqrs
pnpm install
```

The project reads two other repositories as reference material. They live in `repos/`,
which git ignores, so clone them yourself:

```bash
git clone --depth 1 --branch effect@4.0.6 https://github.com/Effect-TS/effect.git repos/effect
git clone https://github.com/davidnussio/moleculer-cqrs repos/moleculer-cqrs
```

- Keep the `repos/effect` tag equal to the `effect` version in `package.json`, so the
  reference source matches the code that runs.
- Read `repos/effect/LLMS.md` before writing Effect code, then
  [`agent-patterns/effect-cqrs.md`](agent-patterns/effect-cqrs.md), which records the Effect 4
  idioms this project relies on.
- The behaviour the framework reproduces comes from `repos/moleculer-cqrs/src/cqrs-event-sourcing.js`
  and `repos/moleculer-cqrs/src/cqrs-fixture.js`.
- Don't edit or import from `repos/`.

pnpm holds back releases younger than 7 days (`minimumReleaseAge`). Don't bypass it when
adding or upgrading a dependency.

## Commands

| Command | What it does |
| --- | --- |
| `pnpm check` | Type check `src`, the tests, the examples and the benchmarks |
| `pnpm lint` | oxlint, then dprint to check formatting |
| `pnpm lint:fix` | Fix what oxlint can, and format with dprint |
| `pnpm test` | Run the test suite |
| `pnpm build` | Compile `src` into `dist`, which is what npm gets |
| `pnpm example` | Run the todo example |
| `pnpm ui` | Serve the bank demo, which runs the framework in the browser |
| `pnpm docs` | Serve the documentation site in `docs` |
| `pnpm docs:build` | Build the documentation into `docs/dist`, with the demos, the API reference and `llms.txt` |
| `pnpm bench` | Measure performance, about 4 minutes |
| `pnpm stress` | Run the stress scenarios at full size, about 5 minutes |
| `pnpm qualify` | Run the qualification suite and write its report, see `test/qualification/README.md` |

Before you open a pull request, run `pnpm check`, `pnpm lint` and `pnpm test`, with
PostgreSQL running if the change touches a store. CI runs the same.

## Workspace

The repository is a pnpm workspace. The root package is the library, the only one that gets
published. `examples` and `docs` are private packages with their own dependencies, so a new
dependency goes in the package that imports it. The commands above run from the root, and
`pnpm test` runs the tests of the examples too.

## Tests with PostgreSQL

The PostgreSQL tests are skipped unless `POSTGRES_URL` is set. `docker-compose.yml` starts a
throwaway database:

```bash
docker compose up -d --wait
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/cqrs pnpm test
docker compose down
```

## Lint and format

The setup follows Effect's own repository. oxlint checks the code with the rules in
`.oxlintrc.json`, Effect's native rules without its private plugin, and dprint formats it
as `dprint.json` says: two spaces, double quotes, no semicolons, no trailing commas and
lines up to 120 characters. `pnpm lint:fix` formats the code for you. dprint downloads its
TypeScript plugin the first time it runs.

TypeScript 7 has no JavaScript API yet, so TypeDoc runs on TypeScript 6, which
`.pnpmfile.cjs` gives it as its own dependency.

## Documentation

The site in `docs/` is built with Astro Starlight. The pages quote the examples between
`// #region name` and `// #endregion` comments, and the build fails if a region goes
missing, so keep the comments when you edit an example. The API reference under
**Reference** is generated from the TSDoc comments in `src/`, so document new exports there.

A change people who use the package will notice belongs in the guides, in `README.md` and,
when it changes how to use the API, in [`LLMS.md`](LLMS.md), the guide for coding agents
that ships with the package.

## Changesets

Every change worth a line in the changelog gets a changeset:

```bash
pnpm changeset
```

Pick the bump and describe the change for the people who use the package, not for the ones
who review it. A change to tests, docs or tooling that doesn't reach the package needs none.

## Releases

Maintainers release from `main`, as `.changeset/README.md` describes:

1. `pnpm version-packages` bumps `package.json` and writes `CHANGELOG.md` from the
   changesets. Commit that to `main`.
2. Publish a GitHub release tagged `v<version>`. `.github/workflows/release.yml` tests the
   package, checks it with `node scripts/check-package.ts`, publishes it to npm and pushes
   any version bump back to the branch.

Publishing uses npm trusted publishing, with no token: npmjs.com lists this repository and
`release.yml` as the package's trusted publisher, so renaming the workflow breaks
publishing.

In this repository `effect-cqrs` resolves to the TypeScript sources, while `publishConfig`
points the published package at `dist`. A change to `exports` belongs in both places, and
after touching the build or `package.json`, run `node scripts/check-package.ts`, which
packs the package, installs it in a new project and runs and type checks it there.
