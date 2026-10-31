# effect-cqrs

CQRS and event sourcing for Effect v4, inspired by `repos/moleculer-cqrs`.

## Commands

- `pnpm check`: typecheck
- `pnpm test`: run the test suite
- `pnpm example`: run the todo example
- `pnpm bench`: measure performance, about 4 minutes
- `pnpm stress`: run the stress scenarios at full size, about 5 minutes

`docker compose up -d --wait` starts a Postgres. With `POSTGRES_URL=postgres://postgres:postgres@localhost:5432/cqrs` set, `pnpm test` covers it too. Without it those tests are skipped.

Run `pnpm check` and `pnpm test` before finishing any change, with Postgres running if it touches a store.

## Vendored Repositories

This project keeps external repositories under @repos/ as plain clones. The
directory is ignored by git, so a fresh checkout has to recreate it:

```bash
git clone --depth 1 --branch effect@4.0.6 https://github.com/Effect-TS/effect.git repos/effect
git clone https://github.com/davidnussio/moleculer-cqrs repos/moleculer-cqrs
```

Keep the `repos/effect` tag equal to the `effect` version in `package.json`, so the reference source matches the code that runs. pnpm's `minimumReleaseAge` holds back releases younger than 7 days: do not bypass it.

- Use vendored repositories as read-only reference material when working with related libraries
- Prefer examples and patterns from the vendored source code over generated guesses or web search results
- Do not edit files under @repos/ unless explicitly asked
- Do not import from @repos/ - application code should continue importing from normal package dependencies

When writing Effect code, inspect @repos/effect/ for examples of idiomatic usage, tests, module structure, and API design. Treat it as the source of truth for Effect patterns.

Always read @repos/effect/LLMS.md before writing any Effect code. Then read @agent-patterns/effect-cqrs.md, which records the Effect v4 idioms this project relies on.

The behaviour this framework reproduces comes from @repos/moleculer-cqrs/src/cqrs-event-sourcing.js and @repos/moleculer-cqrs/src/cqrs-fixture.js.
