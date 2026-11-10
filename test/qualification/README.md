# Qualification suite

For a manufacturer of medical device software, effect-cqrs is software of unknown
provenance (SOUP): it wasn't developed under their quality system, so they have to say
what they need from it and show that it delivers. This suite is that evidence, ready to
run: requirements written down, a test for each, and a report that traces one to the
other on the system the tests ran on.

It doesn't certify anything. A device is certified, not a library, and the manufacturer
stays responsible for the choice. What the suite does is turn "we trust this package"
into records an auditor can read.

## Run it

```bash
docker compose up -d --wait
POSTGRES_URL=postgres://postgres:postgres@localhost:5432/cqrs pnpm qualify
```

This runs the tests in this folder and writes `qualification-report/`:

- `REPORT.md`: what was tested (version, commit, lockfile hash), where (Node, operating
  system, drivers, database versions), the result of every requirement and every test.
- `report.json`: the same, for tooling.
- `results.json`: the raw output of the test runner. Its hash is in the report.

The command fails when a test fails or a requirement has no passing test. Without
`POSTGRES_URL` the PostgreSQL tests are reported as not run: a report only covers the
storage configurations it lists as run, so run it on the one the device uses.

The tests are also part of `pnpm test`, so every change to the package is checked
against the requirements.

## What it covers

`requirements.ts` holds the requirements, each with the harm that follows when it
doesn't hold: data loss, data corruption, wrong state, untraceable change, disclosure,
undetected failure, unavailability or uncontrolled change. That column is the link to
the hazards of your own risk analysis.

| Area | Ids | Runs on |
| --- | --- | --- |
| Event log: atomic appends, ordering, concurrency, fidelity, durability, crashes | `ES` | memory, SQLite in memory, SQLite file, PostgreSQL |
| Commands: validation, invariants under concurrency, idempotency, decoding, time travel, upcasting | `CMD` | all four |
| Snapshots | `SNAP` | all four |
| Audit trail: time, correlation, cause, who and which software | `AUD` | all four |
| View models: order, stalling, restart, replay, parking | `VM` | all four |
| Personal data: encryption at rest, erasure, tampering, lost keys | `PD` | all four, with keys in memory |
| Package: exported names, dependencies | `PKG` | |

How this maps to what standards ask for. Check the clause numbers against the editions
you work to:

- **IEC 62304**, 5.3.3 and 5.3.4: the functional requirements of the SOUP item and the
  system it needs. The requirements here are a starting set, and the report's "Where"
  section records the system. 8.1.2: the report identifies the item by name, version,
  commit and lockfile hash. 7.1.2 and 7.1.3: the harm of each requirement, and the known
  anomalies below, feed the risk analysis.
- **ISO 14971**: each requirement is a risk control you can cite, with its verification.
- **21 CFR Part 11 and EU GMP Annex 11**, where events are electronic records: `ES-005`,
  `ES-013` and the `AUD` requirements are what an audit trail needs from its store.
  Access control, signatures and retention are the application's.
- **GDPR**: the `PD` requirements show that personal fields are encrypted and that
  destroying a subject's key makes them unreadable. Whether that is erasure for your
  purposes is a decision for your data protection officer.

## What it doesn't cover

- **Your domain.** The suite shows that the framework runs an aggregate correctly, not
  that your aggregates decide correctly. Test those with `Fixture`.
- **Other modules.** `Saga`, `Outbox`, `CommandApi`, `Leases`, `EventCatalog` and
  `Fixture` have tests in `test/` but no requirements here. If the device relies on
  one, add requirements for it.
- **Performance.** `pnpm bench` and `pnpm stress` measure it, see `bench/REPORT.md`.
- **Key storage in SQL and key management services.** The `PD` tests keep keys in
  memory. `test/PersonalData.test.ts` covers the SQL key store.
- **Security testing**, and anything about the database server, its backups and its
  access rights.
- **Types.** `PKG-001` records the names the package exports at run time. `pnpm check`
  and `scripts/check-package.ts` cover the types.

## What the manufacturer still does

1. Pin the version, with the lockfile, and record it as a SOUP item.
2. Read the requirements, drop those the device doesn't rely on and add what it needs
   beyond them.
3. Run `pnpm qualify` on the Node version, operating system and database the device
   uses, and file the report.
4. Evaluate the known anomalies and the project's open issues against the device's
   hazards, and again with each upgrade. `PKG-001` fails when a release changes what the
   package exports.

## Known anomalies

`requirements.ts` also lists the requirements the package is known not to meet, each
with a workaround. A test shows each one: it is written with `it.fails`, so it passes
while the anomaly is there and fails once a release fixes it. The report gives such a
requirement as not met, never as verified.

None is recorded for this version.

## Changing the suite

- A new requirement goes in `requirements.ts`, and a test claims it by building its
  title with `verifies("ES-015", "...")`. An id that doesn't exist doesn't compile, and
  `pnpm test` fails for a requirement without a test.
- A `describe` title ends in `on <configuration>` through `on(configuration)`, which is
  how the report knows where a test ran.
- A defect that stays for now gets an entry in `anomalies` and an `it.fails` test named
  with `knownAnomaly`. Remove both with the fix.
- After an intended change to the exports, run `pnpm test -u` and review the difference
  in `api-surface.json`.
