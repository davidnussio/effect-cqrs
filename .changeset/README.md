# Changesets

Each change worth a line in the changelog gets a changeset: run `pnpm changeset`,
pick the bump, and describe the change for the people who use the package.

To release, run `pnpm version-packages`, which bumps `package.json` and writes
`CHANGELOG.md` from the changesets, and commit that to `main`. Then publish a GitHub
release tagged `v<version>`: the release workflow tests the package, publishes it to
npm and pushes any version bump back to the branch.
