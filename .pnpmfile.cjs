// TypeScript 7 has no JavaScript compiler API yet, and ESLint's TypeScript parser and
// TypeDoc need one. They get TypeScript 6 as their own dependency instead of the
// project's TypeScript 7 as a peer.
const typescript6 = "6.0.3"

const needsTypescript6 = (name) =>
  name === "typedoc" || name === "typescript-eslint" || name.startsWith("@typescript-eslint/")

module.exports = {
  hooks: {
    readPackage(pkg) {
      if (needsTypescript6(pkg.name) && pkg.peerDependencies?.typescript) {
        delete pkg.peerDependencies.typescript
        pkg.dependencies = { ...pkg.dependencies, typescript: typescript6 }
      }
      return pkg
    }
  }
}
