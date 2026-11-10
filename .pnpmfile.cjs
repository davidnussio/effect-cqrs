// TypeScript 7 has no JavaScript compiler API yet, and TypeDoc needs one. It gets
// TypeScript 6 as its own dependency instead of the project's TypeScript 7 as a peer.
const typescript6 = "6.0.3"

const needsTypescript6 = (name) => name === "typedoc"

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
