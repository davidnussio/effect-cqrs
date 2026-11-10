/**
 * Bundles the server into one file for the Docker image, so that it needs none
 * of the repository's dependencies at run time.
 */
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  resolve: {
    alias: [
      { find: /^effect-cqrs$/, replacement: fileURLToPath(new URL("../../../src/index.ts", import.meta.url)) }
    ]
  },
  ssr: { noExternal: true, target: "node" },
  build: {
    ssr: "main.ts",
    outDir: "dist",
    emptyOutDir: true,
    target: "node24",
    minify: false,
    rollupOptions: { output: { entryFileNames: "server.mjs", inlineDynamicImports: true } }
  }
})
