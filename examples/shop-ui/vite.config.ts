import tailwindcss from "@tailwindcss/vite"
import react from "@vitejs/plugin-react"
import { fileURLToPath } from "node:url"
import { defineConfig } from "vite"

export default defineConfig({
  root: fileURLToPath(new URL(".", import.meta.url)),
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: [
      // The framework's own sources, the same files `pnpm test` runs.
      { find: /^effect-cqrs$/, replacement: fileURLToPath(new URL("../../src/index.ts", import.meta.url)) },
      { find: /^effect-cqrs\/(.*)$/, replacement: fileURLToPath(new URL("../../src/$1.ts", import.meta.url)) }
    ]
  }
})
