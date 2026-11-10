import { defineConfig } from "vitest/config"

// One run covers the library's tests and the examples' tests, which live in the
// examples package with its own config.
export default defineConfig({
  test: {
    projects: [
      { test: { name: "effect-cqrs", include: ["test/**/*.test.ts"] } },
      "examples"
    ]
  }
})
