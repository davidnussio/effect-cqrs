import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    name: "examples",
    include: ["**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**"]
  }
})
