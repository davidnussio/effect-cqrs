/** Runs the tutorial's shop on the in-memory store: `pnpm tutorial`. */
import { NodeRuntime } from "@effect/platform-node"
import { Effect } from "effect"
import { program, Shop } from "./shop.ts"

program.pipe(Effect.provide(Shop), NodeRuntime.runMain)
