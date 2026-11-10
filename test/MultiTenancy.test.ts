/**
 * Tenant isolation with `LayerMap`: each tenant gets its own tables, and with
 * them its own event store, aggregate services and view models.
 */
import { SqliteClient } from "@effect/sql-sqlite-node"
import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Layer, LayerMap } from "effect"
import { AggregateService, PostgresEventStore, SqliteEventStore, ViewModel } from "effect-cqrs"
import { SqlClient } from "effect/sql"
import { postgresClient, postgresUrl } from "../bench/common.ts"
import { listTodos, Todo, TodoList, TodoListStore } from "./fixtures/todo.ts"

class Todos extends Context.Service<Todos, AggregateService.AggregateService<typeof Todo>>()("test/Todos") {}

type Store = "sqlite" | "postgres"

// Tenant ids end up in table names. `sql(name)` quotes them, but keeping them to
// a known alphabet also keeps them inside the database's limits.
const tablePrefix = (tenant: string) => {
  if (!/^[a-z][a-z0-9_]{0,30}$/.test(tenant)) throw new Error(`Not a valid tenant id: ${tenant}`)
  return `tenant_${tenant}`
}

const tenantLayer = (store: Store, prefix: string) => (tenant: string) => {
  const options = {
    eventsTable: `${prefix}${tablePrefix(tenant)}_events`,
    checkpointsTable: `${prefix}${tablePrefix(tenant)}_checkpoints`
  }
  const Storage = store === "sqlite"
    ? Layer.merge(SqliteEventStore.layer(options), SqliteEventStore.layerCheckpoints(options))
    : Layer.merge(PostgresEventStore.layer(options), PostgresEventStore.layerCheckpoints(options))
  return Layer.mergeAll(Layer.effect(Todos, AggregateService.make(Todo)), ViewModel.layer([TodoList])).pipe(
    // A layer value is built once per `LayerMap`, so state of its own needs `Layer.fresh`.
    Layer.provideMerge(Layer.merge(Storage, Layer.fresh(TodoListStore.layer)))
  )
}

const tenantsFor = (store: Store, prefix: string) =>
  LayerMap.make(tenantLayer(store, prefix), { idleTimeToLive: "1 minute" })

type Tenants = Effect.Success<ReturnType<typeof tenantsFor>>

const inTenant = <A, E, R>(
  tenants: Tenants,
  tenant: string,
  effect: Effect.Effect<A, E, R>
) => Effect.provide(effect, tenants.get(tenant))

const scenario = (store: Store, prefix: string) =>
  Effect.gen(function*() {
    const tenants = yield* tenantsFor(store, prefix)

    const create = (title: string) =>
      Effect.gen(function*() {
        const todos = yield* Todos
        const result = yield* todos.commands.createTodo("a", { title })
        yield* ViewModel.ViewModels.use((viewModels) => viewModels.caughtUp(result.position))
      })

    // The same aggregate id in two tenants is two different streams.
    yield* inTenant(tenants, "acme", create("Acme todo"))
    yield* inTenant(tenants, "globex", create("Globex todo"))

    const titles = Effect.map(listTodos, (todos) => todos.map(({ title }) => title))
    assert.deepStrictEqual(yield* inTenant(tenants, "acme", titles), ["Acme todo"])
    assert.deepStrictEqual(yield* inTenant(tenants, "globex", titles), ["Globex todo"])

    const history = Todos.use((todos) => todos.history("a"))
    assert.strictEqual((yield* inTenant(tenants, "acme", history)).length, 1)

    // A tenant built again after eviction reads its events back from its tables.
    yield* tenants.invalidate("acme")
    const state = Todos.use((todos) => todos.readModel("a"))
    assert.deepStrictEqual(yield* inTenant(tenants, "acme", state), { status: "open", title: "Acme todo" })

    const refused = yield* Effect.exit(inTenant(tenants, "Robert'); DROP TABLE", titles))
    assert.isTrue(refused._tag === "Failure")
  }).pipe(Effect.scoped)

describe("Multi-tenancy with LayerMap", () => {
  it.effect("keeps tenants apart on SQLite", () =>
    scenario("sqlite", "").pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))))

  // Needs the database of `docker-compose.yml` and POSTGRES_URL.
  it.live.skipIf(postgresUrl === undefined)("keeps tenants apart on PostgreSQL", () => {
    const prefix = `mt${crypto.randomUUID().replaceAll("-", "").slice(0, 8)}_`
    return Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      yield* Effect.addFinalizer(() =>
        Effect.forEach(["acme", "globex"], (tenant) => {
          const table = `${prefix}${tablePrefix(tenant)}`
          return sql.unsafe(`DROP TABLE IF EXISTS ${table}_events, ${table}_events_head, ${table}_checkpoints`)
        }).pipe(Effect.ignore)
      )
      yield* scenario("postgres", prefix)
    }).pipe(Effect.scoped, Effect.provide(Layer.fresh(postgresClient())))
  })
})
