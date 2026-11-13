/**
 * The library of the blog series on a database, for the last post: the events in
 * SQLite or PostgreSQL, and the keys of personal data in a database of their own.
 */
import { PgClient } from "@effect/sql-pg"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Layer, type Redacted } from "effect"
import { Checkpoints, PersonalData, PostgresEventStore, SqliteEventStore } from "effect-cqrs"
import { Application } from "./library.ts"

// #region sqlite
// The events in one file and the keys in another. The read models and the workflow
// engine live in memory, so their checkpoints do too: on every start, they read the
// log again from the beginning.
export const onSqlite = (files: { readonly events: string; readonly keys: string }) =>
  Layer.mergeAll(
    SqliteEventStore.layer().pipe(Layer.provide(SqliteClient.layer({ filename: files.events }))),
    Checkpoints.layerMemory,
    PersonalData.layerSql().pipe(Layer.provide(SqliteClient.layer({ filename: files.keys })))
  )

// The master key comes from PERSONAL_DATA_MASTER_KEY and PERSONAL_DATA_MASTER_KEY_ID.
export const LibraryOnSqlite = Application.pipe(
  Layer.provideMerge(onSqlite({ events: "./data/library.sqlite", keys: "./data/keys.sqlite" })),
  Layer.provide(PersonalData.layerMasterKeyConfig())
)
// #endregion

// #region postgres
// The same on PostgreSQL, where several processes can share the events.
export const onPostgres = (urls: { readonly events: Redacted.Redacted; readonly keys: Redacted.Redacted }) =>
  Layer.mergeAll(
    PostgresEventStore.layer().pipe(Layer.provide(PgClient.layer({ url: urls.events }))),
    Checkpoints.layerMemory,
    PersonalData.layerSql().pipe(Layer.provide(PgClient.layer({ url: urls.keys })))
  )
// #endregion
