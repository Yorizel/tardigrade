import * as SqliteMigrator from "@effect/sql-sqlite-bun/SqliteMigrator"
import { Effect } from "effect"
import { SqlClient } from "effect/sql"

export const DomainSchemaLive = SqliteMigrator.layer({
  table: "tardigrade_domain_migrations",
  loader: SqliteMigrator.fromRecord({
    "0001_domain": Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TABLE tardigrade_domain_state (
        model TEXT NOT NULL,
        block TEXT NOT NULL,
        instance TEXT NOT NULL,
        state TEXT NOT NULL,
        version INTEGER NOT NULL,
        PRIMARY KEY (model, block, instance)
      )`
      yield* sql`CREATE TABLE tardigrade_domain_receipts (
        model TEXT NOT NULL,
        block TEXT NOT NULL,
        instance TEXT NOT NULL,
        command_id TEXT NOT NULL,
        receipt TEXT NOT NULL,
        context TEXT NOT NULL,
        PRIMARY KEY (model, block, instance, command_id)
      )`
      yield* sql`CREATE TABLE tardigrade_domain_events (
        model TEXT NOT NULL,
        block TEXT NOT NULL,
        instance TEXT NOT NULL,
        seq INTEGER NOT NULL,
        entry TEXT NOT NULL,
        PRIMARY KEY (model, block, instance, seq)
      )`
      yield* sql`CREATE TABLE tardigrade_domain_outbox (
        model TEXT NOT NULL,
        block TEXT NOT NULL,
        instance TEXT NOT NULL,
        command_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        delivery TEXT NOT NULL,
        PRIMARY KEY (model, block, instance, command_id, ordinal)
      )`
      yield* sql`CREATE TABLE tardigrade_domain_scheduled (
        model TEXT NOT NULL,
        block TEXT NOT NULL,
        instance TEXT NOT NULL,
        command_id TEXT NOT NULL,
        due_at INTEGER NOT NULL,
        command TEXT NOT NULL,
        meta TEXT NOT NULL,
        cancelled INTEGER NOT NULL,
        PRIMARY KEY (model, block, instance, command_id)
      )`
      yield* sql`CREATE INDEX tardigrade_domain_scheduled_due ON tardigrade_domain_scheduled (model, cancelled, due_at)`
      yield* sql`CREATE TABLE tardigrade_domain_consumers (
        model TEXT NOT NULL,
        block TEXT NOT NULL,
        instance TEXT NOT NULL,
        source TEXT NOT NULL,
        seq INTEGER NOT NULL,
        PRIMARY KEY (model, block, instance, source)
      )`
    }),
  }),
})
