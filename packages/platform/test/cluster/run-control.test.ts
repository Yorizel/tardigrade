import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ConfigProvider, Effect, Exit, Layer, Option, Schedule, Schema } from "effect"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { RpcTest } from "effect/rpc"
import { SqlClient } from "effect/sql"
import { Activity, DurableDeferred } from "effect/workflow"
import { ExecuteRun, approval, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { RunRpcHandlers, RunRpcs } from "../../src/cluster/run-control"

test("native run RPC authenticates, signals, replays and interrupts without persisting credentials", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const directory = yield* Effect.acquireRelease(
      Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-run-control-"))),
      path => Effect.promise(() => rm(path, { recursive: true, force: true })),
    )
    const database = SqliteClient.layer({ filename: join(directory, "runner.sqlite") })
    const handler = Layer.unwrap(Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      return ExecuteRun.toLayer(Effect.fn("RunControl.testHandler")(function* () {
        yield* Activity.make({ name: "increment", execute: sql`INSERT INTO proof_calls DEFAULT VALUES`.pipe(Effect.orDie, Effect.asVoid) })
        yield* DurableDeferred.await(approval("step-1"))
      }))
    }))
    const application = Layer.mergeAll(handler, RunRpcHandlers).pipe(
      Layer.provide(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer()))),
      Layer.provideMerge(database),
      Layer.provide(Digest.layerWebCrypto),
      Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ WORKER_TO_RUNNER_TOKEN: "run-control-fixture-token" }))),
    )
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TABLE proof_calls (id INTEGER PRIMARY KEY)`
      const client = yield* RpcTest.makeClient(RunRpcs)
      const scope = { threadId: "thread-a", runId: "run-a", owner: "alice" }
      const payload = { ...scope, key: executionKey(scope) }
      const options = { headers: { authorization: "Bearer run-control-fixture-token" } }
      const denied = yield* client["Tardigrade/ExecuteRunDiscard"](payload).pipe(Effect.exit)
      expect(Exit.isFailure(denied)).toBe(true)
      const executionId = yield* client["Tardigrade/ExecuteRunDiscard"](payload, options)
      expect(executionId).toBe(yield* ExecuteRun.executionId(payload))
      for (const conflicting of [{ ...payload, owner: "bob" }, { ...payload, threadId: "other" }, { ...payload, runId: "other" }]) {
        expect(Exit.isFailure(yield* client["Tardigrade/ExecuteRunDiscard"](conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.SignalRunApproval({ ...conflicting, stepId: "step-1" }, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.InterruptRun(conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.PollRun(conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.ResumeRun(conflicting, options).pipe(Effect.exit))).toBe(true)
      }
      yield* client.SignalRunApproval({ ...payload, stepId: "step-1" }, options)
      yield* client["Tardigrade/ExecuteRun"](payload, options)
      yield* client["Tardigrade/ExecuteRun"](payload, options)
      const poll = yield* client.PollRun(payload, options)
      expect(Option.isSome(poll) && poll.value._tag === "Complete" && Exit.isSuccess(poll.value.exit)).toBe(true)
      const counts = yield* sql`SELECT count(*) AS count FROM proof_calls`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ count: Schema.Int })))))
      expect(counts[0]?.count).toBe(1)
      const cancelledPayload = { ...payload, key: executionKey({ ...scope, runId: "run-b" }), runId: "run-b" }
      yield* client["Tardigrade/ExecuteRunDiscard"](cancelledPayload, options)
      yield* client.InterruptRun(cancelledPayload, options)
      const cancelled = yield* Effect.repeat(client.PollRun(cancelledPayload, options), {
        until: result => Option.isSome(result) && result.value._tag === "Complete",
        schedule: Schedule.spaced("10 millis"),
      }).pipe(Effect.timeout("3 seconds"))
      expect(Option.isSome(cancelled) && cancelled.value._tag === "Complete" && Exit.isFailure(cancelled.value.exit)).toBe(true)
      const messages = yield* sql`SELECT payload, headers FROM cluster_messages`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String), headers: Schema.NullOr(Schema.String) })))))
      expect(messages.some(row => row.payload?.includes("run-control-fixture-token") || row.headers?.includes("run-control-fixture-token"))).toBe(false)
    }).pipe(Effect.provide(application))
  })))
})
