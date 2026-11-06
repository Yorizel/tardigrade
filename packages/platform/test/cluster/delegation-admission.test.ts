import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Cause, ConfigProvider, Context, Duration, Effect, Exit, Layer, Schedule } from "effect"
import { Prompt } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { HttpServer } from "effect/http"
import { Activity, Workflow, WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { OpenThread, RequestRun, CancelRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { ReserveDelegation } from "@clavia/tardigrade-domain/Delegation/Delegation.commands"
import { DEFAULT_DELEGATION_LIMITS, Reservation, delegationKey, delegationExecutionKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { RunDelegation, RunDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunDelegation.saga"
import { RunScope, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { internalApi } from "../../src/alchemy/internal"
import { DomainClientLive } from "../../src/cluster/domain-client"

const StateModel = Domain.Model("DelegationAdmissionStateTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace] })
const Child = Workflow.make("Fixture/DelegationAdmissionChild", {
  payload: { key: RunDelegation.payloadSchema.fields.key, ...RunScope.fields },
  idempotencyKey: payload => payload.key,
})
const ChildLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return Child.toLayer(Effect.fn("Fixture.delegationChild")(function* ({ threadId, runId, owner }) {
    yield* Activity.make({ name: "complete-child", execute: api.CompleteRun(threadId, { runId, text: "Child completed" }, { headers: { "x-tardigrade-owner": owner }, commandId: `${threadId}:fixture-complete` }).pipe(Effect.orDie, Effect.asVoid) })
  }))
}))
const Parent = Workflow.make("Fixture/DelegationAdmissionParent", {
  payload: RunDelegation.payloadSchema.fields,
  idempotencyKey: payload => payload.key,
  success: RunDelegation.successSchema,
})
const ParentLive = Parent.toLayer(Effect.fn("Fixture.delegationParent")(function* (payload) {
  return yield* RunDelegation.execute(payload)
}))

test("delegation admission returns actual child state and fences cancellation without accepting drift", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-delegation-admission-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const stateContext = yield* Layer.build(StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(stateContext, Thread)
  const delegations = Context.get(stateContext, Delegation)
  const tokenConfig = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ RUNNER_TO_DOMAIN_TOKEN: "delegation-admission-token" }))
  const endpoint = yield* internalApi.pipe(Effect.provide(Layer.succeedContext(stateContext)), Effect.provide(tokenConfig))
  const serverContext = yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP server")
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${server.address.port}`, RUNNER_TO_DOMAIN_TOKEN: "delegation-admission-token",
  }))
  const runner = Layer.mergeAll(RunDelegationLive, ExecuteDelegationLive, ChildLive, ParentLive).pipe(
    Layer.provide(DomainClientLive),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: join(directory, "runner.sqlite") })),
    Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  yield* Effect.gen(function* () {
    yield* threads.send("parent", new OpenThread({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", new RequestRun({ runId: "run", digest: "run", prompt: Prompt.make("Delegate") }))
    const reservation = Reservation.make({ requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "child-agent", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Complete work") })
    const id = delegationKey("parent", "run", "child")
    const payload = { key: delegationExecutionKey("alice", id), delegationId: id, owner: "alice", reservation }
    yield* RunDelegation.execute(payload, { discard: true })
    yield* Effect.repeat(threads.state(id), {
      until: state => state._tag === "Open" && state.active?.runId === "child",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    const childScope = { threadId: id, runId: "child", owner: "alice" }
    yield* Child.execute({ ...childScope, key: executionKey(childScope) }).pipe(Effect.timeout("5 seconds"))
    const completed = yield* RunDelegation.execute(payload).pipe(Effect.timeout("5 seconds"))
    expect(completed.status).toBe("completed")
    const actual = yield* delegations.state(id)
    if (actual._tag !== "Reserved") return yield* Effect.die("Expected admitted delegation")
    expect(completed).toEqual(actual)
    expect(completed.inbox.some(entry => entry.content._tag === "Result" && entry.content.outcome.receipt?.text === "Child completed")).toBe(true)
    const conflictReservation = { ...reservation, requestId: "conflict" }
    const conflictId = delegationKey("parent", "run", "conflict")
    yield* delegations.send(conflictId, new ReserveDelegation(conflictReservation))
    const before = yield* delegations.state(conflictId)
    const conflict = yield* RunDelegation.execute({ key: delegationExecutionKey("alice", conflictId), delegationId: conflictId, owner: "alice", reservation: { ...conflictReservation, targetActor: "another-child" } }).pipe(Effect.timeout("5 seconds"), Effect.exit)
    expect(Exit.isFailure(conflict)).toBe(true)
    if (Exit.isFailure(conflict)) expect(Cause.hasDies(conflict.cause)).toBe(true)
    expect(yield* delegations.state(conflictId)).toEqual(before)
    const wrongOwner = yield* RunDelegation.execute({ key: delegationExecutionKey("bob", conflictId), delegationId: conflictId, owner: "bob", reservation: conflictReservation }).pipe(Effect.timeout("5 seconds"), Effect.exit)
    expect(Exit.isFailure(wrongOwner)).toBe(true)
    if (Exit.isFailure(wrongOwner)) expect(Cause.hasDies(wrongOwner.cause)).toBe(true)
    expect(yield* delegations.state(conflictId)).toEqual(before)
    const cancelReservation = { ...reservation, requestId: "cancel" }
    const cancelId = delegationKey("parent", "run", "cancel")
    const cancelPayload = { key: delegationExecutionKey("alice", cancelId), delegationId: cancelId, owner: "alice", reservation: cancelReservation }
    const parentId = yield* Parent.execute(cancelPayload, { discard: true })
    yield* Effect.repeat(threads.state(cancelId), {
      until: state => state._tag === "Open" && state.active?.runId === "cancel",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    yield* threads.send("parent", new CancelRun({ runId: "run", reason: "User cancelled delegation parent" }))
    const engine = yield* WorkflowEngine.WorkflowEngine
    yield* engine.interruptUnsafe(Parent, parentId)
    const fenced = yield* Effect.repeat(threads.state(cancelId), {
      until: state => state._tag === "ChildReserved" && state.closed || state._tag === "Open" && state.child?.closed === true,
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect(fenced._tag === "Open" && fenced.active?.status).toBe("cancelled")
    const cancelled = yield* Effect.repeat(delegations.state(cancelId), {
      until: state => state._tag === "Reserved" && state.status === "cancelled",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect(cancelled._tag === "Reserved" && cancelled.inbox.some(entry => entry.content._tag === "Result")).toBe(true)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(runner))
}))), 20_000)
