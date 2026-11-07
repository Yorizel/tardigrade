import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunServices } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { expect, test } from "bun:test"
import { Array as Arr, ConfigProvider, Context, Duration, Effect, Exit, FileSystem, Layer, Option, Path, Ref, Result, Schedule, Stream } from "effect"
import { Prompt } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpServer } from "effect/http"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { CurrentCaller, Session } from "@clavia/tardigrade-domain/Actor.context"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { delegationExecutionKey, delegationKey, DEFAULT_DELEGATION_LIMITS } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { RunBackgroundDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunBackgroundDelegation.saga"
import { ExecuteBackgroundDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteBackgroundDelegation.live"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { BeginStep, CancelBackgroundChild, ConfirmDispatch, OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { InternalSessionLive } from "../../src/alchemy/internal"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { DelegationRpcHandlers, DelegationRpcs } from "../../src/cluster/delegation-control"

const model = Domain.Model("BackgroundRecovery", { aggregates: [Thread, Delegation, CodeExecution, Workspace, SpillCatalog] })
const id = delegationKey("parent", "run", "child")
const reservation = { executionMode: "background" as const, requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "assistant", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read") }
const request = { binding: { delegationId: id, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "assistant" }, childThreadId: id, reservation, maxChildren: 1, tokens: 10, mutation: true }
const scope = { key: delegationExecutionKey("alice", id), owner: "alice", delegationId: id }
const options = { headers: { authorization: "Bearer runner-token" } }

test("background cancellation resumes after the authenticated Domain fence fails", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(BunServices.layer)
  const fs = Context.get(services, FileSystem.FileSystem)
  const path = Context.get(services, Path.Path)
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "background-recovery-" })
  const domainServer = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const runnerServer = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const domainAddress = Context.get(domainServer, HttpServer.HttpServer).address
  const runnerAddress = Context.get(runnerServer, HttpServer.HttpServer).address
  if (domainAddress._tag === "UnixPathAddress" || runnerAddress._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP listeners")
  const runnerUrl = `http://127.0.0.1:${runnerAddress.port}`
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${domainAddress.port}`, TARDIGRADE_RUNNER_URL: runnerUrl, RUNNER_TO_DOMAIN_TOKEN: "domain-token", WORKER_TO_RUNNER_TOKEN: "runner-token" }))
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const failed = yield* Ref.make(0)
  const faults = yield* Ref.make<string[]>([])
  const admissions = yield* Ref.make(0)
  const externalCalls = yield* Ref.make(0)
  const sessionContext = yield* Layer.build(InternalSessionLive.pipe(Layer.provide(config))).pipe(Effect.provide(state))
  const actualSession = Context.get(sessionContext, Session)
  const faultSession = Layer.succeed(Session, (effect, metadata) => actualSession(Effect.gen(function* () {
    if (metadata.rpc._tag === "ReserveChildThread") yield* Ref.update(admissions, count => count + 1)
    if ((metadata.rpc._tag === "StartBackgroundChild" || metadata.rpc._tag === "SettleBackgroundChild") && !(yield* Ref.get(faults)).includes(metadata.rpc._tag)) {
      yield* Ref.update(faults, tags => [...tags, metadata.rpc._tag])
      return yield* Effect.die("Injected Domain transport failure")
    }
    if (metadata.rpc._tag === "FenceChildThread" && (yield* Ref.get(failed)) === 0) {
      yield* Ref.update(failed, count => count + 1)
      return yield* Effect.die("Injected Domain fence failure")
    }
    return yield* effect
  }), metadata))
  const faultSessionContext = yield* Layer.build(faultSession)
  const apiContext = Context.add(sessionContext, Session, Context.get(faultSessionContext, Session))
  const clientContext = yield* Layer.build(ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), faultSession, ReceiptOwnerGuardLive))))
  const client = Context.get(clientContext, ActorInternalApi.Client)
  const domain = yield* ActorInternalApi.pipe(Effect.provide(Context.merge(state, apiContext)))
  yield* Layer.build(HttpServer.serve()(domain.fetch).pipe(Layer.provide(Layer.succeedContext(domainServer))))
  const database = SqliteClient.layer({ filename: path.join(directory, "runner.sqlite") })
  const runner = Layer.mergeAll(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive, DelegationRpcHandlers).pipe(
    Layer.provide(DomainClientLive),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provideMerge(database), Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  const serialization = yield* Layer.build(RpcSerialization.layerNdjson)
  const endpoint = yield* RpcServer.toHttpEffect(DelegationRpcs).pipe(Effect.provide(Context.merge(runnerContext, serialization)))
  yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provide(Layer.succeedContext(runnerServer))))
  const protocol = yield* Layer.build(RpcClient.layerProtocolHttp({ url: runnerUrl }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson))))
  const control = yield* RpcClient.make(DelegationRpcs).pipe(Effect.provide(protocol))
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "parent", instance: "parent", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "parent", prompt: Prompt.make("Parent") }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const unauthorized = yield* control.ResumeBackgroundDelegation(scope, { headers: { authorization: "Bearer wrong" } }).pipe(Effect.result)
  expect(Result.isFailure(unauthorized)).toBe(true)
  const invalid = yield* control.ResumeBackgroundDelegation({ ...scope, key: "wrong" }, options).pipe(Effect.exit)
  expect(Exit.isFailure(invalid)).toBe(true)
  yield* control["Tardigrade/RunBackgroundDelegationDiscard"]({ key: scope.key, owner: scope.owner, request }, options).pipe(Effect.timeout("5 seconds"))
  yield* Effect.repeat(control.PollBackgroundDelegation(scope, options), { until: value => Option.isSome(value) && value.value._tag === "Suspended" && value.value.failed === true, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  const firstAdmissions = yield* Ref.get(admissions)
  expect(firstAdmissions).toBe(1)
  yield* control.ResumeBackgroundDelegation(scope, options)
  yield* Effect.repeat(threads.state(id), { until: child => child._tag === "Open" && child.active?.runId === "child", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  yield* threads.send(id, BeginStep.make({ runId: "child", stepId: "child:1", kind: "tool", mutation: true, requiresApproval: false })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* threads.send(id, ConfirmDispatch.make({ runId: "child", stepId: "child:1" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* Ref.update(externalCalls, count => count + 1)
  yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: id, reason: "Stop" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const suspended = yield* Effect.repeat(control.PollBackgroundDelegation(scope, options), { until: value => Option.isSome(value) && value.value._tag === "Suspended" && value.value.failed === true, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  expect(Option.isSome(suspended) && suspended.value._tag === "Suspended" && suspended.value.failed).toBe(true)
  const failedCount = yield* Ref.get(failed)
  expect(failedCount).toBe(1)
  const before = yield* threads.state("parent")
  expect(before._tag === "Open" && before.background[0]?.status).toBe("cancel_requested")
  const step = BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: true, requiresApproval: false })
  const blocked = yield* threads.send("parent", step).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
  expect(Result.isFailure(blocked) && blocked.failure._tag === "RunRejected" && blocked.failure.reason).toBe("UncertainChild")
  yield* control.ResumeBackgroundDelegation(scope, options)
  yield* Effect.repeat(Ref.get(faults), { until: tags => tags.includes("SettleBackgroundChild"), schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  yield* Effect.repeat(control.PollBackgroundDelegation(scope, options), { until: value => Option.isSome(value) && value.value._tag === "Suspended" && value.value.failed === true, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  yield* control.ResumeBackgroundDelegation(scope, options)
  const complete = yield* Effect.repeat(control.PollBackgroundDelegation(scope, options), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  expect(Option.isSome(complete) && complete.value._tag === "Complete" && Exit.isSuccess(complete.value.exit)).toBe(true)
  const parent = yield* threads.state("parent")
  const child = yield* threads.state(id)
  expect(parent._tag === "Open" && parent.background).toMatchObject([{ status: "unknown", receipt: { runId: "child", status: "unknown" } }])
  expect(child._tag === "Open" && child.child?.closed).toBe(true)
  expect(child._tag === "Open" && child.active?.status).toBe("unknown")
  expect(parent._tag === "Open" && parent.background).toHaveLength(1)
  const stillBlocked = yield* threads.send("parent", step).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
  expect(Result.isFailure(stillBlocked) && stillBlocked.failure._tag === "RunRejected" && stillBlocked.failure.reason).toBe("UncertainChild")
  const finalFailedCount = yield* Ref.get(failed)
  expect(finalFailedCount).toBe(1)
  const admissionCount = yield* Ref.get(admissions)
  expect(admissionCount).toBe(2)
  const history = yield* client.Thread.events(id, { headers: { authorization: "Bearer domain-token", "x-tardigrade-owner": "alice" } }).pipe(Stream.takeUntil(Domain.Read.isCaughtUp), Stream.filter(Domain.Read.isEvent), Stream.runCollect)
  expect(Arr.filter(history, frame => frame.event._tag === "ChildAdmissionReserved")).toHaveLength(1)
  const externalCallCount = yield* Ref.get(externalCalls)
  expect(externalCallCount).toBe(1)
  const finalFaults = yield* Ref.get(faults)
  expect(finalFaults).toEqual(["StartBackgroundChild", "SettleBackgroundChild"])
  return undefined
}))), 20_000)
