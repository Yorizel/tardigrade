import * as Domain from "@alchemy.run/domain"
import { BunHttpServer } from "@effect/platform-bun"
import { expect, test } from "bun:test"
import { ConfigProvider, Context, Deferred, Effect, Layer, Ref, Schedule } from "effect"
import { Prompt } from "effect/ai"
import { HttpServer } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { WorkflowEngine, WorkflowProxyServer } from "effect/workflow"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { DelegationExecutionPolicy } from "@clavia/tardigrade-domain/Delegation/DelegationExecution.policy"
import { ReserveDelegation, RequestDelegationCancellation } from "@clavia/tardigrade-domain/Delegation/Delegation.commands"
import { DEFAULT_DELEGATION_LIMITS, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { ExecuteDelegation } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { DelegationExecutionPolicyLive } from "../../src/alchemy/delegation-policy"
import { DelegationControlLive, DelegationControlSessionLive, DelegationRpcs } from "../../src/cluster/delegation-control"

const model = Domain.Model("BackgroundPolicyTest", { aggregates: [Delegation], policies: [DelegationExecutionPolicy] })

test("policy routes foreground events without launching background reservations", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const serverContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP listener")
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${server.address.port}`, WORKER_TO_RUNNER_TOKEN: "test-token" }))
  const calls = yield* Ref.make(0)
  const release = yield* Deferred.make<void>()
  const execution = ExecuteDelegation.toLayer(Effect.fn("BackgroundPolicyTest.execute")(function* () {
    yield* Ref.update(calls, count => count + 1)
    yield* Deferred.await(release)
  }))
  const handlers = Layer.mergeAll(WorkflowProxyServer.layerRpcHandlers([ExecuteDelegation]), DelegationControlLive, DelegationControlSessionLive).pipe(Layer.provideMerge(execution), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(config))
  const rpcContext = yield* Layer.build(handlers)
  const serialization = yield* Layer.build(RpcSerialization.layerNdjson)
  const endpoint = yield* RpcServer.toHttpEffect(DelegationRpcs).pipe(Effect.provide(Context.merge(rpcContext, serialization)))
  yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provide(Layer.succeedContext(serverContext))))
  const backgroundEvents = yield* Ref.make(0)
  const foregroundEvents = yield* Ref.make(0)
  const policy = Layer.effect(DelegationExecutionPolicy, Effect.gen(function* () {
    const actual = yield* DelegationExecutionPolicy
    return Effect.fn("BackgroundPolicyTest.retry")(function* (input: Parameters<Context.Service.Shape<typeof DelegationExecutionPolicy>>[0]) {
      yield* actual(input)
      if (input.source.id === delegationKey("parent", "run", "background")) {
        yield* actual(input)
        yield* Ref.update(backgroundEvents, count => count + 1)
      } else {
        yield* Ref.update(foregroundEvents, count => count + 1)
      }
    })
  })).pipe(Layer.provide(DelegationExecutionPolicyLive))
  const state = yield* Layer.build(model.layer(policy).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const delegations = Context.get(state, Delegation)
  const reservation = { parentThreadId: "parent", parentRunId: "run", targetActor: "assistant", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read") }
  const background = ReserveDelegation.make({ ...reservation, requestId: "background", executionMode: "background" })
  const foreground = ReserveDelegation.make({ ...reservation, requestId: "foreground" })
  const backgroundId = delegationKey("parent", "run", "background")
  const foregroundId = delegationKey("parent", "run", "foreground")
  yield* Effect.gen(function* () {
    yield* delegations.send(backgroundId, background)
    yield* delegations.send(backgroundId, background)
    yield* delegations.send(backgroundId, RequestDelegationCancellation.make({ reason: "cancel" }))
    yield* delegations.send(foregroundId, foreground)
    yield* delegations.send(foregroundId, foreground)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* Effect.repeat(Ref.get(backgroundEvents), { until: count => count === 2, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeoutOption("3 seconds"))
  yield* Effect.repeat(Ref.get(calls), { until: count => count === 1, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeoutOption("3 seconds"))
  expect({ backgroundEvents: yield* Ref.get(backgroundEvents), calls: yield* Ref.get(calls) }).toEqual({ backgroundEvents: 2, calls: 1 })
  yield* Deferred.succeed(release, undefined)
  yield* Effect.repeat(Ref.get(foregroundEvents), { until: count => count === 1, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("3 seconds"))
}))), 10_000)
