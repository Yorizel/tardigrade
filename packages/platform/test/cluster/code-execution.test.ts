import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Internals } from "@alchemy.run/domain/Model"
import { BunFileSystem, BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Array as Arr, ConfigProvider, Context, Deferred, Duration, Effect, Exit, FileSystem, Layer, Option, Order, Ref, Schedule } from "effect"
import { Prompt } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { HttpServer } from "effect/http"
import { DurableDeferred } from "effect/workflow"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { OpenThread, RequestRun, CancelRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { RequestCode, ApproveCodeCall, CancelCode, PrepareCodeCall, DispatchCodeCall } from "@clavia/tardigrade-domain/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeApproval, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { internalApi } from "../../src/alchemy/internal"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const StateModel = Domain.Model("NativeCodeModeExecutionStateTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const catalog = { tools: ["slow", "fast", "after", "write", "fail", "hold", "blocked"] }

test("native CodeMode uses the Domain journal for concurrent replay, approval and cancellation", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-code-execution-" })
  const stateContext = yield* Layer.build(StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(stateContext, Thread)
  const codes = Context.get(stateContext, CodeExecution)
  const tokenConfig = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ RUNNER_TO_DOMAIN_TOKEN: "code-fixture-token" }))
  const endpointContext = yield* Layer.build(Layer.merge(Layer.succeedContext(stateContext), tokenConfig))
  const endpoint = yield* internalApi.pipe(Effect.provide(endpointContext))
  const serverContext = yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP server")
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${server.address.port}`, RUNNER_TO_DOMAIN_TOKEN: "code-fixture-token",
  }))
  const slow = yield* Deferred.make<void>()
  const hold = yield* Deferred.make<void>()
  const blocked = yield* Deferred.make<void>()
  const invoked = yield* Ref.make<ReadonlyArray<string>>([])
  const methods = CodeMethods.toLayer(Effect.succeed({
    catalog: () => Effect.succeed(catalog),
    plan: Effect.fn("CodeFixture.plan")(({ call }) => Effect.succeed({ ...call, mutation: call.method === "write", requiresApproval: call.method === "write" })),
    invoke: Effect.fn("CodeFixture.invoke")(function* ({ call }) {
      yield* Ref.update(invoked, values => [...values, call.method])
      if (call.method === "blocked") {
        yield* Deferred.await(blocked)
        return { _tag: "Returned" as const, value: "blocked" }
      }
      if (call.method === "hold") {
        yield* Deferred.await(hold)
        return { _tag: "Returned" as const, value: "held" }
      }
      if (call.method === "write" && call.input === "mixed") yield* Deferred.succeed(hold, undefined)
      if (call.method === "fail") return { _tag: "Failed" as const, reason: "Fixture rejection" }
      if (call.method === "slow") {
        yield* Deferred.await(slow)
        return { _tag: "Returned" as const, value: "slow" }
      }
      if (call.method === "after") {
        if (call.input === undefined) return yield* Effect.die("Expected continuation input")
        const pending = yield* codes.state("race")
        expect(pending._tag === "Open" && pending.calls[0]?.status === "dispatched").toBe(true)
        yield* Deferred.succeed(slow, undefined)
        return { _tag: "Returned" as const, value: { after: call.input } }
      }
      return { _tag: "Returned" as const, value: call.method }
    }),
  }))
  const runner = ExecuteCodeLive.pipe(
    Layer.provide(DomainClientLive), Layer.provide(CodeRuntimeLive), Layer.provide(methods),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: `${directory}/runner.sqlite` })),
    Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  yield* Effect.gen(function* () {
    const context = yield* Effect.context<never>()
    expect(Option.isNone(Context.getOption(context, Internals))).toBe(true)
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Execute code") }))
    const request = { threadId: "parent", runId: "run", digest: "race", code: "const winner = await Promise.race([tools.slow(), tools.fast()]); console.log('winner', winner); return { winner, next: await tools.after(winner) };", catalog, ambient: { at: 123, seed: "race" }, limits: DEFAULT_CODE_LIMITS }
    yield* codes.send("race", RequestCode.make(request))
    const racePayload = { key: codeExecutionKey("alice", "race"), executionId: "race", owner: "alice" }
    yield* ExecuteCode.execute(racePayload, { discard: true })
    const race = yield* ExecuteCode.execute(racePayload).pipe(Effect.timeout("5 seconds"))
    expect(race.outcome).toEqual({ _tag: "Returned", value: { winner: "fast", next: { after: "fast" } } })
    yield* ExecuteCode.execute(racePayload)
    const initiallyInvoked = yield* Ref.get(invoked)
    expect(initiallyInvoked).toEqual(["slow", "fast", "after"])
    const journal = yield* codes.state("race")
    expect(journal._tag === "Open" && journal.calls[1]?.settlementSequence).toBe(0)
    expect(journal._tag === "Open" && Arr.sort(Arr.map(journal.calls, call => String(call.settlementSequence)), Order.String)).toEqual(["0", "1", "2"])
    expect(journal._tag === "Open" && journal.logs).toEqual(["winner fast"])
    yield* codes.send("approved", RequestCode.make({ ...request, digest: "approved", code: "return await tools.write({ value: 42 });" }))
    const approvedPayload = { key: codeExecutionKey("alice", "approved"), executionId: "approved", owner: "alice" }
    const approvedId = yield* ExecuteCode.execute(approvedPayload, { discard: true })
    yield* Effect.repeat(codes.state("approved"), {
      until: state => state._tag === "Open" && state.calls.length === 1,
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect((yield* Ref.get(invoked)).includes("write")).toBe(false)
    yield* codes.send("approved", ApproveCodeCall.make({ ordinal: 0 }))
    const deferred = codeApproval(0)
    yield* DurableDeferred.succeed(deferred, { token: DurableDeferred.tokenFromExecutionId(deferred, { workflow: ExecuteCode, executionId: approvedId }), value: undefined })
    expect((yield* ExecuteCode.execute(approvedPayload).pipe(Effect.timeout("5 seconds"))).outcome).toEqual({ _tag: "Returned", value: "write" })
    yield* codes.send("cancelled", RequestCode.make({ ...request, digest: "cancelled", code: "return await tools.write();" }))
    const cancelPayload = { key: codeExecutionKey("alice", "cancelled"), executionId: "cancelled", owner: "alice" }
    const cancelId = yield* ExecuteCode.execute(cancelPayload, { discard: true })
    yield* Effect.repeat(codes.state("cancelled"), {
      until: state => state._tag === "Open" && state.calls.length === 1,
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    yield* codes.send("cancelled", CancelCode.make({ reason: "User cancelled" }))
    yield* DurableDeferred.succeed(deferred, { token: DurableDeferred.tokenFromExecutionId(deferred, { workflow: ExecuteCode, executionId: cancelId }), value: undefined })
    expect((yield* ExecuteCode.execute(cancelPayload).pipe(Effect.timeout("5 seconds"))).outcome._tag).toBe("Failed")
    const cancelled = yield* codes.state("cancelled")
    expect(cancelled._tag === "Open" && cancelled.status).toBe("cancelled")
    const invoked1 = yield* Ref.get(invoked)
    expect(Arr.filter(invoked1, method => method === "write").length).toBe(1)
    yield* codes.send("uncertain", RequestCode.make({ ...request, digest: "uncertain", code: "return await Promise.all([tools.write(), tools.fast()]);" }))
    yield* codes.send("uncertain", PrepareCodeCall.make({ ordinal: 0, package: "tools", method: "write", mutation: true, requiresApproval: false }))
    yield* codes.send("uncertain", DispatchCodeCall.make({ ordinal: 0 }))
    yield* codes.send("uncertain", PrepareCodeCall.make({ ordinal: 1, package: "tools", method: "fast", mutation: false, requiresApproval: false }))
    const uncertain = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "uncertain"), executionId: "uncertain", owner: "alice" }).pipe(Effect.timeout("5 seconds"))
    expect(uncertain.outcome._tag).toBe("Failed")
    const unknown = yield* codes.state("uncertain")
    expect(unknown._tag === "Open" && unknown.status).toBe("unknown")
    const invoked2 = yield* Ref.get(invoked)
    expect(Arr.filter(invoked2, method => method === "write").length).toBe(1)
    yield* codes.send("caught", RequestCode.make({ ...request, digest: "caught", code: "try { await tools.fail(); } catch (error) { return 'caught'; }" }))
    const caught = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "caught"), executionId: "caught", owner: "alice" }).pipe(Effect.timeout("5 seconds"))
    expect(caught.outcome).toEqual({ _tag: "Returned", value: "caught" })
    const invoked3 = yield* Ref.get(invoked)
    expect(Arr.filter(invoked3, method => method === "fast").length).toBe(1)
    yield* codes.send("mixed", RequestCode.make({ ...request, digest: "mixed", code: "const held = tools.hold(); const written = await tools.write('mixed'); return [await held, written];" }))
    const mixedPayload = { key: codeExecutionKey("alice", "mixed"), executionId: "mixed", owner: "alice" }
    const mixedId = yield* ExecuteCode.execute(mixedPayload, { discard: true })
    yield* Effect.repeat(codes.state("mixed"), {
      until: state => state._tag === "Open" && state.calls[0]?.status === "dispatched" && state.calls[1]?.status === "prepared",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    const invoked4 = yield* Ref.get(invoked)
    expect(Arr.filter(invoked4, method => method === "write").length).toBe(1)
    yield* codes.send("mixed", ApproveCodeCall.make({ ordinal: 1 }))
    const mixedApproval = codeApproval(1)
    yield* DurableDeferred.succeed(mixedApproval, { token: DurableDeferred.tokenFromExecutionId(mixedApproval, { workflow: ExecuteCode, executionId: mixedId }), value: undefined })
    expect((yield* ExecuteCode.execute(mixedPayload).pipe(Effect.timeout("5 seconds"))).outcome).toEqual({ _tag: "Returned", value: ["held", "write"] })
    yield* codes.send("live-cancel", RequestCode.make({ ...request, digest: "live-cancel", code: "const held = tools.blocked(); const written = tools.write('cancelled'); return [await held, await written];" }))
    const liveCancelPayload = { key: codeExecutionKey("alice", "live-cancel"), executionId: "live-cancel", owner: "alice" }
    yield* ExecuteCode.execute(liveCancelPayload, { discard: true })
    yield* Effect.repeat(codes.state("live-cancel"), {
      until: state => state._tag === "Open" && state.calls[0]?.status === "dispatched" && state.calls[1]?.status === "prepared",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    yield* codes.send("live-cancel", CancelCode.make({ reason: "Cancel while host call is live" }))
    expect((yield* ExecuteCode.execute(liveCancelPayload).pipe(Effect.timeout("5 seconds"))).outcome._tag).toBe("Failed")
    const invoked5 = yield* Ref.get(invoked)
    expect(Arr.filter(invoked5, method => method === "write").length).toBe(2)
    for (const scenario of [{ id: "parent-approval", source: "return await tools.write();", expected: "prepared" }, { id: "parent-live", source: "return await tools.blocked();", expected: "dispatched" }]) {
      yield* threads.send(scenario.id, OpenThread.make({ actor: "agent", instance: "instance", name: scenario.id, limits: DEFAULT_RUN_LIMITS }))
      yield* threads.send(scenario.id, RequestRun.make({ runId: "parent-run", digest: scenario.id, prompt: Prompt.make("Cancel parent") }))
      yield* codes.send(scenario.id, RequestCode.make({ ...request, threadId: scenario.id, runId: "parent-run", digest: scenario.id, code: scenario.source }))
      const payload = { key: codeExecutionKey("alice", scenario.id), executionId: scenario.id, owner: "alice" }
      yield* ExecuteCode.execute(payload, { discard: true })
      yield* Effect.repeat(codes.state(scenario.id), {
        until: state => state._tag === "Open" && state.calls[0]?.status === scenario.expected,
        schedule: Schedule.spaced("10 millis"),
      }).pipe(Effect.timeout("5 seconds"))
      yield* threads.send(scenario.id, CancelRun.make({ runId: "parent-run", reason: "Parent cancelled without code signal" }))
      expect((yield* ExecuteCode.execute(payload).pipe(Effect.timeout("5 seconds"))).outcome._tag).toBe("Failed")
      const cancelledByParent = yield* codes.state(scenario.id)
      expect(cancelledByParent._tag === "Open" && cancelledByParent.status).toBe("cancelled")
    }
    const before = yield* codes.state("race")
    const forbidden = yield* ExecuteCode.execute({ key: codeExecutionKey("bob", "race"), executionId: "race", owner: "bob" }).pipe(Effect.exit)
    expect(Exit.isFailure(forbidden)).toBe(true)
    const after = yield* codes.state("race")
    expect(after).toEqual(before)
    yield* codes.send("parent-terminal", RequestCode.make({ ...request, digest: "parent-terminal", code: "return await tools.write();" }))
    yield* threads.send("parent", CancelRun.make({ runId: "run", reason: "Parent cancelled" }))
    const stopped = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "parent-terminal"), executionId: "parent-terminal", owner: "alice" })
    expect(stopped.outcome._tag).toBe("Failed")
    const stoppedState = yield* codes.state("parent-terminal")
    expect(stoppedState._tag === "Open" && stoppedState.status).toBe("cancelled")
    const invoked6 = yield* Ref.get(invoked)
    expect(Arr.filter(invoked6, method => method === "write").length).toBe(2)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(runnerContext))
})).pipe(Effect.provide(BunFileSystem.layer))), 20_000)
