import { Command } from "effect/cli"
import { cli } from "../../../apps/cli/src/commands"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunServices } from "@effect/platform-bun"
import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { captureFork } from "@clavia/tardigrade-domain/Fork/Fork.capture"
import { ForkPolicy, ForkRejected, ForkRequest, ForkResult } from "@clavia/tardigrade-domain/Fork/Fork.schemas"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { ThreadDirectory } from "@clavia/tardigrade-domain/Thread/ThreadDirectory.view"
import { CompleteRun, InitializeFork, OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Array as Arr, Console, ConfigProvider, Context, Effect, Layer, Ref, Schema, Scope } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http"
import { forkApi } from "../../src/alchemy/fork"
import { SessionLive } from "../../src/alchemy/session"

const model = Domain.Model("ForkHttpTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog], views: [ThreadDirectory] })
const state = model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "fork-test-token", TARDIGRADE_SUBJECT: "alice" }))
const world = ActorApi.layerClientInProcess.pipe(
  Layer.provide(SessionLive), Layer.provideMerge(state), Layer.provideMerge(config), Layer.provideMerge(Digest.layerWebCrypto), Layer.provideMerge(FetchHttpClient.layer), Layer.provideMerge(BunServices.layer),
)
const run = <A, E>(program: Effect.Effect<A, E, Layer.Success<typeof world> | CurrentCaller | Scope.Scope>) => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(world)
  return yield* program.pipe(Effect.provideContext(services), Effect.provideService(CurrentCaller, { subject: "alice" }))
})))
const headers = { authorization: "Bearer fork-test-token" }
const open = { actor: "agent", instance: "i", name: "source", limits: DEFAULT_RUN_LIMITS }
const input = { sourceThreadId: "source", targetThreadId: "target", throughSeq: 1, name: "branch" }
const serve = Effect.fn("ForkHttp.serve")(function* (app: Effect.Success<typeof forkApi>) {
  const services = yield* Layer.build(HttpServer.serve()(Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    if (new URL(request.url, "http://localhost").pathname !== "/fork") return HttpServerResponse.empty({ status: 404 })
    return yield* app
  })).pipe(Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))))
  const server = Context.get(services, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP server")
  return `http://127.0.0.1:${server.address.port}/fork`
})
const post = Effect.fn("ForkHttp.post")(function* (url: string, request: typeof ForkRequest.Type) {
  const client = yield* HttpClient.HttpClient
  const body = yield* HttpClientRequest.post(url).pipe(HttpClientRequest.setHeaders(headers), HttpClientRequest.schemaBodyJson(ForkRequest)(request))
  return yield* client.execute(body)
})

test("authenticated HTTP forks an exact historical prefix and retries after destination progress", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("source", OpenThread.make(open))
  yield* threads.send("source", RequestRun.make({ runId: "one", digest: "one", prompt: Prompt.make("Historical question") }))
  const checkpoint = yield* threads.send("source", CompleteRun.make({ runId: "one", text: "answer" }))
  yield* threads.send("source", RequestRun.make({ runId: "two", digest: "two", prompt: Prompt.make("Future question") }))
  yield* threads.send("source", CompleteRun.make({ runId: "two", text: "later" }))
  const app = yield* forkApi
  const url = yield* serve(app)
  const request = { ...input, throughSeq: checkpoint.version }
  const response = yield* post(url, request)
  expect(response.status).toBe(200)
  const receipt = yield* HttpClientResponse.schemaBodyJson(ForkResult)(response)
  expect(receipt.origin.throughSeq).toBe(checkpoint.version)
  const fork = yield* threads.state("target")
  if (fork._tag !== "Open") return yield* Effect.die("Expected native fork")
  expect(fork.history).toEqual(Prompt.make("Historical question"))
  expect(fork.active).toBeNull()
  expect(fork.child).toBeNull()
  yield* threads.send("target", RequestRun.make({ runId: "branch", digest: "branch", prompt: Prompt.make("New work") }))
  yield* threads.send("target", CompleteRun.make({ runId: "branch", text: "new answer" }))
  const replay = yield* post(url, request)
  expect(replay.status).toBe(200)
  const repeated = yield* HttpClientResponse.schemaBodyJson(ForkResult)(replay)
  expect(repeated).toEqual(receipt)
  const conflict = yield* post(url, { ...request, name: "other" })
  expect(conflict.status).toBe(409)
  const rejection = yield* HttpClientResponse.schemaBodyJson(ForkRejected)(conflict)
  expect(rejection.reason).toBe("TargetOccupied")
  expect(Arr.map(ActorApi.commands, command => command.tag)).not.toContain("InitializeFork")
  expect(Arr.map(ActorInternalApi.commands, command => command.tag)).not.toContain("InitializeFork")
  return undefined
})))

test("authentication, owner checks and input schemas run before destination writes", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("source", OpenThread.make(open))
  yield* threads.send("foreign", OpenThread.make(open)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const app = yield* forkApi
  const url = yield* serve(app)
  const client = yield* HttpClient.HttpClient
  const request = yield* HttpClientRequest.post(url).pipe(HttpClientRequest.schemaBodyJson(ForkRequest)(input))
  const unsigned = yield* client.execute(request)
  expect(unsigned.status).toBe(401)
  const foreign = yield* post(url, { ...input, sourceThreadId: "foreign" })
  expect(foreign.status).toBe(403)
  const unknown = yield* post(url, { ...input, sourceThreadId: "missing" })
  expect(unknown.status).toBe(403)
  const unsupported = yield* client.get(url)
  expect(unsupported.status).toBe(405)
  const malformed = yield* client.execute(HttpClientRequest.post(url).pipe(HttpClientRequest.setHeaders(headers), HttpClientRequest.bodyText("{", "application/json")))
  expect(malformed.status).toBe(400)
  const forged = yield* client.execute(HttpClientRequest.post(url).pipe(HttpClientRequest.setHeaders(headers), HttpClientRequest.bodyText('{"sourceThreadId":"source","targetThreadId":"target","throughSeq":1,"name":"branch","snapshot":{"owner":"bob"}}', "application/json")))
  expect(forged.status).toBe(400)
  const forgedOwner = yield* client.execute(HttpClientRequest.post(url).pipe(HttpClientRequest.setHeaders(headers), HttpClientRequest.bodyText('{"sourceThreadId":"source","targetThreadId":"target","throughSeq":1,"name":"branch","owner":"bob"}', "application/json")))
  expect(forgedOwner.status).toBe(400)
  const absent = yield* threads.state("target")
  expect(absent._tag).toBe("Unopened")
})))

test("rejected checkpoints and occupied destinations leave their state unchanged", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("source", OpenThread.make(open))
  yield* threads.send("source", RequestRun.make({ runId: "pending", digest: "pending", prompt: Prompt.make("Pending question") }))
  yield* threads.send("owned", OpenThread.make(open))
  yield* threads.send("foreign-target", OpenThread.make(open)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const app = yield* forkApi
  const url = yield* serve(app)
  const incomplete = yield* post(url, { ...input, throughSeq: 2 })
  expect(incomplete.status).toBe(409)
  expect((yield* HttpClientResponse.schemaBodyJson(ForkRejected)(incomplete)).reason).toBe("CheckpointNeedsDetachment")
  const unknown = yield* post(url, { ...input, throughSeq: 3 })
  expect(unknown.status).toBe(409)
  expect((yield* HttpClientResponse.schemaBodyJson(ForkRejected)(unknown)).reason).toBe("UnknownCheckpoint")
  const same = yield* post(url, { ...input, targetThreadId: "source" })
  expect(same.status).toBe(409)
  expect((yield* HttpClientResponse.schemaBodyJson(ForkRejected)(same)).reason).toBe("SameThread")
  const occupied = yield* post(url, { ...input, targetThreadId: "owned" })
  expect(occupied.status).toBe(409)
  expect((yield* HttpClientResponse.schemaBodyJson(ForkRejected)(occupied)).reason).toBe("TargetOccupied")
  const foreign = yield* post(url, { ...input, targetThreadId: "foreign-target" })
  expect(foreign.status).toBe(403)
  expect((yield* threads.state("target"))._tag).toBe("Unopened")
  const owned = yield* threads.state("owned")
  const foreignTarget = yield* threads.state("foreign-target")
  if (owned._tag !== "Open" || foreignTarget._tag !== "Open") return yield* Effect.die("Expected occupied threads")
  expect(owned.fork).toBeUndefined()
  expect(foreignTarget.fork).toBeUndefined()
  expect(foreignTarget.owner).toBe("bob")
  return undefined
})))

test("configured event bound rejects before creating the target", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("source", OpenThread.make(open))
  const policy = { maxEvents: 1, maxPrefixBytes: 10000, maxSnapshotBytes: 10000, timeoutMs: 1000 }
  const policyJson = yield* Schema.encodeEffect(Schema.fromJsonString(ForkPolicy))(policy)
  const policyConfig = ConfigProvider.fromUnknown({ TARDIGRADE_FORK_POLICY: policyJson })
  const app = yield* forkApi.pipe(Effect.provideService(ConfigProvider.ConfigProvider, policyConfig))
  const url = yield* serve(app)
  const response = yield* post(url, { ...input, throughSeq: 2 })
  expect(response.status).toBe(409)
  const rejection = yield* HttpClientResponse.schemaBodyJson(ForkRejected)(response)
  expect(rejection.reason).toBe("EventBudgetExceeded")
  expect(rejection.policy).toEqual(policy)
  expect((yield* threads.state("target"))._tag).toBe("Unopened")
})))

test.each(["alice", "bob"] as const)("receipt lookup cannot bypass a concurrent target claim by %s", owner => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ActorApi.Client
  yield* threads.send("source", OpenThread.make(open))
  const snapshot = yield* captureFork(input, api.Thread.events("source", { headers }))
  const command = InitializeFork.make({ snapshot })
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(InitializeFork))(command)
  const digest = yield* Digest.Digest
  const commandId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(encoded))
  const claimed = yield* Ref.make(false)
  const interleaved = {
    ...threads,
    state: Effect.fn("ForkHttp.claimDuringRead")(function* (id: string) {
      const before = yield* threads.state(id)
      if (id === "target" && before._tag === "Unopened") {
        yield* Ref.set(claimed, true)
        yield* threads.send(id, OpenThread.make(open), { commandId }).pipe(Effect.provideService(CurrentCaller, { subject: owner }))
      }
      return before
    }),
  }
  const app = yield* forkApi.pipe(Effect.provideService(Thread, interleaved))
  const url = yield* serve(app)
  const response = yield* post(url, input)
  const expected = (() => {
    if (owner === "alice") return 409
    return 403
  })()
  expect(response.status).toBe(expected)
  const observed = yield* Ref.get(claimed)
  expect(observed).toBe(true)
  const target = yield* threads.state("target")
  if (target._tag !== "Open") return yield* Effect.die("Expected concurrent claim")
  expect(target.owner).toBe(owner)
  expect(target.fork).toBeUndefined()
  expect(target.history).toEqual(Prompt.empty)
  return undefined
})))


test("the native CLI forks over HTTP with a trailing-slash URL and prints its receipt", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("source", OpenThread.make(open))
  const app = yield* forkApi
  const url = yield* serve(app)
  const output = Arr.empty<string>()
  const console = yield* Console.Console
  const captured: Console.Console = Object.assign(Object.create(console), { log: (...parts: ReadonlyArray<unknown>) => { output.push(Arr.join(Arr.map(parts, String), " ")) } })
  const command = Command.runWith(cli, { version: "test", renderErrors: false })
  const cliConfig = ConfigProvider.fromUnknown({ TARDIGRADE_URL: new URL("/", url).toString(), TARDIGRADE_TOKEN: "fork-test-token" })
  yield* command(["fork", "--source", "source", "--target", "cli-target", "--through", "1", "--name", "cli-branch"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, cliConfig), Effect.provideService(Console.Console, captured))
  expect(output).toHaveLength(1)
  const receipt = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ForkResult))(output[0])
  expect(receipt.threadId).toBe("cli-target")
  expect(receipt.origin.sourceThreadId).toBe("source")
  const target = yield* threads.state("cli-target")
  if (target._tag !== "Open") return yield* Effect.die("Expected CLI fork")
  expect(target.name).toBe("cli-branch")
  return undefined
})))
