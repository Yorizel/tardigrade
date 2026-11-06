import { RunReceiptGuardLive } from "@clavia/tardigrade-domain/RunReceiptGuard.middleware"
import { expect, test } from "bun:test"
import { fileURLToPath } from "node:url"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunServices } from "@effect/platform-bun"
import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CompleteRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { ThreadDirectory } from "@clavia/tardigrade-domain/Thread/ThreadDirectory.view"
import { OutputDeclaration } from "@clavia/tardigrade-domain/Output/Output.schemas"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Array as Arr, ConfigProvider, Console, Effect, Fiber, Layer, Option, Result, Schema, Stream } from "effect"
import { Prompt } from "effect/ai"
import { Command } from "effect/cli"
import { FetchHttpClient, Headers, HttpServer } from "effect/http"
import { cli } from "./commands"
import { sendMessage } from "./chat"

const CliModel = Domain.Model("CliBoundaryTest", { aggregates: [Thread], views: [ThreadDirectory] })
class CliApi extends Domain.Api("Tardigrade", {
  model: CliModel,
  commands: ["OpenThread", "RequestRun", "CancelRun"],
  aggregates: ["Thread"], views: ["ThreadDirectory"], streams: [],
}) {}
const authorization = { authorization: "Bearer cli-test-token" }
const session = Layer.mergeAll(RunReceiptGuardLive, ReceiptOwnerGuardLive, Layer.succeed(Session, (effect, { headers }) => {
  if (!Option.contains(Headers.get(headers, "authorization"), authorization.authorization)) return Effect.fail(Unauthorized.make())
  return effect.pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
}))
const state = CliModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory), Layer.provide(ReceiptOwnerGuardLive))
const endpoint = Layer.unwrap(Effect.gen(function* () {
  const api = yield* CliApi
  return HttpServer.serve()(api.fetch)
}))
const world = endpoint.pipe(
  Layer.provideMerge(state),
  Layer.provideMerge(session),
  Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 })),
  Layer.provideMerge(FetchHttpClient.layer),
  Layer.provideMerge(BunServices.layer),
)
const withWorld = <A, E, R>(program: Effect.Effect<A, E, R>) => Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(world)
  return yield* program.pipe(Effect.provideContext(services))
}))
const fixture = Effect.gen(function* () {
  const listener = yield* HttpServer.HttpServer
  if (listener.address._tag === "UnixPathAddress") return yield* Effect.die("Expected loopback TCP server")
  const url = `http://127.0.0.1:${listener.address.port}`
  const client = yield* ActorApi.clientHttp(url)
  const api = yield* CliApi.clientHttp(url)
  const config = ConfigProvider.fromUnknown({ TARDIGRADE_URL: url, TARDIGRADE_TOKEN: "cli-test-token", TARDIGRADE_SUBJECT: "alice" })
  return { api, client, config }
})
const input = { runId: "run", digest: "digest", prompt: Prompt.make("Literal /message") }
const open = { actor: "default", instance: "cli", name: "chat", limits: DEFAULT_RUN_LIMITS }

test("CLI process reports the HTTP status when a fork token is rejected", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const server = yield* Effect.acquireRelease(
    Effect.sync(() => Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response("Unauthorized", { status: 401 }) })),
    (server) => Effect.promise(() => server.stop(true)),
  )
  const child = yield* Effect.sync(() => Bun.spawn([process.execPath, "run", fileURLToPath(new URL("./main.ts", import.meta.url)), "fork", "--source", "source", "--target", "target", "--through", "1", "--name", "branch"], {
      env: { ...process.env, TARDIGRADE_URL: `http://127.0.0.1:${server.port}`, TARDIGRADE_TOKEN: "invalid" }, stdout: "pipe", stderr: "pipe",
  }))
  const [stdout, stderr, code] = yield* Effect.promise(() => Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]))
  expect(code).not.toBe(0)
  expect(stdout + stderr).toContain("HTTP 401")
}))))

test("native CLI client admits, observes and retries a run over authenticated HTTP", () => Effect.runPromise(withWorld(Effect.gen(function* () {
  const setup = yield* fixture
  yield* setup.api.OpenThread("chat", open, { headers: authorization })
  const threads = yield* Thread
  const complete = yield* setup.api.Thread.watch("chat", { headers: authorization }).pipe(
    Stream.filter(({ state }) => state?._tag === "Open" && state.active?.runId === "run"),
    Stream.take(1), Stream.runDrain,
    Effect.andThen(threads.send("chat", CompleteRun.make({ runId: "run", text: "Native result" }))),
    Effect.forkChild,
  )
  const receipt = yield* sendMessage("chat", input, authorization).pipe(Effect.provideService(ActorApi.Client, setup.client))
  expect(receipt.status).toBe("completed")
  expect(receipt.text).toBe("Native result")
  yield* Fiber.join(complete)
  const replay = yield* sendMessage("chat", input, authorization).pipe(Effect.provideService(ActorApi.Client, setup.client))
  expect(replay.text).toBe("Native result")
  const conflict = yield* sendMessage("chat", { ...input, digest: "different" }, authorization).pipe(Effect.provideService(ActorApi.Client, setup.client), Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure._tag).toBe("RunRejected")
  const rejected = yield* sendMessage("chat", input, { authorization: "Bearer wrong-token" }).pipe(Effect.provideService(ActorApi.Client, setup.client), Effect.result)
  expect(Result.isFailure(rejected)).toBe(true)
  if (Result.isFailure(rejected)) expect(rejected.failure._tag).toBe("Unauthorized")
}))))

test("native CLI watch replays exact cursors and directory listing exposes pagination", () => Effect.runPromise(withWorld(Effect.gen(function* () {
  const setup = yield* fixture
  yield* setup.api.OpenThread("chat", open, { headers: authorization })
  const write = yield* setup.api.OpenThread("second", { ...open, name: "second" }, { headers: authorization })
  yield* setup.api.ThreadDirectory.list("alice", { headers: authorization, atLeast: write })
  const output = yield* Effect.sync(() => new Array<string>())
  const errors = yield* Effect.sync(() => new Array<string>())
  const capture: Console.Console = Object.assign(Object.create(console), {
    log: (...parts: ReadonlyArray<unknown>) => { output.push(Arr.join(Arr.map(parts, String), " ")) },
    error: (...parts: ReadonlyArray<unknown>) => { errors.push(Arr.join(Arr.map(parts, String), " ")) },
  })
  const run = Command.runWith(cli, { version: "test", renderErrors: false })
  yield* run(["watch", "--thread", "chat", "--once"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.provideService(Console.Console, capture))
  expect(output.length).toBe(1)
  const line = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Struct({ cursor: Schema.Finite, event: Schema.Struct({ _tag: Schema.String }) })))(output[0])
  expect(line.cursor).toBe(1)
  expect(line.event._tag).toBe("ThreadOpened")
  yield* run(["watch", "--thread", "chat", "--after", "1", "--once"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.provideService(Console.Console, capture))
  expect(output.length).toBe(1)
  yield* run(["threads", "--take", "1"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.provideService(Console.Console, capture))
  expect(output.length).toBe(2)
  expect(output[1]).toContain("chat\tdefault\tcli\tchat")
  expect(errors).toEqual(["Next page: threads --after 1 --take 1"])
  yield* run(["threads", "--after", "1", "--take", "1"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.provideService(Console.Console, capture))
  expect(output[2]).toContain("second\tdefault\tcli\tsecond")
}))))

test("one-shot CLI sends literal input and retries the same native run without another execution", () => Effect.runPromise(withWorld(Effect.gen(function* () {
  const setup = yield* fixture
  yield* setup.api.OpenThread("chat", open, { headers: authorization })
  const threads = yield* Thread
  const complete = yield* setup.api.Thread.watch("chat", { headers: authorization }).pipe(
    Stream.filter(({ state }) => state?._tag === "Open" && state.active?.runId === "literal-run"),
    Stream.take(1), Stream.runDrain,
    Effect.andThen(threads.send("chat", CompleteRun.make({ runId: "literal-run", text: "Literal response" }))),
    Effect.forkChild,
  )
  const output = yield* Effect.sync(() => new Array<string>())
  const errors = yield* Effect.sync(() => new Array<string>())
  const capture: Console.Console = Object.assign(Object.create(console), {
    log: (...parts: ReadonlyArray<unknown>) => { output.push(Arr.join(Arr.map(parts, String), " ")) },
    error: (...parts: ReadonlyArray<unknown>) => { errors.push(Arr.join(Arr.map(parts, String), " ")) },
  })
  const run = Command.runWith(cli, { version: "test", renderErrors: false })
  const send = run(["--thread", "chat", "--run-id", "literal-run", "--message", "/literal message"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.provideService(Console.Console, capture))
  yield* send
  yield* Fiber.join(complete)
  yield* send
  expect(output).toEqual(["Literal response", "Literal response"])
  expect(errors).toEqual(["Thread: chat", "Thread: chat"])
  const state = yield* setup.api.Thread.get("chat", { headers: authorization })
  if (state?._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.runId).toBe("literal-run")
  expect(state.active?.input).toEqual(Prompt.make("/literal message"))
  expect(state.active?.status).toBe("completed")
  expect(state.receipts.length).toBe(0)
  const changed = yield* run(["--thread", "chat", "--run-id", "literal-run", "--message", "different"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.provideService(Console.Console, capture), Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  if (Result.isFailure(changed)) expect(changed.failure._tag).toBe("RunRejected")
}))))

test("a retry identifier without an existing thread never creates another thread", () => Effect.runPromise(withWorld(Effect.gen(function* () {
  const setup = yield* fixture
  const run = Command.runWith(cli, { version: "test", renderErrors: false })
  const rejected = run(["--run-id", "retry", "--message", "repeat"]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config), Effect.result)
  const attempts = yield* Effect.all([rejected, rejected], { concurrency: 1 })
  for (const result of attempts) {
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("ChatRejected")
      if (result.failure._tag === "ChatRejected") expect(result.failure.reason).toContain("--thread")
    }
  }
  const directory = yield* setup.api.ThreadDirectory.list("alice", { headers: authorization })
  expect(directory.entries).toEqual([])
}))))

test("native CLI rejects obsolete local runtime flags before acquiring its remote client", () => Effect.runPromise(withWorld(Effect.gen(function* () {
  const run = Command.runWith(cli, { version: "test", renderErrors: false })
  const help = yield* Effect.sync(() => new Array<string>())
  const capture: Console.Console = Object.assign(Object.create(console), {
    log: (...parts: ReadonlyArray<unknown>) => { help.push(Arr.join(Arr.map(parts, String), " ")) },
  })
  for (const flag of ["--thread-dir", "--permissions", "--active", "--poll", "--batch-size", "--json", "--width"]) {
    const result = yield* run(["--message", "must not send", flag, "obsolete"]).pipe(Effect.provideService(Console.Console, capture), Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("ShowHelp")
      if (result.failure._tag === "ShowHelp") expect(Arr.some(result.failure.errors, error => error._tag === "UnrecognizedOption")).toBe(true)
    }
  }
  expect(help.length).toBe(7)
}))))


test("one-shot CLI persists its declared output and returns the validated native result", () => Effect.runPromise(withWorld(Effect.gen(function* () {
  const setup = yield* fixture
  yield* setup.api.OpenThread("structured", open, { headers: authorization })
  const output = yield* Schema.decodeEffect(OutputDeclaration)({ name: "report", schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"], additionalProperties: false } })
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OutputDeclaration))(output)
  const threads = yield* Thread
  const complete = yield* setup.api.Thread.watch("structured", { headers: authorization }).pipe(
    Stream.filter(({ state }) => state?._tag === "Open" && state.active?.runId === "structured-run"),
    Stream.take(1), Stream.runDrain,
    Effect.andThen(threads.send("structured", CompleteRun.make({ runId: "structured-run", text: '{"answer":"Native result"}' }))),
    Effect.forkChild,
  )
  const run = Command.runWith(cli, { version: "test", renderErrors: false })
  yield* run(["--thread", "structured", "--run-id", "structured-run", "--message", "Return a report", "--output", encoded]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config))
  yield* Fiber.join(complete)
  const final = yield* setup.api.Thread.get("structured", { headers: authorization })
  if (final?._tag !== "Open" || final.active === null) return yield* Effect.die("Missing structured run")
  expect(final.active.output).toEqual(output)
  expect(final.active.result).toEqual({ answer: "Native result" })
  expect(final.lastReceipt?.result).toEqual({ answer: "Native result" })
  yield* run(["--thread", "structured", "--run-id", "structured-run", "--message", "Return a report", "--output", encoded]).pipe(Effect.provideService(ConfigProvider.ConfigProvider, setup.config))
  const replayed = yield* threads.state("structured")
  expect(replayed).toEqual(final)
}))))
