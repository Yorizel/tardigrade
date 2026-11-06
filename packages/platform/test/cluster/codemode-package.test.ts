import { expect, test } from "bun:test"
import { createRequire } from "node:module"
import { CodeMode, Tool } from "@opencode/codemode"
import { Array as Arr, Context, Deferred, Effect, Fiber, Schema } from "effect"
import { projectPatchesFrom } from "../../../../apps/cli/src/version"
import { publishDependencies, publishSources } from "../../../../tools/publish-manifest"

class Greeting extends Context.Service<Greeting, { readonly prefix: string }>()("@clavia/tardigrade-platform/test/cluster/codemode-package.test/Greeting") {}

test("native tool executes with the root Effect service and returns JSON", () => {
  const tool = Tool.make({
    description: "Greet a person",
    input: Schema.Struct({ name: Schema.String }),
    output: Schema.Struct({ message: Schema.String }),
    execute: Effect.fn("greet")(function* ({ name }) {
      const greeting = yield* Greeting
      return { message: `${greeting.prefix} ${name}` }
    }),
  })
  return Effect.runPromise(CodeMode.execute({
    tools: { greet: tool },
    code: 'return await tools.greet({ name: "Ada" })',
  }).pipe(Effect.provideService(Greeting, { prefix: "Hello" }), Effect.tap(result => Effect.sync(() => {
    expect(result.ok).toBe(true)
    if (result.ok) expect(result.value).toEqual({ message: "Hello Ada" })
  }))))
})

test("interrupting execution interrupts the native tool", () => Effect.runPromise(Effect.gen(function* () {
    const started = yield* Deferred.make<void>()
    const stopped = yield* Deferred.make<void>()
    const tool = Tool.make({
      description: "Wait for interruption",
      input: Schema.Struct({}),
      execute: () => Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Deferred.succeed(stopped, undefined)),
      ),
    })
    const fiber = yield* Effect.forkChild(CodeMode.execute({ tools: { wait: tool }, code: "await tools.wait({})" }))
    yield* Deferred.await(started)
    yield* Fiber.interrupt(fiber)
    const finalized = yield* Deferred.isDone(stopped)
    expect(finalized).toBe(true)
})))

test("publication and generated projects retain the SDK patch and Effect override", () => Effect.runPromise(Effect.gen(function* () {
  const dependencies = publishDependencies(Arr.map(publishSources, source => source.pkg)).dependencies
  const patches = yield* Effect.promise(() => projectPatchesFrom(new URL("../../../../apps/cli/src/init.ts", import.meta.url).href))
  expect(dependencies["@opencode/codemode"]).toBe("2.2.5")
  expect(patches.patchedDependencies["@opencode/codemode@2.2.5"]).toBeDefined()
  expect(patches.patchedDependencies["effect@4.1.2"]).toBeDefined()
  expect(patches.overrides.effect).toBe("4.1.2")
})))

test("execution metadata preserves argument presence and search admission indexes", () => {
  const seen: Array<{ index: number; hasArgument: boolean } | undefined> = []
  const search = Tool.make({
    description: "User search",
    input: Schema.Struct({}),
    output: Schema.Struct({ value: Schema.Finite }),
    execute: (_input, metadata) => Effect.sync(() => {
      seen.push(metadata)
      return { value: 1 }
    }),
  })
  return Effect.runPromise(CodeMode.execute({
    tools: { search },
    code: "await search({}); await tools.search(); return await tools.search({})",
  }).pipe(Effect.tap(result => Effect.sync(() => {
    expect(result.ok).toBe(true)
    expect(seen).toEqual([{ index: 1, hasArgument: false }, { index: 2, hasArgument: true }])
    if (result.ok) expect(result.returnedToolCall).toBe(2)
  }))))
})

test("SDK and project resolve the same Effect installation", () => {
  const rootRequire = createRequire(new URL("../../../../package.json", import.meta.url))
  const sdkRequire = createRequire(import.meta.resolve("@opencode/codemode"))
  expect(sdkRequire.resolve("effect")).toBe(rootRequire.resolve("effect"))
})

test("settlement metadata distinguishes builtin and catalog search", () => {
  const settled: Array<{ readonly index: number; readonly builtin: boolean }> = []
  const search = Tool.make({ description: "user search", input: {}, output: {}, execute: () => Effect.succeed({ found: true }) })
  const runtime = CodeMode.make({ tools: { search }, onToolSettled: call => settled.push(call) })
  return Effect.runPromise(runtime.execute('await search({}); await tools.search(); return await tools.search()')).then(result => {
    expect(result.ok).toBe(true)
    expect(settled).toEqual([{ index: 1, builtin: false }])
  })
})

test("strict arity failure stays authoritative without invoking the host", () => Effect.runPromise(Effect.gen(function* () {
  const invoked: Array<unknown> = []
  const a = Tool.make({ description: "host", input: {}, output: {}, execute: input => Effect.sync(() => { invoked.push(input); return 1 }) })
  for (const code of ['try { await tools.a(1, 2) } catch (e) {} return 42', 'tools.a(1, 2); return 42', 'const alias = tools.a; try { await alias(1, 2) } catch (e) {} return 42']) {
    const result = yield* CodeMode.execute({ tools: { a }, strictJson: true, code })
    expect(result.ok).toBe(false)
    if (!result.ok) expect(result.error.kind).toBe("InvalidToolInput")
  }
  expect(invoked).toEqual([])
})))
