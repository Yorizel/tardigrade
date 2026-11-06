import { expect, test } from "bun:test"
import { CodeMode, Tool } from "@opencode/codemode"
import { Context, Deferred, Effect, Fiber, Schema } from "effect"

class Greeting extends Context.Service<Greeting, { readonly prefix: string }>()("Greeting") {}

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

test("interrupting execution interrupts the native tool", () => {
  return Effect.runPromise(Effect.gen(function* () {
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
    expect(yield* Deferred.isDone(stopped)).toBe(true)
  }))
})

test("publication and generated projects retain the SDK patch and Effect override", async () => {
  const { publishDependencies, publishSources } = await import("../../../../tools/publish-manifest")
  const { projectPatchesFrom } = await import("../../../../apps/cli/src/version")
  const dependencies = publishDependencies(publishSources.map(source => source.pkg)).dependencies
  const patches = await projectPatchesFrom(new URL("../../../../apps/cli/src/init.ts", import.meta.url).href)
  expect(dependencies["@opencode/codemode"]).toBe("2.2.5")
  expect(patches.patchedDependencies["@opencode/codemode@2.2.5"]).toBeDefined()
  expect(patches.patchedDependencies["effect@4.1.2"]).toBeDefined()
  expect(patches.overrides.effect).toBe("4.1.2")
})

test("execution metadata preserves argument presence and search admission indexes", () => {
  const seen: Array<{ index: number; hasArgument: boolean } | undefined> = []
  const search = Tool.make({
    description: "User search",
    input: Schema.Struct({}),
    output: Schema.Struct({ value: Schema.Number }),
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
