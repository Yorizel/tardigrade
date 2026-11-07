import { expect, test } from "bun:test"
import { BunServices } from "@effect/platform-bun"
import { Array as Arr, Effect, Stream } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"

const execute = Effect.fn("GateTest.execute")(function* (args: ReadonlyArray<string>) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  return yield* Effect.scoped(Effect.gen(function* () {
    const child = yield* spawner.spawn(ChildProcess.make(process.execPath, ["run", "tools/gate.ts", ...args], { stdout: "pipe", stderr: "pipe" }))
    const [status, stdout, stderr] = yield* Effect.all([
      child.exitCode,
      child.stdout.pipe(Stream.decodeText(), Stream.runCollect),
      child.stderr.pipe(Stream.decodeText(), Stream.runCollect),
    ], { concurrency: "unbounded" })
    return { status: Number(status), stdout: Arr.join(stdout, ""), stderr: Arr.join(stderr, "") }
  }))
})

test("gate rejects empty and unknown selectors", () => Effect.runPromise(Effect.gen(function* () {
  for (const args of [["--only"], ["--only="], ["--only=missing"], ["--only", "--list"]]) {
    const result = yield* execute(args)
    expect(result.status).toBe(1)
    expect(result.stdout + result.stderr).toContain("GateArgumentsRejected")
  }
}).pipe(Effect.provide(BunServices.layer))), 15_000)

test("gate drains and summarizes every selected task", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute(["--only=lint:docs,lint:effect:policy", "--jobs=2"])
  expect(result.status).toBe(0)
  expect(result.stdout).toContain("PASS lint:docs")
  expect(result.stdout).toContain("PASS lint:effect:policy")
  expect(result.stdout).toContain("Summary")
}).pipe(Effect.provide(BunServices.layer))), 15_000)

test("gate selects native Oxlint regression tests", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute(["--only=test:native-oxlint", "--list"])
  expect(result.status).toBe(0)
  expect(result.stdout).toContain("test:native-oxlint")
}).pipe(Effect.provide(BunServices.layer))), 15_000)
