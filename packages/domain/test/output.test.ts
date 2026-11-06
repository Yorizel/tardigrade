import * as Domain from "@alchemy.run/domain"
import { Record, String as Str, Effect, Layer, Match, Option, Result, Schema } from "effect"
import { expect, test } from "bun:test"
import { validateOutput } from "../src/Output/Output"
import { OutputDeclaration } from "../src/Output/Output.schemas"
import { CurrentCaller } from "../src/Actor.context"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun, CompleteRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, Prompt } from "../src/Thread/Thread.schemas"
import { format as legacyFormat } from "@cfworker/json-schema"

const TestModel = Domain.Model("OutputTest", { aggregates: [Thread] })
const platform = TestModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const run = <A, E>(program: Effect.Effect<A, E, Effect.Services<typeof Thread> | CurrentCaller>) => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(platform)
  return yield* program.pipe(Effect.provideContext(services), Effect.provideService(CurrentCaller, { subject: "alice" }))
})))

const declaration = OutputDeclaration.make({
  name: "answer",
  schema: {
    type: "object",
    properties: { count: { type: "integer" }, label: { type: "string", enum: ["ok", "done"] } },
    required: ["count", "label"],
    additionalProperties: false
  }
})

test("output declaration schema rejects unsupported keywords and incomplete object requirements", () => {
  const unsupported = Schema.decodeResult(OutputDeclaration)({ name: "answer", schema: { type: "string", pattern: "x" } })
  const unsupportedFormat = Schema.decodeResult(OutputDeclaration)({ name: "answer", schema: { type: "object", properties: { value: { type: "string", format: "unknown" } }, required: ["value"], additionalProperties: false } })
  const nestedUnsupported = Schema.decodeResult(OutputDeclaration)({ name: "answer", schema: { type: "array", items: { type: "string", pattern: "x" } } })
  const incomplete = Schema.decodeResult(OutputDeclaration)({ name: "answer", schema: { type: "object", properties: { count: { type: "integer" } }, required: [], additionalProperties: false } })
  expect(Result.isFailure(unsupported)).toBe(true)
  expect(Result.isFailure(unsupportedFormat)).toBe(true)
  expect(Result.isFailure(nestedUnsupported)).toBe(true)
  expect(Result.isFailure(incomplete)).toBe(true)
})

test("output validation uses the declared schema once and returns decoded JSON", () => {
  expect(validateOutput(declaration, '{"count":2,"label":"ok"}')).toMatchObject({ value: { count: 2, label: "ok" } })
  expect(Option.isNone(validateOutput(declaration, '{"count":2.5,"label":"ok"}'))).toBe(true)
  expect(Option.isNone(validateOutput(declaration, '{"count":2,"label":"bad"}'))).toBe(true)
  expect(Option.isNone(validateOutput(declaration, '{"count":2,"label":"ok","extra":true}'))).toBe(true)
  expect(Option.isNone(validateOutput(declaration, "not json"))).toBe(true)
})

test("output formats enforce the existing JSON Schema assertions", () => Effect.runPromise(Effect.gen(function* () {
  const fixtures = {
    date: ["2000-02-29", "1900-02-29", "2024-13-01"],
    duration: ["P1Y2M3DT4H5M6S", "P1,5W", "PT"],
    email: ["person+tag@example-domain.test", "a..b@example.test", "\"quoted\"@example.test"],
    hostname: ["sub.example.test.", "-bad.example", Str.repeat(254)("a")],
    ipv4: ["192.0.2.1", "256.0.2.1", "192.0.2.01"],
    ipv6: ["::ffff:192.0.2.1", "2001:db8::1", "2001:::1"],
    uuid: ["urn:uuid:550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716", "not-a-uuid"]
  } as const
  for (const [format, cases] of Record.toEntries(fixtures)) {
    expect(Record.has(legacyFormat, format)).toBe(true)
    const formatted = OutputDeclaration.make({ name: "answer", schema: { type: "object", properties: { value: { type: "string", format } }, required: ["value"], additionalProperties: false } })
    for (const value of cases) {
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Struct({ value: Schema.String })))({ value })
      expect(Option.isSome(validateOutput(formatted, encoded))).toBe(legacyFormat[format]?.(value) === true)
    }
  }
})))

test("formatted constraints compose through nested objects, arrays, and unions", () => {
  const nested = OutputDeclaration.make({ name: "nested", schema: {
    type: "object",
    properties: {
      values: { type: "array", items: { anyOf: [{ type: "string", format: "ipv6" }, { type: "null" }] } },
      contact: { type: "string", format: "email" }
    },
    required: ["values", "contact"],
    additionalProperties: false
  } })
  expect(Option.isSome(validateOutput(nested, '{"values":["::1",null],"contact":"a@example.test"}'))).toBe(true)
  expect(Option.isNone(validateOutput(nested, '{"values":["::1",null],"contact":"bad"}'))).toBe(true)
  expect(Option.isNone(validateOutput(nested, '{"values":["2001:::1"],"contact":"a@example.test"}'))).toBe(true)
})

test("a declared run persists its declaration and decoded successful result", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt: Prompt.make("Return a count"), output: declaration }))
  yield* threads.send("a", CompleteRun.make({ runId: "r", text: '{"count":2,"label":"ok"}' }))
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.output).toEqual(declaration)
  expect(state.active?.result).toEqual({ count: 2, label: "ok" })
  expect(state.lastReceipt?.result).toEqual({ count: 2, label: "ok" })
})))

test("invalid declared output fails terminally without rewriting provider text", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt: Prompt.make("Return a count"), output: declaration }))
  yield* threads.send("a", CompleteRun.make({ runId: "r", text: '{"count":"two","label":"ok"}' }))
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.status).toBe("failed")
  expect(state.active?.reason).toBe("output_validation_failed")
  expect(state.active?.text).toBe("")
})))


test("the wire profile requires an object root and rejects hidden unknown keywords", () => {
  for (const schema of [
    { type: "string" },
    { anyOf: [{ type: "object", properties: {}, required: [], additionalProperties: false }] },
    { type: "object", properties: {}, required: [], additionalProperties: false, "unknown\nkeyword": true },
    { type: "object", properties: { value: { type: "string", "type\n": "number" } }, required: ["value"], additionalProperties: false },
  ]) expect(Result.isFailure(Schema.decodeResult(OutputDeclaration)({ name: "bad", schema }))).toBe(true)
})

test("nested unions and large JSON integers retain their declared semantics", () => {
  const nested = OutputDeclaration.make({ name: "nested", schema: {
    type: "object", properties: { values: { type: "array", items: { anyOf: [{ type: "integer" }, { type: "null" }] } } }, required: ["values"], additionalProperties: false,
  } })
  expect(Option.isSome(validateOutput(nested, '{"values":[100000000000000000000,null]}'))).toBe(true)
  expect(Option.isNone(validateOutput(nested, '{"values":[1.5]}'))).toBe(true)
})

test("declared output identity is enforced for both active and archived run retries", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  const request = RequestRun.make({ runId: "r", digest: "d", prompt: Prompt.make("Return a count"), output: declaration })
  yield* threads.send("a", request)
  yield* threads.send("a", request)
  const changed = RequestRun.make({ runId: "r", digest: "d", prompt: request.prompt, output: { ...declaration, name: "different" } })
  const activeConflict = yield* threads.send("a", changed).pipe(Effect.result)
  expect(Result.isFailure(activeConflict)).toBe(true)
  yield* threads.send("a", CompleteRun.make({ runId: "r", text: '{"count":2,"label":"ok"}' }))
  yield* threads.send("a", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next task") }))
  const replay = yield* threads.send("a", request)
  expect(replay.reply.receipt?.result).toEqual({ count: 2, label: "ok" })
  expect(replay.reply.receipt?.output).toEqual(declaration)
  const archivedConflict = yield* threads.send("a", changed).pipe(Effect.result)
  expect(Result.isFailure(archivedConflict)).toBe(true)
})))


test("time formats require complete bounded RFC3339 offsets", () => Effect.runPromise(Effect.gen(function* () {
  const cases = [
    ["12:00:00Z", true], ["12:00:00z", true], ["12:00:00+01:30", true], ["12:00:00-23:59", true],
    ["12:00:00", false], ["12:00:00+99:99", false], ["12:00:00+01", false], ["12:00:00+0130", false],
    ["12:00:00+24:00", false], ["12:00:00+00:60", false], ["24:00:00Z", false],
    ["23:59:60Z", true], ["02:29:60+02:30", true], ["23:59:60+02:30", false], ["12:00:60Z", false],
    ["12:00:00.123-00:00", true],
  ] as const
  for (const format of ["time", "date-time"] as const) {
    const formatted = OutputDeclaration.make({ name: "answer", schema: { type: "object", properties: { value: { type: "string", format } }, required: ["value"], additionalProperties: false } })
    for (const [clock, accepted] of cases) {
      const value = Match.value(format).pipe(Match.when("time", () => clock), Match.orElse(() => `2025-01-01T${clock}`))
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Struct({ value: Schema.String })))({ value })
      expect(Option.isSome(validateOutput(formatted, encoded))).toBe(accepted)
    }
  }
})))
