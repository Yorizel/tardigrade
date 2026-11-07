import { expect, test } from "bun:test"
import { ActorApi, ActorModel } from "tardie/domain"
import { AgentsResult } from "tardie/domain/agents-result"
import { SessionLive } from "tardie/platform/alchemy"
import { LocalClusterWorkflowLive } from "tardie/platform/cluster"
import { Budgets, defaultBudgets } from "tardie/platform/codemode"
import { story } from "tardie/platform/testing/story"
import { scene } from "tardie/platform/testing/scene"
import { Effect, Schema } from "effect"

test("native public entrypoints expose their owned contracts", () => {
  expect(ActorApi).toBeDefined()
  expect(ActorModel).toBeDefined()
  expect(AgentsResult).toBeDefined()
  expect(SessionLive).toBeDefined()
  expect(LocalClusterWorkflowLive).toBeDefined()
  expect(Schema.is(Budgets)(defaultBudgets)).toBe(true)
})

test("legacy entrypoints are not public", () => {
  for (const path of ["tardie/core", "tardie/agent", "tardie/deprecated", "tardie/platform/bun", "tardie/platform/cloudflare"]) {
    expect(() => import.meta.resolve(path)).toThrow()
  }
})


test("published testing entrypoints drive native transitions and Effect tools", () => Effect.runPromise(Effect.gen(function* () {
  const input = { source: "return await tools.read()", origin: '"published"', budgets: defaultBudgets, ambient: { at: 0, seed: "published" } }
  story(input, { tools: ["read"] }).expectRequests(requests => {
    expect(requests).toHaveLength(1)
    expect(requests[0]?.method).toBe("read")
  }).settle(0, '{"value":1}').finish(outcome => {
    expect(outcome).toEqual({ _tag: "Success", value: { value: 1 }, logs: [], returnedCallOrdinal: 0 })
  })
  const outcome = yield* scene({ tools: { tools: { read: () => Effect.succeed('{"value":1}') } }, observe: () => undefined })(input)
  expect(outcome).toEqual({ _tag: "Success", value: { value: 1 }, logs: [], returnedCallOrdinal: 0 })
})))
