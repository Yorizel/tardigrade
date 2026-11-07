import { describe, expect, test } from "bun:test"
import { Array as Arr } from "effect"
import { defaultBudgets } from "../../src/codemode/core"
import { story } from "../../src/testing/story"

const input = (source: string) => ({ source, origin: '{"thread":"one"}', budgets: defaultBudgets, ambient: { at: 1700000000000, seed: "test" } })

describe("story", () => {
  test("exposes and explicitly settles sequential commands through a branch", () => {
    const scenario = story(input('const a = await tools.read(); if (a.ok) { return await tools.write({ value: a.value }); } return null;'), {tools: ["read", "write"]})
    expect(scenario.model().pending[0]?.ordinal).toBe(0)
    expect(() => scenario.settle(0, "null")).toThrow("Inspect")
    scenario.expectRequests(requests => expect(requests).toEqual([{ ordinal: 0, package: "tools", method: "read", origin: { thread: "one" } }]))
      .settle(0, '{"ok":true,"value":7}')
      .expectRequests(requests => expect(requests).toEqual([{ ordinal: 1, package: "tools", method: "write", origin: { thread: "one" }, input: { value: 7 } }]))
      .settle(1, '{"saved":true}')
      .finish((outcome, model) => {
        expect(outcome).toEqual({ _tag: "Success", value: { saved: true }, logs: [], returnedCallOrdinal: 1 })
        expect(model.settlements).toHaveLength(2)
      })
  })

  test("rejects unhandled requests and delivers failure messages", () => {
    const scenario = story(input('return await tools.read(null)'), {tools: ["read"]})
    expect(() => scenario.finish(() => {})).toThrow("Unhandled command")
    scenario.expectRequests(requests => expect(requests[0]?.input).toBeNull()).fail(0, "denied")
      .finish(outcome => expect(outcome).toEqual({ _tag: "ToolFailure", reason: "denied" }))
  })
})

test("rejects asynchronous request assertions without granting inspection", () => {
  const scenario = story(input('return await tools.read()'), {tools: ["read"]})
  scenario.expectRequests(() => {})
  expect(() => scenario.expectRequests(() => Promise.resolve())).toThrow("synchronously")
  expect(() => scenario.settle(0, "null")).toThrow("Inspect")
  scenario.expectRequests(() => {}).settle(0, "null").finish(() => {})
})

test("rejects asynchronous outcome assertions and does not inspect then getters", () => {
  const scenario = story(input('return 1'), {})
  expect(() => scenario.finish(() => Promise.resolve())).toThrow("synchronously")
  const thenable = new Proxy({}, { get() { throw new Error("getter invoked") } })
  expect(() => scenario.finish(() => thenable)).toThrow("synchronously")
  scenario.finish(outcome => { expect(outcome._tag).toBe("Success") })
})


test("Story settles simultaneous requests by ordinal and retains the race loser", () => {
  const source = 'const winner = await Promise.race([tools.a(), tools.b()]); return await tools.c(winner);'
  const scenario = story(input(source), { tools: ["a", "b", "c"] })
  scenario.expectRequests(requests => expect(Arr.map(requests, request => request.ordinal)).toEqual([0, 1]))
    .settle(1, '"b"')
    .expectRequests(requests => expect(Arr.map(requests, request => request.ordinal)).toEqual([0, 2]))
    .settle(2, '"done"')
    .expectRequests(requests => expect(Arr.map(requests, request => request.ordinal)).toEqual([0]))
    .settle(0, '"a"')
    .finish(outcome => expect(outcome).toEqual({ _tag: "Success", value: "done", logs: [] }))
})
