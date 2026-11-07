import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Result } from "effect"
import { Prompt } from "effect/ai"
import { expect, test } from "bun:test"
import { delegationKey } from "../src/Delegation/Delegation.schemas"
import { Thread } from "../src/Thread/Thread.aggregate"
import { BeginStep, CompleteRun, ConfirmDispatch, FenceChildThread, MarkUnknown, OpenChildThread, OpenThread, RequestChildRun, RequestRun, ReserveChildThread, ResolveUnknown } from "../src/Thread/Thread.commands"
import { CurrentCaller } from "../src/Actor.context"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"

const TestModel = Domain.Model("ChildFenceTest", { aggregates: [Thread] })
const platform = TestModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const childId = delegationKey("parent", "parent-run", "request")
const binding = { delegationId: childId, parentThreadId: "parent", parentRunId: "parent-run", childRunId: "request", actor: "assistant" }
const childConfig = { binding, name: "child", limits: DEFAULT_RUN_LIMITS }
const childRequest = { binding, digest: "task", prompt: Prompt.make("Read the document") }
const rootConfig = { actor: "assistant", instance: "root", name: "root", limits: DEFAULT_RUN_LIMITS }
const run = <A, E>(program: Effect.Effect<A, E, Layer.Success<typeof platform> | CurrentCaller>) => Effect.runPromise(program.pipe(
  Effect.provideService(CurrentCaller, { subject: "alice" }),
  Effect.provide(platform),
))

test("a fence can exist before child reservation and rejects every late admission", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Parent cancelled" }))
  yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Duplicate cancellation" }))
  const reserve = yield* threads.send(childId, ReserveChildThread.make({ binding })).pipe(Effect.result)
  const open = yield* threads.send(childId, OpenChildThread.make(childConfig)).pipe(Effect.result)
  const request = yield* threads.send(childId, RequestChildRun.make(childRequest)).pipe(Effect.result)
  const rootOpen = yield* threads.send(childId, OpenThread.make(rootConfig)).pipe(Effect.result)
  const rootRun = yield* threads.send(childId, RequestRun.make({ runId: "request", digest: "task", prompt: childRequest.prompt })).pipe(Effect.result)
  expect(Result.isFailure(reserve)).toBe(true)
  expect(Result.isFailure(open)).toBe(true)
  expect(Result.isFailure(request)).toBe(true)
  expect(Result.isFailure(rootOpen)).toBe(true)
  expect(Result.isFailure(rootRun)).toBe(true)
  const state = yield* threads.state(childId)
  if (state._tag !== "ChildReserved") return yield* Effect.die("Expected unopened child fence")
  expect(state.closed).toBe(true)
  expect(state.owner).toBe("alice")
  expect(state.reason).toBe("Parent cancelled")
  expect(state.binding).toEqual(binding)
  return undefined
})))

test("a reserved child requires its internal opening and admission commands", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const unreserved = yield* threads.send(childId, OpenChildThread.make(childConfig)).pipe(Effect.result)
  expect(Result.isFailure(unreserved)).toBe(true)
  yield* threads.send(childId, ReserveChildThread.make({ binding }))
  yield* threads.send(childId, ReserveChildThread.make({ binding }))
  const publicOpen = yield* threads.send(childId, OpenThread.make({ ...rootConfig, instance: childId })).pipe(Effect.result)
  expect(Result.isFailure(publicOpen)).toBe(true)
  yield* threads.send(childId, OpenChildThread.make(childConfig))
  yield* threads.send(childId, OpenChildThread.make(childConfig))
  const publicRun = yield* threads.send(childId, RequestRun.make({ runId: "request", digest: "task", prompt: childRequest.prompt })).pipe(Effect.result)
  expect(Result.isFailure(publicRun)).toBe(true)
  const admitted = yield* threads.send(childId, RequestChildRun.make(childRequest))
  const duplicate = yield* threads.send(childId, RequestChildRun.make(childRequest))
  expect(duplicate.reply).toEqual(admitted.reply)
  const state = yield* threads.state(childId)
  if (state._tag !== "Open") return yield* Effect.die("Expected opened child")
  expect(state.actor).toBe(binding.actor)
  expect(state.instance).toBe(childId)
  expect(state.child?.binding).toEqual(binding)
  expect(state.child?.closed).toBe(false)
  expect(state.active?.runId).toBe(binding.childRunId)
  return undefined
})))

test("binding identities and owners cannot be replaced", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const wrongTarget = yield* threads.send("wrong", ReserveChildThread.make({ binding })).pipe(Effect.result)
  expect(Result.isFailure(wrongTarget)).toBe(true)
  yield* threads.send(childId, ReserveChildThread.make({ binding }))
  const changedActor = { ...binding, actor: "other" }
  const changedParent = { ...binding, parentRunId: "other-parent-run" }
  const open = yield* threads.send(childId, OpenChildThread.make({ ...childConfig, binding: changedActor })).pipe(Effect.result)
  const close = yield* threads.send(childId, FenceChildThread.make({ binding: changedActor, reason: "Stop" })).pipe(Effect.result)
  const parent = yield* threads.send(childId, FenceChildThread.make({ binding: changedParent, reason: "Stop" })).pipe(Effect.result)
  const foreign = yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Stop" })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(open)).toBe(true)
  expect(Result.isFailure(close)).toBe(true)
  expect(Result.isFailure(parent)).toBe(true)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
  const state = yield* threads.state(childId)
  if (state._tag !== "ChildReserved") return yield* Effect.die("Expected child reservation")
  expect(state.closed).toBe(false)
  expect(state.binding).toEqual(binding)
  return undefined
})))

test("an existing root cannot be converted or cancelled through a colliding child ID", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(childId, OpenThread.make(rootConfig))
  yield* threads.send(childId, RequestRun.make({ runId: "root-run", digest: "root", prompt: Prompt.make("Root work") }))
  const reserve = yield* threads.send(childId, ReserveChildThread.make({ binding })).pipe(Effect.result)
  const fence = yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Stop" })).pipe(Effect.result)
  const childOpen = yield* threads.send(childId, OpenChildThread.make(childConfig)).pipe(Effect.result)
  const childRun = yield* threads.send(childId, RequestChildRun.make(childRequest)).pipe(Effect.result)
  expect(Result.isFailure(reserve)).toBe(true)
  expect(Result.isFailure(fence)).toBe(true)
  expect(Result.isFailure(childOpen)).toBe(true)
  expect(Result.isFailure(childRun)).toBe(true)
  const state = yield* threads.state(childId)
  if (state._tag !== "Open") return yield* Effect.die("Expected root")
  expect(state.child).toBe(null)
  expect(state.owner).toBe("alice")
  expect(state.active?.runId).toBe("root-run")
  expect(state.active?.status).toBe("pending")
  return undefined
})))

test("a different owner's root remains isolated from child fencing", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(childId, OpenThread.make(rootConfig)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const result = yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Stop" })).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure._tag).toBe("NotOwner")
  const state = yield* threads.state(childId)
  if (state._tag !== "Open") return yield* Effect.die("Expected root")
  expect(state.owner).toBe("bob")
  expect(state.child).toBe(null)
  return undefined
})))

test("fencing an opened mutation retains uncertainty and rejects late completion", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(childId, ReserveChildThread.make({ binding }))
  yield* threads.send(childId, OpenChildThread.make(childConfig))
  yield* threads.send(childId, RequestChildRun.make(childRequest))
  yield* threads.send(childId, BeginStep.make({ runId: "request", stepId: "request:1", kind: "tool", mutation: true, requiresApproval: false }))
  yield* threads.send(childId, ConfirmDispatch.make({ runId: "request", stepId: "request:1" }))
  yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Parent cancelled" }))
  yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Retry" }))
  const late = yield* threads.send(childId, CompleteRun.make({ runId: "request", text: "Late success" })).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  const state = yield* threads.state(childId)
  if (state._tag !== "Open") return yield* Effect.die("Expected child")
  expect(state.child?.closed).toBe(true)
  expect(state.active?.status).toBe("unknown")
  expect(state.active?.step?.dispatched).toBe(true)
  yield* threads.send(childId, ResolveUnknown.make({ runId: "request", stepId: "request:1", resolution: "failed", evidence: "Provider confirmed failure" }))
  const reopened = yield* threads.send(childId, OpenChildThread.make(childConfig)).pipe(Effect.result)
  const readmitted = yield* threads.send(childId, RequestChildRun.make(childRequest)).pipe(Effect.result)
  expect(Result.isFailure(reopened)).toBe(true)
  expect(Result.isFailure(readmitted)).toBe(true)
  const resolved = yield* threads.state(childId)
  if (resolved._tag !== "Open") return yield* Effect.die("Expected child")
  expect(resolved.child?.closed).toBe(true)
  expect(resolved.active?.status).toBe("failed")
  return undefined
})))

test("an opened child with no dispatched mutation cancels without admitting another run", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(childId, ReserveChildThread.make({ binding }))
  yield* threads.send(childId, OpenChildThread.make(childConfig))
  yield* threads.send(childId, RequestChildRun.make(childRequest))
  yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Stop" }))
  const state = yield* threads.state(childId)
  if (state._tag !== "Open") return yield* Effect.die("Expected child")
  expect(state.active?.status).toBe("cancelled")
  const late = yield* threads.send(childId, RequestChildRun.make(childRequest)).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  expect(Domain.verify(TestModel)).toEqual([])
  return undefined
})))


test("fencing preserves an already uncertain child's evidence", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(childId, ReserveChildThread.make({ binding }))
  yield* threads.send(childId, OpenChildThread.make(childConfig))
  yield* threads.send(childId, RequestChildRun.make(childRequest))
  yield* threads.send(childId, BeginStep.make({ runId: "request", stepId: "request:1", kind: "tool", mutation: true, requiresApproval: false }))
  yield* threads.send(childId, ConfirmDispatch.make({ runId: "request", stepId: "request:1" }))
  yield* threads.send(childId, MarkUnknown.make({ runId: "request", stepId: "request:1", reason: "Original lost receipt" }))
  yield* threads.send(childId, FenceChildThread.make({ binding, reason: "Parent cancelled" }))
  const state = yield* threads.state(childId)
  if (state._tag !== "Open") return yield* Effect.die("Expected child")
  expect(state.child?.closed).toBe(true)
  expect(state.active?.status).toBe("unknown")
  expect(state.active?.reason).toBe("Original lost receipt")
  expect(state.active?.step?.dispatched).toBe(true)
  return undefined
})))
