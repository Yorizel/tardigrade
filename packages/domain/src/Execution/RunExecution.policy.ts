import * as Domain from "@alchemy.run/domain"
import { DateTime, Effect } from "effect"
import { Thread } from "../Thread/Thread.aggregate"
import { RunRequested, RunApproved, RunCancelled, RunFailed, RunBecameUnknown, ExpireRun } from "../Thread/Thread.commands"
import { ExecuteRun, runKey, approval } from "./Run.workflow"

export class RunExecution extends Domain.Policy("RunExecution", { from: Thread, on: [RunRequested, RunApproved, RunCancelled, RunFailed, RunBecameUnknown] }) {}
export const RunExecutionLive = RunExecution.toLayer(Effect.gen(function* () {
  const runs = yield* ExecuteRun
  const threads = yield* Thread
  return Effect.fn("RunExecution")(function* ({ source, event }) {
    if (event._tag === "RunRequested") {
      yield* threads.schedule(source, new ExpireRun({ runId: event.run.runId }), { commandId: `expire:${event.run.runId}`, at: DateTime.makeUnsafe(event.run.deadlineAt) }).pipe(Effect.catchTag("AlreadyExecuted", () => Effect.void))
      const state = yield* threads.state(source)
      if (state._tag !== "Open") return yield* Effect.die("Run thread is unavailable")
      yield* runs.start(runKey(source.id, event.run.runId), { threadId: source.id, runId: event.run.runId, owner: state.owner })
      return
    }
    if (event._tag === "RunApproved") {
      yield* runs.get(runKey(source.id, event.runId)).signal(approval(event.stepId), undefined)
      return
    }
    yield* runs.get(runKey(source.id, event.runId)).interrupt
  })
}))
