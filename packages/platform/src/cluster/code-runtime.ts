import { Worker } from "node:worker_threads"
import { CodeEvaluation, CodeEvaluationResult, CodeEvaluationStarted, CodeEvaluationWorker, CodeRuntime, CodeRuntimeRejected } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { Clock, Deferred, Effect, Schema } from "effect"
import { codeWorkerUrl } from "./code-runtime-worker"

export const CodeRuntimeLive = CodeRuntime.toLayer(Effect.succeed({
  evaluate: Effect.fn("CodeMode.evaluateInWorker")(function* (input) {
    const startedAt = yield* Clock.currentTimeMillis
    const attempt = yield* Effect.scoped(Effect.gen(function* () {
      const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(CodeEvaluation))(input).pipe(Effect.orDie)
      const response = yield* Deferred.make<unknown, CodeRuntimeRejected>()
      const onMessage = (message: unknown) => {
        if (!Schema.is(CodeEvaluationStarted)(message)) Deferred.doneUnsafe(response, Effect.succeed(message))
      }
      const onError = (cause: Error) => Deferred.doneUnsafe(response, Effect.fail(CodeRuntimeRejected.make({ reason: "WorkerFailed", cause })))
      const onExit = (code: number) => Deferred.doneUnsafe(response, Effect.fail(CodeRuntimeRejected.make({ reason: "WorkerExited", cause: code })))
      const worker: Worker = yield* Effect.acquireRelease(
        Effect.try({ try: () => {
          const spawned = new Worker(codeWorkerUrl, { workerData: CodeEvaluationWorker.make({}) })
          spawned.on("message", onMessage)
          spawned.once("error", onError)
          spawned.once("exit", onExit)
          return spawned
        }, catch: cause => CodeRuntimeRejected.make({ reason: "WorkerStartFailed", cause }) }),
        acquired => Effect.promise(() => acquired.terminate()).pipe(Effect.ensuring(Effect.sync(() => {
          acquired.off("message", onMessage)
          acquired.off("error", onError)
          acquired.off("exit", onExit)
        })), Effect.asVoid),
      )
      yield* Effect.try({ try: () => worker.postMessage(encoded), catch: cause => CodeRuntimeRejected.make({ reason: "WorkerSendFailed", cause }) })
      const message = yield* Deferred.await(response)
      const result = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(CodeEvaluationResult))(message).pipe(Effect.mapError(cause => CodeRuntimeRejected.make({ reason: "InvalidWorkerResponse", cause })))
      const now = yield* Clock.currentTimeMillis
      if (now - startedAt >= input.request.limits.maxSegmentMs) return yield* CodeRuntimeRejected.make({ reason: "TimeoutExceeded" })
      if (result._tag === "Defect") return yield* Effect.failCause(result.cause)
      return yield* result.exit
    })).pipe(Effect.timeoutOrElse({ duration: input.request.limits.maxSegmentMs, orElse: () => Effect.fail(CodeRuntimeRejected.make({ reason: "TimeoutExceeded" })) }))
    const completedAt = yield* Clock.currentTimeMillis
    if (completedAt - startedAt >= input.request.limits.maxSegmentMs) return yield* CodeRuntimeRejected.make({ reason: "TimeoutExceeded" })
    return attempt
  }),
}))
