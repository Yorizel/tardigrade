import { Effect, Layer, Schema } from "effect"
import { Prompt } from "effect/ai"
import { ReceiptConflict, RunReceiptGuard } from "./Actor.context"
import { RequestRun } from "./Thread/Thread.commands"
import { ModelChoice } from "./Execution/Model.schemas"
import { OutputDeclaration } from "./Output/Output.schemas"

export const RunReceiptGuardLive = Layer.succeed(RunReceiptGuard, (effect, { payload }) => Effect.gen(function* () {
  const receipt = yield* effect
  const requested = yield* Schema.decodeUnknownEffect(Schema.toType(RequestRun.rpc.payloadSchema))(payload).pipe(Effect.orDie)
  const returned = yield* Schema.decodeUnknownEffect(Schema.toType(RequestRun.rpc.successSchema))(receipt).pipe(Effect.orDie)
  const original = returned.reply.receipt
  if (original === null || original.runId !== requested.input.runId || original.digest !== requested.input.digest || !Schema.toEquivalence(Prompt.Prompt)(original.input, requested.input.prompt) || !Schema.toEquivalence(Schema.UndefinedOr(ModelChoice))(original.requestedModel, requested.input.model) || !Schema.toEquivalence(Schema.NullOr(OutputDeclaration))(original.output, requested.input.output ?? null)) return yield* ReceiptConflict.make()
  return receipt
}))
