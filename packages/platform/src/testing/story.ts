import { Array as Arr, Function as Fn } from "effect"
import { PackageCatalog } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { initial, update, type Input, type Model, type Outcome, type Request } from "../codemode/core"

function createStory(input: Input, catalog: typeof PackageCatalog.Type) {
  let model = update(initial(input, catalog), { _tag: "Start" })
  let inspected = false

  return {
    model: () => model,
    expectRequests(assert: (requests: readonly Request[], model: Model) => unknown) {
      if (model.pending.length === 0) throw new Error("Expected pending requests")
      inspected = false
      if (assert(model.pending, model) !== undefined) throw new Error("Story assertions must return undefined synchronously")
      inspected = true
      return this
    },
    settle(ordinal: number, value?: string) {
      if (!inspected || !Arr.some(model.pending, request => request.ordinal === ordinal)) throw new Error("Inspect the pending request before settling it")
      if (value === undefined) model = update(model, { _tag: "Settled", ordinal, sequence: model.settlements.length })
      else model = update(model, { _tag: "Settled", ordinal, sequence: model.settlements.length, value })
      inspected = false
      return this
    },
    fail(ordinal: number, reason: string) {
      if (!inspected || !Arr.some(model.pending, request => request.ordinal === ordinal)) throw new Error("Inspect the pending request before failing it")
      model = update(model, { _tag: "Failed", ordinal, sequence: model.settlements.length, reason })
      inspected = false
      return this
    },
    finish(assert: (outcome: Outcome, model: Model) => unknown) {
      if (model.pending.length !== 0) throw new Error(`Unhandled command: ${model.pending[0]!.package}.${model.pending[0]!.method} (${model.pending[0]!.ordinal})`)
      if (model.outcome === undefined) throw new Error("Expected an outcome")
      if (assert(model.outcome, model) !== undefined) throw new Error("Story assertions must return undefined synchronously")
    },
  }
}

export const story: { (input: Input, catalog: typeof PackageCatalog.Type): ReturnType<typeof createStory>; (catalog: typeof PackageCatalog.Type): (input: Input) => ReturnType<typeof createStory> } = Fn.dual(2, createStory)
