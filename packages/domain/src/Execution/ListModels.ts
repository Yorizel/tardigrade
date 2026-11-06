import { Tool } from "effect/ai"
import { ModelCatalog } from "./Model.schemas"

export const ListModels = Tool.make("list_models", {
  description: "List model choices permitted for this authenticated actor. Use a returned provider/model pair to select a model for a child. These are configured permissions, not a live provider pricing or capability catalog.",
  parameters: Tool.EmptyParams,
  success: ModelCatalog,
}).annotate(Tool.Readonly, true)
