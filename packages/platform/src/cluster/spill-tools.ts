import { SpillNotOwner, SpillRejected } from "@clavia/tardigrade-domain/Spill/Spill.rejection"
import { GrepSpillsInput, GrepSpillsResult, ReadSpillInput, ReadSpillResult } from "@clavia/tardigrade-domain/Spill/Spill.schemas"
import { Schema } from "effect"
import { Tool, Toolkit } from "effect/ai"

const SpillToolFailure = Schema.Union([SpillRejected, SpillNotOwner])

export const ReadSpill = Tool.make("read_spill", {
  description: "Read a slice of a complete spilled result by its reference in this thread's workspace. Offset and length are character counts; length is capped by the accepted spill policy. The result reports the complete value's size, including when offset is past its end.",
  parameters: ReadSpillInput,
  success: ReadSpillResult,
  failure: SpillToolFailure,
  failureMode: "return",
}).annotate(Tool.Readonly, true)

export const GrepSpills = Tool.make("grep_spills", {
  description: "Search complete stored results for a nonempty substring in this thread's workspace, optionally restricting the search to one reference. Matches report their reference, character offset and surrounding context. The accepted spill policy bounds context and match count; truncation is explicit.",
  parameters: GrepSpillsInput,
  success: GrepSpillsResult,
  failure: SpillToolFailure,
  failureMode: "return",
}).annotate(Tool.Readonly, true)

export const SpillTools = Toolkit.make(ReadSpill, GrepSpills)
