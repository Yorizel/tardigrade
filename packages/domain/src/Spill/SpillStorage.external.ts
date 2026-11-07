import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { RunScope } from "../Execution/Run.workflow"
import { SpillNotOwner, SpillRejected } from "./Spill.rejection"
import { GrepSpillsInput, GrepSpillsResult, ReadSpillInput, ReadSpillResult, SpillIdentity, SpillPointer, SpillPolicy } from "./Spill.schemas"

export class SpillStorage extends Domain.ExternalSystem("SpillStorage", {
  put: {
    args: { scope: RunScope, ref: SpillIdentity.fields.ref, value: Schema.String, policy: SpillPolicy },
    success: SpillPointer,
    error: [SpillRejected, SpillNotOwner],
  },
  read: {
    args: { scope: RunScope, input: ReadSpillInput },
    success: ReadSpillResult,
    error: [SpillRejected, SpillNotOwner],
  },
  grep: {
    args: { scope: RunScope, input: GrepSpillsInput },
    success: GrepSpillsResult,
    error: [SpillRejected, SpillNotOwner],
  },
}) {}
