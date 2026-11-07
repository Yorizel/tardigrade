import * as Domain from "@alchemy.run/domain"
import { Envelope, streamOf } from "@alchemy.run/domain/Aggregate"
import { Array as Arr, Effect, Match, Schema, Stream } from "effect"
import { Digest } from "effect/crypto"
import { Prompt } from "effect/ai"
import { Thread } from "../Thread/Thread.aggregate"
import { ForkSnapshot, ThreadState } from "../Thread/Thread.schemas"
import { contextBoundaries } from "../Execution/Context.compaction"
import { DEFAULT_FORK_POLICY, ForkPolicy, ForkRejected, ForkRequest } from "./Fork.schemas"

export const ThreadHistoryFrame = Schema.Struct({ event: Schema.Union(Thread.definition.events), envelope: Envelope })

const replay = (frames: ReadonlyArray<typeof ThreadHistoryFrame.Type>) => Arr.reduce<typeof ThreadHistoryFrame.Type, typeof ThreadState.Type>(frames, Thread.definition.initial, (state, { event: historyEvent, envelope }) => Match.value(historyEvent).pipe(
  Match.tagsExhaustive({
    BackgroundChildAdmissionReleased: event => Thread.definition.evolve.BackgroundChildAdmissionReleased(state, event, envelope),
    BackgroundChildReserved: event => Thread.definition.evolve.BackgroundChildReserved(state, event, envelope),
    BackgroundChildStarted: event => Thread.definition.evolve.BackgroundChildStarted(state, event, envelope),
    BackgroundChildCancellationRequested: event => Thread.definition.evolve.BackgroundChildCancellationRequested(state, event, envelope),
    BackgroundChildFencedNotStarted: event => Thread.definition.evolve.BackgroundChildFencedNotStarted(state, event, envelope),
    BackgroundChildSettled: event => Thread.definition.evolve.BackgroundChildSettled(state, event, envelope),
    BackgroundChildReconciled: event => Thread.definition.evolve.BackgroundChildReconciled(state, event, envelope),
    ThreadForkInitialized: event => Thread.definition.evolve.ThreadForkInitialized(state, event, envelope),
    RunModelSelected: event => Thread.definition.evolve.RunModelSelected(state, event, envelope),
    ContextCompactionPlanned: event => Thread.definition.evolve.ContextCompactionPlanned(state, event, envelope),
    ContextCompactionDiscarded: event => Thread.definition.evolve.ContextCompactionDiscarded(state, event, envelope),
    ContextCompacted: event => Thread.definition.evolve.ContextCompacted(state, event, envelope),
    ThreadOpened: event => Thread.definition.evolve.ThreadOpened(state, event, envelope),
    ChildAdmissionReserved: event => Thread.definition.evolve.ChildAdmissionReserved(state, event, envelope),
    ChildAdmissionClosed: event => Thread.definition.evolve.ChildAdmissionClosed(state, event, envelope),
    ChildThreadOpened: event => Thread.definition.evolve.ChildThreadOpened(state, event, envelope),
    RunRequested: event => Thread.definition.evolve.RunRequested(state, event, envelope),
    StepReconciled: event => Thread.definition.evolve.StepReconciled(state, event, envelope),
    RunAcknowledged: event => Thread.definition.evolve.RunAcknowledged(state, event, envelope),
    StepOutputRejected: event => Thread.definition.evolve.StepOutputRejected(state, event, envelope),
    StepPrepared: event => Thread.definition.evolve.StepPrepared(state, event, envelope),
    StepDispatched: event => Thread.definition.evolve.StepDispatched(state, event, envelope),
    StepRecorded: event => Thread.definition.evolve.StepRecorded(state, event, envelope),
    RunApproved: event => Thread.definition.evolve.RunApproved(state, event, envelope),
    RunCompleted: event => Thread.definition.evolve.RunCompleted(state, event, envelope),
    RunFailed: event => Thread.definition.evolve.RunFailed(state, event, envelope),
    RunCancelled: event => Thread.definition.evolve.RunCancelled(state, event, envelope),
    RunBecameUnknown: event => Thread.definition.evolve.RunBecameUnknown(state, event, envelope),
  }),
))

export const captureFork = Effect.fn("Fork.capture")(function* <E, R>(request: typeof ForkRequest.Type, events: Stream.Stream<typeof ThreadHistoryFrame.Type | Domain.Read.CaughtUp, E, R>, policy: typeof ForkPolicy.Type = DEFAULT_FORK_POLICY) {
  if (request.sourceThreadId === request.targetThreadId) return yield* ForkRejected.make({ reason: "SameThread", policy })
  if (request.throughSeq > policy.maxEvents) return yield* ForkRejected.make({ reason: "EventBudgetExceeded", policy })
  const prefix = yield* events.pipe(
    Stream.takeUntil(frame => Domain.Read.isCaughtUp(frame) || frame.envelope.seq >= request.throughSeq),
    Stream.runFoldEffect(() => ({ frames: Arr.empty<typeof ThreadHistoryFrame.Type>(), bytes: 2 }), Effect.fn("Fork.readPrefix")(function* (state, frame) {
      if (Domain.Read.isCaughtUp(frame)) return yield* ForkRejected.make({ reason: "UnknownCheckpoint", policy })
      if (frame.envelope.id !== request.sourceThreadId || frame.envelope.stream !== streamOf(Thread.ref(request.sourceThreadId)) || frame.envelope.seq !== state.frames.length + 1) return yield* ForkRejected.make({ reason: "InvalidPrefix", policy })
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ThreadHistoryFrame))(frame).pipe(Effect.orDie)
      const bytes = state.bytes + new TextEncoder().encode(encoded).byteLength + 1
      if (bytes > policy.maxPrefixBytes) return yield* ForkRejected.make({ reason: "PrefixBudgetExceeded", policy })
      return { frames: [...state.frames, frame], bytes }
    })),
  )
  if (prefix.frames.length !== request.throughSeq) return yield* ForkRejected.make({ reason: "UnknownCheckpoint", policy })
  const source = replay(prefix.frames)
  if (source._tag !== "Open") return yield* ForkRejected.make({ reason: "SourceNotOpen", policy })
  if (source.active !== null && (source.active.status !== "completed" || source.active.step !== null || !Schema.toEquivalence(Prompt.Prompt)(source.history, source.active.prompt))) return yield* ForkRejected.make({ reason: "CheckpointNeedsDetachment", policy })
  if (Arr.some(source.background, child => ["reserved", "running", "cancel_requested", "unknown"].includes(child.status))) return yield* ForkRejected.make({ reason: "CheckpointNeedsDetachment", policy })
  const boundaries = contextBoundaries(source.history)
  if (boundaries.invalid || boundaries.pending.size !== 0) return yield* ForkRejected.make({ reason: "CheckpointNeedsDetachment", policy })
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(ThreadHistoryFrame)))(prefix.frames).pipe(Effect.orDie)
  const digest = yield* Digest.Digest
  const prefixDigest = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(encoded)).pipe(Effect.orDie)
  const snapshot = ForkSnapshot.make({ origin: { sourceThreadId: request.sourceThreadId, throughSeq: request.throughSeq, prefixDigest, version: 1 }, owner: source.owner, actor: source.actor, instance: source.instance, name: request.name, limits: source.limits, history: source.history })
  const snapshotJson = yield* Schema.encodeEffect(Schema.fromJsonString(ForkSnapshot))(snapshot).pipe(Effect.orDie)
  if (new TextEncoder().encode(snapshotJson).byteLength > policy.maxSnapshotBytes) return yield* ForkRejected.make({ reason: "SnapshotBudgetExceeded", policy })
  return snapshot
}, (effect, _request, _events, policy = DEFAULT_FORK_POLICY) => effect.pipe(
  Effect.timeoutOrElse({ duration: policy.timeoutMs, orElse: () => Effect.fail(ForkRejected.make({ reason: "ReadTimeout", policy })) }),
))
