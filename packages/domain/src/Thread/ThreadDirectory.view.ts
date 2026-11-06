import * as Domain from "@alchemy.run/domain"
import { Effect, Schema } from "effect"
import { CurrentCaller, Ownership, Session } from "../Actor.context"
import { Thread } from "./Thread.aggregate"
import { NotOwner, ThreadOpened } from "./Thread.commands"
import { ThreadId } from "./Thread.schemas"

export class ThreadRegistered extends Domain.Event("ThreadRegistered", {
  threadId: ThreadId,
  actor: ThreadOpened.fields.actor,
  instance: ThreadOpened.fields.instance,
  name: ThreadOpened.fields.name,
  child: Schema.Boolean,
}) {}

export class ThreadDirectory extends Domain.View("ThreadDirectory", {
  from: [Thread],
  key: Ownership.fields.owner,
  state: Schema.Null,
  initial: null,
  middleware: [Session],
  rejects: [NotOwner],
  events: [ThreadRegistered],
  keyOf: {
    ThreadOpened: ({ event }) => event.owner,
    ChildThreadOpened: ({ event }) => event.owner,
  },
  emit: {
    ThreadOpened: (event, { source }) => ThreadRegistered.make({ threadId: source.id, actor: event.actor, instance: event.instance, name: event.name, child: false }),
    ChildThreadOpened: (event, { source }) => ThreadRegistered.make({ threadId: source.id, actor: event.actor, instance: event.instance, name: event.name, child: true }),
  },
  validate: Effect.fn("ThreadDirectory.authorize")(function* ({ key }) {
    const caller = yield* CurrentCaller
    if (key !== caller.subject) return yield* new NotOwner()
  }),
}) {}
