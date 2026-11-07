import * as Domain from "@alchemy.run/domain"
import { Thread } from "./Thread.aggregate"
import { BackgroundChildCancellationRequested } from "./Thread.commands"

export class BackgroundCancellationPolicy extends Domain.Policy("BackgroundCancellationPolicy", { from: Thread, on: [BackgroundChildCancellationRequested] }) {}
