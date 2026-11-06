#!/usr/bin/env bun
import { Console, Effect, Layer } from "effect"
import { Command } from "effect/cli"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { cli } from "./commands"
import { FetchHttpClient } from "effect/http"

Command.run(cli, { version: "0.0.1" }).pipe(
  Effect.tapErrorTag("ChatRejected", error => Console.error(error.reason)),
  Effect.provide(Layer.merge(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
)
