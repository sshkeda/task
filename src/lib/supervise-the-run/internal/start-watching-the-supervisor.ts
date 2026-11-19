import * as BunServices from "@effect/platform-bun/BunServices";
import { Effect, Logger } from "effect";

import { watchSupervisor } from "./watch-the-supervisor.ts";

Effect.runFork(
  watchSupervisor.pipe(
    Effect.provideService(Logger.CurrentLoggers, new Set()),
    Effect.provide(BunServices.layer),
  ),
);
