import * as BunServices from "@effect/platform-bun/BunServices";
import { Effect } from "effect";

import { watchSupervisor } from "./watch-the-supervisor.ts";

Effect.runFork(watchSupervisor.pipe(Effect.provide(BunServices.layer)));
