import { Context, Effect, Layer, Schema } from "effect";

import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { findTasksReachableFromRootTaskFile } from "./find-tasks-reachable-from-root-task-file.ts";
import { BrokenTasksFound } from "./main-thread-errors.ts";
import { StopState } from "./provide-stop-state.ts";

const isBrokenTasksFound = Schema.is(BrokenTasksFound);

const discoverTasks = Effect.gen(function* () {
  const shutdown = yield* StopState;
  const discovery = yield* findTasksReachableFromRootTaskFile().pipe(
    Effect.catchTag("BrokenTasksFound", (brokenTasks) => Effect.succeed(brokenTasks)),
    Effect.raceFirst(shutdown.awaitFailure.pipe(Effect.as(null))),
  );
  return isBrokenTasksFound(discovery)
    ? { tasksByName: null, brokenTasks: discovery }
    : { tasksByName: discovery, brokenTasks: null };
});

export class DiscoveredTasks extends Context.Service<
  DiscoveredTasks,
  Effect.Success<typeof discoverTasks>
>()(`${TASK_PACKAGE.name}/DiscoveredTasks`) {}

export const DiscoveredTasksLive = Layer.effect(DiscoveredTasks, discoverTasks);
