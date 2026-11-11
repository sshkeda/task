import { Context, Effect, Layer, Schema } from "effect";

import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import {
  findTasksMadeByRootTaskFile,
  findUnexportedRootFileTaskNames,
} from "./find-tasks-made-by-root-task-file.ts";
import { BrokenTasksFound } from "./main-thread-errors.ts";
import { StopState } from "./provide-stop-state.ts";

const isBrokenTasksFound = Schema.is(BrokenTasksFound);

const discoverTasks = Effect.gen(function* () {
  const shutdown = yield* StopState;
  const discovery = yield* findTasksMadeByRootTaskFile().pipe(
    Effect.catchTag("BrokenTasksFound", (brokenTasks) => Effect.succeed(brokenTasks)),
    Effect.raceFirst(shutdown.awaitFailure.pipe(Effect.as(null))),
  );
  if (isBrokenTasksFound(discovery)) {
    return { tasksByName: null, unexportedRootFileTaskNames: [], brokenTasks: discovery };
  }
  return {
    tasksByName: discovery,
    unexportedRootFileTaskNames: discovery === null ? [] : yield* findUnexportedRootFileTaskNames(),
    brokenTasks: null,
  };
});

export class DiscoveredTasks extends Context.Service<
  DiscoveredTasks,
  Effect.Success<typeof discoverTasks>
>()(`${TASK_PACKAGE.name}/DiscoveredTasks`) {}

export const DiscoveredTasksLive = Layer.effect(DiscoveredTasks, discoverTasks);
