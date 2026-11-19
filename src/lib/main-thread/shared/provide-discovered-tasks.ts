import { Context, Effect, Layer, Schema } from "effect";

import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import {
  findRootFileTaskNames,
  findTasksMadeByRootTaskFile,
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
    return {
      tasksByName: null,
      rootFileTaskNames: { made: [], unexported: [] },
      brokenTasks: discovery,
    };
  }
  return {
    tasksByName: discovery,
    rootFileTaskNames:
      discovery === null ? { made: [], unexported: [] } : yield* findRootFileTaskNames(),
    brokenTasks: null,
  };
});

export class DiscoveredTasks extends Context.Service<
  DiscoveredTasks,
  Effect.Success<typeof discoverTasks>
>()(`${TASK_PACKAGE.name}/DiscoveredTasks`) {}

export const DiscoveredTasksLive = Layer.effect(DiscoveredTasks, discoverTasks);
