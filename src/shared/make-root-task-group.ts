import { Effect, Ref } from "effect";

import { TaskNotMadeWhileLoading } from "../lib/errors/index.ts";
import { all, allSettled, make } from "../lib/task/index.ts";
import { findTasksMadeWithName } from "./record-tasks-made.ts";
import type { RootTaskGroup } from "./root-task-group.ts";

const RUN_TASK_LIST = { all, allSettled } as const;

const rootTaskGroupMade = Ref.makeUnsafe(false);

export const makeRootTaskGroup = (rootTaskGroup: typeof RootTaskGroup.Type | null) =>
  Effect.gen(function* () {
    if (rootTaskGroup === null) {
      return;
    }
    if (yield* Ref.getAndSet(rootTaskGroupMade, true)) {
      return;
    }
    const missingTaskName = rootTaskGroup.taskNames.find(
      (taskName) => findTasksMadeWithName(taskName).length === 0,
    );
    const listedTasks = rootTaskGroup.taskNames.flatMap((taskName) =>
      findTasksMadeWithName(taskName).slice(0, 1),
    );
    return make({
      name: rootTaskGroup.name,
      run: Effect.gen(function* () {
        if (missingTaskName !== undefined) {
          return yield* new TaskNotMadeWhileLoading({
            taskName: rootTaskGroup.name,
            requestedTaskName: missingTaskName,
          });
        }
        return yield* RUN_TASK_LIST[rootTaskGroup.mode](listedTasks, {
          concurrency: rootTaskGroup.concurrency,
        });
      }),
    });
  });
