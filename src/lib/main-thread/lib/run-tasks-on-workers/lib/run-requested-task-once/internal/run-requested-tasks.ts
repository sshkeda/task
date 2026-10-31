import { Deferred, Effect, FiberSet } from "effect";

import { TASK_TRACE_LABEL_PREFIX } from "../../../../../../../shared/names-from-package-json.ts";
import {
  AwaitedTaskFailed,
  TaskNotMadeAtTopLevel,
  TaskWaitsOnItself,
} from "../../../../../../errors/index.ts";
import type { makeTaskRun, makeTaskRunsState } from "../../../shared/define-task-run-state.ts";
import { executeTask } from "./execute-task.ts";

const requestedTaskNotDiscovered = (requestedTaskName: string, requestingTaskName: string) =>
  new TaskNotMadeAtTopLevel({ taskName: requestingTaskName, requestedTaskName });

const runTaskOnce = Effect.fn("runTaskOnce")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  resultDeferreds: Map<
    string,
    Deferred.Deferred<
      Effect.Success<ReturnType<typeof executeTask>>,
      Effect.Error<ReturnType<typeof executeTask>>
    >
  >,
  taskName: string,
) {
  const existingResultDeferred = resultDeferreds.get(taskName);
  if (existingResultDeferred !== undefined) {
    return existingResultDeferred;
  }
  taskRunsState.createRecord(taskName);
  const resultDeferred: Parameters<typeof resultDeferreds.set>[1] = Deferred.makeUnsafe();
  resultDeferreds.set(taskName, resultDeferred);
  yield* FiberSet.run(
    taskRunsState.taskFibers,
    executeTask(taskRunsState, taskName).pipe(
      Effect.onExit((taskExit) => Deferred.done(resultDeferred, taskExit)),
      Effect.withParentSpan(taskRunsState.runSpan),
    ),
  );
  return resultDeferred;
});

const findDependencyPathToRequestingTask = (
  taskRunsByName: ReadonlyMap<string, ReturnType<typeof makeTaskRun>>,
  dependencyTaskName: string,
  requestingTaskName: string,
): readonly string[] | null => {
  const visitedTaskNames = new Set<string>();
  const findDependencyPath = (currentTaskName: string): readonly string[] | null => {
    if (currentTaskName === requestingTaskName) {
      return [currentTaskName];
    }
    if (visitedTaskNames.has(currentTaskName)) {
      return null;
    }

    visitedTaskNames.add(currentTaskName);
    for (const nextDependencyTaskName of taskRunsByName.get(currentTaskName)?.awaitedTaskNames ??
      []) {
      const remainingDependencyPath = findDependencyPath(nextDependencyTaskName);
      if (remainingDependencyPath !== null) {
        return [currentTaskName, ...remainingDependencyPath];
      }
    }
    return null;
  };
  return findDependencyPath(dependencyTaskName);
};

export const runRequestedTask = Effect.fn("runRequestedTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  resultDeferreds: Parameters<typeof runTaskOnce>[1],
  taskName: string,
) {
  yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName);
  if (!taskRunsState.discoveredTasksByName.has(taskName)) {
    return yield* requestedTaskNotDiscovered(taskName, taskName);
  }
  const resultDeferred = yield* runTaskOnce(taskRunsState, resultDeferreds, taskName);
  return yield* Deferred.await(resultDeferred);
});

export const runRequestedDependencyTask = Effect.fn("runRequestedDependencyTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  resultDeferreds: Parameters<typeof runTaskOnce>[1],
  dependencyTaskName: string,
  requestingTaskName: string,
) {
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.task`]: dependencyTaskName,
    [`${TASK_TRACE_LABEL_PREFIX}.requested_by`]: requestingTaskName,
  });
  if (!taskRunsState.discoveredTasksByName.has(dependencyTaskName)) {
    return yield* requestedTaskNotDiscovered(dependencyTaskName, requestingTaskName);
  }
  const existingResultDeferred = resultDeferreds.get(dependencyTaskName);
  const isTaskRunInProgress =
    existingResultDeferred !== undefined && !Deferred.isDoneUnsafe(existingResultDeferred);
  if (isTaskRunInProgress) {
    const dependencyCycle = findDependencyPathToRequestingTask(
      taskRunsState.runsByTaskName,
      dependencyTaskName,
      requestingTaskName,
    );
    if (dependencyCycle !== null) {
      return yield* new TaskWaitsOnItself({
        taskName: requestingTaskName,
        cycle: [requestingTaskName, ...dependencyCycle],
      });
    }
  }
  const requestingTaskDependencies =
    taskRunsState.runsByTaskName.get(requestingTaskName)?.awaitedTaskNames;
  requestingTaskDependencies?.push(dependencyTaskName);
  const awaitedResultDeferred = yield* runTaskOnce(
    taskRunsState,
    resultDeferreds,
    dependencyTaskName,
  );
  return yield* Deferred.await(awaitedResultDeferred).pipe(
    Effect.map((result) => structuredClone(result)),
    Effect.mapError(() => new AwaitedTaskFailed({ awaitedTaskName: dependencyTaskName })),
    Effect.ensuring(
      Effect.sync(() => {
        if (requestingTaskDependencies === undefined) {
          return;
        }
        requestingTaskDependencies.splice(
          requestingTaskDependencies.indexOf(dependencyTaskName),
          1,
        );
      }),
    ),
  );
});
