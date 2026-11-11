import { Array as Arrays, Deferred, Effect, FiberSet } from "effect";

import { formatFailureLines } from "../../../../../../../shared/format-error-lines.ts";
import { dieOnInvariantFailure } from "../../../../../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../../../shared/names-from-package-json.ts";
import { printLine } from "../../../../../../../shared/print-to-standard-error.ts";
import type { TaskMadeInBody } from "../../../../../../../shared/rpc.ts";
import {
  AwaitedTaskFailed,
  TaskNotMadeWhileLoading,
  TaskWaitsOnItself,
} from "../../../../../../errors/index.ts";
import { failRun } from "../../../../../shared/fail-run.ts";
import {
  checkTasksMadeWithName,
  findTaskMadeLate,
} from "../../../../../shared/find-tasks-made-by-root-task-file.ts";
import {
  BrokenTasksFound,
  TaskIsolatedInBody,
  TaskMadeMoreThanOnce,
} from "../../../../../shared/main-thread-errors.ts";
import type { TaskDefinitionViolation } from "../../../../../shared/main-thread-errors.ts";
import { StopState } from "../../../../../shared/provide-stop-state.ts";
import { findAssignedWorkerId } from "../../../shared/define-task-run-state.ts";
import type { makeTaskRun, makeTaskRunsState } from "../../../shared/define-task-run-state.ts";
import { executeTask } from "./execute-task.ts";

const requestedTaskNotDiscovered = (requestedTaskName: string, requestingTaskName: string) =>
  new TaskNotMadeWhileLoading({ taskName: requestingTaskName, requestedTaskName });

const refuseBrokenTask = Effect.fn("refuseBrokenTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  violations: Arrays.NonEmptyReadonlyArray<typeof TaskDefinitionViolation.Type>,
) {
  const brokenTasks = new BrokenTasksFound({ violations });
  yield* printLine("Error", formatFailureLines(brokenTasks, null));
  yield* (yield* StopState).recordPrintedFailure(brokenTasks);
  yield* failRun(brokenTasks, taskRunsState.markFailed);
  return yield* new AwaitedTaskFailed({ awaitedTaskName: taskName });
});

const discoverTaskMadeLate = Effect.fn("discoverTaskMadeLate")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  taskFilePath: string,
) {
  const checked = yield* findTaskMadeLate(taskName, taskFilePath).pipe(
    Effect.catch((error) => Effect.die(error)),
  );
  if (Arrays.isReadonlyArrayNonEmpty(checked.violations)) {
    return yield* refuseBrokenTask(taskRunsState, taskName, checked.violations);
  }
  if (checked.discovered !== null) {
    taskRunsState.discoveredTasksByName.set(taskName, checked.discovered);
  }
});

const refuseTaskMadeAgainElsewhere = Effect.fn("refuseTaskMadeAgainElsewhere")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  firstLocation: string,
  secondTaskFilePath: string,
) {
  const checked = yield* findTaskMadeLate(taskName, secondTaskFilePath).pipe(
    Effect.catch((error) => Effect.die(error)),
  );
  const violations = Arrays.isReadonlyArrayNonEmpty(checked.violations)
    ? checked.violations
    : Arrays.make(
        new TaskMadeMoreThanOnce({ taskName, location: firstLocation, madeCount: 2 }),
        new TaskMadeMoreThanOnce({ taskName, location: secondTaskFilePath, madeCount: 2 }),
      );
  return yield* refuseBrokenTask(taskRunsState, taskName, violations);
});

const discoverTaskMadeInBody = Effect.fn("discoverTaskMadeInBody")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  requestingTaskName: string,
  madeInBody: typeof TaskMadeInBody.Type,
) {
  const requestingTaskRun = taskRunsState.runsByTaskName.get(requestingTaskName);
  const workerId = yield* Effect.fromNullishOr(
    requestingTaskRun === undefined ? null : findAssignedWorkerId(requestingTaskRun.status),
  ).pipe(
    dieOnInvariantFailure("task requests", "a task body that requests a task runs on a worker"),
  );
  const discovered = taskRunsState.discoveredTasksByName.get(taskName);
  const isOnlyTaskWithNameOnWorker = madeInBody.locations.length === 1;
  const isAlreadyDiscovered = discovered?.madeOnWorkerId === workerId && isOnlyTaskWithNameOnWorker;
  if (isAlreadyDiscovered) {
    return;
  }
  const isDiscoveredTaskListedByWorker =
    discovered === undefined ||
    ((discovered.madeOnWorkerId === null || discovered.madeOnWorkerId === workerId) &&
      !isOnlyTaskWithNameOnWorker);
  const locations = isDiscoveredTaskListedByWorker
    ? madeInBody.locations
    : [discovered.location, ...madeInBody.locations];
  const checked = yield* checkTasksMadeWithName(
    taskName,
    locations.map((location) => ({ location: location ?? undefined, options: madeInBody.options })),
    workerId,
  ).pipe(Effect.catch((error) => Effect.die(error)));
  const violations = [
    ...checked.violations,
    ...(madeInBody.options.isolated === true
      ? [new TaskIsolatedInBody({ taskName, madeInTaskName: requestingTaskName })]
      : []),
  ];
  if (Arrays.isReadonlyArrayNonEmpty(violations)) {
    return yield* refuseBrokenTask(taskRunsState, taskName, violations);
  }
  if (checked.discovered !== null) {
    taskRunsState.discoveredTasksByName.set(taskName, checked.discovered);
  }
});

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
  dependencyTaskFilePath: string | null,
  dependencyTaskMadeInBody: typeof TaskMadeInBody.Type | null,
) {
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.task`]: dependencyTaskName,
    [`${TASK_TRACE_LABEL_PREFIX}.requested_by`]: requestingTaskName,
  });
  if (dependencyTaskMadeInBody !== null) {
    yield* discoverTaskMadeInBody(
      taskRunsState,
      dependencyTaskName,
      requestingTaskName,
      dependencyTaskMadeInBody,
    );
  }
  const discoveredWithName = taskRunsState.discoveredTasksByName.get(dependencyTaskName);
  const isMadeAgainElsewhere =
    discoveredWithName !== undefined &&
    dependencyTaskMadeInBody === null &&
    dependencyTaskFilePath !== null &&
    discoveredWithName.filePath !== dependencyTaskFilePath;
  if (isMadeAgainElsewhere) {
    return yield* refuseTaskMadeAgainElsewhere(
      taskRunsState,
      dependencyTaskName,
      discoveredWithName.location,
      dependencyTaskFilePath,
    );
  }
  const isMadeLate = discoveredWithName === undefined && dependencyTaskFilePath !== null;
  if (isMadeLate) {
    yield* discoverTaskMadeLate(taskRunsState, dependencyTaskName, dependencyTaskFilePath);
  }
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
    Effect.mapBoth({
      onSuccess: (result) => structuredClone(result),
      onFailure: () => new AwaitedTaskFailed({ awaitedTaskName: dependencyTaskName }),
    }),
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
