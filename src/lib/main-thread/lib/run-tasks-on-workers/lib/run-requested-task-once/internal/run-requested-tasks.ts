import { Array as Arrays, Clock, Deferred, Effect, Exit, FiberSet } from "effect";

import { dieOnInvariantFailure } from "../../../../../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../../../shared/names-from-package-json.ts";
import type { TaskMadeInBody } from "../../../../../../../shared/rpc.ts";
import { startTaskSpan } from "../../../../../../../shared/task-spans.ts";
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
import { TaskOutputFiles } from "../../../../../shared/write-task-output-files.ts";
import { findAssignedWorkerId } from "../../../shared/define-task-run-state.ts";
import type { makeTaskRun, makeTaskRunsState } from "../../../shared/define-task-run-state.ts";
import { executeTask } from "./execute-task.ts";

const requestedTaskNotDiscovered = (requestedTaskName: string, requestingTaskName: string) =>
  new TaskNotMadeWhileLoading({ taskName: requestingTaskName, requestedTaskName });

const refuseBrokenTask = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  violations: Arrays.NonEmptyReadonlyArray<typeof TaskDefinitionViolation.Type>,
) {
  const brokenTasks = new BrokenTasksFound({ violations });
  yield* (yield* StopState).recordRunFailure(brokenTasks);
  yield* failRun(brokenTasks, taskRunsState.markFailed);
  return yield* new AwaitedTaskFailed({ awaitedTaskName: taskName });
});

const discoverTaskMadeLate = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  taskFilePath: string,
) {
  const checked = yield* findTaskMadeLate(taskName, taskFilePath);
  if (Arrays.isReadonlyArrayNonEmpty(checked.violations)) {
    return yield* refuseBrokenTask(taskRunsState, taskName, checked.violations);
  }
  if (checked.discovered !== null) {
    taskRunsState.discoveredTasksByName.set(taskName, checked.discovered);
  }
});

const refuseTaskMadeAgainElsewhere = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  firstLocation: string,
  secondTaskFilePath: string,
) {
  const checked = yield* findTaskMadeLate(taskName, secondTaskFilePath);
  const violations = Arrays.isReadonlyArrayNonEmpty(checked.violations)
    ? checked.violations
    : Arrays.make(
        new TaskMadeMoreThanOnce({ taskName, location: firstLocation, madeCount: 2 }),
        new TaskMadeMoreThanOnce({ taskName, location: secondTaskFilePath, madeCount: 2 }),
      );
  return yield* refuseBrokenTask(taskRunsState, taskName, violations);
});

const discoverTaskMadeInBody = Effect.fn(function* (
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
  const checked = checkTasksMadeWithName(
    taskName,
    locations.map((location) => ({ location: location ?? undefined, options: madeInBody.options })),
    workerId,
  );
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

const REQUESTED_TASK_EVENT = "requested task";

export const runOnce = <Success, Failure, Services>(
  resultDeferreds: Map<string, Deferred.Deferred<Success, Failure>>,
  taskName: string,
  start: (
    resultDeferred: Deferred.Deferred<Success, Failure>,
  ) => Effect.Effect<void, never, Services>,
) =>
  Effect.suspend(() => {
    const existingResultDeferred = resultDeferreds.get(taskName);
    if (existingResultDeferred !== undefined) {
      return Effect.succeed(existingResultDeferred);
    }
    const resultDeferred = Deferred.makeUnsafe<Success, Failure>();
    resultDeferreds.set(taskName, resultDeferred);
    return start(resultDeferred).pipe(
      Effect.onExit((startExit) =>
        Exit.isFailure(startExit)
          ? Deferred.failCause(resultDeferred, startExit.cause)
          : Effect.void,
      ),
      Effect.as(resultDeferred),
    );
  }).pipe(Effect.uninterruptible);

const runTaskOnce = (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  resultDeferreds: Map<
    string,
    Deferred.Deferred<
      Effect.Success<ReturnType<typeof executeTask>>,
      Effect.Error<ReturnType<typeof executeTask>>
    >
  >,
  taskName: string,
) =>
  runOnce(resultDeferreds, taskName, (resultDeferred) =>
    Effect.gen(function* () {
      const taskSpan = yield* startTaskSpan("task", taskName, {
        parent: taskRunsState.runSpan,
        attributes: { [`${TASK_TRACE_LABEL_PREFIX}.task`]: taskName },
      });
      taskRunsState.createRecord(taskName, taskSpan);
      yield* TaskOutputFiles.use((taskOutputFiles) =>
        taskOutputFiles.recordTaskSpan(taskName, taskSpan),
      );
      yield* FiberSet.run(
        taskRunsState.taskFibers,
        executeTask(taskRunsState, taskName).pipe(
          Effect.onExit((taskExit) => Deferred.done(resultDeferred, taskExit)),
          Effect.withParentSpan(taskSpan),
        ),
      );
    }),
  );

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

export const runRequestedTask = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  resultDeferreds: Parameters<typeof runTaskOnce>[1],
  taskName: string,
) {
  if (!taskRunsState.discoveredTasksByName.has(taskName)) {
    return yield* requestedTaskNotDiscovered(taskName, taskName);
  }
  const resultDeferred = yield* runTaskOnce(taskRunsState, resultDeferreds, taskName);
  return yield* Deferred.await(resultDeferred);
});

export const runRequestedDependencyTask = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  resultDeferreds: Parameters<typeof runTaskOnce>[1],
  dependencyTaskName: string,
  requestingTaskName: string,
  dependencyTaskFilePath: string | null,
  dependencyTaskMadeInBody: typeof TaskMadeInBody.Type | null,
) {
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
  const requestingTaskRun = taskRunsState.runsByTaskName.get(requestingTaskName);
  requestingTaskRun?.taskSpan.event(REQUESTED_TASK_EVENT, yield* Clock.currentTimeNanos, {
    [`${TASK_TRACE_LABEL_PREFIX}.wanted_task`]: dependencyTaskName,
  });
  const requestingTaskDependencies = requestingTaskRun?.awaitedTaskNames;
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
