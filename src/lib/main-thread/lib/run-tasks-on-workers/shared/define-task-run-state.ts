import { Cause, Deferred, Effect, Exit, FiberSet, Option, Schema } from "effect";
import type { Fiber } from "effect";

import {
  dieOnInvariantFailure,
  InternalInvariantBroken,
} from "../../../../../shared/invariant-errors.ts";
import type { WorkerExited } from "../../../../../shared/process-errors.ts";
import type { TaskEvent, TaskFiberResult } from "../../../../../shared/rpc.ts";
import { isAwaitedTasksFailure } from "../../../../../shared/task-errors.ts";
import type {
  AwaitedTasksFailure,
  TaskBodyFailed,
  TaskCleanupFailed,
} from "../../../../../shared/task-errors.ts";
import type { findTasksMadeByRootTaskFile } from "../../../shared/find-tasks-made-by-root-task-file.ts";
import { makeStopwatch } from "./make-stopwatch.ts";
import { makeRootTaskStandardInput } from "./share-standard-input-with-root-task.ts";

export const TaskRunStatus = Schema.TaggedUnion({
  Preparing: {},
  Running: { workerId: Schema.Int },
  Serving: { workerId: Schema.Int },
  Closed: { workerId: Schema.Int },
  Passed: { workerId: Schema.Int },
  Failed: {
    workerId: Schema.NullOr(Schema.Int),
    causedByAwaitedTasks: Schema.Boolean,
  },
  Cancelled: { workerId: Schema.NullOr(Schema.Int) },
});

export const makeTaskRun = (initial: {
  readonly status: typeof TaskRunStatus.Type;
  readonly stepBeforeBody: string | null;
}) => ({
  ...initial,
  awaitedTaskNames: new Array<string>(),
  requestFibersByDependencyRequestId: new Map<number, Fiber.Fiber<unknown, unknown>>(),
  bodyRequestedTaskNames: new Set<string>(),
  bodyWaitingOnRequestedTasks: makeStopwatch(),
  bodyOutcomeDeferred: Deferred.makeUnsafe<
    typeof TaskEvent.cases.TaskFiberServed.Type | typeof TaskFiberResult.cases.Returned.Type,
    TaskBodyFailed | TaskCleanupFailed | WorkerExited | typeof AwaitedTasksFailure.Type
  >(),
  bodyLoadingDeferred: Deferred.makeUnsafe<void>(),
  bodyStartedAtMillisecondsDeferred: Deferred.makeUnsafe<number>(),
  bodyCleanupStartedDeferred: Deferred.makeUnsafe<void>(),
  bodyFailedBeforeCleanupDeferred: Deferred.makeUnsafe<void>(),
  stopRequestedAtMillisecondsDeferred: Deferred.makeUnsafe<number>(),
  cleanupLimitPassedDeferred: Deferred.makeUnsafe<void>(),
  bodyFinishedDeferred: Deferred.makeUnsafe<void>(),
});

export const findAssignedWorkerId = (status: typeof TaskRunStatus.Type) =>
  "workerId" in status ? status.workerId : null;

export const isUnfinishedOnWorker = (taskRun: ReturnType<typeof makeTaskRun>, workerId: number) =>
  findAssignedWorkerId(taskRun.status) === workerId &&
  !Deferred.isDoneUnsafe(taskRun.bodyFinishedDeferred);

const isServingStatus = TaskRunStatus.isAnyOf(["Serving"]);

const isServing = (taskRun: ReturnType<typeof makeTaskRun>) => isServingStatus(taskRun.status);

const isOccupyingWorker = (taskRun: ReturnType<typeof makeTaskRun>) =>
  !isServing(taskRun) && taskRun.requestFibersByDependencyRequestId.size === 0;

export const findTaskNamesOccupyingWorker = (
  runsByTaskName: ReadonlyMap<string, ReturnType<typeof makeTaskRun>>,
  workerId: number,
) =>
  [...runsByTaskName]
    .filter(([, taskRun]) => isUnfinishedOnWorker(taskRun, workerId) && isOccupyingWorker(taskRun))
    .map(([taskName]) => taskName);

export const formatWaitingForWorker = (
  workerId: number,
  longestOccupyingTaskName: string,
  otherTaskNames: readonly string[],
) => {
  const others =
    otherTaskNames.length === 0
      ? ""
      : ` and ${otherTaskNames.length} other task${otherTaskNames.length === 1 ? "" : "s"}`;
  return `waiting for worker ${workerId}, which is running "${longestOccupyingTaskName}"${others}`;
};

export const countWorkerLoad = (
  unfinishedTaskRunsOnWorker: readonly ReturnType<typeof makeTaskRun>[],
) => ({
  occupyingTaskCount: unfinishedTaskRunsOnWorker.filter((taskRun) => isOccupyingWorker(taskRun))
    .length,
  servingTaskCount: unfinishedTaskRunsOnWorker.filter((taskRun) => isServing(taskRun)).length,
  unfinishedTaskCount: unfinishedTaskRunsOnWorker.length,
});

export const formatTaskRunStatusForTrace = (status: typeof TaskRunStatus.Type) =>
  TaskRunStatus.guards.Preparing(status) ? "running" : status._tag.toLowerCase();

const withReadOnlyStatus = (
  taskRun: ReturnType<typeof makeTaskRun>,
): Omit<ReturnType<typeof makeTaskRun>, "status"> & {
  readonly status: typeof TaskRunStatus.Type;
} => taskRun;

const requireAssignedWorkerId = (taskName: string, status: typeof TaskRunStatus.Type) => {
  const workerId = findAssignedWorkerId(status);
  return workerId === null
    ? Effect.die(
        new InternalInvariantBroken({
          area: "task lifecycle",
          invariant: `"${taskName}" has an assigned worker before its body completes`,
        }),
      )
    : Effect.succeed(workerId);
};

export const makeTaskRunsState = Effect.gen(function* () {
  const discoveredTasksByName: Effect.Success<ReturnType<typeof findTasksMadeByRootTaskFile>> =
    new Map();
  const taskNamesWhoseFailureReachedAnAwaitingTask = new Set<string>();
  const taskRunsByName = new Map<string, ReturnType<typeof makeTaskRun>>();
  const taskRunsSeenByCallers: ReadonlyMap<
    string,
    ReturnType<typeof withReadOnlyStatus>
  > = taskRunsByName;
  const taskNamesAssignedToWorker = new Map<number, Set<string>>();
  const workerLoad = { changed: Deferred.makeUnsafe<void>() };
  return {
    discoveredTasksByName,
    taskNamesWhoseFailureReachedAnAwaitingTask,
    runsByTaskName: taskRunsSeenByCallers,
    taskFibers: yield* FiberSet.make(),
    rootTaskStandardInput: yield* makeRootTaskStandardInput,
    runSpan: yield* Effect.currentSpan.pipe(
      dieOnInvariantFailure("task runs", "task execution has a run span"),
    ),
    createRecord: (taskName: string) => {
      const taskRun = makeTaskRun({
        status: TaskRunStatus.cases.Preparing.make({}),
        stepBeforeBody: null,
      });
      taskRunsByName.set(taskName, taskRun);
      return withReadOnlyStatus(taskRun);
    },
    recordWorkerAssignment: (taskName: string, workerId: number) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        const isNotAwaitingWorker =
          taskRun === undefined || !TaskRunStatus.guards.Preparing(taskRun.status);
        if (isNotAwaitingWorker) {
          return yield* Effect.die(
            new InternalInvariantBroken({
              area: "task lifecycle",
              invariant: `"${taskName}" is waiting before worker assignment`,
            }),
          );
        }
        taskRun.status = TaskRunStatus.cases.Running.make({ workerId });
        const assignedTaskNames = taskNamesAssignedToWorker.get(workerId) ?? new Set();
        taskNamesAssignedToWorker.set(workerId, assignedTaskNames.add(taskName));
      }),
    readWorkerLoad: (workerId: number) => {
      const assignedTaskNames = taskNamesAssignedToWorker.get(workerId) ?? new Set();
      const unfinishedTaskRuns: ReturnType<typeof makeTaskRun>[] = [];
      for (const taskName of assignedTaskNames) {
        const taskRun = taskRunsByName.get(taskName);
        const isNoLongerOnWorker =
          taskRun === undefined || !isUnfinishedOnWorker(taskRun, workerId);
        if (isNoLongerOnWorker) {
          assignedTaskNames.delete(taskName);
        } else {
          unfinishedTaskRuns.push(taskRun);
        }
      }
      return countWorkerLoad(unfinishedTaskRuns);
    },
    awaitNextWorkerLoadChange: () => Deferred.await(workerLoad.changed),
    refreshBodyClocks: (taskName: string, nowMilliseconds: number) => {
      Deferred.doneUnsafe(workerLoad.changed, Exit.void);
      workerLoad.changed = Deferred.makeUnsafe();
      const taskRun = taskRunsByName.get(taskName);
      if (taskRun === undefined) {
        return;
      }
      taskRun.bodyWaitingOnRequestedTasks.update(
        taskRun.requestFibersByDependencyRequestId.size > 0,
        nowMilliseconds,
      );
    },
    markServing: (taskName: string) => {
      const taskRun = taskRunsByName.get(taskName);
      if (taskRun === undefined) {
        return;
      }
      if (TaskRunStatus.guards.Running(taskRun.status)) {
        taskRun.status = TaskRunStatus.cases.Serving.make({
          workerId: taskRun.status.workerId,
        });
      }
    },
    markClosed: (taskName: string) => {
      const taskRun = taskRunsByName.get(taskName);
      if (taskRun === undefined) {
        return false;
      }
      if (!TaskRunStatus.guards.Serving(taskRun.status)) {
        return false;
      }
      taskRun.status = TaskRunStatus.cases.Closed.make({
        workerId: taskRun.status.workerId,
      });
      return true;
    },
    markPassed: (taskName: string) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun !== undefined) {
          taskRun.status = TaskRunStatus.cases.Passed.make({
            workerId: yield* requireAssignedWorkerId(taskName, taskRun.status),
          });
        }
      }),
    markEndedFromExit: (
      taskName: string,
      taskExit: Exit.Exit<Schema.Json | undefined, unknown>,
    ) => {
      const taskRun = taskRunsByName.get(taskName);
      const isNotEndingFromFailure =
        taskRun === undefined ||
        (!TaskRunStatus.guards.Preparing(taskRun.status) &&
          !TaskRunStatus.guards.Running(taskRun.status)) ||
        !Exit.isFailure(taskExit);
      if (isNotEndingFromFailure) {
        return;
      }
      const workerId = findAssignedWorkerId(taskRun.status);
      if (Cause.hasInterruptsOnly(taskExit.cause)) {
        taskRun.status = TaskRunStatus.cases.Cancelled.make({ workerId });
        return;
      }
      const failure = Cause.findErrorOption(taskExit.cause);
      taskRun.status = TaskRunStatus.cases.Failed.make({
        workerId,
        causedByAwaitedTasks: Option.exists(failure, isAwaitedTasksFailure),
      });
    },
    recordFailureReachedAwaitingTask: (taskName: string) => {
      taskNamesWhoseFailureReachedAnAwaitingTask.add(taskName);
    },
    markFailed: (taskName: string) =>
      Effect.sync(() => {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun !== undefined) {
          taskRun.status = TaskRunStatus.cases.Failed.make({
            workerId: findAssignedWorkerId(taskRun.status),
            causedByAwaitedTasks: false,
          });
        }
      }),
  };
});
