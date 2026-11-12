import { Cause, Clock, Deferred, Effect, Exit, FiberSet, Option, Ref } from "effect";
import type { Fiber, Schema } from "effect";

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
import type { WorkerTerminatedForAnotherTask } from "../../../shared/main-thread-errors.ts";
import {
  isOnWorker,
  moveTaskRunOutcome,
  moveTaskRunStatus,
  TaskRunOutcome,
  TaskRunEvent,
  TaskRunOutcomeEvent,
  TaskRunStatus,
} from "./define-task-run-status.ts";
import { hasCleanupStarted, moveTaskStopStage, TaskStopStage } from "./define-task-stop-stage.ts";
import type { TaskStopEvent } from "./define-task-stop-stage.ts";
import { makeStopwatch } from "./make-stopwatch.ts";
import { makeRootTaskStandardInput } from "./share-standard-input-with-root-task.ts";

const makeTaskStop = () => {
  const stage = Ref.makeUnsafe<typeof TaskStopStage.Type>(TaskStopStage.cases.NotAsked.make({}));
  const cleanupStartedDeferred = Deferred.makeUnsafe<void>();
  return {
    readStage: () => Ref.getUnsafe(stage),
    awaitCleanupStarted: Deferred.await(cleanupStartedDeferred),
    move: (event: typeof TaskStopEvent.Type) =>
      Ref.modify(stage, (current) => {
        const moved = moveTaskStopStage(current, event);
        return [moved, moved[0]];
      }).pipe(
        Effect.tap(([next]) =>
          hasCleanupStarted(next) ? Deferred.done(cleanupStartedDeferred, Exit.void) : Effect.void,
        ),
        Effect.uninterruptible,
      ),
  };
};

export const makeTaskRun = (initial: {
  readonly status: typeof TaskRunStatus.Type;
  readonly outcome: typeof TaskRunOutcome.Type;
}) => ({
  ...initial,
  awaitedTaskNames: new Array<string>(),
  requestFibersByDependencyRequestId: new Map<number, Fiber.Fiber<unknown, unknown>>(),
  dependencyRequestIdsAwaitingAnswer: new Set<number>(),
  bodyRequestedTaskNames: new Set<string>(),
  bodyWaitingOnRequestedTasks: makeStopwatch(),
  bodyOutcomeDeferred: Deferred.makeUnsafe<
    typeof TaskEvent.cases.TaskFiberServed.Type | typeof TaskFiberResult.cases.Returned.Type,
    | TaskBodyFailed
    | TaskCleanupFailed
    | WorkerExited
    | WorkerTerminatedForAnotherTask
    | typeof AwaitedTasksFailure.Type
  >(),
  bodyStartedAtMillisecondsDeferred: Deferred.makeUnsafe<number>(),
  stop: makeTaskStop(),
  bodyFinishedDeferred: Deferred.makeUnsafe<void>(),
});

export const findAssignedWorkerId = (status: typeof TaskRunStatus.Type) =>
  "workerId" in status ? status.workerId : null;

export const isUnfinishedOnWorker = (taskRun: ReturnType<typeof makeTaskRun>, workerId: number) =>
  isOnWorker(taskRun.status) && taskRun.status.workerId === workerId;

const isServing = (taskRun: ReturnType<typeof makeTaskRun>) =>
  TaskRunStatus.guards.Serving(taskRun.status);

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
});

const formatServedForTrace = (status: typeof TaskRunStatus.Type) =>
  TaskRunStatus.guards.Closed(status) ? "closed" : "serving";

export const formatTaskRunForTrace = (taskRun: {
  readonly status: typeof TaskRunStatus.Type;
  readonly outcome: typeof TaskRunOutcome.Type;
}) =>
  TaskRunOutcome.match(taskRun.outcome, {
    Pending: () => "running",
    Served: () => formatServedForTrace(taskRun.status),
    Passed: () => "passed",
    Failed: () => "failed",
    Cancelled: () => "cancelled",
  });

const withReadOnlyStatus = (
  taskRun: ReturnType<typeof makeTaskRun>,
): Omit<ReturnType<typeof makeTaskRun>, "outcome" | "status"> & {
  readonly status: typeof TaskRunStatus.Type;
  readonly outcome: typeof TaskRunOutcome.Type;
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

const hasBodyFinished = TaskRunStatus.isAnyOf(["Closed", "Finished"]);

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
  const moveTaskRun = (taskName: string, event: typeof TaskRunEvent.Type) =>
    Effect.gen(function* () {
      const taskRun = taskRunsByName.get(taskName);
      if (taskRun === undefined) {
        return "ignored" as const;
      }
      const [next, action] = moveTaskRunStatus(taskRun.status, event);
      taskRun.status = next;
      if (action === "recordBodyStart") {
        yield* Deferred.succeed(
          taskRun.bodyStartedAtMillisecondsDeferred,
          yield* Clock.currentTimeMillis,
        );
      }
      if (hasBodyFinished(next)) {
        yield* Deferred.done(taskRun.bodyFinishedDeferred, Exit.void);
      }
      return action;
    }).pipe(Effect.uninterruptible);
  const moveOutcome = (taskName: string, event: typeof TaskRunOutcomeEvent.Type) => {
    const taskRun = taskRunsByName.get(taskName);
    if (taskRun !== undefined) {
      taskRun.outcome = moveTaskRunOutcome(taskRun.outcome, event)[0];
    }
  };
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
        outcome: TaskRunOutcome.cases.Pending.make({}),
      });
      taskRunsByName.set(taskName, taskRun);
      return withReadOnlyStatus(taskRun);
    },
    moveTaskRun,
    recordWorkerAssignment: (taskName: string, workerId: number) =>
      Effect.gen(function* () {
        const action = yield* moveTaskRun(taskName, TaskRunEvent.cases.Assign.make({ workerId }));
        if (action === "ignored") {
          return yield* Effect.die(
            new InternalInvariantBroken({
              area: "task lifecycle",
              invariant: `"${taskName}" is waiting before worker assignment`,
            }),
          );
        }
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
    markServed: (taskName: string) =>
      Effect.sync(() => {
        moveOutcome(taskName, TaskRunOutcomeEvent.cases.Serve.make({}));
      }),
    markPassed: (taskName: string) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun !== undefined) {
          yield* requireAssignedWorkerId(taskName, taskRun.status);
          moveOutcome(taskName, TaskRunOutcomeEvent.cases.Pass.make({}));
        }
      }),
    markEndedFromExit: (
      taskName: string,
      taskExit: Exit.Exit<Schema.Json | undefined, unknown>,
    ) => {
      if (Exit.isSuccess(taskExit)) {
        return;
      }
      if (Cause.hasInterruptsOnly(taskExit.cause)) {
        moveOutcome(taskName, TaskRunOutcomeEvent.cases.Cancel.make({}));
        return;
      }
      moveOutcome(
        taskName,
        TaskRunOutcomeEvent.cases.FailOnExit.make({
          causedByAwaitedTasks: Option.exists(
            Cause.findErrorOption(taskExit.cause),
            isAwaitedTasksFailure,
          ),
        }),
      );
    },
    recordFailureReachedAwaitingTask: (taskName: string) => {
      taskNamesWhoseFailureReachedAnAwaitingTask.add(taskName);
    },
    markFailed: (taskName: string) =>
      Effect.sync(() => {
        moveOutcome(taskName, TaskRunOutcomeEvent.cases.Fail.make({}));
      }),
  };
});
