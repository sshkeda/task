import { Cause, Clock, Deferred, Effect, Exit, FiberSet, Option, Ref } from "effect";
import type { Fiber, Schema, Tracer } from "effect";

import {
  dieOnInvariantFailure,
  InternalInvariantBroken,
} from "../../../../../shared/invariant-errors.ts";
import { nameTask } from "../../../../../shared/label-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import type { WorkerExited } from "../../../../../shared/process-errors.ts";
import { describeTraceFailure } from "../../../../../shared/report-cause.ts";
import type { TaskEvent, TaskFiberResult } from "../../../../../shared/rpc.ts";
import { isAwaitedTasksFailure } from "../../../../../shared/task-errors.ts";
import type {
  AwaitedTasksFailure,
  TaskBodyFailed,
  TaskCleanupFailed,
} from "../../../../../shared/task-errors.ts";
import {
  endTaskSpan,
  recordTaskSpanFailure,
  startTaskSpan,
} from "../../../../../shared/task-spans.ts";
import type { findTasksMadeByRootTaskFile } from "../../../shared/find-tasks-made-by-root-task-file.ts";
import type { WorkerTerminatedForAnotherTask } from "../../../shared/main-thread-errors.ts";
import { printTaskEnd, printTaskStart } from "../../../shared/write-task-output-files.ts";
import {
  isOnWorker,
  moveTaskRunOutcome,
  moveTaskRunStatus,
  TaskRunOutcome,
  TaskRunEvent,
  TaskRunOutcomeEvent,
  TaskRunStatus,
} from "./define-task-run-status.ts";
import {
  findStopRequestedAt,
  hasCleanupStarted,
  moveTaskStopStage,
  TaskStopStage,
} from "./define-task-stop-stage.ts";
import type { TaskStopEvent } from "./define-task-stop-stage.ts";
import { makeStopwatch } from "./make-stopwatch.ts";
import { makeRootTaskStandardInput } from "./share-standard-input-with-root-task.ts";

const STOP_FINISHED_EVENT = "stop finished";

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
  readonly taskSpan: Tracer.Span;
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
  runningCommandsById: new Map<
    string,
    {
      readonly programName: string;
      readonly calledAt: string | null;
      readonly startedAtMilliseconds: number;
    }
  >(),
  stop: makeTaskStop(),
  bodyFinishedDeferred: Deferred.makeUnsafe<void>(),
  cleanupSpan: Ref.makeUnsafe<Tracer.Span | null>(null),
  hasExecutionEnded: false,
  hasWorkerEndedTask: false,
  hasPrintedStart: false,
  hasEnded: false,
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

export const countWorkerLoad = (
  unfinishedTaskRunsOnWorker: readonly ReturnType<typeof makeTaskRun>[],
) => ({
  occupyingTaskCount: unfinishedTaskRunsOnWorker.filter((taskRun) => isOccupyingWorker(taskRun))
    .length,
  servingTaskCount: unfinishedTaskRunsOnWorker.filter((taskRun) => isServing(taskRun)).length,
});

const formatServedForTrace = (status: typeof TaskRunStatus.Type) =>
  TaskRunStatus.guards.Closed(status) ? "closed" : "serving";

const formatTaskRunForTrace = (taskRun: {
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

const nameStateOnEndedLine = (state: ReturnType<typeof formatTaskRunForTrace>) =>
  state === "cancelled" ? "stopped" : state;

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
          invariant: `${nameTask(taskName)} has an assigned worker before its body completes`,
        }),
      )
    : Effect.succeed(workerId);
};

const hasBodyFinished = TaskRunStatus.isAnyOf(["Closed", "Finished"]);

const endCleanupSpan = (taskRun: ReturnType<typeof makeTaskRun>) =>
  Effect.gen(function* () {
    const cleanupSpan = yield* Ref.get(taskRun.cleanupSpan);
    if (cleanupSpan === null) {
      return;
    }
    if (findStopRequestedAt(taskRun.stop.readStage()) !== null) {
      cleanupSpan.event(STOP_FINISHED_EVENT, yield* Clock.currentTimeNanos, {});
    }
    yield* endTaskSpan(cleanupSpan);
  });

const printEndAndEndSpans = (taskName: string, taskRun: ReturnType<typeof makeTaskRun>) =>
  Effect.gen(function* () {
    const state = formatTaskRunForTrace(taskRun);
    const endsWithoutClosedLine =
      taskRun.hasPrintedStart && !TaskRunStatus.guards.Closed(taskRun.status);
    if (endsWithoutClosedLine) {
      yield* printTaskEnd(taskName, nameStateOnEndedLine(state));
    }
    yield* endCleanupSpan(taskRun);
    taskRun.taskSpan.attribute(`${TASK_TRACE_LABEL_PREFIX}.state`, state);
    yield* endTaskSpan(taskRun.taskSpan);
  });

const endTaskRun = (taskName: string, taskRun: ReturnType<typeof makeTaskRun>) =>
  Effect.suspend(() => {
    if (taskRun.hasEnded) {
      return Effect.void;
    }
    taskRun.hasEnded = true;
    return printEndAndEndSpans(taskName, taskRun);
  }).pipe(Effect.uninterruptible);

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
  const moveOutcome = (
    taskName: string,
    event: typeof TaskRunOutcomeEvent.Type,
    failure: Parameters<typeof recordTaskSpanFailure>[1] | null = null,
  ) => {
    const taskRun = taskRunsByName.get(taskName);
    if (taskRun === undefined) {
      return;
    }
    const [next, action] = moveTaskRunOutcome(taskRun.outcome, event);
    taskRun.outcome = next;
    const failedNow =
      failure !== null && action !== "ignored" && TaskRunOutcome.guards.Failed(next);
    if (failedNow) {
      recordTaskSpanFailure(taskRun.taskSpan, failure);
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
    createRecord: (taskName: string, taskSpan: Tracer.Span) => {
      const taskRun = makeTaskRun({
        status: TaskRunStatus.cases.Preparing.make({}),
        outcome: TaskRunOutcome.cases.Pending.make({}),
        taskSpan,
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
              invariant: `${nameTask(taskName)} is waiting before worker assignment`,
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
    markServedStopped: (taskName: string) =>
      Effect.sync(() => {
        moveOutcome(taskName, TaskRunOutcomeEvent.cases.StopServed.make({}));
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
        describeTraceFailure(taskExit.cause),
      );
    },
    printStart: (taskName: string) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun === undefined) {
          return;
        }
        yield* printTaskStart(taskName);
        taskRun.hasPrintedStart = true;
      }).pipe(Effect.uninterruptible),
    endTaskSpanAfterExecution: (taskName: string) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun === undefined) {
          return;
        }
        taskRun.hasExecutionEnded = true;
        const isOffWorker =
          taskRun.hasWorkerEndedTask || TaskRunStatus.guards.Preparing(taskRun.status);
        if (isOffWorker) {
          yield* endTaskRun(taskName, taskRun);
        }
      }),
    endTaskSpanAfterWorker: (taskName: string) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun === undefined) {
          return;
        }
        taskRun.hasWorkerEndedTask = true;
        yield* endCleanupSpan(taskRun);
        if (taskRun.hasExecutionEnded) {
          yield* endTaskRun(taskName, taskRun);
        }
      }),
    openCleanupSpan: (taskName: string, startTime: bigint) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        if (taskRun === undefined) {
          return null;
        }
        const openedCleanupSpan = yield* Ref.get(taskRun.cleanupSpan);
        if (openedCleanupSpan !== null) {
          return openedCleanupSpan;
        }
        const cleanupSpan = yield* startTaskSpan("cleanup", `cleanup ${taskName}`, {
          parent: taskRun.taskSpan,
          attributes: { [`${TASK_TRACE_LABEL_PREFIX}.task`]: taskName },
          startTime,
        });
        yield* Ref.set(taskRun.cleanupSpan, cleanupSpan);
        return cleanupSpan;
      }),
    recordCleanupFailure: (
      taskName: string,
      failure: { readonly _tag: string; readonly message: string },
    ) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        const cleanupSpan = taskRun === undefined ? null : yield* Ref.get(taskRun.cleanupSpan);
        if (cleanupSpan !== null) {
          recordTaskSpanFailure(cleanupSpan, describeTraceFailure(Cause.fail(failure)));
        }
      }),
    recordFailureReachedAwaitingTask: (taskName: string) => {
      taskNamesWhoseFailureReachedAnAwaitingTask.add(taskName);
    },
    markFailed: (taskName: string, failure: { readonly _tag: string; readonly message: string }) =>
      Effect.sync(() => {
        moveOutcome(
          taskName,
          TaskRunOutcomeEvent.cases.Fail.make({}),
          describeTraceFailure(Cause.fail(failure)),
        );
      }),
  };
});
