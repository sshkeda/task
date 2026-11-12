import { Cause, Context, Deferred, Effect, Exit, FiberSet } from "effect";
import type { FileSystem, Path, Schema } from "effect";

import { printLine, printTaskLines } from "../../../../../shared/print-to-standard-error.ts";
import type { TaskMadeInBody } from "../../../../../shared/rpc.ts";
import type { TaskRequestFailure } from "../../../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import { failRunOnFailure, failTask } from "../../../shared/fail-run.ts";
import {
  ServingTaskRequestsItself,
  TaskCleanupCutOff,
} from "../../../shared/main-thread-errors.ts";
import type { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import type { RunRootTaskConfig } from "../../../shared/provide-run-root-task-config.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import type { WorkerEvent, WorkerPool } from "../../../shared/provide-worker-pool.ts";
import { reportTaskFailure } from "../../../shared/report-task-failure.ts";
import type { TraceFile } from "../../../shared/write-trace-file.ts";
import {
  runRequestedTask,
  runRequestedDependencyTask,
} from "../lib/run-requested-task-once/index.ts";
import { makeTaskRunsState } from "../shared/define-task-run-state.ts";
import { isInBody, TaskRunOutcome, TaskRunStatus } from "../shared/define-task-run-status.ts";
import { hasCleanupLimitPassed, TaskStopEvent } from "../shared/define-task-stop-stage.ts";
import { interruptDependencyRequests, stopTask } from "../shared/stop-task.ts";
import { handleWorkerEvent } from "./handle-worker-events.ts";

export const makeTaskRuns = Effect.gen(function* () {
  const taskExecutionServices = yield* Effect.context<
    | FileSystem.FileSystem
    | Path.Path
    | ProcessGroups
    | RunRootTaskConfig
    | StopState
    | TraceFile
    | WorkerPool
  >();
  const taskRunsState = yield* makeTaskRunsState;
  const resultDeferreds: Parameters<typeof runRequestedTask>[1] = new Map();
  const {
    discoveredTasksByName,
    taskNamesWhoseFailureReachedAnAwaitingTask,
    runsByTaskName: taskRunsByName,
    markFailed,
  } = taskRunsState;
  const requestDependencyTaskResult = (
    dependencyTaskName: string,
    requestingTaskName: string,
    dependencyTaskFilePath: string | null,
    dependencyTaskMadeInBody: typeof TaskMadeInBody.Type | null,
  ): Effect.Effect<Schema.Json | undefined, typeof TaskRequestFailure.Type> =>
    runRequestedDependencyTask(
      taskRunsState,
      resultDeferreds,
      dependencyTaskName,
      requestingTaskName,
      dependencyTaskFilePath,
      dependencyTaskMadeInBody,
    ).pipe(Effect.provide(taskExecutionServices));
  return {
    registerDiscoveredTasks: (
      entries: ReadonlyMap<string, Parameters<typeof discoveredTasksByName.set>[1]>,
    ) =>
      Effect.sync(() => {
        for (const [taskName, discoveredTask] of entries) {
          discoveredTasksByName.set(taskName, discoveredTask);
        }
      }),
    requestRootTaskResult: (taskName: string) =>
      runRequestedTask(taskRunsState, resultDeferreds, taskName).pipe(
        Effect.tapError(() =>
          Effect.sync(() => taskNamesWhoseFailureReachedAnAwaitingTask.add(taskName)),
        ),
        Effect.provide(taskExecutionServices),
      ),
    requestDependencyTaskResult,
    handleWorkerEvent: (event: typeof WorkerEvent.Type) =>
      handleWorkerEvent(taskRunsState, requestDependencyTaskResult, event).pipe(
        failRunOnFailure(markFailed),
        Effect.provide(taskExecutionServices),
      ),
    stopAll: Effect.gen(function* () {
      const stopState = yield* StopState;
      yield* stopState.begin;
      const unfinishedTaskNames = new Set(
        [...taskRunsByName]
          .filter(([, taskRun]) => TaskRunOutcome.guards.Pending(taskRun.outcome))
          .map(([taskName]) => taskName),
      );
      yield* FiberSet.clear(taskRunsState.taskFibers);
      yield* Effect.forEach(
        taskRunsByName.values(),
        (taskRun) =>
          taskRun.stop
            .move(TaskStopEvent.cases.EndRun.make({}))
            .pipe(
              Effect.flatMap(([, action]) =>
                action === "interruptRequests" ? interruptDependencyRequests(taskRun) : Effect.void,
              ),
            ),
        { discard: true },
      );
      const workerWatches = yield* FiberSet.make<void, never>();
      const unorderedStops = Array.from(taskRunsByName, ([taskName, taskRun]) => ({
        taskName,
        isServing: TaskRunStatus.guards.Serving(taskRun.status),
        isCompleted: TaskRunOutcome.guards.Passed(taskRun.outcome),
        requestedTaskNames: taskRun.bodyRequestedTaskNames,
        stopped: Deferred.makeUnsafe<void>(),
      }));
      const stopsByTaskName = new Map(unorderedStops.map((stop) => [stop.taskName, stop]));
      const requestersByStop = new Map(
        unorderedStops.map((stop) => [stop, new Array<(typeof unorderedStops)[number]>()]),
      );
      for (const requester of unorderedStops) {
        for (const taskName of requester.requestedTaskNames) {
          const requested = stopsByTaskName.get(taskName);
          if (requested !== undefined) {
            requestersByStop.get(requested)?.push(requester);
          }
        }
      }
      const servingTasksReachedByStop = new Map(
        unorderedStops.map((stop) => [stop, new Array<(typeof unorderedStops)[number]>()]),
      );
      for (const served of unorderedStops.filter((stop) => stop.isServing)) {
        const reached = new Set<(typeof unorderedStops)[number]>();
        const toWalk = [served];
        for (const stop of toWalk) {
          for (const requester of requestersByStop.get(stop) ?? []) {
            if (!reached.has(requester)) {
              reached.add(requester);
              if (requester !== served) {
                servingTasksReachedByStop.get(requester)?.push(served);
              }
              if (requester.isCompleted) {
                toWalk.push(requester);
              }
            }
          }
        }
      }
      const findServingTasksReachedBy = (requester: (typeof unorderedStops)[number]) =>
        servingTasksReachedByStop.get(requester) ?? [];
      const requestedBeforeRequesters = new Array<(typeof unorderedStops)[number]>();
      const visitedStops = new Set<(typeof unorderedStops)[number]>();
      const requestPath = new Array<(typeof unorderedStops)[number]>();
      const servingCycles = new Array<ServingTaskRequestsItself>();
      const visit = (stop: (typeof unorderedStops)[number]) => {
        if (visitedStops.has(stop)) {
          return;
        }
        visitedStops.add(stop);
        requestPath.push(stop);
        for (const requested of findServingTasksReachedBy(stop)) {
          if (requestPath.includes(requested)) {
            servingCycles.push(
              new ServingTaskRequestsItself({
                taskName: stop.taskName,
                cycle: [stop, ...requestPath.slice(requestPath.indexOf(requested))].map(
                  (cycleStop) => cycleStop.taskName,
                ),
              }),
            );
          }
          visit(requested);
        }
        requestPath.pop();
        requestedBeforeRequesters.push(stop);
      };
      for (const stop of unorderedStops) {
        visit(stop);
      }
      yield* Effect.forEach(
        servingCycles,
        (servingCycle) =>
          stopState
            .recordFirstFailure(servingCycle)
            .pipe(Effect.andThen(failTask(servingCycle, markFailed))),
        { discard: true },
      );
      const stops = requestedBeforeRequesters.toReversed();
      const earlierStops = new Set<(typeof unorderedStops)[number]>();
      const earlierRequestersByStop = new Map<
        (typeof unorderedStops)[number],
        (typeof unorderedStops)[number][]
      >();
      for (const requester of stops) {
        earlierStops.add(requester);
        for (const served of findServingTasksReachedBy(requester)) {
          if (!earlierStops.has(served)) {
            const earlierRequesters = earlierRequestersByStop.get(served) ?? [];
            earlierRequesters.push(requester);
            earlierRequestersByStop.set(served, earlierRequesters);
          }
        }
      }
      yield* Effect.forEach(
        stops,
        (stop) =>
          Effect.forEach(
            earlierRequestersByStop.get(stop) ?? [],
            (requester) => Deferred.await(requester.stopped),
            { discard: true },
          ).pipe(
            Effect.andThen(
              stopTask(taskRunsState, stop.taskName, (watch) =>
                FiberSet.run(workerWatches, watch).pipe(Effect.asVoid),
              ),
            ),
            failRunOnFailure(markFailed),
            Effect.ensuring(
              unfinishedTaskNames.has(stop.taskName)
                ? printTaskLines("Info", stop.taskName, "stopped: the run ended before it finished")
                : Effect.void,
            ),
            Effect.ensuring(Deferred.done(stop.stopped, Exit.void)),
          ),
        { concurrency: "unbounded", discard: true },
      );
    }).pipe(Effect.scoped, Effect.provide(taskExecutionServices)),
    awaitServingTaskClosure: (taskName: string) =>
      Effect.gen(function* () {
        const taskRun = taskRunsByName.get(taskName);
        const isNotServing = taskRun === undefined || !TaskRunStatus.guards.Serving(taskRun.status);
        if (isNotServing) {
          return;
        }
        yield* printLine("Info", `${taskName} is serving`);
        yield* Deferred.await(taskRun.bodyFinishedDeferred);
      }),
    findTaskNamesStillInBodyOrCleanup: Effect.sync(() =>
      [...taskRunsByName]
        .filter(
          ([, taskRun]) =>
            isInBody(taskRun.status) && !hasCleanupLimitPassed(taskRun.stop.readStage()),
        )
        .map(([taskName]) => taskName),
    ),
    reportCleanupsCutOff: (taskNames: readonly string[]) =>
      Effect.forEach(
        taskNames,
        (taskName) =>
          markFailed(taskName).pipe(
            Effect.andThen(
              reportTaskFailure(taskName, Cause.fail(new TaskCleanupCutOff({ taskName }))),
            ),
          ),
        { discard: true },
      ).pipe(Effect.provide(taskExecutionServices)),
    markFailed,
    counts: Effect.sync(() => {
      const taskRuns = [...taskRunsByName];
      return {
        total: taskRunsByName.size,
        taskNames: taskRuns.map(([taskName]) => taskName),
        failedTaskNames: [...discoveredTasksByName.keys()].filter((taskName) => {
          const taskRun = taskRunsByName.get(taskName);
          return (
            taskRun !== undefined &&
            TaskRunOutcome.guards.Failed(taskRun.outcome) &&
            !taskRun.outcome.causedByAwaitedTasks
          );
        }),
        stopped: taskRuns.filter(([, taskRun]) => TaskRunOutcome.guards.Cancelled(taskRun.outcome))
          .length,
        unawaitedFailedTaskNames: taskRuns
          .filter(
            ([taskName, taskRun]) =>
              TaskRunOutcome.guards.Failed(taskRun.outcome) &&
              !taskNamesWhoseFailureReachedAnAwaitingTask.has(taskName),
          )
          .map(([taskName]) => taskName),
      };
    }),
  };
});

export class TaskRuns extends Context.Service<TaskRuns, Effect.Success<typeof makeTaskRuns>>()(
  `${TASK_PACKAGE.name}/TaskRuns`,
) {}
