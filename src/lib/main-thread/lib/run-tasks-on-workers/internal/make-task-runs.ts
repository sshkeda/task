import { Context, Deferred, Effect, Exit, FiberSet } from "effect";
import type { FileSystem, Schema } from "effect";

import { printLine, printTaskLines } from "../../../../../shared/print-to-standard-error.ts";
import type { TaskRequestFailure } from "../../../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import { failRunOnFailure, failTask } from "../../../shared/fail-run.ts";
import { ServingTaskRequestsItself } from "../../../shared/main-thread-errors.ts";
import type { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import type { RunRootTaskConfig } from "../../../shared/provide-run-root-task-config.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import type { WorkerEvent, WorkerPool } from "../../../shared/provide-worker-pool.ts";
import type { TraceFile } from "../../../shared/write-trace-file.ts";
import {
  runRequestedTask,
  runRequestedDependencyTask,
} from "../lib/run-requested-task-once/index.ts";
import { makeTaskRunsState, TaskRunStatus } from "../shared/define-task-run-state.ts";
import { stopTask } from "../shared/stop-task.ts";
import { handleWorkerEvent } from "./handle-worker-events.ts";

export const makeTaskRuns = Effect.gen(function* () {
  const taskExecutionServices = yield* Effect.context<
    FileSystem.FileSystem | ProcessGroups | RunRootTaskConfig | StopState | TraceFile | WorkerPool
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
  ): Effect.Effect<Schema.Json | undefined, typeof TaskRequestFailure.Type> =>
    runRequestedDependencyTask(
      taskRunsState,
      resultDeferreds,
      dependencyTaskName,
      requestingTaskName,
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
          .filter(
            ([, taskRun]) =>
              TaskRunStatus.guards.Preparing(taskRun.status) ||
              TaskRunStatus.guards.Running(taskRun.status),
          )
          .map(([taskName]) => taskName),
      );
      yield* FiberSet.clear(taskRunsState.taskFibers);
      const workerWatches = yield* FiberSet.make<void, never>();
      const unorderedStops = Array.from(taskRunsByName, ([taskName, taskRun]) => ({
        taskName,
        isServing: TaskRunStatus.guards.Serving(taskRun.status),
        isCompleted: TaskRunStatus.guards.Passed(taskRun.status),
        requestedTaskNames: taskRun.bodyRequestedTaskNames,
        stopped: Deferred.makeUnsafe<void>(),
      }));
      const findTasksRequestedBy = (requester: (typeof unorderedStops)[number]) =>
        unorderedStops.filter((stop) => requester.requestedTaskNames.has(stop.taskName));
      const findServingTasksReachedBy = (requester: (typeof unorderedStops)[number]) => {
        const reached = new Set<(typeof unorderedStops)[number]>();
        const reach = (stop: (typeof unorderedStops)[number]) => {
          for (const requested of findTasksRequestedBy(stop)) {
            if (!reached.has(requested)) {
              reached.add(requested);
              if (requested.isCompleted) {
                reach(requested);
              }
            }
          }
        };
        reach(requester);
        return [...reached].filter((stop) => stop.isServing);
      };
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
      yield* Effect.forEach(
        stops,
        (stop, index) =>
          Effect.forEach(
            stops
              .slice(0, index)
              .filter((requester) => findServingTasksReachedBy(requester).includes(stop)),
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
    markFailed,
    counts: Effect.sync(() => {
      const taskRuns = [...taskRunsByName];
      return {
        total: taskRunsByName.size,
        failedTaskNames: [...discoveredTasksByName.keys()].filter((taskName) => {
          const taskRun = taskRunsByName.get(taskName);
          return (
            taskRun !== undefined &&
            TaskRunStatus.guards.Failed(taskRun.status) &&
            !taskRun.status.causedByAwaitedTasks
          );
        }),
        stopped: taskRuns.filter(([, taskRun]) => TaskRunStatus.guards.Cancelled(taskRun.status))
          .length,
        unawaitedFailedTaskNames: taskRuns
          .filter(
            ([taskName, taskRun]) =>
              TaskRunStatus.guards.Failed(taskRun.status) &&
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
