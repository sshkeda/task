import {
  Cause,
  Array as Arrays,
  Clock,
  Deferred,
  Effect,
  Exit,
  Schema,
  Stream,
  Fiber,
} from "effect";

import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import { formatSeconds } from "../../../shared/format-task-lines.ts";
import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { printLine } from "../../../shared/print-to-standard-error.ts";
import { availableParallelism } from "../../../shared/read-environment-config.ts";
import { TaskBodyFailed } from "../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { TaskRuns } from "../lib/run-tasks-on-workers/index.ts";
import { failRun, failRunOnFailure } from "../shared/fail-run.ts";
import {
  RootTaskWorkingDirectoryUnavailable,
  StopRequested,
  TasksNeverRan,
  UnawaitedTasksFailed,
} from "../shared/main-thread-errors.ts";
import { DiscoveredTasks } from "../shared/provide-discovered-tasks.ts";
import { ProcessGroups } from "../shared/provide-process-groups.ts";
import { RunRootTaskConfig } from "../shared/provide-run-root-task-config.ts";
import { StopState } from "../shared/provide-stop-state.ts";
import { WorkerPool } from "../shared/provide-worker-pool.ts";

const stopEverythingStarted = Effect.fn("stopEverythingStarted")(function* (
  workerEventHandlingFiber: Fiber.Fiber<void>,
) {
  const shutdown = yield* StopState;
  const taskRuns = yield* TaskRuns;
  const workerPool = yield* WorkerPool;
  yield* taskRuns.stopAll.pipe(Effect.raceFirst(shutdown.awaitForcedStop));
  const processGroups = yield* ProcessGroups;
  const killAllProcessGroups = shutdown.isForced.pipe(
    Effect.flatMap(processGroups.killAll),
    failRunOnFailure(taskRuns.markFailed),
  );
  const finalizerCommandsWouldEndUnreportedWithTheirWorkers = yield* shutdown.isForced;
  if (finalizerCommandsWouldEndUnreportedWithTheirWorkers) {
    yield* workerPool.awaitPostedBodyStartsHandled;
    const cutOffTaskNames = yield* taskRuns.findTaskNamesStillInBodyOrCleanup;
    yield* workerPool.terminateAllWorkers;
    yield* killAllProcessGroups;
    yield* taskRuns.reportCleanupsCutOff(cutOffTaskNames);
  }
  yield* workerPool.requestExitOfAllWorkers;
  const workersGone = yield* Deferred.make<false>();
  yield* Effect.all(
    [
      workerPool.awaitWorkerShutdown.pipe(Effect.ensuring(Deferred.succeed(workersGone, false))),
      Effect.raceFirst(
        shutdown.awaitForcedStop.pipe(Effect.as(true)),
        Deferred.await(workersGone),
      ).pipe(Effect.flatMap((forced) => (forced ? killAllProcessGroups : Effect.void))),
    ],
    { concurrency: "unbounded", discard: true },
  );
  yield* killAllProcessGroups;
  yield* Fiber.await(workerEventHandlingFiber);
  return yield* shutdown.outcome;
});

const isStopRequested = Schema.is(StopRequested);

const isTaskBodyFailed = Schema.is(TaskBodyFailed);

const LARGEST_PROCESS_EXIT_CODE = 255;

const MOST_NAMED_FAILED_TASKS = 3;

const exitCodeOfRunEndingFailure = (
  runEndingFailure:
    | Effect.Success<ReturnType<typeof stopEverythingStarted>>
    | TasksNeverRan
    | UnawaitedTasksFailed
    | null,
  rootTaskName: string,
) => {
  if (runEndingFailure === null) {
    return 0;
  }
  if (isStopRequested(runEndingFailure)) {
    return runEndingFailure.exitCode;
  }
  const isRootTaskBodyFailure =
    isTaskBodyFailed(runEndingFailure) && runEndingFailure.taskName === rootTaskName;
  const failedCommandExitCode = isRootTaskBodyFailure
    ? runEndingFailure.failedCommandExitCode
    : undefined;
  const isFailedCommandExitCodeUsable =
    failedCommandExitCode !== undefined &&
    failedCommandExitCode >= 1 &&
    failedCommandExitCode <= LARGEST_PROCESS_EXIT_CODE;
  return isFailedCommandExitCodeUsable ? failedCommandExitCode : 1;
};

export const runRootTaskFiber = Effect.gen(function* () {
  const runRootTaskConfig = yield* RunRootTaskConfig;
  const { rootTaskName } = runRootTaskConfig;
  const runSpan = yield* Effect.currentSpan.pipe(
    dieOnInvariantFailure("main-thread tracing", "the run has a parent span"),
  );
  const shutdown = yield* StopState;
  const discoveredTasks = yield* DiscoveredTasks;
  const workerPool = yield* WorkerPool;
  const taskRuns = yield* TaskRuns;
  if (discoveredTasks.brokenTasks !== null) {
    yield* printLine("Error", formatFailureLines(discoveredTasks.brokenTasks, null));
    yield* shutdown.recordPrintedFailure(discoveredTasks.brokenTasks);
    yield* failRun(discoveredTasks.brokenTasks, taskRuns.markFailed);
  }
  const workerEventHandlingFiber = yield* workerPool.events.pipe(
    Stream.groupByKey((workerEvent) => workerEvent.workerId),
    Stream.mapEffect(([, events]) => Stream.runForEach(events, taskRuns.handleWorkerEvent), {
      concurrency: "unbounded",
    }),
    Stream.runDrain,
    Effect.withParentSpan(runSpan),
    Effect.forkScoped,
  );
  if (discoveredTasks.tasksByName !== null) {
    yield* taskRuns.registerDiscoveredTasks(discoveredTasks.tasksByName);
    yield* Effect.try({
      try: () => {
        process.chdir(runRootTaskConfig.rootTaskPackageJsonDirectoryPath);
      },
      catch: (cause) =>
        new RootTaskWorkingDirectoryUnavailable({
          packageJsonDirectoryPath: runRootTaskConfig.rootTaskPackageJsonDirectoryPath,
          cause,
        }),
    });
    yield* Effect.gen(function* () {
      yield* taskRuns.requestRootTaskResult(rootTaskName);
      yield* taskRuns.awaitServingTaskClosure(rootTaskName);
    }).pipe(
      Effect.matchEffect({
        onFailure: (error) => shutdown.recordFirstFailure(error),
        onSuccess: (success) => Effect.succeed(success),
      }),
      Effect.raceFirst(shutdown.awaitFailure),
    );
  }

  const failure = yield* stopEverythingStarted(workerEventHandlingFiber);
  const taskRunCounts = yield* taskRuns.counts;
  const elapsedMilliseconds =
    (yield* Clock.currentTimeMillis) - runRootTaskConfig.runStartedAtMilliseconds;
  const unawaitedFailedTaskNames = failure === null ? taskRunCounts.unawaitedFailedTaskNames : [];
  const unawaitedTasksFailed = Arrays.isArrayNonEmpty(unawaitedFailedTaskNames)
    ? new UnawaitedTasksFailed({ failedTaskNames: unawaitedFailedTaskNames })
    : null;
  const isEveryRootFileTaskDue =
    failure === null && unawaitedTasksFailed === null && runRootTaskConfig.checksForgottenTasks;
  const neverRanTaskNames = isEveryRootFileTaskDue
    ? discoveredTasks.unexportedRootFileTaskNames.filter(
        (taskName) => !taskRunCounts.taskNames.includes(taskName),
      )
    : [];
  const tasksNeverRan = Arrays.isArrayNonEmpty(neverRanTaskNames)
    ? new TasksNeverRan({
        taskNames: neverRanTaskNames,
        filePath: runRootTaskConfig.rootTaskFilePath,
      })
    : null;
  const runEndingFailure = failure ?? unawaitedTasksFailed ?? tasksNeverRan;
  const runPassed = runEndingFailure === null;
  const failedTaskCount = taskRunCounts.failedTaskNames.length;
  const totalTasks = `${taskRunCounts.total} ${taskRunCounts.total === 1 ? "task" : "tasks"}`;
  const namedFailedTaskNames = taskRunCounts.failedTaskNames.slice(0, MOST_NAMED_FAILED_TASKS);
  const unnamedFailedTaskCount = failedTaskCount - namedFailedTaskNames.length;
  const failedTasks = [
    namedFailedTaskNames.join(", "),
    ...(unnamedFailedTaskCount === 0 ? [] : [`and ${unnamedFailedTaskCount} more`]),
    `(${failedTaskCount} of ${totalTasks})`,
  ].join(" ");
  const caughtFailures = `${failedTaskCount} caught ${failedTaskCount === 1 ? "failure" : "failures"}`;
  const tasksWithFailures = runPassed ? [totalTasks, caughtFailures] : [failedTasks];
  const tasksWithoutFailures = runPassed ? [totalTasks] : [`0 of ${totalTasks}`];
  const tasks = failedTaskCount === 0 ? tasksWithoutFailures : tasksWithFailures;
  const counts = [
    ...tasks,
    ...(taskRunCounts.stopped === 0 ? [] : [`${taskRunCounts.stopped} stopped`]),
    formatSeconds(elapsedMilliseconds),
  ].join(", ");
  const isFailureUnprinted =
    failure !== null && (isStopRequested(failure) || !(yield* shutdown.wasPrinted(failure)));
  const runFailures = [
    ...(unawaitedTasksFailed === null ? [] : [unawaitedTasksFailed]),
    ...(tasksNeverRan === null ? [] : [tasksNeverRan]),
    ...(isFailureUnprinted ? [failure] : []),
  ];
  yield* printLine("Info", runPassed ? `passed: ${counts}` : `failed: ${counts}`);
  for (const runFailure of runFailures) {
    yield* printLine("Error", formatFailureLines(runFailure, null));
  }
  const exitCode = exitCodeOfRunEndingFailure(runEndingFailure, rootTaskName);
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.version`]: TASK_PACKAGE.version,
    [`${TASK_TRACE_LABEL_PREFIX}.entry`]: runRootTaskConfig.rootTaskFilePath,
    [`${TASK_TRACE_LABEL_PREFIX}.root.task`]: rootTaskName,
    [`${TASK_TRACE_LABEL_PREFIX}.exit.code`]: exitCode,
    [`${TASK_TRACE_LABEL_PREFIX}.elapsed.milliseconds`]: elapsedMilliseconds,
    [`${TASK_TRACE_LABEL_PREFIX}.workers.started`]: yield* workerPool.startedWorkerCount,
    [`${TASK_TRACE_LABEL_PREFIX}.workers.limit`]: runRootTaskConfig.workerCount,
    "host.cpus": yield* availableParallelism,
  });
  runSpan.end(
    yield* Clock.currentTimeNanos,
    runEndingFailure === null ? Exit.void : Exit.fail(runEndingFailure),
  );
  return {
    exitCode,
    runEndingCause: runEndingFailure === null ? null : Cause.fail(runEndingFailure),
  };
});
