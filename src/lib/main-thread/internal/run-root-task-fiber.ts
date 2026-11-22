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

import { formatSeconds } from "../../../shared/format-seconds.ts";
import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { labelLine, nameTask } from "../../../shared/label-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { offerLines, readLastStandardErrorWrite } from "../../../shared/print-to-standard-error.ts";
import { availableParallelism } from "../../../shared/read-environment-config.ts";
import { TaskBodyFailed } from "../../../shared/task-errors.ts";
import { endTaskSpan } from "../../../shared/task-spans.ts";
import { TaskRuns } from "../lib/run-tasks-on-workers/index.ts";
import { failRun, failRunOnFailure } from "../shared/fail-run.ts";
import {
  NoTaskStarted,
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
import { readTerminalCommands } from "../shared/track-terminal-commands.ts";
import { TaskOutputFiles } from "../shared/write-task-output-files.ts";
import { decideNoTaskStarted } from "./decide-no-task-started.ts";
import { formatRunningLine, printRunningLinesWhileQuiet } from "./print-running-lines.ts";

const stopEverythingStarted = Effect.fn(function* (workerEventHandlingFiber: Fiber.Fiber<void>) {
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
    yield* taskRuns.reportStopsCutOff(cutOffTaskNames);
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

const MOST_NAMED_TASKS = 3;

const countTasks = (count: number) => `${count} ${count === 1 ? "task" : "tasks"}`;

const nameSomeTasks = (taskNames: readonly string[], total: number) => {
  const named = taskNames.slice(0, MOST_NAMED_TASKS).map((taskName) => nameTask(taskName));
  const unnamedCount = taskNames.length - named.length;
  return [
    ...named,
    ...(unnamedCount === 0 ? [] : [`and ${unnamedCount} more`]),
    `(${taskNames.length} of ${countTasks(total)})`,
  ].join(" ");
};

export const formatSummaryLine = (run: {
  readonly runPassed: boolean;
  readonly isStopped: boolean;
  readonly total: number;
  readonly failedTaskNames: readonly string[];
  readonly stoppedTaskNames: readonly string[];
  readonly elapsedMilliseconds: number;
}) => {
  const failedCount = run.failedTaskNames.length;
  const stoppedCount = run.stoppedTaskNames.length;
  const stopped = stoppedCount === 0 ? [] : [`${stoppedCount} stopped`];
  const seconds = formatSeconds(run.elapsedMilliseconds);
  if (run.runPassed) {
    const caughtFailures =
      failedCount === 0
        ? []
        : [`${failedCount} caught ${failedCount === 1 ? "failure" : "failures"}`];
    return labelLine(
      "passed",
      null,
      [countTasks(run.total), ...caughtFailures, ...stopped, seconds].join(", "),
    );
  }
  const isStoppedWithoutFailures = run.isStopped && failedCount === 0;
  if (isStoppedWithoutFailures) {
    const stoppedTasks =
      stoppedCount === 0
        ? `0 of ${countTasks(run.total)}`
        : nameSomeTasks(run.stoppedTaskNames, run.total);
    return labelLine("stopped", null, [stoppedTasks, seconds].join(", "));
  }
  const failedTasks =
    failedCount === 0
      ? `0 of ${countTasks(run.total)}`
      : nameSomeTasks(run.failedTaskNames, run.total);
  return labelLine("failed", null, [failedTasks, ...stopped, seconds].join(", "));
};

const exitCodeOfRunEndingFailure = (
  runEndingFailure:
    | Effect.Success<ReturnType<typeof stopEverythingStarted>>
    | NoTaskStarted
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

export const handleWorkerEventsAsTheyArrive = Effect.gen(function* () {
  const workerPool = yield* WorkerPool;
  const taskRuns = yield* TaskRuns;
  yield* workerPool.events.pipe(
    Stream.groupByKey((workerEvent) => workerEvent.workerId),
    Stream.mapEffect(([, events]) => Stream.runForEachArray(events, taskRuns.handleWorkerEvents), {
      concurrency: "unbounded",
    }),
    Stream.runDrain,
  );
});

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
    yield* shutdown.recordRunFailure(discoveredTasks.brokenTasks);
    yield* failRun(discoveredTasks.brokenTasks, taskRuns.markFailed);
  }
  const workerEventHandlingFiber = yield* handleWorkerEventsAsTheyArrive.pipe(
    Effect.withParentSpan(runSpan),
    Effect.forkScoped,
  );
  const runningLinesFiber = yield* printRunningLinesWhileQuiet({
    readLastWrite: readLastStandardErrorWrite,
    readTerminalCommands,
    describe: (nowMilliseconds) =>
      Effect.gen(function* () {
        return formatRunningLine({
          isStopping: yield* shutdown.isStopping,
          ...(yield* taskRuns.describeOwnWork(nowMilliseconds)),
          runMilliseconds: nowMilliseconds - runRootTaskConfig.runStartedAtMilliseconds,
        });
      }),
    print: (line) => offerLines("status", line),
  }).pipe(Effect.forkScoped);
  if (discoveredTasks.tasksByName !== null) {
    yield* taskRuns.registerDiscoveredTasks(discoveredTasks.tasksByName);
    yield* Effect.gen(function* () {
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

  yield* Fiber.interrupt(runningLinesFiber);
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
    ? discoveredTasks.rootFileTaskNames.unexported.filter(
        (taskName) => !taskRunCounts.taskNames.includes(taskName),
      )
    : [];
  const tasksNeverRan = Arrays.isArrayNonEmpty(neverRanTaskNames)
    ? new TasksNeverRan({
        taskNames: neverRanTaskNames,
        filePath: runRootTaskConfig.rootTaskFilePath,
      })
    : null;
  const notStartedTaskNames = decideNoTaskStarted({
    madeTaskNames: discoveredTasks.rootFileTaskNames.made,
    startedTaskNames: taskRunCounts.taskNames,
    rootTaskGroup: runRootTaskConfig.rootTaskGroup,
    runAlreadyFailed: failure !== null || unawaitedTasksFailed !== null || tasksNeverRan !== null,
  });
  const noTaskStarted =
    notStartedTaskNames === null
      ? null
      : new NoTaskStarted({
          madeTaskNames: notStartedTaskNames,
          filePath: runRootTaskConfig.rootTaskFilePath,
        });
  const runEndingFailure = failure ?? unawaitedTasksFailed ?? tasksNeverRan ?? noTaskStarted;
  const summaryLine = formatSummaryLine({
    runPassed: runEndingFailure === null,
    isStopped: isStopRequested(runEndingFailure),
    total: taskRunCounts.total,
    failedTaskNames: taskRunCounts.failedTaskNames,
    stoppedTaskNames: taskRunCounts.stoppedTaskNames,
    elapsedMilliseconds,
  });
  const exitCode = exitCodeOfRunEndingFailure(runEndingFailure, rootTaskName);
  for (const [key, value] of Object.entries({
    "process.exit.code": exitCode,
    [`${TASK_TRACE_LABEL_PREFIX}.elapsed.milliseconds`]: elapsedMilliseconds,
    [`${TASK_TRACE_LABEL_PREFIX}.workers.started`]: yield* workerPool.startedWorkerCount,
    [`${TASK_TRACE_LABEL_PREFIX}.workers.limit`]: runRootTaskConfig.workerCount,
    [`${TASK_TRACE_LABEL_PREFIX}.workers.available`]: yield* availableParallelism,
  })) {
    runSpan.attribute(key, value);
  }
  yield* endTaskSpan(runSpan, runEndingFailure === null ? Exit.void : Exit.fail(runEndingFailure));
  const taskOutputFiles = yield* TaskOutputFiles;
  return {
    exitCode,
    summaryLine,
    printRecapLines:
      runEndingFailure === null
        ? Effect.void
        : taskOutputFiles.printRecapLines(taskRunCounts.failedTaskNames),
    runEndingCause: runEndingFailure === null ? null : Cause.fail(runEndingFailure),
  };
});
