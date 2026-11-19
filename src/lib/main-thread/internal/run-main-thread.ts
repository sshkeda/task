import { Cause, Clock, Effect, Layer, Ref } from "effect";
import type { Queue } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpExporter, OtlpSerialization } from "effect/observability";

import { printLine } from "../../../shared/print-to-standard-error.ts";
import { runtimeConfigProviderLayer } from "../../../shared/read-environment-config.ts";
import { failureValuesOfCause } from "../../../shared/select-cause-reasons.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { isEnvironmentUsageError, USAGE_ERROR_EXIT_CODE } from "../../../shared/usage-errors.ts";
import { sweepBeforeExit } from "../../supervise-the-run/index.ts";
import { TaskRunsLive } from "../lib/run-tasks-on-workers/index.ts";
import { DiscoveredTasksLive } from "../shared/provide-discovered-tasks.ts";
import { ProcessGroupsLive } from "../shared/provide-process-groups.ts";
import { RunRootTaskConfigLive } from "../shared/provide-run-root-task-config.ts";
import { StopState } from "../shared/provide-stop-state.ts";
import { WorkerPoolLive } from "../shared/provide-worker-pool.ts";
import { stopRequestsLayer } from "./listen-for-stop-requests.ts";
import { formatRunLine, RunDirectory } from "./provide-run-directory.ts";
import { formatSummaryLine } from "./run-root-task-fiber.ts";
import type { runRootTaskFiber } from "./run-root-task-fiber.ts";
import { stopsAtFirstFailure } from "./stops-at-first-failure.ts";
import { TaskOutputFilesLive, TraceFileLive, RuntimeTracingLive } from "./trace-the-run.ts";

export const MainThreadLive = (
  runArgument: Parameters<typeof RunRootTaskConfigLive>[0],
  signals: Queue.Dequeue<(typeof STOP_SIGNALS)[number]>,
  supervisorProcessId: number | null,
) =>
  stopsAtFirstFailure(TaskRunsLive.pipe(Layer.provideMerge(WorkerPoolLive))).pipe(
    Layer.provideMerge(DiscoveredTasksLive),
    Layer.provideMerge(
      stopsAtFirstFailure(
        ProcessGroupsLive.pipe(
          Layer.provideMerge(RuntimeTracingLive),
          Layer.provideMerge(TaskOutputFilesLive),
          Layer.provideMerge(TraceFileLive),
          Layer.provideMerge(
            Layer.mergeAll(
              FetchHttpClient.layer,
              runtimeConfigProviderLayer,
              OtlpExporter.layerFlusher,
              OtlpSerialization.layerJson,
            ),
          ),
          Layer.provideMerge(RunRootTaskConfigLive(runArgument, supervisorProcessId)),
        ),
      ),
    ),
    Layer.provideMerge(stopRequestsLayer(signals)),
  );

const FAILURE_EXIT_CODE = 1;

const exitCodeOfFailure = (cause: Cause.Cause<unknown>) => {
  const failureValues = failureValuesOfCause(cause);
  const isUsageFailure =
    failureValues.length > 0 &&
    failureValues.every((failureValue) => isEnvironmentUsageError(failureValue));
  return isUsageFailure ? USAGE_ERROR_EXIT_CODE : FAILURE_EXIT_CODE;
};

export const exitCodeOfTheRun = <Requirements>(
  runningRootTask: Effect.Effect<
    Effect.Success<typeof runRootTaskFiber>,
    Layer.Error<ReturnType<typeof MainThreadLive>>,
    Requirements
  >,
) =>
  Effect.gen(function* () {
    const shutdown = yield* StopState;
    const { exitCode, summaryLine, printRecapLines, endedBeforeItsSummaryBy, runEndingCause } =
      yield* runningRootTask.pipe(
        Effect.map(
          (
            ended,
          ): {
            readonly exitCode: number;
            readonly summaryLine: string | null;
            readonly printRecapLines: Effect.Effect<void>;
            readonly endedBeforeItsSummaryBy: "failure" | "stop" | null;
            readonly runEndingCause: Cause.Cause<unknown> | null;
          } => ({ ...ended, endedBeforeItsSummaryBy: null }),
        ),
        Effect.catchTag("StopRequested", (failure) =>
          Effect.succeed({
            exitCode: failure.exitCode,
            summaryLine: null,
            printRecapLines: Effect.void,
            endedBeforeItsSummaryBy: "stop" as const,
            runEndingCause: Cause.fail(failure),
          }),
        ),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : Effect.succeed({
                exitCode: exitCodeOfFailure(cause),
                summaryLine: null,
                printRecapLines: Effect.void,
                endedBeforeItsSummaryBy: "failure" as const,
                runEndingCause: cause,
              }),
        ),
      );
    const lastErrorLines = [
      ...(yield* sweepBeforeExit),
      ...(yield* shutdown.formatUnreportedFailures(runEndingCause)),
    ];
    for (const errorLines of lastErrorLines) {
      yield* printLine(errorLines);
    }
    yield* printRecapLines;
    const runDirectory = yield* Ref.get(yield* RunDirectory);
    if (runDirectory === null) {
      if (summaryLine !== null) {
        yield* printLine(summaryLine);
      }
      return exitCode;
    }
    const summaryOfAnEndBeforeAnyTask =
      endedBeforeItsSummaryBy === null
        ? null
        : formatSummaryLine({
            runPassed: false,
            isStopped: endedBeforeItsSummaryBy === "stop",
            total: 0,
            failedTaskNames: [],
            stoppedTaskNames: [],
            elapsedMilliseconds:
              (yield* Clock.currentTimeMillis) - runDirectory.runStartedAtMilliseconds,
          });
    const lastSummaryLine = summaryLine ?? summaryOfAnEndBeforeAnyTask;
    if (lastSummaryLine !== null) {
      yield* printLine(lastSummaryLine);
    }
    yield* printLine(formatRunLine(runDirectory.path));
    return exitCode;
  });
