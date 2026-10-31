import { Cause, Effect, Layer } from "effect";
import type { Queue } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpExporter, OtlpSerialization } from "effect/observability";

import { formatCauseLines, formatFailureLines } from "../../../shared/format-error-lines.ts";
import { printLine } from "../../../shared/print-to-standard-error.ts";
import { runtimeConfigProviderLayer } from "../../../shared/read-environment-config.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import type { Task } from "../../task/index.ts";
import { ModuleGraphLive } from "../lib/find-code-each-task-depends-on/index.ts";
import { TaskRunsLive } from "../lib/run-tasks-on-workers/index.ts";
import { DiscoveredTasksLive } from "../shared/provide-discovered-tasks.ts";
import { ProcessGroupsLive } from "../shared/provide-process-groups.ts";
import { RunRootTaskConfigLive } from "../shared/provide-run-root-task-config.ts";
import { StopState } from "../shared/provide-stop-state.ts";
import { WorkerPoolLive } from "../shared/provide-worker-pool.ts";
import { runCalledTwiceLayer, stopRequestsLayer } from "./listen-for-stop-requests.ts";
import type { runRootTaskFiber } from "./run-root-task-fiber.ts";
import { stopsAtFirstFailure } from "./stops-at-first-failure.ts";
import { TraceFileLive, RuntimeTracingLive } from "./trace-the-run.ts";

export const MainThreadLive = (
  rootTask: Task,
  signals: Queue.Dequeue<(typeof STOP_SIGNALS)[number]>,
  supervisorProcessId: number | null,
) =>
  stopsAtFirstFailure(TaskRunsLive.pipe(Layer.provideMerge(WorkerPoolLive))).pipe(
    Layer.provideMerge(DiscoveredTasksLive),
    Layer.provideMerge(
      stopsAtFirstFailure(
        ProcessGroupsLive.pipe(
          Layer.provideMerge(RuntimeTracingLive),
          Layer.provideMerge(TraceFileLive),
          Layer.provideMerge(ModuleGraphLive),
          Layer.provideMerge(
            Layer.mergeAll(
              FetchHttpClient.layer,
              runtimeConfigProviderLayer,
              OtlpExporter.layerFlusher,
              OtlpSerialization.layerJson,
            ),
          ),
          Layer.provideMerge(runCalledTwiceLayer),
          Layer.provideMerge(RunRootTaskConfigLive(rootTask, supervisorProcessId)),
        ),
      ),
    ),
    Layer.provideMerge(stopRequestsLayer(signals)),
  );

export const exitCodeOfTheRun = <Requirements>(
  runningRootTask: Effect.Effect<
    Effect.Success<typeof runRootTaskFiber>,
    Effect.Error<typeof runRootTaskFiber> | Layer.Error<ReturnType<typeof MainThreadLive>>,
    Requirements
  >,
) =>
  Effect.gen(function* () {
    const shutdown = yield* StopState;
    const { exitCode, runEndingCause } = yield* runningRootTask.pipe(
      Effect.catchTag("StopRequested", (failure) =>
        printLine("Error", formatFailureLines(failure, null)).pipe(
          Effect.as({ exitCode: failure.exitCode, runEndingCause: Cause.fail(failure) }),
        ),
      ),
      Effect.matchCauseEffect({
        onFailure: (cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause)
            : printLine("Error", formatCauseLines(cause, null)).pipe(
                Effect.as({ exitCode: 1, runEndingCause: cause }),
              ),
        onSuccess: (ended) => Effect.succeed(ended),
      }),
    );
    if (runEndingCause !== null) {
      yield* printLine("Info", yield* shutdown.formatRunFailureReasonLine(runEndingCause));
    }
    return exitCode;
  });
