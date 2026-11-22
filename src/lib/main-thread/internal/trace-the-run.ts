import {
  Clock,
  Context,
  DateTime,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Logger,
  Option,
  Path,
  Predicate,
  Ref,
  Scope,
  Tracer,
} from "effect";

import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  awaitLineWritten,
  printLine,
  queueLineToAwaitItsWrite,
} from "../../../shared/print-to-standard-error.ts";
import { endTaskSpan, startTaskSpan } from "../../../shared/task-spans.ts";
import { RunRootTaskConfig } from "../shared/provide-run-root-task-config.ts";
import { StopState } from "../shared/provide-stop-state.ts";
import { TaskOutputFiles, makeTaskOutputFiles } from "../shared/write-task-output-files.ts";
import { TraceFile, makeTraceFile } from "../shared/write-trace-file.ts";
import { formatRunLine, RunDirectory } from "./provide-run-directory.ts";

const RUN_DIRECTORY_RETENTION = "14 days";

const TRACE_FILE_NAME = "trace.jsonl";

const RUN_PROCESS_ID_PATTERN = /Z-(?<processId>\d+)$/u;

const isRunProcessAlive = (runDirectoryName: string) => {
  const processId = Number(
    RUN_PROCESS_ID_PATTERN.exec(runDirectoryName)?.groups?.["processId"] ?? 0,
  );
  if (processId === 0) {
    return false;
  }
  try {
    process.kill(processId, 0);
    return true;
  } catch (error) {
    return Predicate.hasProperty(error, "code") && error.code === "EPERM";
  }
};

const runDirectoryPathOf = (runRootTaskConfig: RunRootTaskConfig["Service"]) => {
  const timestamp = DateTime.formatIso(
    DateTime.makeUnsafe(runRootTaskConfig.runStartedAtMilliseconds),
  ).replaceAll(":", "-");
  return `${runRootTaskConfig.stateDirectoryPath}/runs/${runRootTaskConfig.projectRunsDirectoryName}/${timestamp}-${runRootTaskConfig.runProcessId}`;
};

const deleteRunDirectoriesOlderThanRetention = Effect.fn(function* (
  currentRunDirectoryPath: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projectRunsDirectoryPath = path.dirname(currentRunDirectoryPath);
  const oldestKeptMilliseconds =
    (yield* Clock.currentTimeMillis) - Duration.toMillis(RUN_DIRECTORY_RETENTION);
  const runDirectoryNames = yield* fileSystem.readDirectory(projectRunsDirectoryPath);
  yield* Effect.forEach(
    runDirectoryNames.filter(
      (runDirectoryName) =>
        runDirectoryName !== path.basename(currentRunDirectoryPath) &&
        !isRunProcessAlive(runDirectoryName),
    ),
    (runDirectoryName) => {
      const runDirectoryPath = path.join(projectRunsDirectoryPath, runDirectoryName);
      return fileSystem.stat(path.join(runDirectoryPath, TRACE_FILE_NAME)).pipe(
        Effect.flatMap((information) => {
          const isExpiredRunDirectory =
            information.type === "File" &&
            Option.exists(information.mtime, (mtime) => mtime.getTime() < oldestKeptMilliseconds);
          return isExpiredRunDirectory
            ? fileSystem.remove(runDirectoryPath, { recursive: true })
            : Effect.void;
        }),
        Effect.catchTag("PlatformError", () => Effect.void),
      );
    },
    { discard: true },
  );
});

export const TraceFileLive = Layer.effect(
  TraceFile,
  Effect.gen(function* () {
    const runRootTaskConfig = yield* RunRootTaskConfig;
    const runDirectoryPath = runDirectoryPathOf(runRootTaskConfig);
    const traceFilePath = `${runDirectoryPath}/${TRACE_FILE_NAME}`;
    const made = yield* Ref.make<Effect.Success<ReturnType<typeof makeTraceFile>> | null>(null);
    const sweepOfOldRunDirectories = yield* Ref.make<Fiber.Fiber<void> | null>(null);
    const sweepScope = yield* Scope.fork(yield* Effect.scope);
    yield* Effect.addFinalizer(() =>
      Ref.get(made).pipe(
        Effect.flatMap((madeTraceFile) =>
          madeTraceFile === null
            ? Effect.void
            : madeTraceFile.whyIncomplete.pipe(
                Effect.flatMap((whyIncomplete) =>
                  whyIncomplete === null
                    ? Effect.void
                    : printLine(formatFailureLines(whyIncomplete)),
                ),
              ),
        ),
        Effect.andThen(
          Effect.flatMap(Ref.get(sweepOfOldRunDirectories), (sweep) =>
            sweep === null ? Effect.void : Fiber.join(sweep),
          ),
        ),
      ),
    );
    const traceFile = yield* makeTraceFile({
      traceFilePath,
      createdBelow: runRootTaskConfig.stateDirectoryPath,
    });
    yield* Ref.set(made, traceFile);
    const runDirectory = yield* RunDirectory;
    const shutdown = yield* StopState;
    const isComplete = (yield* traceFile.whyIncomplete) === null;
    const runLineWritten = yield* shutdown
      .holdStopAnnouncementsDuring(
        Effect.gen(function* () {
          if (yield* shutdown.isStopping) {
            return null;
          }
          yield* Ref.set(runDirectory, {
            path: runDirectoryPath,
            runStartedAtMilliseconds: runRootTaskConfig.runStartedAtMilliseconds,
          });
          return isComplete
            ? yield* queueLineToAwaitItsWrite(formatRunLine(runDirectoryPath))
            : null;
        }),
      )
      .pipe(Effect.uninterruptible);
    if (runLineWritten !== null) {
      yield* awaitLineWritten(runLineWritten);
      yield* Ref.set(
        sweepOfOldRunDirectories,
        yield* deleteRunDirectoriesOlderThanRetention(runDirectoryPath).pipe(
          Effect.catchTag("PlatformError", () => Effect.void),
          Effect.forkIn(sweepScope),
        ),
      );
    }
    return traceFile;
  }),
);

export const TaskOutputFilesLive = Layer.effect(
  TaskOutputFiles,
  Effect.gen(function* () {
    const traceFile = yield* TraceFile;
    const taskOutputFiles = yield* makeTaskOutputFiles({
      runDirectoryPath: runDirectoryPathOf(yield* RunRootTaskConfig),
      isRunDirectoryWritable: (yield* traceFile.whyIncomplete) === null,
      hasStopSignal: (yield* StopState).hasStopSignal,
    });
    yield* Effect.addFinalizer(() =>
      taskOutputFiles.whyIncomplete.pipe(
        Effect.flatMap((whyIncomplete) =>
          whyIncomplete === null ? Effect.void : printLine(formatFailureLines(whyIncomplete)),
        ),
      ),
    );
    return taskOutputFiles;
  }),
);

export const RuntimeTracingLive = Layer.effectContext(
  Effect.gen(function* () {
    const trace = yield* TraceFile;
    const { rootTaskName, rootTaskFilePath } = yield* RunRootTaskConfig;
    const runSpan = yield* Effect.acquireRelease(
      startTaskSpan("run", `run ${rootTaskName}`, {
        parent: null,
        attributes: {
          [`${TASK_TRACE_LABEL_PREFIX}.root.task`]: rootTaskName,
          [`${TASK_TRACE_LABEL_PREFIX}.entry`]: rootTaskFilePath,
        },
      }),
      (span, exit) => endTaskSpan(span, exit),
    ).pipe(Effect.withTracer(trace.tracer));
    return Context.make(Tracer.Tracer, trace.tracer).pipe(
      Context.add(Logger.CurrentLoggers, new Set([trace.logger])),
      Context.add(Tracer.ParentSpan, runSpan),
    );
  }),
);
