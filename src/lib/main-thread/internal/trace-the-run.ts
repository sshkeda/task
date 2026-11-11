import {
  Clock,
  Context,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Logger,
  Option,
  Path,
  Ref,
  Tracer,
} from "effect";

import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import { formatIsoTimestamp } from "../../../shared/format-task-lines.ts";
import { printLine } from "../../../shared/print-to-standard-error.ts";
import { TraceFileIncomplete } from "../shared/main-thread-errors.ts";
import { RunRootTaskConfig } from "../shared/provide-run-root-task-config.ts";
import { TraceFile, makeTraceFile } from "../shared/write-trace-file.ts";

const TRACE_RETENTION = "14 days";

const deleteTracesOlderThanRetention = Effect.fn("deleteTracesOlderThanRetention")(function* (
  currentTraceFilePath: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const traceDirectoryPath = path.dirname(currentTraceFilePath);
  const oldestKeptMilliseconds =
    (yield* Clock.currentTimeMillis) - Duration.toMillis(TRACE_RETENTION);
  const fileNames = yield* fileSystem.readDirectory(traceDirectoryPath);
  yield* Effect.forEach(
    fileNames.filter(
      (fileName) => fileName.endsWith(".jsonl") && fileName !== path.basename(currentTraceFilePath),
    ),
    (fileName) => {
      const traceFilePath = path.join(traceDirectoryPath, fileName);
      return fileSystem.stat(traceFilePath).pipe(
        Effect.flatMap((information) => {
          const isExpiredTraceFile =
            information.type === "File" &&
            Option.exists(information.mtime, (mtime) => mtime.getTime() < oldestKeptMilliseconds);
          return isExpiredTraceFile ? fileSystem.remove(traceFilePath) : Effect.void;
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
    const timestamp = formatIsoTimestamp(runRootTaskConfig.runStartedAtMilliseconds).replaceAll(
      ":",
      "-",
    );
    const traceFilePath = `${runRootTaskConfig.stateDirectoryPath}/traces/${runRootTaskConfig.traceDirectoryName}/${timestamp}-${runRootTaskConfig.runProcessId}.jsonl`;
    const made = yield* Ref.make<Effect.Success<ReturnType<typeof makeTraceFile>> | null>(null);
    const sweepOfOldTraces = yield* Ref.make<Fiber.Fiber<void> | null>(null);
    yield* Effect.addFinalizer(() =>
      Ref.get(made).pipe(
        Effect.flatMap((madeTraceFile) =>
          madeTraceFile === null
            ? Effect.void
            : madeTraceFile.whyIncomplete.pipe(
                Effect.flatMap((whyIncomplete) =>
                  whyIncomplete === null
                    ? printLine("Info", `wrote the trace to ${traceFilePath}`)
                    : printLine(
                        "Error",
                        formatFailureLines(
                          new TraceFileIncomplete({ traceFilePath, reason: whyIncomplete }),
                          null,
                        ),
                      ),
                ),
              ),
        ),
        Effect.andThen(
          Effect.flatMap(Ref.get(sweepOfOldTraces), (sweep) =>
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
    if ((yield* traceFile.whyIncomplete) === null) {
      yield* Ref.set(
        sweepOfOldTraces,
        yield* deleteTracesOlderThanRetention(traceFilePath).pipe(
          Effect.catchTag("PlatformError", () => Effect.void),
          Effect.forkScoped,
        ),
      );
    }
    return traceFile;
  }),
);

export const RuntimeTracingLive = Layer.effectContext(
  Effect.gen(function* () {
    const trace = yield* TraceFile;
    const runSpan = yield* Effect.makeSpanScoped("runRootTaskFiber").pipe(
      Effect.withTracer(trace.tracer),
    );
    return Context.make(Tracer.Tracer, trace.tracer).pipe(
      Context.add(Logger.CurrentLoggers, new Set([trace.logger])),
      Context.add(Tracer.ParentSpan, runSpan),
    );
  }),
);
