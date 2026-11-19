import { Clock, Context, Deferred, Effect, FileSystem, Path, Ref, Semaphore } from "effect";
import type { PlatformError, Tracer } from "effect";

import { writeCommandRecap } from "../../../shared/command-recap-file.ts";
import { labelLine, nameTask, prefixTaskLines } from "../../../shared/label-lines.ts";
import { joinOutput, OutputToJoin } from "../../../shared/label-output.ts";
import type { OutputPiece, PRINTED_TO } from "../../../shared/label-output.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  offerLineList,
  offerLines,
  offerOutputPieces,
  taskLinesPrinterDeferred,
} from "../../../shared/print-to-standard-error.ts";
import type { LINE_ORIGINS } from "../../../shared/print-to-standard-error.ts";
import type { StackFrameBlocks } from "../../../shared/rpc.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { describeCutCommandOutput } from "./cut-command-output.ts";
import { formatRecapLines } from "./format-recap-lines.ts";
import { formatSeconds } from "./format-task-lines.ts";
import { makeHeldCommandOutput } from "./hold-command-output.ts";
import { TaskOutputFilesIncomplete } from "./main-thread-errors.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";

const textEncoder = new TextEncoder();

export const OUTPUT_FILE_NAME_ATTRIBUTE = `${TASK_TRACE_LABEL_PREFIX}.output.file.name`;

export class TaskOutputFiles extends Context.Service<
  TaskOutputFiles,
  Effect.Success<ReturnType<typeof makeTaskOutputFiles>>
>()(`${TASK_PACKAGE.name}/TaskOutputFiles`) {}

const sourceOf = (taskName: string, streamId: number) => `${nameTask(taskName)} ${streamId}`;

const bytesOf = (pieces: readonly (typeof OutputPiece.Type)[]) =>
  textEncoder.encode(pieces.map((piece) => `${piece.text}${piece.endsLine ? "\n" : ""}`).join(""));

const labelWrites = (
  writes: readonly {
    readonly taskName: string;
    readonly streamId: number;
    readonly pieces: readonly (typeof OutputPiece.Type)[];
    readonly printedTo: (typeof PRINTED_TO)[number];
  }[],
) => {
  const printedOutputs: (typeof OutputToJoin.cases.Piece.Type)[] = [];
  const outputsByTaskName = new Map<string, (typeof OutputToJoin.cases.Piece.Type)[]>();
  for (const { taskName, streamId, pieces, printedTo } of writes) {
    const outputsOfTheTask = outputsByTaskName.get(taskName) ?? [];
    outputsByTaskName.set(taskName, outputsOfTheTask);
    for (const piece of pieces) {
      const output = OutputToJoin.cases.Piece.make({
        source: sourceOf(taskName, streamId),
        taskName,
        piece,
      });
      if (printedTo === "terminal and file") {
        printedOutputs.push(output);
      }
      outputsOfTheTask.push(output);
    }
  }
  return { printedOutputs, outputsByTaskName };
};

const offerPrintedOutputs = (
  printedOutputs: readonly (typeof OutputToJoin.cases.Piece.Type)[],
  onWritten: Effect.Effect<void>,
) => (printedOutputs.length === 0 ? onWritten : offerOutputPieces(printedOutputs, onWritten));

export const makeTaskOutputFiles = Effect.fn(function* (input: {
  readonly runDirectoryPath: string;
  readonly isRunDirectoryWritable: boolean;
  readonly hasStopSignal: Effect.Effect<boolean>;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const oneLineAtATime = yield* Semaphore.make(1);
  const whyIncomplete = yield* Ref.make<TaskOutputFilesIncomplete | null>(null);
  const filesByTaskName = new Map<
    string,
    {
      readonly fileName: string;
      readonly filePath: string;
      isComplete: boolean;
      hasLines: boolean;
      openSource: string | null;
    }
  >();
  const taskSpansByTaskName = new Map<string, Tracer.Span>();
  const heldOutputsByTaskName = new Map<string, ReturnType<typeof makeHeldCommandOutput>>();
  const pointedFileNamesByTaskName = new Map<string, string>();
  const latestErrorsByTaskName = new Map<string, Parameters<typeof formatRecapLines>[1]>();
  const taskNamesOwedAPointer = new Set<string>();
  const stackFramesByTaskName = new Map<string, typeof StackFrameBlocks.Type>();
  const failedTaskNames = new Set<string>();
  const awaitedTaskNamesWithPrintedOutput = new Set<string>();
  const noTaskRuns: ReadonlyMap<string, { readonly bodyRequestedTaskNames: ReadonlySet<string> }> =
    new Map();
  const awaitedTasks = yield* Ref.make(noTaskRuns);
  const startedTaskNames = new Set<string>();
  const endedTaskNames = new Set<string>();
  const keepsAllItsOutput = (taskName: string) =>
    filesByTaskName.get(taskName)?.isComplete === true;
  const printsWhereItGoes = <Write extends { readonly taskName: string }>(
    write: Write & { readonly printedTo: (typeof PRINTED_TO)[number] },
    isStopping: boolean,
  ) => {
    const printsAsItArrives =
      !keepsAllItsOutput(write.taskName) || (failedTaskNames.has(write.taskName) && !isStopping);
    return printsAsItArrives ? { ...write, printedTo: "terminal and file" as const } : write;
  };
  const holdOutput = (
    writes: readonly Pick<
      Parameters<typeof labelWrites>[0][number],
      "pieces" | "printedTo" | "streamId" | "taskName"
    >[],
  ) => {
    for (const { taskName, streamId, pieces, printedTo } of writes) {
      const isHeld = printedTo === "held" && !endedTaskNames.has(taskName);
      if (isHeld) {
        const heldOutput = heldOutputsByTaskName.get(taskName) ?? makeHeldCommandOutput();
        heldOutputsByTaskName.set(taskName, heldOutput);
        heldOutput.hold(streamId, bytesOf(pieces));
      }
    }
  };
  const recordWhyIncomplete = (failedStep: string, cause: PlatformError.PlatformError) =>
    Ref.update(
      whyIncomplete,
      (firstReason) =>
        firstReason ??
        new TaskOutputFilesIncomplete({
          runDirectoryPath: input.runDirectoryPath,
          failedStep,
          cause,
        }),
    );
  const fileOf = (taskName: string) => {
    if (!input.isRunDirectoryWritable) {
      return null;
    }
    const knownFile = filesByTaskName.get(taskName);
    if (knownFile !== undefined) {
      return knownFile;
    }
    const fileName = `${filesByTaskName.size + 1}.output`;
    const file = {
      fileName,
      filePath: path.join(input.runDirectoryPath, fileName),
      isComplete: true,
      hasLines: false,
      openSource: null,
    };
    filesByTaskName.set(taskName, file);
    return file;
  };
  const appendTaskOutput = (taskName: string, outputs: readonly (typeof OutputToJoin.Type)[]) =>
    Effect.gen(function* () {
      const file = fileOf(taskName);
      const cannotWrite = file === null || !file.isComplete;
      if (cannotWrite) {
        return;
      }
      const texts: string[] = [];
      for (const output of outputs) {
        const [openSource, joined] = joinOutput(file.openSource, output);
        file.openSource = openSource;
        texts.push(...joined);
      }
      if (texts.length === 0) {
        return;
      }
      const written = yield* fileSystem
        .writeFile(file.filePath, textEncoder.encode(texts.join("")), { flag: "a", mode: 0o600 })
        .pipe(
          Effect.as(true),
          Effect.catchTag("PlatformError", (error) =>
            Effect.suspend(() => {
              file.isComplete = false;
              return recordWhyIncomplete(`could not write ${file.filePath}`, error);
            }).pipe(Effect.as(false)),
          ),
        );
      const isFirstLineOfTheFile = written && !file.hasLines;
      if (isFirstLineOfTheFile) {
        file.hasLines = true;
        taskSpansByTaskName.get(taskName)?.attribute(OUTPUT_FILE_NAME_ATTRIBUTE, file.fileName);
      }
    });
  const appendTaskLines = (taskName: string, text: string) =>
    appendTaskOutput(taskName, [OutputToJoin.cases.Line.make({ text })]);
  const printTaskLines = (taskName: string, lines: string) =>
    appendTaskLines(taskName, lines).pipe(
      Effect.andThen(offerLines("runner", lines)),
      oneLineAtATime.withPermits(1),
    );
  yield* Deferred.succeed(taskLinesPrinterDeferred, printTaskLines);
  const findWrittenFile = (taskName: string) =>
    Effect.sync(() => {
      const file = filesByTaskName.get(taskName);
      const holdsAllTheOutput = file !== undefined && file.isComplete && file.hasLines;
      return holdsAllTheOutput ? file : null;
    });
  const findWrittenFilePath = (taskName: string) =>
    Effect.map(findWrittenFile(taskName), (file) => file?.filePath ?? null);
  const printHeldOutput = (taskName: string) =>
    Effect.gen(function* () {
      const heldOutput = heldOutputsByTaskName.get(taskName);
      if (heldOutput === undefined) {
        return;
      }
      heldOutputsByTaskName.delete(taskName);
      if (heldOutput.isCut()) {
        const filePath = yield* findWrittenFilePath(taskName);
        yield* offerLines(
          "task",
          labelLine("warning", taskName, describeCutCommandOutput(filePath)),
        );
      }
      const outputs = [...heldOutput.replay()].flatMap(({ streamId, pieces }) =>
        pieces.map((piece) =>
          OutputToJoin.cases.Piece.make({ source: sourceOf(taskName, streamId), taskName, piece }),
        ),
      );
      yield* offerPrintedOutputs(outputs, Effect.void);
    });
  const appendStreamOutput = (taskName: string, outputs: readonly (typeof OutputToJoin.Type)[]) =>
    appendTaskOutput(taskName, outputs).pipe(
      Effect.andThen(
        Effect.suspend(() =>
          keepsAllItsOutput(taskName) ? Effect.void : printHeldOutput(taskName),
        ),
      ),
    );
  const findAwaitedTaskNames = (taskName: string) =>
    Effect.map(Ref.get(awaitedTasks), (taskRunsByName) => {
      const reached = new Set([taskName]);
      for (const reachedTaskName of reached) {
        for (const awaitedTaskName of taskRunsByName.get(reachedTaskName)?.bodyRequestedTaskNames ??
          []) {
          reached.add(awaitedTaskName);
        }
      }
      reached.delete(taskName);
      return reached;
    });
  const printAwaitedHeldOutput = (taskName: string) =>
    Effect.gen(function* () {
      const hasPrinted =
        failedTaskNames.has(taskName) || awaitedTaskNamesWithPrintedOutput.has(taskName);
      if (hasPrinted) {
        return;
      }
      awaitedTaskNamesWithPrintedOutput.add(taskName);
      yield* printHeldOutput(taskName);
    });
  const pointAtOutputFile = (taskName: string) =>
    Effect.gen(function* () {
      taskNamesOwedAPointer.delete(taskName);
      const file = yield* findWrittenFile(taskName);
      const isFirstPointer =
        file !== null && !pointedFileNamesByTaskName.has(taskName) && !(yield* input.hasStopSignal);
      if (isFirstPointer) {
        const line = labelLine("error", taskName, `its output is in ${file.filePath}`);
        pointedFileNamesByTaskName.set(taskName, file.fileName);
        for (const { error, frames } of stackFramesByTaskName.get(taskName) ?? []) {
          yield* appendTaskLines(
            taskName,
            [error, ...frames].map((text) => labelLine("error", taskName, text)).join("\n"),
          );
        }
        yield* appendTaskLines(taskName, line);
        yield* offerLines("runner", line);
      }
    });
  const endTask = (taskName: string) =>
    Effect.gen(function* () {
      if (taskNamesOwedAPointer.has(taskName)) {
        yield* pointAtOutputFile(taskName);
      }
      endedTaskNames.add(taskName);
      heldOutputsByTaskName.delete(taskName);
    });
  return {
    runDirectoryPath: input.runDirectoryPath,
    commandRecapDirectoryPath: input.isRunDirectoryWritable ? input.runDirectoryPath : null,
    readAwaitedTasksFrom: (taskRunsByName: typeof noTaskRuns) =>
      Ref.set(awaitedTasks, taskRunsByName),
    keepStackFrames: (taskName: string, stackFrames: typeof StackFrameBlocks.Type) =>
      Effect.sync(() => {
        stackFramesByTaskName.set(taskName, stackFrames);
      }),
    addStackFrames: (taskName: string, stackFrames: typeof StackFrameBlocks.Type) =>
      Effect.sync(() => {
        stackFramesByTaskName.set(taskName, [
          ...(stackFramesByTaskName.get(taskName) ?? []),
          ...stackFrames,
        ]);
      }),
    recordTaskSpan: (taskName: string, span: Tracer.Span) =>
      Effect.sync(() => {
        taskSpansByTaskName.set(taskName, span);
      }),
    printTaskOutputs: (taskName: string, loggedTexts: readonly string[]) => {
      const lineList = loggedTexts.map((text) => prefixTaskLines(taskName, text).join("\n"));
      return appendTaskOutput(
        taskName,
        lineList.map((text) => OutputToJoin.cases.Line.make({ text })),
      ).pipe(Effect.andThen(offerLineList("task", lineList)), oneLineAtATime.withPermits(1));
    },
    printStreamOutput: (
      taskName: string,
      streamId: number,
      pieces: readonly (typeof OutputPiece.Type)[],
      onWritten: Effect.Effect<void>,
      printedTo: (typeof PRINTED_TO)[number],
    ) =>
      Effect.gen(function* () {
        const outputs = pieces.map((piece) =>
          OutputToJoin.cases.Piece.make({ source: sourceOf(taskName, streamId), taskName, piece }),
        );
        yield* appendStreamOutput(taskName, outputs);
        const write = printsWhereItGoes(
          { taskName, streamId, pieces, printedTo },
          yield* input.hasStopSignal,
        );
        holdOutput([write]);
        yield* offerPrintedOutputs(write.printedTo === "held" ? [] : outputs, onWritten);
      }).pipe(oneLineAtATime.withPermits(1)),
    printStreamOutputs: (
      writes: Parameters<typeof labelWrites>[0],
      onWritten: Effect.Effect<void>,
    ) =>
      Effect.gen(function* () {
        yield* Effect.forEach(
          labelWrites(writes).outputsByTaskName,
          ([taskName, outputsOfTheTask]) => appendStreamOutput(taskName, outputsOfTheTask),
          { discard: true },
        );
        const isStopping = yield* input.hasStopSignal;
        const writesWhereTheyGo = writes.map((write) => printsWhereItGoes(write, isStopping));
        holdOutput(writesWhereTheyGo);
        yield* offerPrintedOutputs(labelWrites(writesWhereTheyGo).printedOutputs, onWritten);
      }).pipe(oneLineAtATime.withPermits(1)),
    printTaskLines,
    printTaskProgress: <Failure, Requirements>(
      taskName: string,
      makeLine: (fileName: string | null) => Effect.Effect<string, Failure, Requirements>,
      endsTheTask: boolean,
      origin: (typeof LINE_ORIGINS)[number],
    ) =>
      (endsTheTask
        ? endTask(taskName)
        : Effect.sync(() => {
            startedTaskNames.add(taskName);
          })
      ).pipe(
        Effect.andThen(Effect.suspend(() => makeLine(fileOf(taskName)?.fileName ?? null))),
        Effect.flatMap((line) =>
          appendTaskLines(taskName, line).pipe(Effect.andThen(offerLines(origin, line))),
        ),
        oneLineAtATime.withPermits(1),
      ),
    settleHeldOutput: (
      taskName: string,
      streamIds: readonly number[],
      outcome: "failed" | "passed",
    ) =>
      Effect.sync(() => {
        const heldOutput = heldOutputsByTaskName.get(taskName);
        if (outcome === "passed") {
          heldOutput?.release(new Set(streamIds));
        } else {
          heldOutput?.keepFailed(new Set(streamIds));
        }
      }),
    endClosedTask: (taskName: string) => endTask(taskName).pipe(oneLineAtATime.withPermits(1)),
    printTaskErrors: (
      taskName: string,
      errorLineList: readonly { readonly printedTaskName: string; readonly text: string }[],
      recap: Parameters<typeof formatRecapLines>[1] | undefined,
    ) =>
      Effect.gen(function* () {
        if (recap !== undefined) {
          latestErrorsByTaskName.set(taskName, recap);
        }
        const errorLines = errorLineList
          .map(({ printedTaskName, text }) => labelLine("error", printedTaskName, text))
          .join("\n");
        const withOutput = !(yield* input.hasStopSignal);
        if (withOutput) {
          yield* Effect.forEach(yield* findAwaitedTaskNames(taskName), printAwaitedHeldOutput, {
            discard: true,
          });
          yield* printHeldOutput(taskName);
          failedTaskNames.add(taskName);
        }
        yield* appendTaskLines(taskName, errorLines);
        yield* offerLines("runner", errorLines);
        if (!withOutput) {
          return;
        }
        const printsNoMoreLines = !startedTaskNames.has(taskName) || endedTaskNames.has(taskName);
        yield* printsNoMoreLines
          ? pointAtOutputFile(taskName)
          : Effect.sync(() => {
              taskNamesOwedAPointer.add(taskName);
            });
      }).pipe(oneLineAtATime.withPermits(1)),
    printRecapLines: (failedTaskNamesOfTheRun: readonly string[]) =>
      Effect.gen(function* () {
        if (yield* input.hasStopSignal) {
          return;
        }
        const recapLines = failedTaskNamesOfTheRun.flatMap((taskName) => {
          const latestError = latestErrorsByTaskName.get(taskName);
          return latestError === undefined
            ? []
            : formatRecapLines(taskName, latestError, pointedFileNamesByTaskName.get(taskName));
        });
        if (recapLines.length > 0) {
          yield* writeCommandRecap(
            fileSystem,
            recapLines.map((line) => line.slice("recap: ".length)),
          );
          yield* offerLines("runner", recapLines.join("\n"));
        }
      }).pipe(oneLineAtATime.withPermits(1)),
    whyIncomplete: Ref.get(whyIncomplete),
  };
});

export const printTaskOutputs = (taskName: string, loggedTexts: readonly string[]) =>
  TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.printTaskOutputs(taskName, loggedTexts));

export const printStreamOutput = (
  ...printed: Parameters<TaskOutputFiles["Service"]["printStreamOutput"]>
) => TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.printStreamOutput(...printed));

export const printStreamOutputs = (
  ...printed: Parameters<TaskOutputFiles["Service"]["printStreamOutputs"]>
) => TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.printStreamOutputs(...printed));

export const printTaskLines = (taskName: string, lines: string) =>
  TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.printTaskLines(taskName, lines));

export const settleHeldOutput = (
  ...settled: Parameters<TaskOutputFiles["Service"]["settleHeldOutput"]>
) => TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.settleHeldOutput(...settled));

export const keepStackFrames = (taskName: string, stackFrames: typeof StackFrameBlocks.Type) =>
  TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.keepStackFrames(taskName, stackFrames));

export const addStackFrames = (taskName: string, stackFrames: typeof StackFrameBlocks.Type) =>
  TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.addStackFrames(taskName, stackFrames));

export const endClosedTask = (taskName: string) =>
  TaskOutputFiles.use((taskOutputFiles) => taskOutputFiles.endClosedTask(taskName));

const printTaskProgress = (
  label: "ended" | "started",
  taskName: string,
  describeTheTask: (fileName: string | null) => readonly string[],
  origin: (typeof LINE_ORIGINS)[number],
) =>
  Effect.gen(function* () {
    const { runStartedAtMilliseconds } = yield* RunRootTaskConfig;
    const taskOutputFiles = yield* TaskOutputFiles;
    yield* taskOutputFiles.printTaskProgress(
      taskName,
      (fileName) =>
        Effect.map(Clock.currentTimeMillis, (nowMilliseconds) =>
          labelLine(
            label,
            null,
            [
              nameTask(taskName),
              formatSeconds(nowMilliseconds - runStartedAtMilliseconds),
              ...describeTheTask(fileName),
            ].join(", "),
          ),
        ),
      label === "ended",
      origin,
    );
  });

export const printTaskStart = (taskName: string) =>
  printTaskProgress(
    "started",
    taskName,
    (fileName) => (fileName === null ? [] : [fileName]),
    "progress",
  );

export const printTaskEnd = (taskName: string, state: string) =>
  printTaskProgress("ended", taskName, () => [state], state === "passed" ? "progress" : "task");
