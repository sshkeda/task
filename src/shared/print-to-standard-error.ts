import { Deferred, Effect, Exit, Queue, Ref, Runtime, Schema, Stdio, Stream } from "effect";
import type { Cause } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "./error-fields.ts";
import { formatCauseErrorLines, formatFailureLines } from "./format-error-lines.ts";
import { joinMessages } from "./join-messages.ts";
import { OutputToJoin } from "./label-output.ts";
import { isInsideTaskWorkerThread } from "./mark-task-worker-thread-contexts.ts";
import { reportWrappedCause } from "./report-cause.ts";

export class StandardErrorWriteFailed extends Schema.TaggedError<StandardErrorWriteFailed>()(
  "StandardErrorWriteFailed",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(this, "could not write to standard error, so task stopped the run");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }

  public override get [Runtime.errorReported]() {
    return false;
  }
}

export const standardErrorWriteFailedDeferred = Deferred.makeUnsafe<StandardErrorWriteFailed>();

export const standardErrorClosedDeferred = Deferred.makeUnsafe<"pipe" | "terminal">();

export const LINE_ORIGINS = ["runner", "task", "progress", "status"] as const;

const lastStandardErrorWriteRef = Ref.makeUnsafe({
  atMilliseconds: 0,
  isLineOpen: false,
  nextWrite: Deferred.makeUnsafe<void>(),
});

export const readLastStandardErrorWrite = Ref.get(lastStandardErrorWriteRef).pipe(
  Effect.map(({ atMilliseconds, isLineOpen, nextWrite }) => ({
    atMilliseconds,
    isLineOpen,
    awaitNextWrite: Deferred.await(nextWrite),
  })),
);

export const recordStandardErrorWrite = (atMilliseconds: number, isLineOpen: boolean) =>
  Ref.getAndSet(lastStandardErrorWriteRef, {
    atMilliseconds,
    isLineOpen,
    nextWrite: Deferred.makeUnsafe<void>(),
  }).pipe(Effect.flatMap(({ nextWrite }) => Deferred.done(nextWrite, Exit.void)));

export const makeStandardErrorLine = (
  origin: (typeof LINE_ORIGINS)[number],
  output: typeof OutputToJoin.Type,
  onWritten: Effect.Effect<void> = Effect.void,
) => ({ origin, output, onWritten });

export const standardErrorWriterStoppedDeferred = Deferred.makeUnsafe<void>();

export const standardErrorLinesDeferred =
  Deferred.makeUnsafe<Queue.Queue<ReturnType<typeof makeStandardErrorLine>, Cause.Done>>();

export const printedLinesToTheMainThreadDeferred =
  Deferred.makeUnsafe<(taskName: string | null, text: string) => Effect.Effect<void>>();

export const taskLinesPrinterDeferred =
  Deferred.makeUnsafe<(taskName: string, text: string) => Effect.Effect<void>>();

export const offerLines = (origin: (typeof LINE_ORIGINS)[number], text: string) =>
  Deferred.await(standardErrorLinesDeferred).pipe(
    Effect.flatMap((lines) =>
      Queue.offer(lines, makeStandardErrorLine(origin, OutputToJoin.cases.Line.make({ text }))),
    ),
    Effect.asVoid,
  );

export const offerLineList = (origin: (typeof LINE_ORIGINS)[number], texts: readonly string[]) =>
  Deferred.await(standardErrorLinesDeferred).pipe(
    Effect.flatMap((lines) =>
      Queue.offerAll(
        lines,
        texts.map((text) => makeStandardErrorLine(origin, OutputToJoin.cases.Line.make({ text }))),
      ),
    ),
    Effect.asVoid,
  );

export const offerOutputPiece = (
  output: typeof OutputToJoin.cases.Piece.Type,
  onWritten: Effect.Effect<void>,
) =>
  Deferred.await(standardErrorLinesDeferred).pipe(
    Effect.flatMap((lines) =>
      Queue.offer(
        lines,
        makeStandardErrorLine(output.taskName === null ? "runner" : "task", output, onWritten),
      ),
    ),
    Effect.asVoid,
  );

export const offerOutputPieces = (
  outputs: readonly (typeof OutputToJoin.cases.Piece.Type)[],
  onWritten: Effect.Effect<void>,
) =>
  Deferred.await(standardErrorLinesDeferred).pipe(
    Effect.flatMap((lines) =>
      Queue.offerAll(
        lines,
        outputs.map((output, index) =>
          makeStandardErrorLine(
            output.taskName === null ? "runner" : "task",
            output,
            index === outputs.length - 1 ? onWritten : Effect.void,
          ),
        ),
      ),
    ),
    Effect.asVoid,
  );

const printLineAboutTaskOnTheMainThread = (taskName: string, text: string) =>
  Deferred.isDone(taskLinesPrinterDeferred).pipe(
    Effect.flatMap((canWriteTaskFiles) =>
      canWriteTaskFiles
        ? Deferred.await(taskLinesPrinterDeferred).pipe(
            Effect.flatMap((printTaskLines) => printTaskLines(taskName, text)),
          )
        : offerLines("runner", text),
    ),
  );

export const printLineAboutTask = (taskName: string | null, text: string) => {
  if (isInsideTaskWorkerThread()) {
    return Deferred.await(printedLinesToTheMainThreadDeferred).pipe(
      Effect.flatMap((sendLine) => sendLine(taskName, text)),
    );
  }
  return taskName === null
    ? offerLines("runner", text)
    : printLineAboutTaskOnTheMainThread(taskName, text);
};

export const printLine = (text: string) => printLineAboutTask(null, text);

export const queueLineToAwaitItsWrite = (text: string) =>
  Effect.gen(function* () {
    const lines = yield* Deferred.await(standardErrorLinesDeferred);
    const written = yield* Deferred.make<void>();
    yield* Queue.offer(
      lines,
      makeStandardErrorLine(
        "runner",
        OutputToJoin.cases.Line.make({ text }),
        Deferred.done(written, Exit.void),
      ),
    );
    return written;
  });

export const awaitLineWritten = (written: Deferred.Deferred<void>) =>
  Effect.raceFirst(Deferred.await(written), Deferred.await(standardErrorWriterStoppedDeferred));

const writeErrorLinesNow = (errorLines: string) =>
  Stdio.Stdio.pipe(
    Effect.flatMap((stdio) => Stream.make(`${errorLines}\n`).pipe(Stream.run(stdio.stderr()))),
    Effect.ignore,
  );

export const printFailureNow = (failure: Parameters<typeof formatFailureLines>[0]) =>
  writeErrorLinesNow(formatFailureLines(failure));

export const printCauseNow = <Failure>(cause: Cause.Cause<Failure>) =>
  writeErrorLinesNow(formatCauseErrorLines(cause));
