import { Array as Arrays, Effect, Logger, LogLevel, Formatter, References } from "effect";

import { formatCauseLines } from "../../../../../shared/format-error-lines.ts";
import { waitWhileTaskLogsAreHeld } from "../../../../../shared/hold-task-logs.ts";
import type { OutputPiece, PRINTED_TO } from "../../../../../shared/label-output.ts";
import type { TaskLines } from "../../../../../shared/mark-task-worker-thread-contexts.ts";
import { TaskEvent, WorkerLine } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import {
  takeTaskOutputBytes,
  reserveTaskOutputBytes,
} from "../../../../../shared/task-output-budget.ts";
import { makeTraceLogRecordJson } from "../../../shared/make-trace-log-record.ts";

const makeLineLogger = (
  startMessage: typeof StartTaskFiber.Type,
  taskLogHold: Int32Array,
  reportToMainThreadNow: (event: typeof TaskEvent.Type) => void,
  makeLine: (logLevel: LogLevel.LogLevel, text: string) => typeof WorkerLine.Type,
) =>
  Logger.make((logEntry) => {
    const text = Arrays.ensure(logEntry.message)
      .map((part) => (typeof part === "string" ? part : Formatter.format(part)))
      .join(" ");
    const line = [
      ...(text === "" ? [] : [text]),
      ...(logEntry.cause.reasons.length === 0
        ? []
        : [
            formatCauseLines(
              logEntry.cause,
              startMessage.taskName,
              LogLevel.isGreaterThanOrEqualTo(logEntry.logLevel, "Error")
                ? "error"
                : logEntry.logLevel.toLowerCase(),
            ),
          ]),
    ].join("\n");
    waitWhileTaskLogsAreHeld(taskLogHold);
    reportToMainThreadNow(
      TaskEvent.cases.StandardErrorLineRequested.make({
        line: makeLine(logEntry.logLevel, line),
        traceLogRecordJson: makeTraceLogRecordJson(logEntry),
      }),
    );
  });

export const makeTaskLogger = (
  startMessage: typeof StartTaskFiber.Type,
  taskLogHold: Int32Array,
  reportToMainThreadNow: (event: typeof TaskEvent.Type) => void,
) =>
  makeLineLogger(startMessage, taskLogHold, reportToMainThreadNow, (_logLevel, text) =>
    WorkerLine.cases.Logged.make({ taskName: startMessage.taskName, text }),
  );

const printWithOnly =
  (logger: Logger.Logger<unknown, void>) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    effect.pipe(
      Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
      Effect.provideService(References.MinimumLogLevel, "All"),
    );

export const makeTaskLines = (
  startMessage: typeof StartTaskFiber.Type,
  taskLogHold: Int32Array,
  taskOutputBudget: Int32Array,
  reportToMainThreadNow: (event: typeof TaskEvent.Type) => void,
  postStreamOutput: (
    taskName: string,
    streamId: number,
    pieces: readonly (typeof OutputPiece.Type)[],
    spanIds: { readonly traceId: string; readonly spanId: string } | undefined,
  ) => void,
): (typeof TaskLines)["Service"] => {
  const { taskName } = startMessage;
  const reportLogger = makeLineLogger(
    startMessage,
    taskLogHold,
    reportToMainThreadNow,
    (logLevel, text) =>
      WorkerLine.cases.Reported.make({
        taskName,
        label: LogLevel.isGreaterThanOrEqualTo(logLevel, "Error") ? "error" : "warning",
        text,
      }),
  );
  const printHeldPieces =
    (
      streamId: number,
      pieces: readonly (typeof OutputPiece.Type)[],
      printedTo: (typeof PRINTED_TO)[number],
    ) =>
    (heldBytes: number) => {
      const line = WorkerLine.cases.StreamWrote.make({
        taskName,
        streamId,
        pieces,
        heldBytes,
        printedTo,
      });
      return Effect.log(pieces.map((piece) => piece.text).join("\n")).pipe(
        printWithOnly(makeLineLogger(startMessage, taskLogHold, reportToMainThreadNow, () => line)),
      );
    };
  return {
    printStreamOutput: (streamId, pieces, printedTo) =>
      takeTaskOutputBytes(taskOutputBudget, reserveTaskOutputBytes(taskName, pieces)).pipe(
        Effect.flatMap(printHeldPieces(streamId, pieces, printedTo)),
      ),
    settleHeldOutput: (streamIds, outcome) =>
      Effect.sync(() => {
        waitWhileTaskLogsAreHeld(taskLogHold);
        reportToMainThreadNow(
          TaskEvent.cases.StandardErrorLineRequested.make({
            line: WorkerLine.cases.HeldOutputSettled.make({ taskName, streamIds, outcome }),
          }),
        );
      }),
    postStreamOutputNow: (streamId, pieces, spanIds) => {
      postStreamOutput(taskName, streamId, pieces, spanIds);
    },
    printWarning: (text) => Effect.logWarning(text).pipe(printWithOnly(reportLogger)),
    printError: (text) => Effect.logError(text).pipe(printWithOnly(reportLogger)),
  };
};
