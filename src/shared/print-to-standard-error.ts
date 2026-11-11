import { Deferred, Effect, Queue, Runtime, Schema } from "effect";
import type { Cause, LogLevel } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "./error-fields.ts";
import { prefixLines } from "./format-task-lines.ts";
import { isInsideTaskWorkerThread } from "./mark-task-worker-thread-contexts.ts";
import type { StandardErrorLine } from "./rpc.ts";

export class StandardErrorWriteFailed extends Schema.TaggedError<StandardErrorWriteFailed>()(
  "StandardErrorWriteFailed",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    errnoCode: Schema.NullOr(Schema.String),
    reason: Schema.String,
  },
) {
  public override get message() {
    return `could not write to standard error (${this.reason}), so task stopped the run`;
  }

  public override get [Runtime.errorReported]() {
    return false;
  }
}

export const standardErrorWriteFailedDeferred = Deferred.makeUnsafe<StandardErrorWriteFailed>();

export const standardErrorClosedDeferred = Deferred.makeUnsafe<"pipe" | "terminal">();

export const standardErrorLinesDeferred =
  Deferred.makeUnsafe<Queue.Queue<typeof StandardErrorLine.Type, Cause.Done>>();

export const standardErrorLinesToTheMainThreadDeferred =
  Deferred.makeUnsafe<(logLevel: LogLevel.LogLevel, text: string) => Effect.Effect<void>>();

export const printLine = (logLevel: LogLevel.LogLevel, text: string) =>
  isInsideTaskWorkerThread()
    ? Deferred.await(standardErrorLinesToTheMainThreadDeferred).pipe(
        Effect.flatMap((sendLine) => sendLine(logLevel, text)),
      )
    : Deferred.await(standardErrorLinesDeferred).pipe(
        Effect.flatMap((lines) => Queue.offer(lines, { logLevel, text: `${text}\n` })),
        Effect.asVoid,
      );

export const printTaskLines = (logLevel: LogLevel.LogLevel, taskName: string, text: string) =>
  printLine(logLevel, prefixLines(taskName, text));
