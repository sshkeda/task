import { Array as Arrays, Logger, Formatter } from "effect";

import { formatCauseLines } from "../../../../../shared/format-error-lines.ts";
import { prefixLines } from "../../../../../shared/format-task-lines.ts";
import { waitWhileTaskLogsAreHeld } from "../../../../../shared/hold-task-logs.ts";
import { TaskEvent } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import type { makeTaskBodyRecord } from "../../../shared/define-task-body-record.ts";
import { makeTraceLogRecordJson } from "./make-trace-log-record.ts";

export const makeTaskLogger = (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  taskLogHold: Int32Array,
  reportToMainThreadNow: (event: typeof TaskEvent.Type) => void,
) =>
  Logger.make((logEntry) => {
    const text = Arrays.ensure(logEntry.message)
      .map((part) => (typeof part === "string" ? part : Formatter.format(part)))
      .join(" ");
    const line = [
      ...(text === "" ? [] : [text]),
      ...(logEntry.cause.reasons.length === 0
        ? []
        : [formatCauseLines(logEntry.cause, startMessage.taskName)]),
    ].join("\n");
    body.logs.push(line);
    waitWhileTaskLogsAreHeld(taskLogHold);
    reportToMainThreadNow(
      TaskEvent.cases.StandardErrorLineRequested.make({
        logLevel: logEntry.logLevel,
        text: prefixLines(startMessage.taskName, line),
        traceLogRecordJson: makeTraceLogRecordJson(logEntry),
      }),
    );
  });
