import { Cause, Effect, Predicate, Struct } from "effect";

import {
  describeErrorLines,
  formatCauseLines,
  listErrorLines,
} from "../../../shared/format-error-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { describeLinesNotShown, recapLine } from "../../../shared/recap-entries.ts";
import { reportCause } from "../../../shared/report-cause.ts";
import { failureValuesOfReason } from "../../../shared/select-cause-reasons.ts";
import { isAwaitedTasksFailurePrintable } from "../../../shared/task-errors.ts";
import type { PrintableError } from "../../errors/index.ts";
import { makeFailureEntry, StopState } from "./provide-stop-state.ts";
import { TaskOutputFiles } from "./write-task-output-files.ts";

export const reportFailure = (failure: Error) => reportCause(Cause.fail(failure)).failures;

const failureEntriesOfItsOwn = (cause: Cause.Cause<unknown>) => {
  const failureReasons = cause.reasons.filter((reason) => !Cause.isInterruptReason(reason));
  if (failureReasons.length === 0) {
    return cause.reasons.length === 0 ? [] : [makeFailureEntry(cause, reportCause(cause).failures)];
  }
  return failureReasons
    .map((reason) =>
      makeFailureEntry(
        failureValuesOfReason(reason)[0],
        reportCause(Cause.fromReasons([reason])).failures.filter(
          (printable) => !isAwaitedTasksFailurePrintable(printable),
        ),
      ),
    )
    .filter((entry) => entry.printables.length > 0);
};

const MOST_RECAPPED_ERROR_LINES = 10;

const recapErrorLines = (detailLines: readonly string[]) => {
  const nonBlankIndexes = detailLines.flatMap((line, index) => (/\S/u.test(line) ? [index] : []));
  const shownIndexes = nonBlankIndexes.slice(0, MOST_RECAPPED_ERROR_LINES);
  const linesNotShown = (nonBlankIndexes.at(-1) ?? 0) - (shownIndexes.at(-1) ?? 0);
  return [
    ...shownIndexes.map((index) => detailLines[index] ?? ""),
    ...(linesNotShown > 0 ? [describeLinesNotShown(linesNotShown)] : []),
  ];
};

const recapError = (error: PrintableError) => {
  const [firstLine = "", ...detailLines] = describeErrorLines(Struct.omit(error, ["context"]));
  return {
    text: recapLine(firstLine),
    outputLines: error.outputLines ?? recapErrorLines(detailLines),
  };
};

const printTaskErrors = Effect.fn(function* (
  taskName: string,
  printables: readonly PrintableError[],
) {
  if (printables.length === 0) {
    return;
  }
  const shutdown = yield* StopState;
  const located = yield* shutdown.locatePrintables(taskName, printables);
  const lastError = located.at(-1);
  const recap = lastError === undefined ? undefined : recapError(lastError);
  yield* TaskOutputFiles.use((taskOutputFiles) =>
    taskOutputFiles.printTaskErrors(taskName, listErrorLines(located, taskName), recap),
  );
});

export const reportTaskFailure = Effect.fn(function* (
  taskName: string,
  cause: Parameters<StopState["Service"]["recordTaskFailure"]>[1],
) {
  const shutdown = yield* StopState;
  const addedPrintables = yield* shutdown.recordTaskFailure(
    taskName,
    cause,
    failureEntriesOfItsOwn(cause),
  );
  yield* printTaskErrors(taskName, addedPrintables);
  const details = cause.reasons
    .filter(Cause.isFailReason)
    .map((reason) =>
      Predicate.hasProperty(reason.error, "details") ? reason.error.details : undefined,
    )
    .filter((detail) => Predicate.isString(detail));
  yield* Effect.logInfo(
    `failed: ${details.length === 0 ? formatCauseLines(cause, taskName) : details.join("\n")}`,
  ).pipe(Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName));
}, Effect.uninterruptible);
