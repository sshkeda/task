import { Cause, Effect, Predicate } from "effect";

import { formatCauseLines } from "../../../shared/format-error-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { printTaskLines } from "../../../shared/print-to-standard-error.ts";
import { reportCause } from "../../../shared/report-cause.ts";
import { StopState } from "./provide-stop-state.ts";

export const reportFailure = (failure: Error) => reportCause(Cause.fail(failure)).failures;

export const reportTaskFailure = Effect.fn("reportTaskFailure")(function* (
  taskName: string,
  cause: Parameters<StopState["Service"]["recordTaskFailureCause"]>[1],
) {
  const shutdown = yield* StopState;
  yield* shutdown.recordTaskFailureCause(taskName, cause);
  const errorLines = formatCauseLines(cause, taskName);
  if (cause.reasons.length > 0) {
    yield* printTaskLines("Error", taskName, errorLines);
  }
  const failures = cause.reasons.filter(Cause.isFailReason).map((reason) => reason.error);
  for (const failure of failures) {
    yield* shutdown.recordPrintedFailure(failure);
  }
  const details = failures
    .map((failure) => (Predicate.hasProperty(failure, "details") ? failure.details : undefined))
    .filter((detail) => Predicate.isString(detail));
  yield* Effect.logInfo(`failed: ${details.length === 0 ? errorLines : details.join("\n")}`).pipe(
    Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName),
  );
}, Effect.uninterruptible);
