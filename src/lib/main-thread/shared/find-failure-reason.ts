import { Cause, Schema } from "effect";

import { reportCause } from "../../../shared/report-cause.ts";
import { isAwaitedTaskFailed } from "../../../shared/task-errors.ts";
import { SettledTasksFailed } from "../../errors/index.ts";
import type { PrintableError } from "../../errors/index.ts";
import { UnawaitedTasksFailed } from "./main-thread-errors.ts";

const isFailureOfOtherTasks = Schema.is(Schema.Union([SettledTasksFailed, UnawaitedTasksFailed]));

const namesOfFailedTasksBehind = (reason: Cause.Reason<unknown>): readonly string[] => {
  if (!Cause.isFailReason(reason)) {
    return [];
  }
  if (isAwaitedTaskFailed(reason.error)) {
    return [reason.error.awaitedTaskName];
  }
  return isFailureOfOtherTasks(reason.error) ? reason.error.failedTaskNames : [];
};

export const findFailureReason = (
  runEndingCause: Cause.Cause<unknown>,
  failureCauseByTaskName: ReadonlyMap<string, Cause.Cause<unknown>>,
): PrintableError | null => {
  for (const reason of runEndingCause.reasons) {
    if (Cause.isInterruptReason(reason)) {
      continue;
    }
    const failedTaskNames = namesOfFailedTasksBehind(reason);
    if (failedTaskNames.length === 0) {
      return reportCause(Cause.fromReasons([reason])).failures[0];
    }
    for (const failedTaskName of failedTaskNames) {
      const failureCause = failureCauseByTaskName.get(failedTaskName);
      const originalReason =
        failureCause === undefined ? null : findFailureReason(failureCause, failureCauseByTaskName);
      if (originalReason !== null) {
        return originalReason;
      }
    }
  }
  return null;
};
