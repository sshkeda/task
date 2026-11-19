import { Array as Arrays, Cause, Option, Schema, Struct } from "effect";

import { findProjectLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { reportCause } from "../../../shared/report-cause.ts";
import { failureValuesOfReason } from "../../../shared/select-cause-reasons.ts";
import {
  isAwaitedTaskFailed,
  isAwaitedTasksFailurePrintable,
} from "../../../shared/task-errors.ts";
import { SettledTasksFailed } from "../../errors/index.ts";
import type { PrintableError } from "../../errors/index.ts";
import { UnawaitedTasksFailed } from "./main-thread-errors.ts";

const isFailureOfOtherTasks = Schema.is(Schema.Union([SettledTasksFailed, UnawaitedTasksFailed]));

const FailureWithAwaitedTasks = Schema.Struct({ awaitedTaskNames: Schema.Array(Schema.String) });

const namesOfAwaitedTasksBehind = (failure: unknown): readonly string[] =>
  Option.match(Schema.decodeUnknownOption(FailureWithAwaitedTasks)(failure), {
    onNone: () => [],
    onSome: (withAwaitedTasks) => withAwaitedTasks.awaitedTaskNames,
  });

const namesOfFailedTasksBehind = (reason: Cause.Reason<unknown>): readonly string[] => {
  if (!Cause.isFailReason(reason)) {
    return [];
  }
  if (isAwaitedTaskFailed(reason.error)) {
    return [reason.error.awaitedTaskName];
  }
  if (isFailureOfOtherTasks(reason.error)) {
    return reason.error.failedTaskNames;
  }
  const [firstPrintable] = reportCause(Cause.fromReasons([reason])).failures;
  return isAwaitedTasksFailurePrintable(firstPrintable)
    ? namesOfAwaitedTasksBehind(reason.error)
    : [];
};

const isOnlyFailureOfOtherTasks = (reason: Cause.Reason<unknown>) =>
  Cause.isFailReason(reason) &&
  (isAwaitedTaskFailed(reason.error) || isFailureOfOtherTasks(reason.error));

const findOwnPrintable = (reason: Cause.Reason<unknown>) => {
  const printables = reportCause(Cause.fromReasons([reason])).failures;
  return (
    printables.find((printable) => !isAwaitedTasksFailurePrintable(printable)) ?? printables[0]
  );
};

const FailureOfTask = Schema.Struct({ taskName: Schema.String });

export const taskNameOf = (reason: Cause.Reason<unknown>) =>
  Cause.isFailReason(reason)
    ? Option.getOrNull(
        Option.map(
          Schema.decodeUnknownOption(FailureOfTask)(reason.error),
          (failure) => failure.taskName,
        ),
      )
    : null;

export const findFailureReason = (
  runEndingCause: Cause.Cause<unknown>,
  failureCauseByTaskName: ReadonlyMap<string, Cause.Cause<unknown>>,
  failedTaskName: string | null = null,
): {
  readonly reason: PrintableError;
  readonly taskName: string | null;
  readonly failureValue: unknown;
} | null => {
  for (const reason of runEndingCause.reasons) {
    if (Cause.isInterruptReason(reason)) {
      continue;
    }
    const originalReason = Arrays.findFirst(namesOfFailedTasksBehind(reason), (taskName) =>
      Option.fromNullishOr(
        Option.match(Option.fromUndefinedOr(failureCauseByTaskName.get(taskName)), {
          onNone: () => null,
          onSome: (failureCause) =>
            findFailureReason(failureCause, failureCauseByTaskName, taskName),
        }),
      ),
    );
    if (Option.isSome(originalReason)) {
      return originalReason.value;
    }
    if (!isOnlyFailureOfOtherTasks(reason)) {
      return {
        reason: findOwnPrintable(reason),
        taskName: taskNameOf(reason) ?? failedTaskName,
        failureValue: failureValuesOfReason(reason)[0],
      };
    }
  }
  return null;
};

export const locatePrintable = (
  printable: PrintableError,
  ownTaskName: string | null,
  discoveredTasksByName: ReadonlyMap<string, { readonly location: string }>,
): PrintableError => {
  const placingTaskName = printable.taskName ?? ownTaskName;
  const isPlacedByTask = printable.location === undefined && placingTaskName !== null;
  const placingTask = isPlacedByTask ? discoveredTasksByName.get(placingTaskName) : undefined;
  const location = findProjectLocation(placingTask?.location);
  return location === undefined ? printable : { ...Struct.omit(printable, ["location"]), location };
};
