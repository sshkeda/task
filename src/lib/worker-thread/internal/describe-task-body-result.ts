import { Cause, Effect, Exit, Option, Schema } from "effect";

import { reportCause } from "../../../shared/report-cause.ts";
import { TaskFiberResult } from "../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../shared/rpc.ts";
import { failuresOfCause, failureValuesOfReason } from "../../../shared/select-cause-reasons.ts";
import { isAwaitedTaskFailed, isAwaitedTasksFailure } from "../../../shared/task-errors.ts";
import { CapturedCommandFailed, CommandFailed } from "../../errors/index.ts";
import type { makeTaskBodyRecord } from "../shared/define-task-body-record.ts";
import { encodeTaskOutput } from "../shared/encode-task-output.ts";

const isCommandFailed = Schema.is(Schema.Union([CommandFailed, CapturedCommandFailed]));

const awaitedTaskNameOf = (reason: Cause.Reason<unknown>) => {
  const [failureValue] = failureValuesOfReason(reason);
  return isAwaitedTaskFailed(failureValue) ? failureValue.awaitedTaskName : null;
};

const withoutRepeatedAwaitedFailures = (cause: Cause.Cause<unknown>) => {
  const awaitedTaskNames = new Set<string>();
  return Cause.fromReasons(
    cause.reasons.filter((reason) => {
      const awaitedTaskName = awaitedTaskNameOf(reason);
      if (awaitedTaskName === null) {
        return true;
      }
      const isRepeated = awaitedTaskNames.has(awaitedTaskName);
      awaitedTaskNames.add(awaitedTaskName);
      return !isRepeated;
    }),
  );
};

export const describeBodyFailure = (bodyCause: Cause.Cause<unknown>) => {
  const cause = withoutRepeatedAwaitedFailures(bodyCause);
  const failureCause = failuresOfCause(cause);
  const hasOnlyOneFailure = failureCause.reasons.length === 1;
  const firstFailure = Cause.findErrorOption(failureCause);
  const awaitedTasksFailure = Option.filter(firstFailure, isAwaitedTasksFailure);
  const hasOnlyAwaitedFailure = hasOnlyOneFailure && Option.isSome(awaitedTasksFailure);
  if (hasOnlyAwaitedFailure) {
    return TaskFiberResult.cases.AwaitedTasksFailed.make({ failure: awaitedTasksFailure.value });
  }
  const failedCommand = Option.filter(firstFailure, isCommandFailed);
  const failedCommandExitCode = Option.getOrNull(
    Option.flatMapNullishOr(failedCommand, (failure) => failure.code),
  );
  const hasOnlyCommandFailure = hasOnlyOneFailure && failedCommandExitCode !== null;
  return TaskFiberResult.cases.Failed.make({
    ...reportCause(cause),
    ...(hasOnlyCommandFailure ? { failedCommandExitCode } : {}),
  });
};

export const describeTaskBodyResult = (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  taskExit: Exit.Exit<unknown, unknown>,
  elapsedMilliseconds: number,
) => {
  if (Exit.isSuccess(taskExit)) {
    return encodeTaskOutput(startMessage.taskName, "returned", taskExit.value).pipe(
      Effect.map((returnedValue) =>
        TaskFiberResult.cases.Returned.make({
          ...(returnedValue === undefined ? {} : { value: returnedValue }),
          elapsedMilliseconds,
        }),
      ),
      Effect.catchTag("TaskOutputInvalid", (error) =>
        Effect.succeed(TaskFiberResult.cases.Failed.make(reportCause(Cause.fail(error)))),
      ),
    );
  }
  if (!Cause.hasInterruptsOnly(taskExit.cause)) {
    return Effect.succeed(describeBodyFailure(taskExit.cause));
  }
  return Effect.succeed(
    Option.match(body.causeReportedBeforeCleanup, {
      onNone: () => TaskFiberResult.cases.Interrupted.make({}),
      onSome: describeBodyFailure,
    }),
  );
};

const failureValueOf = (reason: Cause.Reason<unknown>): unknown => {
  if (Cause.isFailReason(reason)) {
    return reason.error;
  }
  return Cause.isDieReason(reason) ? reason.defect : reason;
};

export const describeCleanupFailure = (
  body: ReturnType<typeof makeTaskBodyRecord>,
  cleanupExit: Exit.Exit<unknown, unknown>,
  taskExit: Exit.Exit<unknown, unknown>,
) => {
  const isBodyOverBeforeCleanup =
    body.returnedBeforeCleanup || Option.isSome(body.causeReportedBeforeCleanup);
  if (!isBodyOverBeforeCleanup) {
    return null;
  }
  const reasonsReportedBeforeCleanup = Option.match(body.causeReportedBeforeCleanup, {
    onNone: () => [],
    onSome: (causeReportedBeforeCleanup) => causeReportedBeforeCleanup.reasons,
  });
  const reportedFailureValues = new Set(
    reasonsReportedBeforeCleanup.map((reason) => failureValueOf(reason)),
  );
  const reportedAwaitedTaskNames = new Set(
    reasonsReportedBeforeCleanup.flatMap((reason) => awaitedTaskNameOf(reason) ?? []),
  );
  const reasonsAddedByFinalizers = Exit.isFailure(taskExit)
    ? taskExit.cause.reasons.filter((reason) => {
        const awaitedTaskName = awaitedTaskNameOf(reason);
        const isReported =
          reportedFailureValues.has(failureValueOf(reason)) ||
          (awaitedTaskName !== null && reportedAwaitedTaskNames.has(awaitedTaskName));
        return !Cause.isInterruptReason(reason) && !isReported;
      })
    : [];
  const reasonsOfProcessCleanup = Exit.isFailure(cleanupExit) ? cleanupExit.cause.reasons : [];
  const cleanupFailureReasons = [...reasonsOfProcessCleanup, ...reasonsAddedByFinalizers];
  return cleanupFailureReasons.length === 0
    ? null
    : TaskFiberResult.cases.CleanupFailed.make(
        reportCause(Cause.fromReasons(cleanupFailureReasons)),
      );
};
