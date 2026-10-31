import { Cause, Effect, Exit, Option, Schema } from "effect";

import { reportCause } from "../../../shared/report-cause.ts";
import { TaskFiberResult } from "../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../shared/rpc.ts";
import { failuresOfCause } from "../../../shared/select-cause-reasons.ts";
import { isAwaitedTasksFailure } from "../../../shared/task-errors.ts";
import { CommandFailed } from "../../errors/index.ts";
import { decodeTaskResultJson } from "../shared/decode-task-result-json.ts";
import type { makeTaskBodyRecord } from "../shared/define-task-body-record.ts";

const isCommandFailed = Schema.is(CommandFailed);

export const describeBodyFailure = (cause: Cause.Cause<unknown>) => {
  const failureCause = failuresOfCause(cause);
  const hasOnlyOneFailure = failureCause.reasons.length === 1;
  const firstFailure = Cause.findErrorOption(failureCause);
  const awaitedTasksFailure = Option.filter(firstFailure, isAwaitedTasksFailure);
  const hasOnlyAwaitedFailure = hasOnlyOneFailure && Option.isSome(awaitedTasksFailure);
  if (hasOnlyAwaitedFailure) {
    return TaskFiberResult.cases.AwaitedTasksFailed.make({ failure: awaitedTasksFailure.value });
  }
  const failedCommand = Option.filter(firstFailure, isCommandFailed);
  const hasOnlyCommandFailure = hasOnlyOneFailure && Option.isSome(failedCommand);
  return TaskFiberResult.cases.Failed.make({
    ...reportCause(cause),
    ...(hasOnlyCommandFailure ? { failedCommandExitCode: failedCommand.value.code } : {}),
  });
};

export const describeTaskBodyResult = (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  taskExit: Exit.Exit<unknown, unknown>,
  elapsedMilliseconds: number,
) => {
  if (Exit.isSuccess(taskExit)) {
    return decodeTaskResultJson(startMessage.taskName, "returned", taskExit.value).pipe(
      Effect.map((returnedValue) =>
        TaskFiberResult.cases.Returned.make({
          ...(returnedValue === undefined ? {} : { value: returnedValue }),
          elapsedMilliseconds,
        }),
      ),
      Effect.catchTag("TaskResultNotJson", (error) =>
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
  if (Option.isNone(body.causeReportedBeforeCleanup)) {
    return null;
  }
  const causeReportedBeforeCleanup = body.causeReportedBeforeCleanup.value;
  const reportedFailureValues = new Set(causeReportedBeforeCleanup.reasons.map(failureValueOf));
  const reasonsAddedByFinalizers = Exit.isFailure(taskExit)
    ? taskExit.cause.reasons.filter(
        (reason) =>
          !Cause.isInterruptReason(reason) && !reportedFailureValues.has(failureValueOf(reason)),
      )
    : [];
  const reasonsOfProcessCleanup = Exit.isFailure(cleanupExit) ? cleanupExit.cause.reasons : [];
  const cleanupFailureReasons = [...reasonsOfProcessCleanup, ...reasonsAddedByFinalizers];
  return cleanupFailureReasons.length === 0
    ? null
    : TaskFiberResult.cases.CleanupFailed.make(
        reportCause(Cause.fromReasons(cleanupFailureReasons)),
      );
};
