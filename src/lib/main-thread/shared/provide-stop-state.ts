import { Context, Deferred, Effect, Exit, Layer, Ref } from "effect";
import type { Cause } from "effect";

import {
  FAILURE_REASON_NOT_RECORDED_LINE,
  formatFailureReasonLine,
} from "../../../shared/format-error-lines.ts";
import { holdTaskLogsDuring, makeTaskLogHold } from "../../../shared/hold-task-logs.ts";
import type { STOP_REASONS } from "../../../shared/stop-signals.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import {
  findFirstFailure,
  moveRunStopStage,
  RunStopEvent,
  RunStopStage,
} from "./define-run-stop-stage.ts";
import type { RunFailure } from "./define-run-stop-stage.ts";
import { findFailureReason } from "./find-failure-reason.ts";
import { FailureReasonNotFormatted } from "./main-thread-errors.ts";

const makeShutdown = Effect.gen(function* () {
  const stage = yield* Ref.make<typeof RunStopStage.Type>(RunStopStage.cases.Running.make({}));
  const failed = yield* Deferred.make<typeof RunFailure.Type>();
  const forced = yield* Deferred.make<void>();
  const move = (event: typeof RunStopEvent.Type) =>
    Effect.gen(function* () {
      const [next, action] = yield* Ref.modify(stage, (current) => {
        const moved = moveRunStopStage(current, event);
        return [moved, moved[0]];
      });
      const firstFailure = findFirstFailure(next);
      if (firstFailure !== null) {
        yield* Deferred.succeed(failed, firstFailure);
      }
      if (RunStopStage.guards.Forced(next)) {
        yield* Deferred.done(forced, Exit.void);
      }
      return action;
    }).pipe(Effect.uninterruptible);
  const awaitFailure = Deferred.await(failed);
  const printedFailures = new Set<typeof RunFailure.Type>();
  const failureCauseByTaskName = new Map<string, Cause.Cause<typeof RunFailure.Type>>();
  const taskLogHold = makeTaskLogHold();
  return {
    taskLogHold,
    holdTaskLogsDuring: holdTaskLogsDuring(taskLogHold),
    recordTaskFailureCause: (taskName: string, cause: Cause.Cause<typeof RunFailure.Type>) =>
      Effect.sync(() => {
        if (!failureCauseByTaskName.has(taskName)) {
          failureCauseByTaskName.set(taskName, cause);
        }
      }),
    formatRunFailureReasonLine: (runEndingCause: Cause.Cause<unknown>) =>
      Effect.try({
        try: () => {
          const reason = findFailureReason(runEndingCause, failureCauseByTaskName);
          return reason === null
            ? FAILURE_REASON_NOT_RECORDED_LINE
            : formatFailureReasonLine(reason);
        },
        catch: (cause) => new FailureReasonNotFormatted({ cause }),
      }).pipe(
        Effect.catchTag("FailureReasonNotFormatted", (failure) =>
          Effect.succeed(`${failure._tag}: ${failure.message}`),
        ),
      ),
    recordFirstFailure: (failure: typeof RunFailure.Type) =>
      move(RunStopEvent.cases.Fail.make({ failure })).pipe(
        Effect.map((action) => action === "reportFailure"),
      ),
    recordPrintedFailure: (failure: typeof RunFailure.Type) =>
      Effect.sync(() => {
        printedFailures.add(failure);
      }),
    wasPrinted: (failure: typeof RunFailure.Type) =>
      Effect.sync(() => printedFailures.has(failure)),
    requestStop: (signal: (typeof STOP_REASONS)[number]) =>
      move(RunStopEvent.cases.Signal.make({ signal })).pipe(
        Effect.map((action) => action === "announceStop"),
      ),
    forceStop: (failure: typeof RunFailure.Type) =>
      Effect.asVoid(move(RunStopEvent.cases.Force.make({ failure }))),
    begin: Effect.asVoid(move(RunStopEvent.cases.BeginEnding.make({}))),
    isStopping: Effect.map(Ref.get(stage), (current) => !RunStopStage.guards.Running(current)),
    hasFailed: Effect.map(Ref.get(stage), (current) => findFirstFailure(current) !== null),
    awaitFailure,
    awaitForcedStop: Deferred.await(forced),
    isForced: Effect.map(Ref.get(stage), RunStopStage.guards.Forced),
    outcome: Effect.map(Ref.get(stage), (current) =>
      RunStopStage.guards.Forced(current) ? current.outcome : findFirstFailure(current),
    ),
  };
});

export class StopState extends Context.Service<StopState, Effect.Success<typeof makeShutdown>>()(
  `${TASK_PACKAGE.name}/StopState`,
) {}

export const StopStateLive = Layer.effect(StopState, makeShutdown);
