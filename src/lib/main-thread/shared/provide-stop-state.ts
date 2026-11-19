import { Cause, Context, Deferred, Effect, Exit, Layer, Ref, Schema, Semaphore } from "effect";

import {
  FAILURE_REASON_NOT_RECORDED_LINE,
  formatErrorLines,
  formatFailureLines,
} from "../../../shared/format-error-lines.ts";
import { holdTaskLogsDuring, makeTaskLogHold } from "../../../shared/hold-task-logs.ts";
import { printLine } from "../../../shared/print-to-standard-error.ts";
import { reportCause } from "../../../shared/report-cause.ts";
import {
  failureValuesOfCause,
  failureValuesOfReason,
} from "../../../shared/select-cause-reasons.ts";
import type { STOP_REASONS } from "../../../shared/stop-signals.ts";
import { isAwaitedTasksFailurePrintable } from "../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { AwaitedTaskFailed, SettledTasksFailed } from "../../errors/index.ts";
import type { PrintableError } from "../../errors/index.ts";
import {
  findFirstFailure,
  moveRunStopStage,
  RunStopEvent,
  RunStopStage,
} from "./define-run-stop-stage.ts";
import type { RunFailure } from "./define-run-stop-stage.ts";
import { findFailureReason, locatePrintable, taskNameOf } from "./find-failure-reason.ts";
import {
  FailureReasonNotFormatted,
  StopRequested,
  UnawaitedTasksFailed,
} from "./main-thread-errors.ts";

export const makeFailureEntry = <FailureValue>(
  failureValue: FailureValue,
  printables: readonly PrintableError[],
) => ({ failureValue, printables });

const isPropagatedFailure = Schema.is(
  Schema.Union([AwaitedTaskFailed, SettledTasksFailed, UnawaitedTasksFailed, StopRequested]),
);

const makeShutdown = Effect.gen(function* () {
  const stage = yield* Ref.make<typeof RunStopStage.Type>(RunStopStage.cases.Running.make({}));
  const failed = yield* Deferred.make<typeof RunFailure.Type>();
  const forced = yield* Deferred.make<void>();
  const stopSignalReceived = yield* Deferred.make<void>();
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
      if (RunStopStage.guards.StoppingOnSignal(next)) {
        yield* Deferred.done(stopSignalReceived, Exit.void);
      }
      if (RunStopStage.guards.Forced(next)) {
        yield* Deferred.done(forced, Exit.void);
      }
      return action;
    }).pipe(Effect.uninterruptible);
  const awaitFailure = Deferred.await(failed);
  const failureCauseByTaskName = new Map<string, Cause.Cause<typeof RunFailure.Type>>();
  const reportedFailureValues = new Set<unknown>();
  const taskLogHold = makeTaskLogHold();
  const oneStopAnnouncementAtATime = yield* Semaphore.make(1);
  const taskLocationsByName = yield* Ref.make<ReadonlyMap<string, { readonly location: string }>>(
    new Map(),
  );
  const locatePrintables = (taskName: string | null, printables: readonly PrintableError[]) =>
    Effect.map(Ref.get(taskLocationsByName), (discoveredTasksByName) =>
      printables.map((printable) => locatePrintable(printable, taskName, discoveredTasksByName)),
    );
  const printablesOfUnreportedReasons = (runEndingCause: Cause.Cause<unknown>) =>
    runEndingCause.reasons
      .filter((reason) => {
        const failureValues = failureValuesOfReason(reason);
        return (
          !Cause.isInterruptReason(reason) &&
          !failureValues.some(
            (failureValue) =>
              reportedFailureValues.has(failureValue) || isPropagatedFailure(failureValue),
          )
        );
      })
      .flatMap((reason) =>
        reportCause(Cause.fromReasons([reason]))
          .failures.filter((printable) => !isAwaitedTasksFailurePrintable(printable))
          .map((printable) => ({ taskName: taskNameOf(reason), printable })),
      );
  const findUnreportedFailures = (runEndingCause: Cause.Cause<unknown>) => ({
    unreported: printablesOfUnreportedReasons(runEndingCause),
    reasonNotFound: findFailureReason(runEndingCause, failureCauseByTaskName) === null,
  });
  return {
    taskLogHold,
    holdTaskLogsDuring: holdTaskLogsDuring(taskLogHold),
    holdStopAnnouncementsDuring: oneStopAnnouncementAtATime.withPermits(1),
    recordTaskFailure: (
      taskName: string,
      cause: Cause.Cause<typeof RunFailure.Type>,
      entriesOfItsOwn: readonly ReturnType<typeof makeFailureEntry<unknown>>[],
    ) =>
      Effect.sync(() => {
        if (!failureCauseByTaskName.has(taskName)) {
          failureCauseByTaskName.set(taskName, cause);
        }
        const unreportedEntries = entriesOfItsOwn.filter(
          (entry) => !reportedFailureValues.has(entry.failureValue),
        );
        for (const failureValue of [
          ...failureValuesOfCause(cause),
          ...entriesOfItsOwn.map((entry) => entry.failureValue),
        ]) {
          reportedFailureValues.add(failureValue);
        }
        return unreportedEntries.flatMap((entry) => entry.printables);
      }),
    recordRunFailure: (failure: typeof RunFailure.Type) =>
      Effect.gen(function* () {
        if (reportedFailureValues.has(failure)) {
          return;
        }
        reportedFailureValues.add(failure);
        yield* printLine(
          formatErrorLines(
            yield* locatePrintables(null, reportCause(Cause.fail(failure)).failures),
            null,
          ),
        );
      }),
    locatePrintables,
    readTaskLocationsFrom: (
      discoveredTasksByName: ReadonlyMap<string, { readonly location: string }>,
    ) => Ref.set(taskLocationsByName, discoveredTasksByName),
    formatUnreportedFailures: (runEndingCause: Cause.Cause<unknown> | null) =>
      runEndingCause === null
        ? Effect.succeed([])
        : Effect.try({
            try: () => findUnreportedFailures(runEndingCause),
            catch: (cause) => new FailureReasonNotFormatted({ cause }),
          }).pipe(
            Effect.flatMap(({ unreported, reasonNotFound }) =>
              Effect.map(
                Effect.forEach(unreported, ({ taskName, printable }) =>
                  Effect.map(locatePrintables(taskName, [printable]), (printables) =>
                    formatErrorLines(printables, taskName),
                  ),
                ),
                (located) => [
                  ...located,
                  ...(reasonNotFound ? [FAILURE_REASON_NOT_RECORDED_LINE] : []),
                ],
              ),
            ),
            Effect.catchTag("FailureReasonNotFormatted", (failure) =>
              Effect.succeed([formatFailureLines(failure)]),
            ),
          ),
    recordFirstFailure: (failure: typeof RunFailure.Type) =>
      move(RunStopEvent.cases.Fail.make({ failure })).pipe(
        Effect.map((action) => action === "reportFailure"),
      ),
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
    awaitStopSignal: Deferred.await(stopSignalReceived),
    hasStopSignal: Deferred.isDone(stopSignalReceived),
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
