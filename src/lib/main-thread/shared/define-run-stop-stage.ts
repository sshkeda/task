import { Schema } from "effect";

import { StandardErrorWriteFailed } from "../../../shared/print-to-standard-error.ts";
import {
  ProcCommandLineUnreadable,
  ProcDirectoryUnlistable,
  ProcessCommandLineNotRead,
  ProcessNotSignaled,
  ProcessTableNotRead,
  ProcRunMarkerUnreadable,
  ProcStatMalformed,
  ProcStatUnreadable,
  SystemLibraryNotLoaded,
  WorkerExited,
} from "../../../shared/process-errors.ts";
import { STOP_REASONS } from "../../../shared/stop-signals.ts";
import {
  ServedTaskFailed,
  TaskBodyFailed,
  TaskCleanupFailed,
  TaskSetupTimedOut,
  TaskTimedOut,
} from "../../../shared/task-errors.ts";
import {
  AwaitedTaskFailed,
  SettledTasksFailed,
  TaskNotMadeWhileLoading,
  TaskWaitsOnItself,
} from "../../errors/index.ts";
import {
  BrokenTasksFound,
  ServingTaskRequestsItself,
  StopRequested,
  TaskCleanupCutOff,
  TaskCleanupFrozeWorker,
  TaskCleanupNotStarted,
  TaskCleanupTimedOut,
  WorkerTerminatedForAnotherTask,
} from "./main-thread-errors.ts";

export const RunFailure = Schema.Union([
  AwaitedTaskFailed,
  BrokenTasksFound,
  ProcCommandLineUnreadable,
  ProcDirectoryUnlistable,
  ProcessCommandLineNotRead,
  ProcessNotSignaled,
  ProcessTableNotRead,
  ProcRunMarkerUnreadable,
  ProcStatMalformed,
  ProcStatUnreadable,
  ServedTaskFailed,
  ServingTaskRequestsItself,
  SettledTasksFailed,
  StandardErrorWriteFailed,
  StopRequested,
  SystemLibraryNotLoaded,
  TaskBodyFailed,
  TaskCleanupCutOff,
  TaskCleanupFailed,
  TaskCleanupFrozeWorker,
  TaskCleanupNotStarted,
  TaskCleanupTimedOut,
  TaskNotMadeWhileLoading,
  TaskSetupTimedOut,
  TaskTimedOut,
  TaskWaitsOnItself,
  WorkerExited,
  WorkerTerminatedForAnotherTask,
]);

export const RunStopStage = Schema.TaggedUnion({
  Running: {},
  Failing: { firstFailure: RunFailure },
  StoppingOnSignal: { firstFailure: RunFailure },
  EndingNormally: {},
  Forced: { firstFailure: RunFailure, outcome: RunFailure },
});

export const RunStopEvent = Schema.TaggedUnion({
  BeginEnding: {},
  Fail: { failure: RunFailure },
  Force: { failure: RunFailure },
  Signal: { signal: Schema.Literals(STOP_REASONS) },
});

const isStopRequested = Schema.is(StopRequested);

const hasFirstFailure = RunStopStage.isAnyOf(["Failing", "Forced", "StoppingOnSignal"]);

export const findFirstFailure = (stage: typeof RunStopStage.Type) =>
  hasFirstFailure(stage) ? stage.firstFailure : null;

const stay = (stage: typeof RunStopStage.Type) => [stage, "none"] as const;

const force = (firstFailure: typeof RunFailure.Type, failure: typeof RunFailure.Type) => {
  const isSignalAfterAnotherFailure = isStopRequested(failure) && !isStopRequested(firstFailure);
  return [
    RunStopStage.cases.Forced.make({
      firstFailure,
      outcome: isSignalAfterAnotherFailure ? firstFailure : failure,
    }),
    "none",
  ] as const;
};

const stopOnSignal = (firstFailure: typeof RunFailure.Type) =>
  [RunStopStage.cases.StoppingOnSignal.make({ firstFailure }), "announceStop"] as const;

const moveBeforeAnyFailure = (event: typeof RunStopEvent.Type) =>
  RunStopEvent.match(event, {
    BeginEnding: () => [RunStopStage.cases.EndingNormally.make({}), "none"] as const,
    Fail: ({ failure }) =>
      [RunStopStage.cases.Failing.make({ firstFailure: failure }), "reportFailure"] as const,
    Force: ({ failure }) => force(failure, failure),
    Signal: ({ signal }) => stopOnSignal(new StopRequested({ signal })),
  });

export const moveRunStopStage = (
  stage: typeof RunStopStage.Type,
  event: typeof RunStopEvent.Type,
) =>
  RunStopStage.match(stage, {
    Running: () => moveBeforeAnyFailure(event),
    EndingNormally: () =>
      RunStopEvent.guards.BeginEnding(event) ? stay(stage) : moveBeforeAnyFailure(event),
    Failing: ({ firstFailure }) =>
      RunStopEvent.match(event, {
        BeginEnding: () => stay(stage),
        Fail: () => stay(stage),
        Force: ({ failure }) => force(firstFailure, failure),
        Signal: () => stopOnSignal(firstFailure),
      }),
    StoppingOnSignal: ({ firstFailure }) =>
      RunStopEvent.match(event, {
        BeginEnding: () => stay(stage),
        Fail: () => stay(stage),
        Force: ({ failure }) => force(firstFailure, failure),
        Signal: ({ signal }) =>
          signal === "SIGHUP" ? stay(stage) : force(firstFailure, new StopRequested({ signal })),
      }),
    Forced: () => stay(stage),
  });
