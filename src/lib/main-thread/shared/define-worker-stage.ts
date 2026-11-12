import { Schema } from "effect";

import { UncaughtWorkerError } from "../../../shared/process-errors.ts";

const TerminatingTask = { terminatingTaskName: Schema.NullOr(Schema.String) };

export const WorkerEnding = Schema.TaggedUnion({
  Crashed: {},
  ExitedOnRequest: {},
  Terminated: TerminatingTask,
});

export const WorkerStage = Schema.TaggedUnion({
  Busy: { unfinishedTaskCount: Schema.Int },
  Idle: {},
  ExitRequested: {},
  Terminating: TerminatingTask,
  Exited: { ending: WorkerEnding },
  OutputDrained: {},
});

export const WorkerStageEvent = Schema.TaggedUnion({
  AssignTask: {},
  DrainOutput: {},
  EndTask: {},
  Exit: { exitCode: Schema.Int, uncaughtError: Schema.NullOr(UncaughtWorkerError) },
  RequestExit: {},
  Terminate: TerminatingTask,
});

export const isUsable = WorkerStage.isAnyOf(["Busy", "Idle"]);

const isTerminable = WorkerStage.isAnyOf(["Busy", "ExitRequested", "Idle"]);

const stay = (stage: typeof WorkerStage.Type) => [stage, "none"] as const;

const exitAs = (ending: typeof WorkerEnding.Type) =>
  [WorkerStage.cases.Exited.make({ ending }), "reportExit"] as const;

const moveOnExit = (
  stage: typeof WorkerStage.Type,
  event: typeof WorkerStageEvent.cases.Exit.Type,
) => {
  const isClean = event.exitCode === 0 && event.uncaughtError === null;
  return WorkerStage.match(stage, {
    Busy: () => exitAs(WorkerEnding.cases.Crashed.make({})),
    Idle: () => exitAs(WorkerEnding.cases.Crashed.make({})),
    ExitRequested: () =>
      exitAs(
        isClean ? WorkerEnding.cases.ExitedOnRequest.make({}) : WorkerEnding.cases.Crashed.make({}),
      ),
    Terminating: ({ terminatingTaskName }) =>
      exitAs(WorkerEnding.cases.Terminated.make({ terminatingTaskName })),
    Exited: () => stay(stage),
    OutputDrained: () => stay(stage),
  });
};

const moveOnEndTask = (stage: typeof WorkerStage.Type) => {
  if (!WorkerStage.guards.Busy(stage)) {
    return stay(stage);
  }
  return stage.unfinishedTaskCount > 1
    ? ([
        WorkerStage.cases.Busy.make({ unfinishedTaskCount: stage.unfinishedTaskCount - 1 }),
        "none",
      ] as const)
    : ([WorkerStage.cases.Idle.make({}), "none"] as const);
};

export const moveWorkerStage = (
  stage: typeof WorkerStage.Type,
  event: typeof WorkerStageEvent.Type,
) =>
  WorkerStageEvent.match(event, {
    AssignTask: () => {
      if (WorkerStage.guards.Idle(stage)) {
        return [WorkerStage.cases.Busy.make({ unfinishedTaskCount: 1 }), "none"] as const;
      }
      return WorkerStage.guards.Busy(stage)
        ? ([
            WorkerStage.cases.Busy.make({ unfinishedTaskCount: stage.unfinishedTaskCount + 1 }),
            "none",
          ] as const)
        : stay(stage);
    },
    DrainOutput: () =>
      WorkerStage.guards.Exited(stage)
        ? ([WorkerStage.cases.OutputDrained.make({}), "none"] as const)
        : stay(stage),
    EndTask: () => moveOnEndTask(stage),
    Exit: (exit) => moveOnExit(stage, exit),
    RequestExit: () =>
      isUsable(stage)
        ? ([WorkerStage.cases.ExitRequested.make({}), "sendExit"] as const)
        : stay(stage),
    Terminate: ({ terminatingTaskName }) =>
      isTerminable(stage)
        ? ([WorkerStage.cases.Terminating.make({ terminatingTaskName }), "terminate"] as const)
        : stay(stage),
  });
