import { Schema } from "effect";

const StopRequestedAt = { stopRequestedAtMilliseconds: Schema.Int };

export const TaskStopStage = Schema.TaggedUnion({
  NotAsked: {},
  FailedBeforeStop: {},
  StopSent: StopRequestedAt,
  StoppedBeforeBody: StopRequestedAt,
  FailedBeforeCleanup: StopRequestedAt,
  CleaningUp: StopRequestedAt,
  CleanupNotStarted: {},
  CleanupTimedOut: { watchesWorker: Schema.Boolean },
});

export const TaskStopEvent = Schema.TaggedUnion({
  EndRun: {},
  FailBeforeCleanup: {},
  PassLimit: {},
  RequestStop: StopRequestedAt,
  StartCleanup: {},
  StopBeforeBody: {},
});

export const hasCleanupStarted = TaskStopStage.isAnyOf([
  "CleaningUp",
  "CleanupTimedOut",
  "FailedBeforeCleanup",
  "FailedBeforeStop",
]);

export const hasCleanupLimitPassed = TaskStopStage.isAnyOf([
  "CleanupNotStarted",
  "CleanupTimedOut",
]);

const isStopWithinLimit = TaskStopStage.isAnyOf([
  "CleaningUp",
  "FailedBeforeCleanup",
  "StopSent",
  "StoppedBeforeBody",
]);

const isAwaitingAnswer = TaskStopStage.isAnyOf(["FailedBeforeCleanup", "StopSent"]);

export const findStopRequestedAt = (stage: typeof TaskStopStage.Type) =>
  isStopWithinLimit(stage) ? stage.stopRequestedAtMilliseconds : null;

const stay = (stage: typeof TaskStopStage.Type) => [stage, "none"] as const;

const moveOnPassLimit = (stage: typeof TaskStopStage.Type) =>
  TaskStopStage.match(stage, {
    NotAsked: () => stay(stage),
    FailedBeforeStop: () => stay(stage),
    StopSent: () => [TaskStopStage.cases.CleanupNotStarted.make({}), "failNotAnswered"] as const,
    StoppedBeforeBody: () =>
      [TaskStopStage.cases.CleanupNotStarted.make({}), "failStillLoading"] as const,
    FailedBeforeCleanup: () =>
      [
        TaskStopStage.cases.CleanupTimedOut.make({ watchesWorker: false }),
        "failTimedOutNotAnswered",
      ] as const,
    CleaningUp: () =>
      [
        TaskStopStage.cases.CleanupTimedOut.make({ watchesWorker: true }),
        "failTimedOutAndWatch",
      ] as const,
    CleanupNotStarted: () => stay(stage),
    CleanupTimedOut: ({ watchesWorker }) =>
      watchesWorker ? ([stage, "watch"] as const) : stay(stage),
  });

export const moveTaskStopStage = (
  stage: typeof TaskStopStage.Type,
  event: typeof TaskStopEvent.Type,
) =>
  TaskStopEvent.match(event, {
    EndRun: () =>
      hasCleanupStarted(stage) ? ([stage, "interruptRequests"] as const) : stay(stage),
    FailBeforeCleanup: () => {
      if (TaskStopStage.guards.NotAsked(stage)) {
        return [TaskStopStage.cases.FailedBeforeStop.make({}), "none"] as const;
      }
      return TaskStopStage.guards.StopSent(stage)
        ? ([
            TaskStopStage.cases.FailedBeforeCleanup.make({
              stopRequestedAtMilliseconds: stage.stopRequestedAtMilliseconds,
            }),
            "none",
          ] as const)
        : stay(stage);
    },
    PassLimit: () => moveOnPassLimit(stage),
    RequestStop: ({ stopRequestedAtMilliseconds }) => {
      if (TaskStopStage.guards.NotAsked(stage)) {
        return [
          TaskStopStage.cases.StopSent.make({ stopRequestedAtMilliseconds }),
          "sendStop",
        ] as const;
      }
      return TaskStopStage.guards.FailedBeforeStop(stage)
        ? ([
            TaskStopStage.cases.FailedBeforeCleanup.make({ stopRequestedAtMilliseconds }),
            "sendStop",
          ] as const)
        : stay(stage);
    },
    StartCleanup: () =>
      isAwaitingAnswer(stage)
        ? ([
            TaskStopStage.cases.CleaningUp.make({
              stopRequestedAtMilliseconds: stage.stopRequestedAtMilliseconds,
            }),
            "none",
          ] as const)
        : stay(stage),
    StopBeforeBody: () =>
      TaskStopStage.guards.StopSent(stage)
        ? ([
            TaskStopStage.cases.StoppedBeforeBody.make({
              stopRequestedAtMilliseconds: stage.stopRequestedAtMilliseconds,
            }),
            "none",
          ] as const)
        : stay(stage),
  });
