import { Schema } from "effect";

const OnWorker = { workerId: Schema.Int };

export const TaskRunStatus = Schema.TaggedUnion({
  Preparing: {},
  Assigned: OnWorker,
  Loading: OnWorker,
  Running: OnWorker,
  Serving: OnWorker,
  Failing: OnWorker,
  Finished: OnWorker,
  Closed: OnWorker,
});

export const TaskRunEvent = Schema.TaggedUnion({
  Assign: OnWorker,
  End: {},
  FailBeforeCleanup: {},
  Load: {},
  Serve: {},
  StartBody: {},
});

export const TaskRunOutcome = Schema.TaggedUnion({
  Pending: {},
  Served: {},
  Passed: {},
  Failed: { causedByAwaitedTasks: Schema.Boolean },
  Cancelled: {},
});

export const TaskRunOutcomeEvent = Schema.TaggedUnion({
  Cancel: {},
  Fail: {},
  FailOnExit: { causedByAwaitedTasks: Schema.Boolean },
  Pass: {},
  Serve: {},
});

export const isOnWorker = TaskRunStatus.isAnyOf([
  "Assigned",
  "Failing",
  "Loading",
  "Running",
  "Serving",
]);

export const isBeforeBody = TaskRunStatus.isAnyOf(["Assigned", "Loading", "Preparing"]);

const isBeforeBodyOnWorker = TaskRunStatus.isAnyOf(["Assigned", "Loading"]);

export const isInBody = TaskRunStatus.isAnyOf(["Failing", "Running", "Serving"]);

export const isEndedByTheRun = TaskRunOutcome.isAnyOf(["Cancelled", "Failed"]);

const ignore = (status: typeof TaskRunStatus.Type) => [status, "ignored"] as const;

const moveOnEnd = (status: typeof TaskRunStatus.Type) =>
  TaskRunStatus.match(status, {
    Preparing: () => ignore(status),
    Assigned: ({ workerId }) =>
      [TaskRunStatus.cases.Finished.make({ workerId }), "settleOutcome"] as const,
    Loading: ({ workerId }) =>
      [TaskRunStatus.cases.Finished.make({ workerId }), "settleOutcome"] as const,
    Running: ({ workerId }) =>
      [TaskRunStatus.cases.Finished.make({ workerId }), "settleOutcome"] as const,
    Serving: ({ workerId }) => [TaskRunStatus.cases.Closed.make({ workerId }), "close"] as const,
    Failing: ({ workerId }) =>
      [TaskRunStatus.cases.Finished.make({ workerId }), "reportFailure"] as const,
    Finished: () => ignore(status),
    Closed: () => ignore(status),
  });

export const moveTaskRunStatus = (
  status: typeof TaskRunStatus.Type,
  event: typeof TaskRunEvent.Type,
) =>
  TaskRunEvent.match(event, {
    Assign: ({ workerId }) =>
      TaskRunStatus.guards.Preparing(status)
        ? ([TaskRunStatus.cases.Assigned.make({ workerId }), "none"] as const)
        : ignore(status),
    End: () => moveOnEnd(status),
    FailBeforeCleanup: () => {
      if (TaskRunStatus.guards.Serving(status)) {
        return [status, "none"] as const;
      }
      return TaskRunStatus.guards.Running(status)
        ? ([
            TaskRunStatus.cases.Failing.make({ workerId: status.workerId }),
            "settleOutcome",
          ] as const)
        : ignore(status);
    },
    Load: () =>
      TaskRunStatus.guards.Assigned(status)
        ? ([TaskRunStatus.cases.Loading.make({ workerId: status.workerId }), "none"] as const)
        : ignore(status),
    Serve: () =>
      TaskRunStatus.guards.Running(status)
        ? ([
            TaskRunStatus.cases.Serving.make({ workerId: status.workerId }),
            "settleOutcome",
          ] as const)
        : ignore(status),
    StartBody: () =>
      isBeforeBodyOnWorker(status)
        ? ([
            TaskRunStatus.cases.Running.make({ workerId: status.workerId }),
            "recordBodyStart",
          ] as const)
        : ignore(status),
  });

const ignoreOutcome = (outcome: typeof TaskRunOutcome.Type) => [outcome, "ignored"] as const;

const settlePending = (outcome: typeof TaskRunOutcome.Type, next: typeof TaskRunOutcome.Type) =>
  TaskRunOutcome.guards.Pending(outcome) ? ([next, "none"] as const) : ignoreOutcome(outcome);

export const moveTaskRunOutcome = (
  outcome: typeof TaskRunOutcome.Type,
  event: typeof TaskRunOutcomeEvent.Type,
) =>
  TaskRunOutcomeEvent.match(event, {
    Cancel: () => settlePending(outcome, TaskRunOutcome.cases.Cancelled.make({})),
    Fail: () =>
      [TaskRunOutcome.cases.Failed.make({ causedByAwaitedTasks: false }), "none"] as const,
    FailOnExit: ({ causedByAwaitedTasks }) =>
      settlePending(outcome, TaskRunOutcome.cases.Failed.make({ causedByAwaitedTasks })),
    Pass: () => settlePending(outcome, TaskRunOutcome.cases.Passed.make({})),
    Serve: () => settlePending(outcome, TaskRunOutcome.cases.Served.make({})),
  });
