import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import {
  moveTaskRunOutcome,
  moveTaskRunStatus,
  TaskRunEvent,
  TaskRunOutcome,
  TaskRunOutcomeEvent,
  TaskRunStatus,
} from "../../src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-run-status.ts";

const WORKER_ID = 1;

const OTHER_WORKER_ID = 2;

const onWorker = { workerId: WORKER_ID };

const describeStatus = (status: typeof TaskRunStatus.Type) =>
  [
    status._tag,
    ...TaskRunStatus.match(status, {
      Preparing: () => [],
      Assigned: ({ workerId }) => [workerId],
      Loading: ({ workerId }) => [workerId],
      Running: ({ workerId }) => [workerId],
      Serving: ({ workerId }) => [workerId],
      Failing: ({ workerId }) => [workerId],
      Finished: ({ workerId }) => [workerId],
      Closed: ({ workerId }) => [workerId],
    }),
  ].join(" ");

const describeEvent = TaskRunEvent.match({
  Assign: ({ workerId }) => `Assign ${workerId}`,
  End: () => "End",
  FailBeforeCleanup: () => "FailBeforeCleanup",
  Load: () => "Load",
  Serve: () => "Serve",
  StartBody: () => "StartBody",
});

const statuses = [
  TaskRunStatus.cases.Preparing.make({}),
  TaskRunStatus.cases.Assigned.make(onWorker),
  TaskRunStatus.cases.Loading.make(onWorker),
  TaskRunStatus.cases.Running.make(onWorker),
  TaskRunStatus.cases.Serving.make(onWorker),
  TaskRunStatus.cases.Failing.make(onWorker),
  TaskRunStatus.cases.Finished.make(onWorker),
  TaskRunStatus.cases.Closed.make(onWorker),
];

const events = [
  TaskRunEvent.cases.Assign.make({ workerId: OTHER_WORKER_ID }),
  TaskRunEvent.cases.End.make({}),
  TaskRunEvent.cases.FailBeforeCleanup.make({}),
  TaskRunEvent.cases.Load.make({}),
  TaskRunEvent.cases.Serve.make({}),
  TaskRunEvent.cases.StartBody.make({}),
];

const EXPECTED_MOVES = `
Preparing | Assign 2 -> Assigned 2 | none
Preparing | End -> Preparing | ignored
Preparing | FailBeforeCleanup -> Preparing | ignored
Preparing | Load -> Preparing | ignored
Preparing | Serve -> Preparing | ignored
Preparing | StartBody -> Preparing | ignored
Assigned 1 | Assign 2 -> Assigned 1 | ignored
Assigned 1 | End -> Finished 1 | settleOutcome
Assigned 1 | FailBeforeCleanup -> Assigned 1 | ignored
Assigned 1 | Load -> Loading 1 | none
Assigned 1 | Serve -> Assigned 1 | ignored
Assigned 1 | StartBody -> Running 1 | recordBodyStart
Loading 1 | Assign 2 -> Loading 1 | ignored
Loading 1 | End -> Finished 1 | settleOutcome
Loading 1 | FailBeforeCleanup -> Loading 1 | ignored
Loading 1 | Load -> Loading 1 | ignored
Loading 1 | Serve -> Loading 1 | ignored
Loading 1 | StartBody -> Running 1 | recordBodyStart
Running 1 | Assign 2 -> Running 1 | ignored
Running 1 | End -> Finished 1 | settleOutcome
Running 1 | FailBeforeCleanup -> Failing 1 | settleOutcome
Running 1 | Load -> Running 1 | ignored
Running 1 | Serve -> Serving 1 | settleOutcome
Running 1 | StartBody -> Running 1 | ignored
Serving 1 | Assign 2 -> Serving 1 | ignored
Serving 1 | End -> Closed 1 | close
Serving 1 | FailBeforeCleanup -> Serving 1 | none
Serving 1 | Load -> Serving 1 | ignored
Serving 1 | Serve -> Serving 1 | ignored
Serving 1 | StartBody -> Serving 1 | ignored
Failing 1 | Assign 2 -> Failing 1 | ignored
Failing 1 | End -> Finished 1 | reportFailure
Failing 1 | FailBeforeCleanup -> Failing 1 | ignored
Failing 1 | Load -> Failing 1 | ignored
Failing 1 | Serve -> Failing 1 | ignored
Failing 1 | StartBody -> Failing 1 | ignored
Finished 1 | Assign 2 -> Finished 1 | ignored
Finished 1 | End -> Finished 1 | ignored
Finished 1 | FailBeforeCleanup -> Finished 1 | ignored
Finished 1 | Load -> Finished 1 | ignored
Finished 1 | Serve -> Finished 1 | ignored
Finished 1 | StartBody -> Finished 1 | ignored
Closed 1 | Assign 2 -> Closed 1 | ignored
Closed 1 | End -> Closed 1 | ignored
Closed 1 | FailBeforeCleanup -> Closed 1 | ignored
Closed 1 | Load -> Closed 1 | ignored
Closed 1 | Serve -> Closed 1 | ignored
Closed 1 | StartBody -> Closed 1 | ignored
`;

export const everyMove = Task.make({
  name: "the task run status moves as its transition table says for every status and event",
  run: Effect.sync(() => {
    const moves = statuses.flatMap((status) =>
      events.map((event) => {
        const [next, action] = moveTaskRunStatus(status, event);
        return `${describeStatus(status)} | ${describeEvent(event)} -> ${describeStatus(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_MOVES);
  }),
});

const describeOutcome = (outcome: typeof TaskRunOutcome.Type) => {
  const isFailedByAwaitedTasks =
    TaskRunOutcome.guards.Failed(outcome) && outcome.causedByAwaitedTasks;
  return isFailedByAwaitedTasks ? "Failed by awaited tasks" : outcome._tag;
};

const describeOutcomeEvent = TaskRunOutcomeEvent.match({
  Cancel: () => "Cancel",
  Fail: () => "Fail",
  FailOnExit: ({ causedByAwaitedTasks }) =>
    causedByAwaitedTasks ? "FailOnExit by awaited tasks" : "FailOnExit",
  Pass: () => "Pass",
  Serve: () => "Serve",
});

const outcomes = [
  TaskRunOutcome.cases.Pending.make({}),
  TaskRunOutcome.cases.Served.make({}),
  TaskRunOutcome.cases.Passed.make({}),
  TaskRunOutcome.cases.Failed.make({ causedByAwaitedTasks: false }),
  TaskRunOutcome.cases.Failed.make({ causedByAwaitedTasks: true }),
  TaskRunOutcome.cases.Cancelled.make({}),
];

const outcomeEvents = [
  TaskRunOutcomeEvent.cases.Cancel.make({}),
  TaskRunOutcomeEvent.cases.Fail.make({}),
  TaskRunOutcomeEvent.cases.FailOnExit.make({ causedByAwaitedTasks: true }),
  TaskRunOutcomeEvent.cases.Pass.make({}),
  TaskRunOutcomeEvent.cases.Serve.make({}),
];

const EXPECTED_OUTCOME_MOVES = `
Pending | Cancel -> Cancelled | none
Pending | Fail -> Failed | none
Pending | FailOnExit by awaited tasks -> Failed by awaited tasks | none
Pending | Pass -> Passed | none
Pending | Serve -> Served | none
Served | Cancel -> Served | ignored
Served | Fail -> Failed | none
Served | FailOnExit by awaited tasks -> Served | ignored
Served | Pass -> Served | ignored
Served | Serve -> Served | ignored
Passed | Cancel -> Passed | ignored
Passed | Fail -> Failed | none
Passed | FailOnExit by awaited tasks -> Passed | ignored
Passed | Pass -> Passed | ignored
Passed | Serve -> Passed | ignored
Failed | Cancel -> Failed | ignored
Failed | Fail -> Failed | none
Failed | FailOnExit by awaited tasks -> Failed | ignored
Failed | Pass -> Failed | ignored
Failed | Serve -> Failed | ignored
Failed by awaited tasks | Cancel -> Failed by awaited tasks | ignored
Failed by awaited tasks | Fail -> Failed | none
Failed by awaited tasks | FailOnExit by awaited tasks -> Failed by awaited tasks | ignored
Failed by awaited tasks | Pass -> Failed by awaited tasks | ignored
Failed by awaited tasks | Serve -> Failed by awaited tasks | ignored
Cancelled | Cancel -> Cancelled | ignored
Cancelled | Fail -> Failed | none
Cancelled | FailOnExit by awaited tasks -> Cancelled | ignored
Cancelled | Pass -> Cancelled | ignored
Cancelled | Serve -> Cancelled | ignored
`;

export const everyOutcomeMove = Task.make({
  name: "the task run outcome moves as its transition table says for every outcome and event",
  run: Effect.sync(() => {
    const moves = outcomes.flatMap((outcome) =>
      outcomeEvents.map((event) => {
        const [next, action] = moveTaskRunOutcome(outcome, event);
        return `${describeOutcome(outcome)} | ${describeOutcomeEvent(event)} -> ${describeOutcome(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_OUTCOME_MOVES);
  }),
});
