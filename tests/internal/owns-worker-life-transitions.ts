import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import {
  moveWorkerStage,
  WorkerEnding,
  WorkerStage,
  WorkerStageEvent,
} from "../../src/lib/main-thread/shared/define-worker-stage.ts";

const ONE_TASK = 1;

const TWO_TASKS = 2;

const CLEAN_EXIT_CODE = 0;

const CRASH_EXIT_CODE = 7;

const TERMINATING_TASK_NAME = "hanger";

const describeTerminatingTask = (terminatingTaskName: string | null) =>
  terminatingTaskName === null ? "for no task" : `for ${terminatingTaskName}`;

const describeEnding = WorkerEnding.match({
  Crashed: () => "Crashed",
  ExitedOnRequest: () => "ExitedOnRequest",
  Terminated: ({ terminatingTaskName }) =>
    `Terminated ${describeTerminatingTask(terminatingTaskName)}`,
});

const describeStage = WorkerStage.match({
  Busy: ({ unfinishedTaskCount }) => `Busy ${unfinishedTaskCount}`,
  Idle: () => "Idle",
  ExitRequested: () => "ExitRequested",
  Terminating: ({ terminatingTaskName }) =>
    `Terminating ${describeTerminatingTask(terminatingTaskName)}`,
  Exited: ({ ending }) => `Exited ${describeEnding(ending)}`,
  OutputDrained: () => "OutputDrained",
});

const describeEvent = WorkerStageEvent.match({
  AssignTask: () => "AssignTask",
  DrainOutput: () => "DrainOutput",
  EndTask: () => "EndTask",
  Exit: ({ exitCode, uncaughtError }) =>
    `Exit ${exitCode}${uncaughtError === null ? "" : ` on ${uncaughtError.message}`}`,
  RequestExit: () => "RequestExit",
  Terminate: ({ terminatingTaskName }) =>
    `Terminate ${describeTerminatingTask(terminatingTaskName)}`,
});

const stages = [
  WorkerStage.cases.Busy.make({ unfinishedTaskCount: TWO_TASKS }),
  WorkerStage.cases.Busy.make({ unfinishedTaskCount: ONE_TASK }),
  WorkerStage.cases.Idle.make({}),
  WorkerStage.cases.ExitRequested.make({}),
  WorkerStage.cases.Terminating.make({ terminatingTaskName: TERMINATING_TASK_NAME }),
  WorkerStage.cases.Terminating.make({ terminatingTaskName: null }),
  WorkerStage.cases.Exited.make({ ending: WorkerEnding.cases.Crashed.make({}) }),
  WorkerStage.cases.OutputDrained.make({}),
];

const events = [
  WorkerStageEvent.cases.AssignTask.make({}),
  WorkerStageEvent.cases.DrainOutput.make({}),
  WorkerStageEvent.cases.EndTask.make({}),
  WorkerStageEvent.cases.Exit.make({ exitCode: CLEAN_EXIT_CODE, uncaughtError: null }),
  WorkerStageEvent.cases.Exit.make({ exitCode: CRASH_EXIT_CODE, uncaughtError: null }),
  WorkerStageEvent.cases.Exit.make({
    exitCode: CLEAN_EXIT_CODE,
    uncaughtError: { message: "escaped", location: null },
  }),
  WorkerStageEvent.cases.RequestExit.make({}),
  WorkerStageEvent.cases.Terminate.make({ terminatingTaskName: TERMINATING_TASK_NAME }),
  WorkerStageEvent.cases.Terminate.make({ terminatingTaskName: null }),
];

const EXPECTED_MOVES = `
Busy 2 | AssignTask -> Busy 3 | none
Busy 2 | DrainOutput -> Busy 2 | none
Busy 2 | EndTask -> Busy 1 | none
Busy 2 | Exit 0 -> Exited Crashed | reportExit
Busy 2 | Exit 7 -> Exited Crashed | reportExit
Busy 2 | Exit 0 on escaped -> Exited Crashed | reportExit
Busy 2 | RequestExit -> ExitRequested | sendExit
Busy 2 | Terminate for hanger -> Terminating for hanger | terminate
Busy 2 | Terminate for no task -> Terminating for no task | terminate
Busy 1 | AssignTask -> Busy 2 | none
Busy 1 | DrainOutput -> Busy 1 | none
Busy 1 | EndTask -> Idle | none
Busy 1 | Exit 0 -> Exited Crashed | reportExit
Busy 1 | Exit 7 -> Exited Crashed | reportExit
Busy 1 | Exit 0 on escaped -> Exited Crashed | reportExit
Busy 1 | RequestExit -> ExitRequested | sendExit
Busy 1 | Terminate for hanger -> Terminating for hanger | terminate
Busy 1 | Terminate for no task -> Terminating for no task | terminate
Idle | AssignTask -> Busy 1 | none
Idle | DrainOutput -> Idle | none
Idle | EndTask -> Idle | none
Idle | Exit 0 -> Exited Crashed | reportExit
Idle | Exit 7 -> Exited Crashed | reportExit
Idle | Exit 0 on escaped -> Exited Crashed | reportExit
Idle | RequestExit -> ExitRequested | sendExit
Idle | Terminate for hanger -> Terminating for hanger | terminate
Idle | Terminate for no task -> Terminating for no task | terminate
ExitRequested | AssignTask -> ExitRequested | none
ExitRequested | DrainOutput -> ExitRequested | none
ExitRequested | EndTask -> ExitRequested | none
ExitRequested | Exit 0 -> Exited ExitedOnRequest | reportExit
ExitRequested | Exit 7 -> Exited Crashed | reportExit
ExitRequested | Exit 0 on escaped -> Exited Crashed | reportExit
ExitRequested | RequestExit -> ExitRequested | none
ExitRequested | Terminate for hanger -> Terminating for hanger | terminate
ExitRequested | Terminate for no task -> Terminating for no task | terminate
Terminating for hanger | AssignTask -> Terminating for hanger | none
Terminating for hanger | DrainOutput -> Terminating for hanger | none
Terminating for hanger | EndTask -> Terminating for hanger | none
Terminating for hanger | Exit 0 -> Exited Terminated for hanger | reportExit
Terminating for hanger | Exit 7 -> Exited Terminated for hanger | reportExit
Terminating for hanger | Exit 0 on escaped -> Exited Terminated for hanger | reportExit
Terminating for hanger | RequestExit -> Terminating for hanger | none
Terminating for hanger | Terminate for hanger -> Terminating for hanger | none
Terminating for hanger | Terminate for no task -> Terminating for hanger | none
Terminating for no task | AssignTask -> Terminating for no task | none
Terminating for no task | DrainOutput -> Terminating for no task | none
Terminating for no task | EndTask -> Terminating for no task | none
Terminating for no task | Exit 0 -> Exited Terminated for no task | reportExit
Terminating for no task | Exit 7 -> Exited Terminated for no task | reportExit
Terminating for no task | Exit 0 on escaped -> Exited Terminated for no task | reportExit
Terminating for no task | RequestExit -> Terminating for no task | none
Terminating for no task | Terminate for hanger -> Terminating for no task | none
Terminating for no task | Terminate for no task -> Terminating for no task | none
Exited Crashed | AssignTask -> Exited Crashed | none
Exited Crashed | DrainOutput -> OutputDrained | none
Exited Crashed | EndTask -> Exited Crashed | none
Exited Crashed | Exit 0 -> Exited Crashed | none
Exited Crashed | Exit 7 -> Exited Crashed | none
Exited Crashed | Exit 0 on escaped -> Exited Crashed | none
Exited Crashed | RequestExit -> Exited Crashed | none
Exited Crashed | Terminate for hanger -> Exited Crashed | none
Exited Crashed | Terminate for no task -> Exited Crashed | none
OutputDrained | AssignTask -> OutputDrained | none
OutputDrained | DrainOutput -> OutputDrained | none
OutputDrained | EndTask -> OutputDrained | none
OutputDrained | Exit 0 -> OutputDrained | none
OutputDrained | Exit 7 -> OutputDrained | none
OutputDrained | Exit 0 on escaped -> OutputDrained | none
OutputDrained | RequestExit -> OutputDrained | none
OutputDrained | Terminate for hanger -> OutputDrained | none
OutputDrained | Terminate for no task -> OutputDrained | none
`;

export const everyMove = Task.make({
  name: "a worker's stage moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = stages.flatMap((stage) =>
      events.map((event) => {
        const [next, action] = moveWorkerStage(stage, event);
        return `${describeStage(stage)} | ${describeEvent(event)} -> ${describeStage(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_MOVES);
  }),
});
