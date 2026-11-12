import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import {
  moveStandardInputStage,
  StandardInputAction,
  StandardInputEvent,
  StandardInputStage,
} from "../../src/lib/main-thread/lib/run-tasks-on-workers/shared/define-standard-input-stage.ts";
import { StandardInputRead } from "../../src/shared/rpc.ts";

const ROOT_TASK_NAME = "hook";

const OTHER_TASK_NAME = "reader";

const FAILURE_REASON = "broken pipe";

const CHUNK_BYTES = 3;

const describeRead = StandardInputRead.match({
  Chunks: ({ chunks }) => `Chunks ${chunks.length}`,
  Ended: () => "Ended",
  Failed: ({ reason }) => `Failed ${reason}`,
  Refused: ({ rootTaskName, taskName }) => `Refused ${taskName} for ${rootTaskName}`,
});

const describeStage = StandardInputStage.match({
  NotOpened: () => "NotOpened",
  Reading: () => "Reading",
  Idle: () => "Idle",
  Ended: () => "Ended",
  Failed: ({ reason }) => `Failed ${reason}`,
});

const describeEvent = StandardInputEvent.match({
  Read: ({ taskName }) => `Read by ${taskName}`,
  ReceiveChunks: ({ chunks }) => `ReceiveChunks ${chunks.length}`,
  ReceiveEnd: () => "ReceiveEnd",
  ReceiveFailure: ({ reason }) => `ReceiveFailure ${reason}`,
});

const describeAction = StandardInputAction.match({
  Answer: ({ read }) => `Answer ${describeRead(read)}`,
  DieOutOfOrder: () => "DieOutOfOrder",
  Pull: () => "Pull",
});

const stages = [
  StandardInputStage.cases.NotOpened.make({}),
  StandardInputStage.cases.Reading.make({}),
  StandardInputStage.cases.Idle.make({}),
  StandardInputStage.cases.Ended.make({}),
  StandardInputStage.cases.Failed.make({ reason: FAILURE_REASON }),
];

const events = [
  StandardInputEvent.cases.Read.make({ rootTaskName: ROOT_TASK_NAME, taskName: ROOT_TASK_NAME }),
  StandardInputEvent.cases.Read.make({ rootTaskName: ROOT_TASK_NAME, taskName: OTHER_TASK_NAME }),
  StandardInputEvent.cases.ReceiveChunks.make({ chunks: [new Uint8Array(CHUNK_BYTES)] }),
  StandardInputEvent.cases.ReceiveEnd.make({}),
  StandardInputEvent.cases.ReceiveFailure.make({ reason: FAILURE_REASON }),
];

const EXPECTED_MOVES = `
NotOpened | Read by hook -> Reading | Pull
NotOpened | Read by reader -> NotOpened | Answer Refused reader for hook
NotOpened | ReceiveChunks 1 -> NotOpened | DieOutOfOrder
NotOpened | ReceiveEnd -> NotOpened | DieOutOfOrder
NotOpened | ReceiveFailure broken pipe -> NotOpened | DieOutOfOrder
Reading | Read by hook -> Reading | DieOutOfOrder
Reading | Read by reader -> Reading | Answer Refused reader for hook
Reading | ReceiveChunks 1 -> Idle | Answer Chunks 1
Reading | ReceiveEnd -> Ended | Answer Ended
Reading | ReceiveFailure broken pipe -> Failed broken pipe | Answer Failed broken pipe
Idle | Read by hook -> Reading | Pull
Idle | Read by reader -> Idle | Answer Refused reader for hook
Idle | ReceiveChunks 1 -> Idle | DieOutOfOrder
Idle | ReceiveEnd -> Idle | DieOutOfOrder
Idle | ReceiveFailure broken pipe -> Idle | DieOutOfOrder
Ended | Read by hook -> Ended | Answer Ended
Ended | Read by reader -> Ended | Answer Refused reader for hook
Ended | ReceiveChunks 1 -> Ended | DieOutOfOrder
Ended | ReceiveEnd -> Ended | DieOutOfOrder
Ended | ReceiveFailure broken pipe -> Ended | DieOutOfOrder
Failed broken pipe | Read by hook -> Failed broken pipe | Answer Failed broken pipe
Failed broken pipe | Read by reader -> Failed broken pipe | Answer Refused reader for hook
Failed broken pipe | ReceiveChunks 1 -> Failed broken pipe | DieOutOfOrder
Failed broken pipe | ReceiveEnd -> Failed broken pipe | DieOutOfOrder
Failed broken pipe | ReceiveFailure broken pipe -> Failed broken pipe | DieOutOfOrder
`;

export const everyMove = Task.make({
  name: "standard input's stage moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = stages.flatMap((stage) =>
      events.map((event) => {
        const [next, action] = moveStandardInputStage(stage, event);
        return `${describeStage(stage)} | ${describeEvent(event)} -> ${describeStage(next)} | ${describeAction(action)}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_MOVES);
  }),
});
