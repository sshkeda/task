import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import {
  forgetEndedProcessGroups,
  recordStartedProcessGroup,
} from "../../src/lib/kill-leftover-processes/index.ts";
import type { startedProcessGroup } from "../../src/lib/kill-leftover-processes/index.ts";
import {
  moveTrackedProcessGroupStage,
  trackedGroupOf,
  TrackedProcessGroupEvent,
  TrackedProcessGroupStage,
} from "../../src/lib/kill-leftover-processes/internal/define-tracked-process-group-stage.ts";
import type { TrackedProcessGroup } from "../../src/lib/kill-leftover-processes/internal/define-tracked-process-group-stage.ts";
import { processIdentityKey } from "../../src/shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../src/shared/claim-processes-to-report.ts";

const REGISTRATION = 1;

const LATER_REGISTRATION = 2;

const TASK_NAME = "build";

const OTHER_TASK_NAME = "serve";

const LEADER_START_TIME = "1700000000.0";

const OTHER_LEADER_START_TIME = "1700000000.5";

const MEMBERS_AT_LEADER_EXIT = [{ processId: 4241, startTime: "1700000000.1" }] as const;

const OTHER_MEMBERS_AT_LEADER_EXIT = [{ processId: 4242, startTime: "1700000000.2" }] as const;

const describeMembers = (members: readonly (typeof ProcessIdentity.Type)[] | null) => {
  const listedMembers = members ?? [];
  return listedMembers.length === 0
    ? "none"
    : listedMembers.map((member) => processIdentityKey(member)).join(" ");
};

const PROCESS_GROUP_ID = 4240;

const describeRecord = (record: typeof TrackedProcessGroup.Type | null) =>
  record === null
    ? "untracked"
    : [
        `g${record.generation}`,
        record.taskName,
        `leader ${record.leaderStartTime ?? "unread"}`,
        `exit ${describeMembers(record.membersAtLeaderExit)}`,
      ].join(" ");

const describeStage = (stage: typeof TrackedProcessGroupStage.Type) =>
  `${stage._tag} ${describeRecord(trackedGroupOf(stage))}`;

const describeEvent = TrackedProcessGroupEvent.match({
  End: ({ endedGroup }) => `End ${describeRecord(endedGroup)}`,
  ExitLeader: ({ taskName, membersAtLeaderExit }) =>
    `ExitLeader ${taskName} ${describeMembers(membersAtLeaderExit)}`,
  Start: ({ generation, taskName, leaderStartTime }) =>
    `Start g${generation} ${taskName} leader ${leaderStartTime ?? "unread"}`,
});

const stages = [
  TrackedProcessGroupStage.cases.Untracked.make({}),
  TrackedProcessGroupStage.cases.Started.make({
    generation: REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: LEADER_START_TIME,
  }),
  TrackedProcessGroupStage.cases.Started.make({
    generation: REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: null,
  }),
  TrackedProcessGroupStage.cases.LeaderExited.make({
    generation: REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: LEADER_START_TIME,
    membersAtLeaderExit: MEMBERS_AT_LEADER_EXIT,
  }),
  TrackedProcessGroupStage.cases.LeaderExited.make({
    generation: REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: null,
    membersAtLeaderExit: MEMBERS_AT_LEADER_EXIT,
  }),
];

const end = (
  generation: number,
  taskName: string,
  leaderStartTime: string | null,
  membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[] | null,
) =>
  TrackedProcessGroupEvent.cases.End.make({
    endedGroup: { generation, taskName, leaderStartTime, membersAtLeaderExit },
  });

const events = [
  end(REGISTRATION, TASK_NAME, LEADER_START_TIME, null),
  end(LATER_REGISTRATION, TASK_NAME, LEADER_START_TIME, null),
  end(REGISTRATION, TASK_NAME, OTHER_LEADER_START_TIME, null),
  end(REGISTRATION, OTHER_TASK_NAME, LEADER_START_TIME, null),
  end(REGISTRATION, TASK_NAME, null, null),
  end(LATER_REGISTRATION, TASK_NAME, null, null),
  end(REGISTRATION, TASK_NAME, LEADER_START_TIME, MEMBERS_AT_LEADER_EXIT),
  end(REGISTRATION, TASK_NAME, LEADER_START_TIME, OTHER_MEMBERS_AT_LEADER_EXIT),
  end(REGISTRATION, TASK_NAME, null, MEMBERS_AT_LEADER_EXIT),
  TrackedProcessGroupEvent.cases.ExitLeader.make({
    taskName: TASK_NAME,
    membersAtLeaderExit: [],
  }),
  TrackedProcessGroupEvent.cases.ExitLeader.make({
    taskName: TASK_NAME,
    membersAtLeaderExit: MEMBERS_AT_LEADER_EXIT,
  }),
  TrackedProcessGroupEvent.cases.ExitLeader.make({
    taskName: TASK_NAME,
    membersAtLeaderExit: OTHER_MEMBERS_AT_LEADER_EXIT,
  }),
  TrackedProcessGroupEvent.cases.ExitLeader.make({
    taskName: OTHER_TASK_NAME,
    membersAtLeaderExit: [],
  }),
  TrackedProcessGroupEvent.cases.Start.make({
    generation: LATER_REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: LEADER_START_TIME,
  }),
  TrackedProcessGroupEvent.cases.Start.make({
    generation: LATER_REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: null,
  }),
  TrackedProcessGroupEvent.cases.Start.make({
    generation: REGISTRATION,
    taskName: TASK_NAME,
    leaderStartTime: LEADER_START_TIME,
  }),
];

const EXPECTED_MOVES = `
Untracked untracked | End g1 build leader 1700000000.0 exit none -> Untracked untracked | none
Untracked untracked | End g2 build leader 1700000000.0 exit none -> Untracked untracked | none
Untracked untracked | End g1 build leader 1700000000.5 exit none -> Untracked untracked | none
Untracked untracked | End g1 serve leader 1700000000.0 exit none -> Untracked untracked | none
Untracked untracked | End g1 build leader unread exit none -> Untracked untracked | none
Untracked untracked | End g2 build leader unread exit none -> Untracked untracked | none
Untracked untracked | End g1 build leader 1700000000.0 exit 4241@1700000000.1 -> Untracked untracked | none
Untracked untracked | End g1 build leader 1700000000.0 exit 4242@1700000000.2 -> Untracked untracked | none
Untracked untracked | End g1 build leader unread exit 4241@1700000000.1 -> Untracked untracked | none
Untracked untracked | ExitLeader build none -> Untracked untracked | none
Untracked untracked | ExitLeader build 4241@1700000000.1 -> Untracked untracked | none
Untracked untracked | ExitLeader build 4242@1700000000.2 -> Untracked untracked | none
Untracked untracked | ExitLeader serve none -> Untracked untracked | none
Untracked untracked | Start g2 build leader 1700000000.0 -> Started g2 build leader 1700000000.0 exit none | share
Untracked untracked | Start g2 build leader unread -> Started g2 build leader unread exit none | share
Untracked untracked | Start g1 build leader 1700000000.0 -> Started g1 build leader 1700000000.0 exit none | share
Started g1 build leader 1700000000.0 exit none | End g1 build leader 1700000000.0 exit none -> Untracked untracked | share
Started g1 build leader 1700000000.0 exit none | End g2 build leader 1700000000.0 exit none -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g1 build leader 1700000000.5 exit none -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g1 serve leader 1700000000.0 exit none -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g1 build leader unread exit none -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g2 build leader unread exit none -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g1 build leader 1700000000.0 exit 4241@1700000000.1 -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g1 build leader 1700000000.0 exit 4242@1700000000.2 -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | End g1 build leader unread exit 4241@1700000000.1 -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | ExitLeader build none -> Untracked untracked | share
Started g1 build leader 1700000000.0 exit none | ExitLeader build 4241@1700000000.1 -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | share
Started g1 build leader 1700000000.0 exit none | ExitLeader build 4242@1700000000.2 -> LeaderExited g1 build leader 1700000000.0 exit 4242@1700000000.2 | share
Started g1 build leader 1700000000.0 exit none | ExitLeader serve none -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader 1700000000.0 exit none | Start g2 build leader 1700000000.0 -> Started g2 build leader 1700000000.0 exit none | share
Started g1 build leader 1700000000.0 exit none | Start g2 build leader unread -> Started g2 build leader unread exit none | share
Started g1 build leader 1700000000.0 exit none | Start g1 build leader 1700000000.0 -> Started g1 build leader 1700000000.0 exit none | none
Started g1 build leader unread exit none | End g1 build leader 1700000000.0 exit none -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g2 build leader 1700000000.0 exit none -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g1 build leader 1700000000.5 exit none -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g1 serve leader 1700000000.0 exit none -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g1 build leader unread exit none -> Untracked untracked | share
Started g1 build leader unread exit none | End g2 build leader unread exit none -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g1 build leader 1700000000.0 exit 4241@1700000000.1 -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g1 build leader 1700000000.0 exit 4242@1700000000.2 -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | End g1 build leader unread exit 4241@1700000000.1 -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | ExitLeader build none -> Untracked untracked | share
Started g1 build leader unread exit none | ExitLeader build 4241@1700000000.1 -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | share
Started g1 build leader unread exit none | ExitLeader build 4242@1700000000.2 -> LeaderExited g1 build leader unread exit 4242@1700000000.2 | share
Started g1 build leader unread exit none | ExitLeader serve none -> Started g1 build leader unread exit none | none
Started g1 build leader unread exit none | Start g2 build leader 1700000000.0 -> Started g2 build leader 1700000000.0 exit none | share
Started g1 build leader unread exit none | Start g2 build leader unread -> Started g2 build leader unread exit none | share
Started g1 build leader unread exit none | Start g1 build leader 1700000000.0 -> Started g1 build leader 1700000000.0 exit none | share
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 build leader 1700000000.0 exit none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g2 build leader 1700000000.0 exit none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 build leader 1700000000.5 exit none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 serve leader 1700000000.0 exit none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 build leader unread exit none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g2 build leader unread exit none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 build leader 1700000000.0 exit 4241@1700000000.1 -> Untracked untracked | share
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 build leader 1700000000.0 exit 4242@1700000000.2 -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | End g1 build leader unread exit 4241@1700000000.1 -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | ExitLeader build none -> Untracked untracked | share
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | ExitLeader build 4241@1700000000.1 -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | ExitLeader build 4242@1700000000.2 -> LeaderExited g1 build leader 1700000000.0 exit 4242@1700000000.2 | share
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | ExitLeader serve none -> LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | none
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | Start g2 build leader 1700000000.0 -> Started g2 build leader 1700000000.0 exit none | share
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | Start g2 build leader unread -> Started g2 build leader unread exit none | share
LeaderExited g1 build leader 1700000000.0 exit 4241@1700000000.1 | Start g1 build leader 1700000000.0 -> Started g1 build leader 1700000000.0 exit none | share
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 build leader 1700000000.0 exit none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g2 build leader 1700000000.0 exit none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 build leader 1700000000.5 exit none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 serve leader 1700000000.0 exit none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 build leader unread exit none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g2 build leader unread exit none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 build leader 1700000000.0 exit 4241@1700000000.1 -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 build leader 1700000000.0 exit 4242@1700000000.2 -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | End g1 build leader unread exit 4241@1700000000.1 -> Untracked untracked | share
LeaderExited g1 build leader unread exit 4241@1700000000.1 | ExitLeader build none -> Untracked untracked | share
LeaderExited g1 build leader unread exit 4241@1700000000.1 | ExitLeader build 4241@1700000000.1 -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | ExitLeader build 4242@1700000000.2 -> LeaderExited g1 build leader unread exit 4242@1700000000.2 | share
LeaderExited g1 build leader unread exit 4241@1700000000.1 | ExitLeader serve none -> LeaderExited g1 build leader unread exit 4241@1700000000.1 | none
LeaderExited g1 build leader unread exit 4241@1700000000.1 | Start g2 build leader 1700000000.0 -> Started g2 build leader 1700000000.0 exit none | share
LeaderExited g1 build leader unread exit 4241@1700000000.1 | Start g2 build leader unread -> Started g2 build leader unread exit none | share
LeaderExited g1 build leader unread exit 4241@1700000000.1 | Start g1 build leader 1700000000.0 -> Started g1 build leader 1700000000.0 exit none | share
`;

export const everyMove = Task.make({
  name: "a process group the run tracks and shares with its supervisor moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = stages.flatMap((stage) =>
      events.map((event) => {
        const [next, action] = moveTrackedProcessGroupStage(stage, event);
        return `${describeStage(stage)} | ${describeEvent(event)} -> ${describeStage(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_MOVES);
  }),
});

export const reusedRegistration = Task.make({
  name: "a kill that saw an earlier registration of a process group ID never forgets a later registration of that ID, even when the task and its unread start times match",
  run: Effect.gen(function* () {
    const startedProcessGroups = new Map<number, ReturnType<typeof startedProcessGroup>>();
    recordStartedProcessGroup(startedProcessGroups, PROCESS_GROUP_ID, TASK_NAME, null);
    const earlier = yield* Effect.fromNullishOr(startedProcessGroups.get(PROCESS_GROUP_ID));
    recordStartedProcessGroup(startedProcessGroups, PROCESS_GROUP_ID, TASK_NAME, null);
    const later = yield* Effect.fromNullishOr(startedProcessGroups.get(PROCESS_GROUP_ID));
    const forgotten = yield* forgetEndedProcessGroups(startedProcessGroups)(
      new Map([[PROCESS_GROUP_ID, earlier]]),
    );
    expect(forgotten).toEqual([]);
    expect(startedProcessGroups.get(PROCESS_GROUP_ID)).toBe(later);
    expect(
      yield* forgetEndedProcessGroups(startedProcessGroups)(new Map([[PROCESS_GROUP_ID, later]])),
    ).toEqual([PROCESS_GROUP_ID]);
    expect(startedProcessGroups.has(PROCESS_GROUP_ID)).toBe(false);
  }),
});
