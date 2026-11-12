import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import {
  LeftoverProcessEvent,
  LeftoverProcessStage,
  moveLeftoverProcessStage,
} from "../../src/lib/kill-leftover-processes/internal/define-leftover-process-stage.ts";
import {
  moveProcessGroupStage,
  ProcessGroupEvent,
  ProcessGroupStage,
} from "../../src/lib/kill-leftover-processes/internal/define-process-group-stage.ts";

const LATEST_MEMBER_START_TIME = "1700000000.1";

const WITNESS = { processId: 4242, processGroupId: 4240, startTime: "1700000000.0" } as const;

const SIGNAL_RESULTS = ["EPERM", "ESRCH", "sent"] as const;

const describeGroupStage = ProcessGroupStage.match({
  Started: () => "Started",
  LeaderExited: ({ latestMemberStartTime }) => `LeaderExited at ${latestMemberStartTime}`,
  ProbedAlive: () => "ProbedAlive",
  WitnessFound: ({ witness }) => `WitnessFound ${witness.processId}`,
  StillTheSame: () => "StillTheSame",
  KillDenied: () => "KillDenied",
  NotSignalable: () => "NotSignalable",
  ChangedBeforeKill: () => "ChangedBeforeKill",
  Ended: () => "Ended",
});

const describeGroupEvent = ProcessGroupEvent.match({
  CheckMembers: ({ hasOnlyEndedMembers }) =>
    `CheckMembers ${hasOnlyEndedMembers ? "only ended" : "some live"}`,
  ExitLeader: ({ latestMemberStartTime }) =>
    `ExitLeader ${latestMemberStartTime === null ? "with no members" : `at ${latestMemberStartTime}`}`,
  FindWitness: ({ witness }) =>
    `FindWitness ${witness === null ? "none" : String(witness.processId)}`,
  KillGroup: ({ result }) => `KillGroup ${result}`,
  Probe: ({ result }) => `Probe ${result}`,
  Recheck: ({ isStillTheSame }) => `Recheck ${isStillTheSame ? "same" : "changed"}`,
});

const groupStages = [
  ProcessGroupStage.cases.Started.make({}),
  ProcessGroupStage.cases.LeaderExited.make({ latestMemberStartTime: LATEST_MEMBER_START_TIME }),
  ProcessGroupStage.cases.ProbedAlive.make({}),
  ProcessGroupStage.cases.WitnessFound.make({ witness: WITNESS }),
  ProcessGroupStage.cases.StillTheSame.make({}),
  ProcessGroupStage.cases.KillDenied.make({}),
  ProcessGroupStage.cases.NotSignalable.make({}),
  ProcessGroupStage.cases.ChangedBeforeKill.make({}),
  ProcessGroupStage.cases.Ended.make({}),
];

const groupEvents = [
  ProcessGroupEvent.cases.ExitLeader.make({ latestMemberStartTime: null }),
  ProcessGroupEvent.cases.ExitLeader.make({ latestMemberStartTime: LATEST_MEMBER_START_TIME }),
  ...SIGNAL_RESULTS.map((result) => ProcessGroupEvent.cases.Probe.make({ result })),
  ProcessGroupEvent.cases.FindWitness.make({ witness: null }),
  ProcessGroupEvent.cases.FindWitness.make({ witness: WITNESS }),
  ProcessGroupEvent.cases.Recheck.make({ isStillTheSame: true }),
  ProcessGroupEvent.cases.Recheck.make({ isStillTheSame: false }),
  ...SIGNAL_RESULTS.map((result) => ProcessGroupEvent.cases.KillGroup.make({ result })),
  ProcessGroupEvent.cases.CheckMembers.make({ hasOnlyEndedMembers: true }),
  ProcessGroupEvent.cases.CheckMembers.make({ hasOnlyEndedMembers: false }),
];

const describeLeftoverStage = LeftoverProcessStage.match({
  Listed: () => "Listed",
  Owned: () => "Owned",
  CommandRead: () => "CommandRead",
  StillTheSame: () => "StillTheSame",
  Signalled: ({ result }) => `Signalled ${result}`,
  Killed: () => "Killed",
  NotKillable: () => "NotKillable",
  AlreadyReported: () => "AlreadyReported",
  NotOwned: () => "NotOwned",
  Replaced: () => "Replaced",
  Gone: () => "Gone",
});

const describeLeftoverEvent = LeftoverProcessEvent.match({
  Claim: ({ isFirstReport }) => `Claim ${isFirstReport ? "first" : "again"}`,
  Inspect: ({ isListed, isOwned }) =>
    `Inspect ${isListed ? "listed" : "unlisted"} ${isOwned ? "owned" : "unowned"}`,
  ReadCommand: ({ hasCommand }) => `ReadCommand ${hasCommand ? "read" : "gone"}`,
  Recheck: ({ isStillTheSame }) => `Recheck ${isStillTheSame ? "same" : "changed"}`,
  Signal: ({ result }) => `Signal ${result}`,
});

const leftoverStages = [
  LeftoverProcessStage.cases.Listed.make({}),
  LeftoverProcessStage.cases.Owned.make({}),
  LeftoverProcessStage.cases.CommandRead.make({}),
  LeftoverProcessStage.cases.StillTheSame.make({}),
  LeftoverProcessStage.cases.Signalled.make({ result: "sent" }),
  LeftoverProcessStage.cases.Signalled.make({ result: "EPERM" }),
  LeftoverProcessStage.cases.Killed.make({}),
  LeftoverProcessStage.cases.NotKillable.make({}),
  LeftoverProcessStage.cases.AlreadyReported.make({}),
  LeftoverProcessStage.cases.NotOwned.make({}),
  LeftoverProcessStage.cases.Replaced.make({}),
  LeftoverProcessStage.cases.Gone.make({}),
];

const leftoverEvents = [
  LeftoverProcessEvent.cases.Inspect.make({ isListed: false, isOwned: true }),
  LeftoverProcessEvent.cases.Inspect.make({ isListed: true, isOwned: false }),
  LeftoverProcessEvent.cases.Inspect.make({ isListed: true, isOwned: true }),
  LeftoverProcessEvent.cases.ReadCommand.make({ hasCommand: false }),
  LeftoverProcessEvent.cases.ReadCommand.make({ hasCommand: true }),
  LeftoverProcessEvent.cases.Recheck.make({ isStillTheSame: false }),
  LeftoverProcessEvent.cases.Recheck.make({ isStillTheSame: true }),
  ...SIGNAL_RESULTS.map((result) => LeftoverProcessEvent.cases.Signal.make({ result })),
  LeftoverProcessEvent.cases.Claim.make({ isFirstReport: true }),
  LeftoverProcessEvent.cases.Claim.make({ isFirstReport: false }),
];

const EXPECTED_GROUP_MOVES = `
Started | ExitLeader with no members -> Ended | forget
Started | ExitLeader at 1700000000.1 -> LeaderExited at 1700000000.1 | record
Started | Probe EPERM -> ProbedAlive | findWitness
Started | Probe ESRCH -> Ended | forget
Started | Probe sent -> ProbedAlive | findWitness
Started | FindWitness none -> Started | none
Started | FindWitness 4242 -> Started | none
Started | Recheck same -> Started | none
Started | Recheck changed -> Started | none
Started | KillGroup EPERM -> Started | none
Started | KillGroup ESRCH -> Started | none
Started | KillGroup sent -> Started | none
Started | CheckMembers only ended -> Started | none
Started | CheckMembers some live -> Started | none
LeaderExited at 1700000000.1 | ExitLeader with no members -> Ended | forget
LeaderExited at 1700000000.1 | ExitLeader at 1700000000.1 -> LeaderExited at 1700000000.1 | record
LeaderExited at 1700000000.1 | Probe EPERM -> ProbedAlive | findWitness
LeaderExited at 1700000000.1 | Probe ESRCH -> Ended | forget
LeaderExited at 1700000000.1 | Probe sent -> ProbedAlive | findWitness
LeaderExited at 1700000000.1 | FindWitness none -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | FindWitness 4242 -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | Recheck same -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | Recheck changed -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | KillGroup EPERM -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | KillGroup ESRCH -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | KillGroup sent -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | CheckMembers only ended -> LeaderExited at 1700000000.1 | none
LeaderExited at 1700000000.1 | CheckMembers some live -> LeaderExited at 1700000000.1 | none
ProbedAlive | ExitLeader with no members -> ProbedAlive | none
ProbedAlive | ExitLeader at 1700000000.1 -> ProbedAlive | none
ProbedAlive | Probe EPERM -> ProbedAlive | none
ProbedAlive | Probe ESRCH -> ProbedAlive | none
ProbedAlive | Probe sent -> ProbedAlive | none
ProbedAlive | FindWitness none -> Ended | forget
ProbedAlive | FindWitness 4242 -> WitnessFound 4242 | killMembers
ProbedAlive | Recheck same -> ProbedAlive | none
ProbedAlive | Recheck changed -> ProbedAlive | none
ProbedAlive | KillGroup EPERM -> ProbedAlive | none
ProbedAlive | KillGroup ESRCH -> ProbedAlive | none
ProbedAlive | KillGroup sent -> ProbedAlive | none
ProbedAlive | CheckMembers only ended -> ProbedAlive | none
ProbedAlive | CheckMembers some live -> ProbedAlive | none
WitnessFound 4242 | ExitLeader with no members -> WitnessFound 4242 | none
WitnessFound 4242 | ExitLeader at 1700000000.1 -> WitnessFound 4242 | none
WitnessFound 4242 | Probe EPERM -> WitnessFound 4242 | none
WitnessFound 4242 | Probe ESRCH -> WitnessFound 4242 | none
WitnessFound 4242 | Probe sent -> WitnessFound 4242 | none
WitnessFound 4242 | FindWitness none -> WitnessFound 4242 | none
WitnessFound 4242 | FindWitness 4242 -> WitnessFound 4242 | none
WitnessFound 4242 | Recheck same -> StillTheSame | killGroup
WitnessFound 4242 | Recheck changed -> ChangedBeforeKill | keep
WitnessFound 4242 | KillGroup EPERM -> WitnessFound 4242 | none
WitnessFound 4242 | KillGroup ESRCH -> WitnessFound 4242 | none
WitnessFound 4242 | KillGroup sent -> WitnessFound 4242 | none
WitnessFound 4242 | CheckMembers only ended -> WitnessFound 4242 | none
WitnessFound 4242 | CheckMembers some live -> WitnessFound 4242 | none
StillTheSame | ExitLeader with no members -> StillTheSame | none
StillTheSame | ExitLeader at 1700000000.1 -> StillTheSame | none
StillTheSame | Probe EPERM -> StillTheSame | none
StillTheSame | Probe ESRCH -> StillTheSame | none
StillTheSame | Probe sent -> StillTheSame | none
StillTheSame | FindWitness none -> StillTheSame | none
StillTheSame | FindWitness 4242 -> StillTheSame | none
StillTheSame | Recheck same -> StillTheSame | none
StillTheSame | Recheck changed -> StillTheSame | none
StillTheSame | KillGroup EPERM -> KillDenied | checkMembers
StillTheSame | KillGroup ESRCH -> Ended | forget
StillTheSame | KillGroup sent -> Ended | forget
StillTheSame | CheckMembers only ended -> StillTheSame | none
StillTheSame | CheckMembers some live -> StillTheSame | none
KillDenied | ExitLeader with no members -> KillDenied | none
KillDenied | ExitLeader at 1700000000.1 -> KillDenied | none
KillDenied | Probe EPERM -> KillDenied | none
KillDenied | Probe ESRCH -> KillDenied | none
KillDenied | Probe sent -> KillDenied | none
KillDenied | FindWitness none -> KillDenied | none
KillDenied | FindWitness 4242 -> KillDenied | none
KillDenied | Recheck same -> KillDenied | none
KillDenied | Recheck changed -> KillDenied | none
KillDenied | KillGroup EPERM -> KillDenied | none
KillDenied | KillGroup ESRCH -> KillDenied | none
KillDenied | KillGroup sent -> KillDenied | none
KillDenied | CheckMembers only ended -> Ended | forget
KillDenied | CheckMembers some live -> NotSignalable | keep
NotSignalable | ExitLeader with no members -> NotSignalable | none
NotSignalable | ExitLeader at 1700000000.1 -> NotSignalable | none
NotSignalable | Probe EPERM -> NotSignalable | none
NotSignalable | Probe ESRCH -> NotSignalable | none
NotSignalable | Probe sent -> NotSignalable | none
NotSignalable | FindWitness none -> NotSignalable | none
NotSignalable | FindWitness 4242 -> NotSignalable | none
NotSignalable | Recheck same -> NotSignalable | none
NotSignalable | Recheck changed -> NotSignalable | none
NotSignalable | KillGroup EPERM -> NotSignalable | none
NotSignalable | KillGroup ESRCH -> NotSignalable | none
NotSignalable | KillGroup sent -> NotSignalable | none
NotSignalable | CheckMembers only ended -> NotSignalable | none
NotSignalable | CheckMembers some live -> NotSignalable | none
ChangedBeforeKill | ExitLeader with no members -> ChangedBeforeKill | none
ChangedBeforeKill | ExitLeader at 1700000000.1 -> ChangedBeforeKill | none
ChangedBeforeKill | Probe EPERM -> ChangedBeforeKill | none
ChangedBeforeKill | Probe ESRCH -> ChangedBeforeKill | none
ChangedBeforeKill | Probe sent -> ChangedBeforeKill | none
ChangedBeforeKill | FindWitness none -> ChangedBeforeKill | none
ChangedBeforeKill | FindWitness 4242 -> ChangedBeforeKill | none
ChangedBeforeKill | Recheck same -> ChangedBeforeKill | none
ChangedBeforeKill | Recheck changed -> ChangedBeforeKill | none
ChangedBeforeKill | KillGroup EPERM -> ChangedBeforeKill | none
ChangedBeforeKill | KillGroup ESRCH -> ChangedBeforeKill | none
ChangedBeforeKill | KillGroup sent -> ChangedBeforeKill | none
ChangedBeforeKill | CheckMembers only ended -> ChangedBeforeKill | none
ChangedBeforeKill | CheckMembers some live -> ChangedBeforeKill | none
Ended | ExitLeader with no members -> Ended | none
Ended | ExitLeader at 1700000000.1 -> Ended | none
Ended | Probe EPERM -> Ended | none
Ended | Probe ESRCH -> Ended | none
Ended | Probe sent -> Ended | none
Ended | FindWitness none -> Ended | none
Ended | FindWitness 4242 -> Ended | none
Ended | Recheck same -> Ended | none
Ended | Recheck changed -> Ended | none
Ended | KillGroup EPERM -> Ended | none
Ended | KillGroup ESRCH -> Ended | none
Ended | KillGroup sent -> Ended | none
Ended | CheckMembers only ended -> Ended | none
Ended | CheckMembers some live -> Ended | none
`;

const EXPECTED_LEFTOVER_MOVES = `
Listed | Inspect unlisted owned -> Gone | none
Listed | Inspect listed unowned -> NotOwned | none
Listed | Inspect listed owned -> Owned | readCommand
Listed | ReadCommand gone -> Listed | none
Listed | ReadCommand read -> Listed | none
Listed | Recheck changed -> Listed | none
Listed | Recheck same -> Listed | none
Listed | Signal EPERM -> Listed | none
Listed | Signal ESRCH -> Listed | none
Listed | Signal sent -> Listed | none
Listed | Claim first -> Listed | none
Listed | Claim again -> Listed | none
Owned | Inspect unlisted owned -> Owned | none
Owned | Inspect listed unowned -> Owned | none
Owned | Inspect listed owned -> Owned | none
Owned | ReadCommand gone -> Gone | none
Owned | ReadCommand read -> CommandRead | recheck
Owned | Recheck changed -> Owned | none
Owned | Recheck same -> Owned | none
Owned | Signal EPERM -> Owned | none
Owned | Signal ESRCH -> Owned | none
Owned | Signal sent -> Owned | none
Owned | Claim first -> Owned | none
Owned | Claim again -> Owned | none
CommandRead | Inspect unlisted owned -> CommandRead | none
CommandRead | Inspect listed unowned -> CommandRead | none
CommandRead | Inspect listed owned -> CommandRead | none
CommandRead | ReadCommand gone -> CommandRead | none
CommandRead | ReadCommand read -> CommandRead | none
CommandRead | Recheck changed -> Replaced | none
CommandRead | Recheck same -> StillTheSame | signal
CommandRead | Signal EPERM -> CommandRead | none
CommandRead | Signal ESRCH -> CommandRead | none
CommandRead | Signal sent -> CommandRead | none
CommandRead | Claim first -> CommandRead | none
CommandRead | Claim again -> CommandRead | none
StillTheSame | Inspect unlisted owned -> StillTheSame | none
StillTheSame | Inspect listed unowned -> StillTheSame | none
StillTheSame | Inspect listed owned -> StillTheSame | none
StillTheSame | ReadCommand gone -> StillTheSame | none
StillTheSame | ReadCommand read -> StillTheSame | none
StillTheSame | Recheck changed -> StillTheSame | none
StillTheSame | Recheck same -> StillTheSame | none
StillTheSame | Signal EPERM -> Signalled EPERM | claim
StillTheSame | Signal ESRCH -> Gone | none
StillTheSame | Signal sent -> Signalled sent | claim
StillTheSame | Claim first -> StillTheSame | none
StillTheSame | Claim again -> StillTheSame | none
Signalled sent | Inspect unlisted owned -> Signalled sent | none
Signalled sent | Inspect listed unowned -> Signalled sent | none
Signalled sent | Inspect listed owned -> Signalled sent | none
Signalled sent | ReadCommand gone -> Signalled sent | none
Signalled sent | ReadCommand read -> Signalled sent | none
Signalled sent | Recheck changed -> Signalled sent | none
Signalled sent | Recheck same -> Signalled sent | none
Signalled sent | Signal EPERM -> Signalled sent | none
Signalled sent | Signal ESRCH -> Signalled sent | none
Signalled sent | Signal sent -> Signalled sent | none
Signalled sent | Claim first -> Killed | reportKilled
Signalled sent | Claim again -> AlreadyReported | none
Signalled EPERM | Inspect unlisted owned -> Signalled EPERM | none
Signalled EPERM | Inspect listed unowned -> Signalled EPERM | none
Signalled EPERM | Inspect listed owned -> Signalled EPERM | none
Signalled EPERM | ReadCommand gone -> Signalled EPERM | none
Signalled EPERM | ReadCommand read -> Signalled EPERM | none
Signalled EPERM | Recheck changed -> Signalled EPERM | none
Signalled EPERM | Recheck same -> Signalled EPERM | none
Signalled EPERM | Signal EPERM -> Signalled EPERM | none
Signalled EPERM | Signal ESRCH -> Signalled EPERM | none
Signalled EPERM | Signal sent -> Signalled EPERM | none
Signalled EPERM | Claim first -> NotKillable | reportNotKillable
Signalled EPERM | Claim again -> AlreadyReported | none
Killed | Inspect unlisted owned -> Killed | none
Killed | Inspect listed unowned -> Killed | none
Killed | Inspect listed owned -> Killed | none
Killed | ReadCommand gone -> Killed | none
Killed | ReadCommand read -> Killed | none
Killed | Recheck changed -> Killed | none
Killed | Recheck same -> Killed | none
Killed | Signal EPERM -> Killed | none
Killed | Signal ESRCH -> Killed | none
Killed | Signal sent -> Killed | none
Killed | Claim first -> Killed | none
Killed | Claim again -> Killed | none
NotKillable | Inspect unlisted owned -> NotKillable | none
NotKillable | Inspect listed unowned -> NotKillable | none
NotKillable | Inspect listed owned -> NotKillable | none
NotKillable | ReadCommand gone -> NotKillable | none
NotKillable | ReadCommand read -> NotKillable | none
NotKillable | Recheck changed -> NotKillable | none
NotKillable | Recheck same -> NotKillable | none
NotKillable | Signal EPERM -> NotKillable | none
NotKillable | Signal ESRCH -> NotKillable | none
NotKillable | Signal sent -> NotKillable | none
NotKillable | Claim first -> NotKillable | none
NotKillable | Claim again -> NotKillable | none
AlreadyReported | Inspect unlisted owned -> AlreadyReported | none
AlreadyReported | Inspect listed unowned -> AlreadyReported | none
AlreadyReported | Inspect listed owned -> AlreadyReported | none
AlreadyReported | ReadCommand gone -> AlreadyReported | none
AlreadyReported | ReadCommand read -> AlreadyReported | none
AlreadyReported | Recheck changed -> AlreadyReported | none
AlreadyReported | Recheck same -> AlreadyReported | none
AlreadyReported | Signal EPERM -> AlreadyReported | none
AlreadyReported | Signal ESRCH -> AlreadyReported | none
AlreadyReported | Signal sent -> AlreadyReported | none
AlreadyReported | Claim first -> AlreadyReported | none
AlreadyReported | Claim again -> AlreadyReported | none
NotOwned | Inspect unlisted owned -> NotOwned | none
NotOwned | Inspect listed unowned -> NotOwned | none
NotOwned | Inspect listed owned -> NotOwned | none
NotOwned | ReadCommand gone -> NotOwned | none
NotOwned | ReadCommand read -> NotOwned | none
NotOwned | Recheck changed -> NotOwned | none
NotOwned | Recheck same -> NotOwned | none
NotOwned | Signal EPERM -> NotOwned | none
NotOwned | Signal ESRCH -> NotOwned | none
NotOwned | Signal sent -> NotOwned | none
NotOwned | Claim first -> NotOwned | none
NotOwned | Claim again -> NotOwned | none
Replaced | Inspect unlisted owned -> Replaced | none
Replaced | Inspect listed unowned -> Replaced | none
Replaced | Inspect listed owned -> Replaced | none
Replaced | ReadCommand gone -> Replaced | none
Replaced | ReadCommand read -> Replaced | none
Replaced | Recheck changed -> Replaced | none
Replaced | Recheck same -> Replaced | none
Replaced | Signal EPERM -> Replaced | none
Replaced | Signal ESRCH -> Replaced | none
Replaced | Signal sent -> Replaced | none
Replaced | Claim first -> Replaced | none
Replaced | Claim again -> Replaced | none
Gone | Inspect unlisted owned -> Gone | none
Gone | Inspect listed unowned -> Gone | none
Gone | Inspect listed owned -> Gone | none
Gone | ReadCommand gone -> Gone | none
Gone | ReadCommand read -> Gone | none
Gone | Recheck changed -> Gone | none
Gone | Recheck same -> Gone | none
Gone | Signal EPERM -> Gone | none
Gone | Signal ESRCH -> Gone | none
Gone | Signal sent -> Gone | none
Gone | Claim first -> Gone | none
Gone | Claim again -> Gone | none
`;

export const everyGroupMove = Task.make({
  name: "a leftover process group's stage moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = groupStages.flatMap((stage) =>
      groupEvents.map((event) => {
        const [next, action] = moveProcessGroupStage(stage, event);
        return `${describeGroupStage(stage)} | ${describeGroupEvent(event)} -> ${describeGroupStage(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_GROUP_MOVES);
  }),
});

export const everyLeftoverMove = Task.make({
  name: "a leftover process's stage moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = leftoverStages.flatMap((stage) =>
      leftoverEvents.map((event) => {
        const [next, action] = moveLeftoverProcessStage(stage, event);
        return `${describeLeftoverStage(stage)} | ${describeLeftoverEvent(event)} -> ${describeLeftoverStage(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_LEFTOVER_MOVES);
  }),
});
