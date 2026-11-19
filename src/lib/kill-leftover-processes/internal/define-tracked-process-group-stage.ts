import { Schema } from "effect";

import { ProcessIdentity, processIdentityKey } from "../../../shared/claim-processes-to-report.ts";
import {
  lifeStageOf,
  moveProcessGroupStage,
  ProcessGroupEvent,
  ProcessGroupStage,
} from "./define-process-group-stage.ts";

export const UNREGISTERED_GENERATION = 0;

export const TrackedProcessGroup = Schema.Struct({
  generation: Schema.Int,
  taskName: Schema.String,
  leaderStartTime: Schema.NullOr(Schema.String),
  membersAtLeaderExit: Schema.NullOr(Schema.Array(ProcessIdentity)),
});

export const TrackedProcessGroupStage = Schema.TaggedUnion({
  Untracked: {},
  Started: {
    generation: Schema.Int,
    taskName: Schema.String,
    leaderStartTime: Schema.NullOr(Schema.String),
  },
  LeaderExited: {
    generation: Schema.Int,
    taskName: Schema.String,
    leaderStartTime: Schema.NullOr(Schema.String),
    membersAtLeaderExit: Schema.Array(ProcessIdentity),
  },
});

export const TrackedProcessGroupEvent = Schema.TaggedUnion({
  End: { endedGroup: TrackedProcessGroup },
  ExitLeader: { taskName: Schema.String, membersAtLeaderExit: Schema.Array(ProcessIdentity) },
  Start: {
    generation: Schema.Int,
    taskName: Schema.String,
    leaderStartTime: Schema.NullOr(Schema.String),
  },
});

export const trackedStageOf = (trackedGroup: typeof TrackedProcessGroup.Type | undefined) => {
  if (trackedGroup === undefined) {
    return TrackedProcessGroupStage.cases.Untracked.make({});
  }
  const { generation, taskName, leaderStartTime, membersAtLeaderExit } = trackedGroup;
  return membersAtLeaderExit === null
    ? TrackedProcessGroupStage.cases.Started.make({ generation, taskName, leaderStartTime })
    : TrackedProcessGroupStage.cases.LeaderExited.make({
        generation,
        taskName,
        leaderStartTime,
        membersAtLeaderExit,
      });
};

export const trackedGroupOf = TrackedProcessGroupStage.match({
  Untracked: () => null,
  Started: ({ generation, taskName, leaderStartTime }) => ({
    generation,
    taskName,
    leaderStartTime,
    membersAtLeaderExit: null,
  }),
  LeaderExited: ({ generation, taskName, leaderStartTime, membersAtLeaderExit }) => ({
    generation,
    taskName,
    leaderStartTime,
    membersAtLeaderExit,
  }),
});

const keyOfMembers = (members: readonly (typeof ProcessIdentity.Type)[] | null) =>
  members === null ? null : members.map((member) => processIdentityKey(member)).join(" ");

const isSameRecord = (
  left: typeof TrackedProcessGroup.Type,
  right: typeof TrackedProcessGroup.Type,
) =>
  left.generation === right.generation &&
  left.taskName === right.taskName &&
  left.leaderStartTime === right.leaderStartTime &&
  keyOfMembers(left.membersAtLeaderExit) === keyOfMembers(right.membersAtLeaderExit);

const stay = (stage: typeof TrackedProcessGroupStage.Type) => [stage, "none"] as const;

const untrack = [TrackedProcessGroupStage.cases.Untracked.make({}), "share"] as const;

const exitLeader = (
  stage: typeof TrackedProcessGroupStage.Type,
  trackedGroup: typeof TrackedProcessGroup.Type,
  membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
) => {
  const [life] = moveProcessGroupStage(
    lifeStageOf(trackedGroup),
    ProcessGroupEvent.cases.ExitLeader.make({ membersAtLeaderExit }),
  );
  if (!ProcessGroupStage.guards.LeaderExited(life)) {
    return untrack;
  }
  const isUnchanged =
    keyOfMembers(trackedGroup.membersAtLeaderExit) === keyOfMembers(life.membersAtLeaderExit);
  return isUnchanged
    ? stay(stage)
    : ([
        TrackedProcessGroupStage.cases.LeaderExited.make({
          generation: trackedGroup.generation,
          taskName: trackedGroup.taskName,
          leaderStartTime: trackedGroup.leaderStartTime,
          membersAtLeaderExit: life.membersAtLeaderExit,
        }),
        "share",
      ] as const);
};

export const moveTrackedProcessGroupStage = (
  stage: typeof TrackedProcessGroupStage.Type,
  event: typeof TrackedProcessGroupEvent.Type,
) =>
  TrackedProcessGroupEvent.match(event, {
    End: ({ endedGroup }) => {
      const trackedGroup = trackedGroupOf(stage);
      const isTheEndedRecord = trackedGroup !== null && isSameRecord(trackedGroup, endedGroup);
      return isTheEndedRecord ? untrack : stay(stage);
    },
    ExitLeader: ({ taskName, membersAtLeaderExit }) => {
      const trackedGroup = trackedGroupOf(stage);
      return trackedGroup?.taskName === taskName
        ? exitLeader(stage, trackedGroup, membersAtLeaderExit)
        : stay(stage);
    },
    Start: ({ generation, taskName, leaderStartTime }) => {
      const started = TrackedProcessGroupStage.cases.Started.make({
        generation,
        taskName,
        leaderStartTime,
      });
      const trackedGroup = trackedGroupOf(stage);
      const isAlreadyStarted =
        trackedGroup !== null &&
        isSameRecord(trackedGroup, {
          generation,
          taskName,
          leaderStartTime,
          membersAtLeaderExit: null,
        });
      return isAlreadyStarted ? stay(stage) : ([started, "share"] as const);
    },
  });
