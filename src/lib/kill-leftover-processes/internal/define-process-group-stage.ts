import { Schema } from "effect";

export const SignalResult = Schema.Literals(["EPERM", "ESRCH", "sent"]);

export const ListedIdentity = Schema.Struct({
  processId: Schema.Int,
  processGroupId: Schema.Int,
  startTime: Schema.String,
});

export const ProcessGroupStage = Schema.TaggedUnion({
  Started: {},
  LeaderExited: { latestMemberStartTime: Schema.String },
  ProbedAlive: {},
  WitnessFound: { witness: ListedIdentity },
  StillTheSame: {},
  KillDenied: {},
  NotSignalable: {},
  ChangedBeforeKill: {},
  Ended: {},
});

export const ProcessGroupEvent = Schema.TaggedUnion({
  CheckMembers: { hasOnlyEndedMembers: Schema.Boolean },
  ExitLeader: { latestMemberStartTime: Schema.NullOr(Schema.String) },
  FindWitness: { witness: Schema.NullOr(ListedIdentity) },
  KillGroup: { result: SignalResult },
  Probe: { result: SignalResult },
  Recheck: { isStillTheSame: Schema.Boolean },
});

export const lifeStageOf = (startedGroup: {
  readonly latestMemberStartTimeAtLeaderExit: string | null;
}) =>
  startedGroup.latestMemberStartTimeAtLeaderExit === null
    ? ProcessGroupStage.cases.Started.make({})
    : ProcessGroupStage.cases.LeaderExited.make({
        latestMemberStartTime: startedGroup.latestMemberStartTimeAtLeaderExit,
      });

const isLife = ProcessGroupStage.isAnyOf(["LeaderExited", "Started"]);

const stay = (stage: typeof ProcessGroupStage.Type) => [stage, "none"] as const;

const end = [ProcessGroupStage.cases.Ended.make({}), "forget"] as const;

export const moveProcessGroupStage = (
  stage: typeof ProcessGroupStage.Type,
  event: typeof ProcessGroupEvent.Type,
) =>
  ProcessGroupEvent.match(event, {
    CheckMembers: ({ hasOnlyEndedMembers }) => {
      if (!ProcessGroupStage.guards.KillDenied(stage)) {
        return stay(stage);
      }
      return hasOnlyEndedMembers
        ? end
        : ([ProcessGroupStage.cases.NotSignalable.make({}), "keep"] as const);
    },
    ExitLeader: ({ latestMemberStartTime }) => {
      if (!isLife(stage)) {
        return stay(stage);
      }
      return latestMemberStartTime === null
        ? end
        : ([
            ProcessGroupStage.cases.LeaderExited.make({ latestMemberStartTime }),
            "record",
          ] as const);
    },
    Probe: ({ result }) => {
      if (!isLife(stage)) {
        return stay(stage);
      }
      return result === "ESRCH"
        ? end
        : ([ProcessGroupStage.cases.ProbedAlive.make({}), "findWitness"] as const);
    },
    FindWitness: ({ witness }) => {
      if (!ProcessGroupStage.guards.ProbedAlive(stage)) {
        return stay(stage);
      }
      return witness === null
        ? end
        : ([ProcessGroupStage.cases.WitnessFound.make({ witness }), "killMembers"] as const);
    },
    Recheck: ({ isStillTheSame }) => {
      if (!ProcessGroupStage.guards.WitnessFound(stage)) {
        return stay(stage);
      }
      return isStillTheSame
        ? ([ProcessGroupStage.cases.StillTheSame.make({}), "killGroup"] as const)
        : ([ProcessGroupStage.cases.ChangedBeforeKill.make({}), "keep"] as const);
    },
    KillGroup: ({ result }) => {
      if (!ProcessGroupStage.guards.StillTheSame(stage)) {
        return stay(stage);
      }
      return result === "EPERM"
        ? ([ProcessGroupStage.cases.KillDenied.make({}), "checkMembers"] as const)
        : end;
    },
  });
