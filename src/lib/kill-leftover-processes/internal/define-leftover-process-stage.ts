import { Schema } from "effect";

import { SignalResult } from "./define-process-group-stage.ts";

export const LeftoverProcessStage = Schema.TaggedUnion({
  Listed: {},
  Owned: {},
  CommandRead: {},
  StillTheSame: {},
  Signalled: { result: Schema.Literals(["EPERM", "sent"]) },
  Killed: {},
  NotKillable: {},
  AlreadyReported: {},
  NotOwned: {},
  Replaced: {},
  Gone: {},
});

export const LeftoverProcessEvent = Schema.TaggedUnion({
  Claim: { isFirstReport: Schema.Boolean },
  Inspect: { isListed: Schema.Boolean, isOwned: Schema.Boolean },
  ReadCommand: { hasCommand: Schema.Boolean },
  Recheck: { isStillTheSame: Schema.Boolean },
  Signal: { result: SignalResult },
});

const stay = (stage: typeof LeftoverProcessStage.Type) => [stage, "none"] as const;

const gone = [LeftoverProcessStage.cases.Gone.make({}), "none"] as const;

const moveOnInspect = (
  stage: typeof LeftoverProcessStage.Type,
  event: typeof LeftoverProcessEvent.cases.Inspect.Type,
) => {
  if (!LeftoverProcessStage.guards.Listed(stage)) {
    return stay(stage);
  }
  if (!event.isListed) {
    return gone;
  }
  return event.isOwned
    ? ([LeftoverProcessStage.cases.Owned.make({}), "readCommand"] as const)
    : ([LeftoverProcessStage.cases.NotOwned.make({}), "none"] as const);
};

const moveOnClaim = (
  stage: typeof LeftoverProcessStage.Type,
  event: typeof LeftoverProcessEvent.cases.Claim.Type,
) => {
  if (!LeftoverProcessStage.guards.Signalled(stage)) {
    return stay(stage);
  }
  if (!event.isFirstReport) {
    return [LeftoverProcessStage.cases.AlreadyReported.make({}), "none"] as const;
  }
  return stage.result === "sent"
    ? ([LeftoverProcessStage.cases.Killed.make({}), "reportKilled"] as const)
    : ([LeftoverProcessStage.cases.NotKillable.make({}), "reportNotKillable"] as const);
};

export const moveLeftoverProcessStage = (
  stage: typeof LeftoverProcessStage.Type,
  event: typeof LeftoverProcessEvent.Type,
) =>
  LeftoverProcessEvent.match(event, {
    Claim: (claim) => moveOnClaim(stage, claim),
    Inspect: (inspect) => moveOnInspect(stage, inspect),
    ReadCommand: ({ hasCommand }) => {
      if (!LeftoverProcessStage.guards.Owned(stage)) {
        return stay(stage);
      }
      return hasCommand
        ? ([LeftoverProcessStage.cases.CommandRead.make({}), "recheck"] as const)
        : gone;
    },
    Recheck: ({ isStillTheSame }) => {
      if (!LeftoverProcessStage.guards.CommandRead(stage)) {
        return stay(stage);
      }
      return isStillTheSame
        ? ([LeftoverProcessStage.cases.StillTheSame.make({}), "signal"] as const)
        : ([LeftoverProcessStage.cases.Replaced.make({}), "none"] as const);
    },
    Signal: ({ result }) => {
      if (!LeftoverProcessStage.guards.StillTheSame(stage)) {
        return stay(stage);
      }
      return result === "ESRCH"
        ? gone
        : ([LeftoverProcessStage.cases.Signalled.make({ result }), "claim"] as const);
    },
  });
