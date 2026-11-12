import { Schema } from "effect";

import { STOP_SIGNALS } from "../../../shared/stop-signals.ts";

const StopSignal = Schema.Literals(STOP_SIGNALS);

const HeldSignals = Schema.NonEmptyArray(StopSignal);

const Unacknowledged = { unacknowledged: Schema.Array(StopSignal) };

const LIVE_STAGES = {
  Starting: {},
  Ready: {},
  HoldingStop: { signal: StopSignal },
  HoldingForcedStop: { signals: HeldSignals },
  Stopping: Unacknowledged,
  Forced: Unacknowledged,
  Swept: {},
};

const LiveStage = Schema.TaggedUnion(LIVE_STAGES);

export const SupervisorStage = Schema.TaggedUnion({
  ...LIVE_STAGES,
  Suspended: { resumeTo: LiveStage },
  SweepingAfterExit: {},
  SweepingAfterGrace: {},
  SweepingAfterFailure: {},
  Done: {},
  Failed: {},
});

export const SupervisorEvent = Schema.TaggedUnion({
  AcknowledgeStop: { signal: StopSignal },
  EndSweep: {},
  Exit: { bySignal: Schema.Boolean },
  FailJobControl: {},
  PassGrace: {},
  PassHold: {},
  ReportReady: {},
  Resume: {},
  Stop: { signal: StopSignal },
  Suspend: {},
});

export const SupervisorAction = Schema.TaggedUnion({
  Forward: { signal: StopSignal },
  Ignore: {},
  None: {},
  Resume: {},
  Suspend: {},
  Sweep: {},
});

const isLive = SupervisorStage.isAnyOf([
  "Forced",
  "HoldingForcedStop",
  "HoldingStop",
  "Ready",
  "Starting",
  "Stopping",
  "Swept",
]);

const isForced = SupervisorStage.isAnyOf(["Forced", "HoldingForcedStop"]);

const isStopping = SupervisorStage.isAnyOf([
  "Forced",
  "HoldingForcedStop",
  "HoldingStop",
  "Stopping",
]);

export const isEnded = SupervisorStage.isAnyOf(["Done", "Failed"]);

const unwrapSuspended = (stage: typeof SupervisorStage.Type) =>
  SupervisorStage.guards.Suspended(stage) ? stage.resumeTo : stage;

export const hasReceivedStop = (stage: typeof SupervisorStage.Type) =>
  isStopping(unwrapSuspended(stage));

export const hasBeenForced = (stage: typeof SupervisorStage.Type) =>
  isForced(unwrapSuspended(stage));

const to = (stage: typeof SupervisorStage.Type, action: typeof SupervisorAction.Type) =>
  [stage, action] as const;

const ignore = (stage: typeof SupervisorStage.Type) =>
  to(stage, SupervisorAction.cases.Ignore.make({}));

const none = (stage: typeof SupervisorStage.Type) =>
  to(stage, SupervisorAction.cases.None.make({}));

const forward = (stage: typeof SupervisorStage.Type, signal: (typeof STOP_SIGNALS)[number]) =>
  to(stage, SupervisorAction.cases.Forward.make({ signal }));

const forwardFirst = (
  stage: typeof SupervisorStage.Type,
  unacknowledged: readonly (typeof STOP_SIGNALS)[number][],
) => {
  const [first] = unacknowledged;
  return first === undefined ? none(stage) : forward(stage, first);
};

const forwardUnlessWaiting = (
  stage: typeof SupervisorStage.Type,
  waitingOn: readonly (typeof STOP_SIGNALS)[number][],
  signal: (typeof STOP_SIGNALS)[number],
) => (waitingOn.length === 0 ? forward(stage, signal) : none(stage));

const sweep = (stage: typeof SupervisorStage.Type) =>
  to(stage, SupervisorAction.cases.Sweep.make({}));

const releaseHeld = (stage: typeof LiveStage.Type) => {
  if (LiveStage.guards.HoldingStop(stage)) {
    return forward(
      SupervisorStage.cases.Stopping.make({ unacknowledged: [stage.signal] }),
      stage.signal,
    );
  }
  return LiveStage.guards.HoldingForcedStop(stage)
    ? forwardFirst(
        SupervisorStage.cases.Forced.make({ unacknowledged: stage.signals }),
        stage.signals,
      )
    : ignore(stage);
};

const isForwarding = LiveStage.isAnyOf(["Forced", "Stopping"]);

const moveOnAcknowledge = (stage: typeof LiveStage.Type, signal: (typeof STOP_SIGNALS)[number]) => {
  if (!isForwarding(stage)) {
    return ignore(stage);
  }
  const isTheForwardedStop = stage.unacknowledged[0] === signal;
  if (!isTheForwardedStop) {
    return ignore(stage);
  }
  const unacknowledged = stage.unacknowledged.slice(1);
  return forwardFirst(
    LiveStage.guards.Forced(stage)
      ? SupervisorStage.cases.Forced.make({ unacknowledged })
      : SupervisorStage.cases.Stopping.make({ unacknowledged }),
    unacknowledged,
  );
};

const moveOnExit = (stage: typeof LiveStage.Type, bySignal: boolean) => {
  if (LiveStage.guards.Swept(stage)) {
    return none(SupervisorStage.cases.Done.make({}));
  }
  const skippedItsCleanup = bySignal || isForced(stage);
  return skippedItsCleanup
    ? sweep(SupervisorStage.cases.SweepingAfterExit.make({}))
    : none(SupervisorStage.cases.Done.make({}));
};

const moveOnStop = (stage: typeof LiveStage.Type, signal: (typeof STOP_SIGNALS)[number]) => {
  const isHangUpDuringStop = signal === "SIGHUP" && isStopping(stage);
  if (isHangUpDuringStop) {
    return ignore(stage);
  }
  return LiveStage.match(stage, {
    Starting: () => none(SupervisorStage.cases.HoldingStop.make({ signal })),
    Ready: () => forward(SupervisorStage.cases.Stopping.make({ unacknowledged: [signal] }), signal),
    HoldingStop: (held) =>
      none(SupervisorStage.cases.HoldingForcedStop.make({ signals: [held.signal, signal] })),
    HoldingForcedStop: (held) =>
      none(SupervisorStage.cases.HoldingForcedStop.make({ signals: [...held.signals, signal] })),
    Stopping: ({ unacknowledged }) =>
      forwardUnlessWaiting(
        SupervisorStage.cases.Forced.make({ unacknowledged: [...unacknowledged, signal] }),
        unacknowledged,
        signal,
      ),
    Forced: ({ unacknowledged }) =>
      forwardUnlessWaiting(
        SupervisorStage.cases.Forced.make({ unacknowledged: [...unacknowledged, signal] }),
        unacknowledged,
        signal,
      ),
    Swept: () => ignore(stage),
  });
};

const moveLive = (stage: typeof LiveStage.Type, event: typeof SupervisorEvent.Type) =>
  SupervisorEvent.match(event, {
    AcknowledgeStop: ({ signal }) => moveOnAcknowledge(stage, signal),
    EndSweep: () => ignore(stage),
    Exit: ({ bySignal }) => moveOnExit(stage, bySignal),
    FailJobControl: () => sweep(SupervisorStage.cases.SweepingAfterFailure.make({})),
    PassGrace: () =>
      isForced(stage) ? sweep(SupervisorStage.cases.SweepingAfterGrace.make({})) : ignore(stage),
    PassHold: () => releaseHeld(stage),
    ReportReady: () =>
      LiveStage.guards.Starting(stage)
        ? none(SupervisorStage.cases.Ready.make({}))
        : releaseHeld(stage),
    Resume: () => to(stage, SupervisorAction.cases.Resume.make({})),
    Stop: ({ signal }) => moveOnStop(stage, signal),
    Suspend: () =>
      to(
        SupervisorStage.cases.Suspended.make({ resumeTo: stage }),
        SupervisorAction.cases.Suspend.make({}),
      ),
  });

const moveSuspended = (resumeTo: typeof LiveStage.Type, event: typeof SupervisorEvent.Type) => {
  if (SupervisorEvent.guards.Resume(event)) {
    return to(resumeTo, SupervisorAction.cases.Resume.make({}));
  }
  const [next, action] = SupervisorEvent.guards.Suspend(event)
    ? to(resumeTo, SupervisorAction.cases.Suspend.make({}))
    : moveLive(resumeTo, event);
  return isLive(next)
    ? to(SupervisorStage.cases.Suspended.make({ resumeTo: next }), action)
    : to(next, action);
};

const endSweep = (
  stage: typeof SupervisorStage.Type,
  next: typeof SupervisorStage.Type,
  event: typeof SupervisorEvent.Type,
) => (SupervisorEvent.guards.EndSweep(event) ? none(next) : ignore(stage));

export const moveSupervisorStage = (
  stage: typeof SupervisorStage.Type,
  event: typeof SupervisorEvent.Type,
) =>
  SupervisorStage.match(stage, {
    Starting: (live) => moveLive(live, event),
    Ready: (live) => moveLive(live, event),
    HoldingStop: (live) => moveLive(live, event),
    HoldingForcedStop: (live) => moveLive(live, event),
    Stopping: (live) => moveLive(live, event),
    Forced: (live) => moveLive(live, event),
    Swept: (live) => moveLive(live, event),
    Suspended: ({ resumeTo }) => moveSuspended(resumeTo, event),
    SweepingAfterExit: () => endSweep(stage, SupervisorStage.cases.Done.make({}), event),
    SweepingAfterGrace: () => endSweep(stage, SupervisorStage.cases.Swept.make({}), event),
    SweepingAfterFailure: () => endSweep(stage, SupervisorStage.cases.Failed.make({}), event),
    Done: () => ignore(stage),
    Failed: () => ignore(stage),
  });
