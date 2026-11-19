import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import {
  moveSupervisorStage,
  SupervisorAction,
  SupervisorEvent,
  SupervisorStage,
} from "../../src/lib/supervise-the-run/internal/define-supervisor-stage.ts";

const describeStage = (stage: typeof SupervisorStage.Type): string =>
  SupervisorStage.match(stage, {
    Starting: () => "Starting",
    Ready: () => "Ready",
    HoldingStop: ({ signal }) => `HoldingStop ${signal}`,
    HoldingForcedStop: ({ signals }) => `HoldingForcedStop ${signals.join(" ")}`,
    Stopping: ({ unacknowledged }) => ["Stopping", ...unacknowledged].join(" "),
    Forced: ({ unacknowledged }) => ["Forced", ...unacknowledged].join(" "),
    Swept: () => "Swept",
    Suspended: ({ resumeTo }) => `Suspended (${describeStage(resumeTo)})`,
    SweepingBeforeExit: () => "SweepingBeforeExit",
    SweepingAfterExit: () => "SweepingAfterExit",
    SweepingAfterGrace: () => "SweepingAfterGrace",
    SweepingAfterFailure: () => "SweepingAfterFailure",
    Done: () => "Done",
    Failed: () => "Failed",
  });

const describeEvent = SupervisorEvent.match({
  AcknowledgeStop: ({ signal }) => `AcknowledgeStop ${signal}`,
  ChangeChild: () => "ChangeChild",
  EndSweep: () => "EndSweep",
  Exit: ({ bySignal }) => (bySignal ? "Exit by signal" : "Exit"),
  FailJobControl: () => "FailJobControl",
  PassGrace: () => "PassGrace",
  PassHold: () => "PassHold",
  ReportReady: () => "ReportReady",
  Resume: () => "Resume",
  Stop: ({ signal }) => `Stop ${signal}`,
  StopTerminalCommand: () => "StopTerminalCommand",
  Suspend: () => "Suspend",
  SweepBeforeExit: () => "SweepBeforeExit",
});

const describeAction = SupervisorAction.match({
  FindStoppedTerminalCommands: () => "FindStoppedTerminalCommands",
  Forward: ({ signal }) => `Forward ${signal}`,
  Ignore: () => "ignored",
  None: () => "none",
  Resume: () => "Resume",
  Suspend: () => "Suspend",
  Sweep: () => "Sweep",
});

const stages = [
  SupervisorStage.cases.Starting.make({}),
  SupervisorStage.cases.Ready.make({}),
  SupervisorStage.cases.HoldingStop.make({ signal: "SIGINT" }),
  SupervisorStage.cases.HoldingForcedStop.make({ signals: ["SIGINT", "SIGTERM"] }),
  SupervisorStage.cases.Stopping.make({ unacknowledged: [] }),
  SupervisorStage.cases.Stopping.make({ unacknowledged: ["SIGINT"] }),
  SupervisorStage.cases.Forced.make({ unacknowledged: [] }),
  SupervisorStage.cases.Forced.make({ unacknowledged: ["SIGINT", "SIGTERM"] }),
  SupervisorStage.cases.Swept.make({}),
  SupervisorStage.cases.Suspended.make({ resumeTo: SupervisorStage.cases.Ready.make({}) }),
  SupervisorStage.cases.Suspended.make({
    resumeTo: SupervisorStage.cases.HoldingStop.make({ signal: "SIGINT" }),
  }),
  SupervisorStage.cases.SweepingBeforeExit.make({}),
  SupervisorStage.cases.SweepingAfterExit.make({}),
  SupervisorStage.cases.SweepingAfterGrace.make({}),
  SupervisorStage.cases.SweepingAfterFailure.make({}),
  SupervisorStage.cases.Done.make({}),
  SupervisorStage.cases.Failed.make({}),
];

const events = [
  SupervisorEvent.cases.AcknowledgeStop.make({ signal: "SIGINT" }),
  SupervisorEvent.cases.ChangeChild.make({}),
  SupervisorEvent.cases.EndSweep.make({}),
  SupervisorEvent.cases.Exit.make({ bySignal: false }),
  SupervisorEvent.cases.Exit.make({ bySignal: true }),
  SupervisorEvent.cases.FailJobControl.make({}),
  SupervisorEvent.cases.PassGrace.make({}),
  SupervisorEvent.cases.PassHold.make({}),
  SupervisorEvent.cases.ReportReady.make({}),
  SupervisorEvent.cases.Resume.make({}),
  SupervisorEvent.cases.Stop.make({ signal: "SIGHUP" }),
  SupervisorEvent.cases.Stop.make({ signal: "SIGTERM" }),
  SupervisorEvent.cases.StopTerminalCommand.make({}),
  SupervisorEvent.cases.Suspend.make({}),
  SupervisorEvent.cases.SweepBeforeExit.make({}),
];

const EXPECTED_MOVES = `
Starting | AcknowledgeStop SIGINT -> Starting | ignored
Starting | ChangeChild -> Starting | FindStoppedTerminalCommands
Starting | EndSweep -> Starting | ignored
Starting | Exit -> Done | none
Starting | Exit by signal -> SweepingAfterExit | Sweep
Starting | FailJobControl -> SweepingAfterFailure | Sweep
Starting | PassGrace -> Starting | ignored
Starting | PassHold -> Starting | ignored
Starting | ReportReady -> Ready | none
Starting | Resume -> Starting | Resume
Starting | Stop SIGHUP -> HoldingStop SIGHUP | none
Starting | Stop SIGTERM -> HoldingStop SIGTERM | none
Starting | StopTerminalCommand -> Suspended (Starting) | Suspend
Starting | Suspend -> Suspended (Starting) | Suspend
Starting | SweepBeforeExit -> Starting | ignored
Ready | AcknowledgeStop SIGINT -> Ready | ignored
Ready | ChangeChild -> Ready | FindStoppedTerminalCommands
Ready | EndSweep -> Ready | ignored
Ready | Exit -> Done | none
Ready | Exit by signal -> SweepingAfterExit | Sweep
Ready | FailJobControl -> SweepingAfterFailure | Sweep
Ready | PassGrace -> Ready | ignored
Ready | PassHold -> Ready | ignored
Ready | ReportReady -> Ready | ignored
Ready | Resume -> Ready | Resume
Ready | Stop SIGHUP -> Stopping SIGHUP | Forward SIGHUP
Ready | Stop SIGTERM -> Stopping SIGTERM | Forward SIGTERM
Ready | StopTerminalCommand -> Suspended (Ready) | Suspend
Ready | Suspend -> Suspended (Ready) | Suspend
Ready | SweepBeforeExit -> Ready | ignored
HoldingStop SIGINT | AcknowledgeStop SIGINT -> HoldingStop SIGINT | ignored
HoldingStop SIGINT | ChangeChild -> HoldingStop SIGINT | FindStoppedTerminalCommands
HoldingStop SIGINT | EndSweep -> HoldingStop SIGINT | ignored
HoldingStop SIGINT | Exit -> Done | none
HoldingStop SIGINT | Exit by signal -> SweepingAfterExit | Sweep
HoldingStop SIGINT | FailJobControl -> SweepingAfterFailure | Sweep
HoldingStop SIGINT | PassGrace -> HoldingStop SIGINT | ignored
HoldingStop SIGINT | PassHold -> Stopping SIGINT | Forward SIGINT
HoldingStop SIGINT | ReportReady -> Stopping SIGINT | Forward SIGINT
HoldingStop SIGINT | Resume -> HoldingStop SIGINT | Resume
HoldingStop SIGINT | Stop SIGHUP -> HoldingStop SIGINT | ignored
HoldingStop SIGINT | Stop SIGTERM -> HoldingForcedStop SIGINT SIGTERM | none
HoldingStop SIGINT | StopTerminalCommand -> Suspended (HoldingStop SIGINT) | Suspend
HoldingStop SIGINT | Suspend -> Suspended (HoldingStop SIGINT) | Suspend
HoldingStop SIGINT | SweepBeforeExit -> HoldingStop SIGINT | ignored
HoldingForcedStop SIGINT SIGTERM | AcknowledgeStop SIGINT -> HoldingForcedStop SIGINT SIGTERM | ignored
HoldingForcedStop SIGINT SIGTERM | ChangeChild -> HoldingForcedStop SIGINT SIGTERM | FindStoppedTerminalCommands
HoldingForcedStop SIGINT SIGTERM | EndSweep -> HoldingForcedStop SIGINT SIGTERM | ignored
HoldingForcedStop SIGINT SIGTERM | Exit -> SweepingAfterExit | Sweep
HoldingForcedStop SIGINT SIGTERM | Exit by signal -> SweepingAfterExit | Sweep
HoldingForcedStop SIGINT SIGTERM | FailJobControl -> SweepingAfterFailure | Sweep
HoldingForcedStop SIGINT SIGTERM | PassGrace -> SweepingAfterGrace | Sweep
HoldingForcedStop SIGINT SIGTERM | PassHold -> Forced SIGINT SIGTERM | Forward SIGINT
HoldingForcedStop SIGINT SIGTERM | ReportReady -> Forced SIGINT SIGTERM | Forward SIGINT
HoldingForcedStop SIGINT SIGTERM | Resume -> HoldingForcedStop SIGINT SIGTERM | Resume
HoldingForcedStop SIGINT SIGTERM | Stop SIGHUP -> HoldingForcedStop SIGINT SIGTERM | ignored
HoldingForcedStop SIGINT SIGTERM | Stop SIGTERM -> HoldingForcedStop SIGINT SIGTERM SIGTERM | none
HoldingForcedStop SIGINT SIGTERM | StopTerminalCommand -> Suspended (HoldingForcedStop SIGINT SIGTERM) | Suspend
HoldingForcedStop SIGINT SIGTERM | Suspend -> Suspended (HoldingForcedStop SIGINT SIGTERM) | Suspend
HoldingForcedStop SIGINT SIGTERM | SweepBeforeExit -> SweepingBeforeExit | none
Stopping | AcknowledgeStop SIGINT -> Stopping | ignored
Stopping | ChangeChild -> Stopping | FindStoppedTerminalCommands
Stopping | EndSweep -> Stopping | ignored
Stopping | Exit -> Done | none
Stopping | Exit by signal -> SweepingAfterExit | Sweep
Stopping | FailJobControl -> SweepingAfterFailure | Sweep
Stopping | PassGrace -> Stopping | ignored
Stopping | PassHold -> Stopping | ignored
Stopping | ReportReady -> Stopping | ignored
Stopping | Resume -> Stopping | Resume
Stopping | Stop SIGHUP -> Stopping | ignored
Stopping | Stop SIGTERM -> Forced SIGTERM | Forward SIGTERM
Stopping | StopTerminalCommand -> Suspended (Stopping) | Suspend
Stopping | Suspend -> Suspended (Stopping) | Suspend
Stopping | SweepBeforeExit -> Stopping | ignored
Stopping SIGINT | AcknowledgeStop SIGINT -> Stopping | none
Stopping SIGINT | ChangeChild -> Stopping SIGINT | FindStoppedTerminalCommands
Stopping SIGINT | EndSweep -> Stopping SIGINT | ignored
Stopping SIGINT | Exit -> Done | none
Stopping SIGINT | Exit by signal -> SweepingAfterExit | Sweep
Stopping SIGINT | FailJobControl -> SweepingAfterFailure | Sweep
Stopping SIGINT | PassGrace -> Stopping SIGINT | ignored
Stopping SIGINT | PassHold -> Stopping SIGINT | ignored
Stopping SIGINT | ReportReady -> Stopping SIGINT | ignored
Stopping SIGINT | Resume -> Stopping SIGINT | Resume
Stopping SIGINT | Stop SIGHUP -> Stopping SIGINT | ignored
Stopping SIGINT | Stop SIGTERM -> Forced SIGINT SIGTERM | none
Stopping SIGINT | StopTerminalCommand -> Suspended (Stopping SIGINT) | Suspend
Stopping SIGINT | Suspend -> Suspended (Stopping SIGINT) | Suspend
Stopping SIGINT | SweepBeforeExit -> Stopping SIGINT | ignored
Forced | AcknowledgeStop SIGINT -> Forced | ignored
Forced | ChangeChild -> Forced | FindStoppedTerminalCommands
Forced | EndSweep -> Forced | ignored
Forced | Exit -> SweepingAfterExit | Sweep
Forced | Exit by signal -> SweepingAfterExit | Sweep
Forced | FailJobControl -> SweepingAfterFailure | Sweep
Forced | PassGrace -> SweepingAfterGrace | Sweep
Forced | PassHold -> Forced | ignored
Forced | ReportReady -> Forced | ignored
Forced | Resume -> Forced | Resume
Forced | Stop SIGHUP -> Forced | ignored
Forced | Stop SIGTERM -> Forced SIGTERM | Forward SIGTERM
Forced | StopTerminalCommand -> Suspended (Forced) | Suspend
Forced | Suspend -> Suspended (Forced) | Suspend
Forced | SweepBeforeExit -> SweepingBeforeExit | none
Forced SIGINT SIGTERM | AcknowledgeStop SIGINT -> Forced SIGTERM | Forward SIGTERM
Forced SIGINT SIGTERM | ChangeChild -> Forced SIGINT SIGTERM | FindStoppedTerminalCommands
Forced SIGINT SIGTERM | EndSweep -> Forced SIGINT SIGTERM | ignored
Forced SIGINT SIGTERM | Exit -> SweepingAfterExit | Sweep
Forced SIGINT SIGTERM | Exit by signal -> SweepingAfterExit | Sweep
Forced SIGINT SIGTERM | FailJobControl -> SweepingAfterFailure | Sweep
Forced SIGINT SIGTERM | PassGrace -> SweepingAfterGrace | Sweep
Forced SIGINT SIGTERM | PassHold -> Forced SIGINT SIGTERM | ignored
Forced SIGINT SIGTERM | ReportReady -> Forced SIGINT SIGTERM | ignored
Forced SIGINT SIGTERM | Resume -> Forced SIGINT SIGTERM | Resume
Forced SIGINT SIGTERM | Stop SIGHUP -> Forced SIGINT SIGTERM | ignored
Forced SIGINT SIGTERM | Stop SIGTERM -> Forced SIGINT SIGTERM SIGTERM | none
Forced SIGINT SIGTERM | StopTerminalCommand -> Suspended (Forced SIGINT SIGTERM) | Suspend
Forced SIGINT SIGTERM | Suspend -> Suspended (Forced SIGINT SIGTERM) | Suspend
Forced SIGINT SIGTERM | SweepBeforeExit -> SweepingBeforeExit | none
Swept | AcknowledgeStop SIGINT -> Swept | ignored
Swept | ChangeChild -> Swept | FindStoppedTerminalCommands
Swept | EndSweep -> Swept | ignored
Swept | Exit -> Done | none
Swept | Exit by signal -> Done | none
Swept | FailJobControl -> SweepingAfterFailure | Sweep
Swept | PassGrace -> Swept | ignored
Swept | PassHold -> Swept | ignored
Swept | ReportReady -> Swept | ignored
Swept | Resume -> Swept | Resume
Swept | Stop SIGHUP -> Swept | ignored
Swept | Stop SIGTERM -> Swept | ignored
Swept | StopTerminalCommand -> Suspended (Swept) | Suspend
Swept | Suspend -> Suspended (Swept) | Suspend
Swept | SweepBeforeExit -> Swept | ignored
Suspended (Ready) | AcknowledgeStop SIGINT -> Suspended (Ready) | ignored
Suspended (Ready) | ChangeChild -> Suspended (Ready) | FindStoppedTerminalCommands
Suspended (Ready) | EndSweep -> Suspended (Ready) | ignored
Suspended (Ready) | Exit -> Done | none
Suspended (Ready) | Exit by signal -> SweepingAfterExit | Sweep
Suspended (Ready) | FailJobControl -> SweepingAfterFailure | Sweep
Suspended (Ready) | PassGrace -> Suspended (Ready) | ignored
Suspended (Ready) | PassHold -> Suspended (Ready) | ignored
Suspended (Ready) | ReportReady -> Suspended (Ready) | ignored
Suspended (Ready) | Resume -> Ready | Resume
Suspended (Ready) | Stop SIGHUP -> Suspended (Stopping SIGHUP) | Forward SIGHUP
Suspended (Ready) | Stop SIGTERM -> Suspended (Stopping SIGTERM) | Forward SIGTERM
Suspended (Ready) | StopTerminalCommand -> Suspended (Ready) | ignored
Suspended (Ready) | Suspend -> Suspended (Ready) | Suspend
Suspended (Ready) | SweepBeforeExit -> Suspended (Ready) | ignored
Suspended (HoldingStop SIGINT) | AcknowledgeStop SIGINT -> Suspended (HoldingStop SIGINT) | ignored
Suspended (HoldingStop SIGINT) | ChangeChild -> Suspended (HoldingStop SIGINT) | FindStoppedTerminalCommands
Suspended (HoldingStop SIGINT) | EndSweep -> Suspended (HoldingStop SIGINT) | ignored
Suspended (HoldingStop SIGINT) | Exit -> Done | none
Suspended (HoldingStop SIGINT) | Exit by signal -> SweepingAfterExit | Sweep
Suspended (HoldingStop SIGINT) | FailJobControl -> SweepingAfterFailure | Sweep
Suspended (HoldingStop SIGINT) | PassGrace -> Suspended (HoldingStop SIGINT) | ignored
Suspended (HoldingStop SIGINT) | PassHold -> Suspended (Stopping SIGINT) | Forward SIGINT
Suspended (HoldingStop SIGINT) | ReportReady -> Suspended (Stopping SIGINT) | Forward SIGINT
Suspended (HoldingStop SIGINT) | Resume -> HoldingStop SIGINT | Resume
Suspended (HoldingStop SIGINT) | Stop SIGHUP -> Suspended (HoldingStop SIGINT) | ignored
Suspended (HoldingStop SIGINT) | Stop SIGTERM -> Suspended (HoldingForcedStop SIGINT SIGTERM) | none
Suspended (HoldingStop SIGINT) | StopTerminalCommand -> Suspended (HoldingStop SIGINT) | ignored
Suspended (HoldingStop SIGINT) | Suspend -> Suspended (HoldingStop SIGINT) | Suspend
Suspended (HoldingStop SIGINT) | SweepBeforeExit -> Suspended (HoldingStop SIGINT) | ignored
SweepingBeforeExit | AcknowledgeStop SIGINT -> SweepingBeforeExit | ignored
SweepingBeforeExit | ChangeChild -> SweepingBeforeExit | ignored
SweepingBeforeExit | EndSweep -> Swept | none
SweepingBeforeExit | Exit -> SweepingAfterExit | none
SweepingBeforeExit | Exit by signal -> SweepingAfterExit | none
SweepingBeforeExit | FailJobControl -> SweepingBeforeExit | ignored
SweepingBeforeExit | PassGrace -> SweepingBeforeExit | ignored
SweepingBeforeExit | PassHold -> SweepingBeforeExit | ignored
SweepingBeforeExit | ReportReady -> SweepingBeforeExit | ignored
SweepingBeforeExit | Resume -> SweepingBeforeExit | ignored
SweepingBeforeExit | Stop SIGHUP -> SweepingBeforeExit | ignored
SweepingBeforeExit | Stop SIGTERM -> SweepingBeforeExit | ignored
SweepingBeforeExit | StopTerminalCommand -> SweepingBeforeExit | ignored
SweepingBeforeExit | Suspend -> SweepingBeforeExit | ignored
SweepingBeforeExit | SweepBeforeExit -> SweepingBeforeExit | ignored
SweepingAfterExit | AcknowledgeStop SIGINT -> SweepingAfterExit | ignored
SweepingAfterExit | ChangeChild -> SweepingAfterExit | ignored
SweepingAfterExit | EndSweep -> Done | none
SweepingAfterExit | Exit -> SweepingAfterExit | ignored
SweepingAfterExit | Exit by signal -> SweepingAfterExit | ignored
SweepingAfterExit | FailJobControl -> SweepingAfterExit | ignored
SweepingAfterExit | PassGrace -> SweepingAfterExit | ignored
SweepingAfterExit | PassHold -> SweepingAfterExit | ignored
SweepingAfterExit | ReportReady -> SweepingAfterExit | ignored
SweepingAfterExit | Resume -> SweepingAfterExit | ignored
SweepingAfterExit | Stop SIGHUP -> SweepingAfterExit | ignored
SweepingAfterExit | Stop SIGTERM -> SweepingAfterExit | ignored
SweepingAfterExit | StopTerminalCommand -> SweepingAfterExit | ignored
SweepingAfterExit | Suspend -> SweepingAfterExit | ignored
SweepingAfterExit | SweepBeforeExit -> SweepingAfterExit | ignored
SweepingAfterGrace | AcknowledgeStop SIGINT -> SweepingAfterGrace | ignored
SweepingAfterGrace | ChangeChild -> SweepingAfterGrace | ignored
SweepingAfterGrace | EndSweep -> Swept | none
SweepingAfterGrace | Exit -> SweepingAfterGrace | ignored
SweepingAfterGrace | Exit by signal -> SweepingAfterGrace | ignored
SweepingAfterGrace | FailJobControl -> SweepingAfterGrace | ignored
SweepingAfterGrace | PassGrace -> SweepingAfterGrace | ignored
SweepingAfterGrace | PassHold -> SweepingAfterGrace | ignored
SweepingAfterGrace | ReportReady -> SweepingAfterGrace | ignored
SweepingAfterGrace | Resume -> SweepingAfterGrace | ignored
SweepingAfterGrace | Stop SIGHUP -> SweepingAfterGrace | ignored
SweepingAfterGrace | Stop SIGTERM -> SweepingAfterGrace | ignored
SweepingAfterGrace | StopTerminalCommand -> SweepingAfterGrace | ignored
SweepingAfterGrace | Suspend -> SweepingAfterGrace | ignored
SweepingAfterGrace | SweepBeforeExit -> SweepingAfterGrace | ignored
SweepingAfterFailure | AcknowledgeStop SIGINT -> SweepingAfterFailure | ignored
SweepingAfterFailure | ChangeChild -> SweepingAfterFailure | ignored
SweepingAfterFailure | EndSweep -> Failed | none
SweepingAfterFailure | Exit -> SweepingAfterFailure | ignored
SweepingAfterFailure | Exit by signal -> SweepingAfterFailure | ignored
SweepingAfterFailure | FailJobControl -> SweepingAfterFailure | ignored
SweepingAfterFailure | PassGrace -> SweepingAfterFailure | ignored
SweepingAfterFailure | PassHold -> SweepingAfterFailure | ignored
SweepingAfterFailure | ReportReady -> SweepingAfterFailure | ignored
SweepingAfterFailure | Resume -> SweepingAfterFailure | ignored
SweepingAfterFailure | Stop SIGHUP -> SweepingAfterFailure | ignored
SweepingAfterFailure | Stop SIGTERM -> SweepingAfterFailure | ignored
SweepingAfterFailure | StopTerminalCommand -> SweepingAfterFailure | ignored
SweepingAfterFailure | Suspend -> SweepingAfterFailure | ignored
SweepingAfterFailure | SweepBeforeExit -> SweepingAfterFailure | ignored
Done | AcknowledgeStop SIGINT -> Done | ignored
Done | ChangeChild -> Done | ignored
Done | EndSweep -> Done | ignored
Done | Exit -> Done | ignored
Done | Exit by signal -> Done | ignored
Done | FailJobControl -> Done | ignored
Done | PassGrace -> Done | ignored
Done | PassHold -> Done | ignored
Done | ReportReady -> Done | ignored
Done | Resume -> Done | ignored
Done | Stop SIGHUP -> Done | ignored
Done | Stop SIGTERM -> Done | ignored
Done | StopTerminalCommand -> Done | ignored
Done | Suspend -> Done | ignored
Done | SweepBeforeExit -> Done | ignored
Failed | AcknowledgeStop SIGINT -> Failed | ignored
Failed | ChangeChild -> Failed | ignored
Failed | EndSweep -> Failed | ignored
Failed | Exit -> Failed | ignored
Failed | Exit by signal -> Failed | ignored
Failed | FailJobControl -> Failed | ignored
Failed | PassGrace -> Failed | ignored
Failed | PassHold -> Failed | ignored
Failed | ReportReady -> Failed | ignored
Failed | Resume -> Failed | ignored
Failed | Stop SIGHUP -> Failed | ignored
Failed | Stop SIGTERM -> Failed | ignored
Failed | StopTerminalCommand -> Failed | ignored
Failed | Suspend -> Failed | ignored
Failed | SweepBeforeExit -> Failed | ignored
`;

export const everyMove = Task.make({
  name: "the supervisor's run lifecycle moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = stages.flatMap((stage) =>
      events.map((event) => {
        const [next, action] = moveSupervisorStage(stage, event);
        return `${describeStage(stage)} | ${describeEvent(event)} -> ${describeStage(next)} | ${describeAction(action)}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_MOVES);
  }),
});
