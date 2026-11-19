import { Effect } from "effect";

import { processIdentityKey } from "../../../shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import { labelLine } from "../../../shared/label-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { printLine, printLineAboutTask } from "../../../shared/print-to-standard-error.ts";
import type { RunMarker } from "../../../shared/run-marker.ts";
import {
  LeftoverProcessEvent,
  LeftoverProcessStage,
  moveLeftoverProcessStage,
} from "./define-leftover-process-stage.ts";
import {
  lifeStageOf,
  moveProcessGroupStage,
  ProcessGroupEvent,
  ProcessGroupStage,
} from "./define-process-group-stage.ts";
import { TrackedProcessGroupEvent } from "./define-tracked-process-group-stage.ts";
import { findDescendantProcessIds, findSessionProcessIds } from "./find-process-ids.ts";
import {
  claimUnknownOwnership,
  describeUnknownOwnership,
  findPossibleRunProcesses,
} from "./find-unknown-ownership.ts";
import {
  isStillListedAs,
  listProcesses,
  processGroupHasOnlyEndedMembers,
} from "./list-processes.ts";
import { findMarkedProcesses } from "./mark-run-processes.ts";
import { ProcessNotKillable } from "./process-cleanup-errors.ts";
import { sendSignal } from "./send-signal.ts";
import { findStartedGroupWitness, moveTrackedProcessGroup } from "./started-process-groups.ts";
import type { startedProcessGroup } from "./started-process-groups.ts";

const PROBE_SIGNAL = 0;

const reportKilledProcesses = (
  reportedResults: readonly {
    readonly processId: number;
    readonly command: string;
    readonly ownerTaskName: string | null;
    readonly action: "reportKilled" | "reportNotKillable";
  }[],
) =>
  Effect.forEach(
    reportedResults,
    (killResult) =>
      Effect.gen(function* () {
        const isKilled = killResult.action === "reportKilled";
        const killedProcess = `${killResult.processId} (${killResult.command})`;
        yield* isKilled
          ? printLineAboutTask(
              killResult.ownerTaskName,
              labelLine(
                "killed",
                killResult.ownerTaskName,
                killResult.ownerTaskName === null
                  ? `${killedProcess}, started by no known task`
                  : killedProcess,
              ),
            )
          : printLineAboutTask(
              killResult.ownerTaskName,
              formatFailureLines(
                new ProcessNotKillable({
                  processId: killResult.processId,
                  command: killResult.command,
                  ownerTaskName: killResult.ownerTaskName,
                }),
              ),
            );
        yield* Effect.logInfo(isKilled ? "process killed" : "process could not be killed").pipe(
          Effect.annotateLogs({
            "process.pid": killResult.processId,
            "process.executable.name": killResult.command,
            ...(killResult.ownerTaskName === null
              ? {}
              : { [`${TASK_TRACE_LABEL_PREFIX}.task`]: killResult.ownerTaskName }),
          }),
        );
      }),
    { discard: true },
  );

const reportUnknownOwnership = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  possibleRunProcesses: ReadonlyMap<number, unknown>,
  claimUnknownOwnershipToReport: Parameters<typeof claimUnknownOwnership>[1],
) =>
  describeUnknownOwnership(listedProcesses, possibleRunProcesses).pipe(
    Effect.flatMap((unknownOwnerships) =>
      claimUnknownOwnership(unknownOwnerships, claimUnknownOwnershipToReport),
    ),
    Effect.flatMap((claimed) =>
      Effect.forEach(
        claimed,
        (unknownOwnership) => printLine(formatFailureLines(unknownOwnership)),
        { discard: true },
      ),
    ),
  );

const probeProcessGroups = Effect.fn(function* (
  taskName: string | null,
  selectedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
  sendSignalNow: typeof sendSignal,
) {
  return yield* Effect.forEach([...selectedProcessGroups], ([processGroupId, startedGroup]) =>
    Effect.map(sendSignalNow(taskName, -processGroupId, PROBE_SIGNAL), (result) => ({
      processGroupId,
      startedGroup,
      stage: moveProcessGroupStage(
        lifeStageOf(startedGroup),
        ProcessGroupEvent.cases.Probe.make({ result }),
      )[0],
    })),
  );
});

const findEndedProcessGroups = (
  groups: readonly {
    readonly processGroupId: number;
    readonly startedGroup: ReturnType<typeof startedProcessGroup>;
    readonly stage: typeof ProcessGroupStage.Type;
  }[],
) =>
  new Map(
    groups
      .filter((group) => ProcessGroupStage.guards.Ended(group.stage))
      .map((group) => [group.processGroupId, group.startedGroup] as const),
  );

const terminateSelectedProcesses = Effect.fn(function* (input: {
  readonly startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>;
  readonly probedGroups: Effect.Success<ReturnType<typeof probeProcessGroups>>;
  readonly ownsProcess: (ownerTaskName: string | null) => boolean;
  readonly taskName: string | null;
  readonly sessionId: number | null;
  readonly runMarker: typeof RunMarker.Type | null;
  readonly killsMarkedProcesses: boolean;
  readonly claimUnknownOwnershipToReport: Parameters<typeof claimUnknownOwnership>[1] | null;
  readonly adoptingProcessId: number | null;
  readonly listProcessesNow: typeof listProcesses;
  readonly sendSignalNow: typeof sendSignal;
  readonly claimProcessesToReport: (
    processes: readonly (typeof ProcessIdentity.Type)[],
  ) => Effect.Effect<readonly (typeof ProcessIdentity.Type)[]>;
}) {
  const listedProcesses = yield* input.listProcessesNow(input.taskName);
  const findChildProcessIds = (parentProcessIds: readonly number[]) =>
    [...listedProcesses]
      .filter(([, listedProcess]) => parentProcessIds.includes(listedProcess.parentProcessId))
      .map(([processId]) => processId);
  const childProcessIds = findChildProcessIds([process.pid]);
  const adoptedProcessIds =
    input.adoptingProcessId === null
      ? []
      : findChildProcessIds([input.adoptingProcessId]).filter(
          (processId) => processId !== process.pid,
        );
  const probedGroupIds = new Set(input.probedGroups.map((group) => group.processGroupId));
  const { markedProcesses, markedIdentities, unreadableProcesses } = yield* findMarkedProcesses(
    input.runMarker,
    input.killsMarkedProcesses
      ? listedProcesses
      : new Map(
          [...listedProcesses].filter(([, listedProcess]) =>
            probedGroupIds.has(listedProcess.processGroupId),
          ),
        ),
  );
  const witnessedGroups = input.probedGroups.map((group) => ({
    ...group,
    stage: moveProcessGroupStage(
      group.stage,
      ProcessGroupEvent.cases.FindWitness.make({
        witness: ProcessGroupStage.guards.ProbedAlive(group.stage)
          ? findStartedGroupWitness(
              listedProcesses,
              group.processGroupId,
              group.startedGroup,
              markedIdentities,
            )
          : null,
      }),
    )[0],
  }));
  const recheckedGroups = yield* Effect.forEach(witnessedGroups, (group) =>
    ProcessGroupStage.guards.WitnessFound(group.stage)
      ? Effect.map(isStillListedAs(input.taskName, group.stage.witness), (isStillTheSame) => ({
          ...group,
          stage: moveProcessGroupStage(
            group.stage,
            ProcessGroupEvent.cases.Recheck.make({ isStillTheSame }),
          )[0],
        }))
      : Effect.succeed(group),
  );
  const killableGroups = new Map(
    recheckedGroups
      .filter((group) => ProcessGroupStage.guards.StillTheSame(group.stage))
      .map((group) => [group.processGroupId, group.startedGroup.taskName] as const),
  );

  const processIds = [
    ...findDescendantProcessIds(listedProcesses, [...childProcessIds, ...adoptedProcessIds]),
    ...findSessionProcessIds(listedProcesses, input.sessionId),
    ...(input.killsMarkedProcesses ? markedProcesses.keys() : []),
    ...[...listedProcesses]
      .filter(([, listedProcess]) => killableGroups.has(listedProcess.processGroupId))
      .map(([processId]) => processId),
  ];
  const processRows = new Map(
    processIds.flatMap((processId) => {
      const listedProcess = listedProcesses.get(processId);
      const isGone = listedProcess === undefined || listedProcess.hasEnded;
      return isGone ? [] : [[processId, listedProcess] as const];
    }),
  );

  const findOwner = (processId: number): string | null => {
    const row = processRows.get(processId);
    if (row === undefined) {
      return null;
    }

    return (
      input.startedProcessGroups.get(processId)?.taskName ??
      findOwner(row.parentProcessId) ??
      killableGroups.get(row.processGroupId) ??
      markedProcesses.get(processId) ??
      null
    );
  };
  const leftovers = (yield* Effect.forEach(
    [...new Set(processIds)].flatMap((processId) => {
      const row = processRows.get(processId);
      const ownerTaskName = findOwner(processId);
      const [stage] = moveLeftoverProcessStage(
        LeftoverProcessStage.cases.Listed.make({}),
        LeftoverProcessEvent.cases.Inspect.make({
          isListed: row !== undefined,
          isOwned: input.ownsProcess(ownerTaskName),
        }),
      );
      const isOwnedAndListed = row !== undefined && LeftoverProcessStage.guards.Owned(stage);
      return isOwnedAndListed ? [{ processId, row, ownerTaskName, stage }] : [];
    }),
    (leftover) =>
      Effect.gen(function* () {
        const command = yield* leftover.row.readCommandLineUnlessGone;
        const [stage] = moveLeftoverProcessStage(
          leftover.stage,
          LeftoverProcessEvent.cases.ReadCommand.make({ hasCommand: command !== null }),
        );
        const hasReadCommand = command !== null && LeftoverProcessStage.guards.CommandRead(stage);
        return hasReadCommand
          ? [
              {
                processId: leftover.processId,
                startTime: leftover.row.startTime,
                processGroupId: leftover.row.processGroupId,
                command,
                ownerTaskName: leftover.ownerTaskName,
                stage,
              },
            ]
          : [];
      }),
  )).flat();
  const signalledLeftovers = yield* Effect.forEach(leftovers, (leftover) =>
    Effect.gen(function* () {
      const [rechecked] = moveLeftoverProcessStage(
        leftover.stage,
        LeftoverProcessEvent.cases.Recheck.make({
          isStillTheSame: yield* isStillListedAs(input.taskName, leftover),
        }),
      );
      if (!LeftoverProcessStage.guards.StillTheSame(rechecked)) {
        return { ...leftover, stage: rechecked };
      }
      const result = yield* input.sendSignalNow(input.taskName, leftover.processId, "SIGKILL");
      return {
        ...leftover,
        stage: moveLeftoverProcessStage(
          rechecked,
          LeftoverProcessEvent.cases.Signal.make({ result }),
        )[0],
      };
    }),
  );
  const killedGroups = yield* Effect.forEach(recheckedGroups, (group) =>
    ProcessGroupStage.guards.StillTheSame(group.stage)
      ? Effect.map(
          input.sendSignalNow(input.taskName, -group.processGroupId, "SIGKILL"),
          (result) => ({
            ...group,
            stage: moveProcessGroupStage(
              group.stage,
              ProcessGroupEvent.cases.KillGroup.make({ result }),
            )[0],
          }),
        )
      : Effect.succeed(group),
  );
  const checkedGroups = yield* Effect.forEach(killedGroups, (group) =>
    ProcessGroupStage.guards.KillDenied(group.stage)
      ? Effect.map(
          processGroupHasOnlyEndedMembers(input.taskName, group.processGroupId),
          (hasOnlyEndedMembers) => ({
            ...group,
            stage: moveProcessGroupStage(
              group.stage,
              ProcessGroupEvent.cases.CheckMembers.make({ hasOnlyEndedMembers }),
            )[0],
          }),
        )
      : Effect.succeed(group),
  );
  const signaledResults = signalledLeftovers.flatMap((leftover) =>
    LeftoverProcessStage.guards.Signalled(leftover.stage)
      ? [{ ...leftover, stage: leftover.stage }]
      : [],
  );
  const firstReportedProcessKeys = new Set(
    (yield* input.claimProcessesToReport(
      signaledResults.map((killResult) => ({
        processId: killResult.processId,
        startTime: killResult.startTime,
      })),
    )).map((identity) => processIdentityKey(identity)),
  );
  const reportedResults = signaledResults.flatMap((killResult) => {
    const [, action] = moveLeftoverProcessStage(
      killResult.stage,
      LeftoverProcessEvent.cases.Claim.make({
        isFirstReport: firstReportedProcessKeys.has(processIdentityKey(killResult)),
      }),
    );
    const isReported = action === "reportKilled" || action === "reportNotKillable";
    return isReported ? [{ ...killResult, action }] : [];
  });
  const killed = reportedResults.filter((killResult) => killResult.action === "reportKilled");
  yield* Effect.logInfo("listed processes to clean up").pipe(
    Effect.annotateLogs({
      ...(input.taskName === null ? {} : { [`${TASK_TRACE_LABEL_PREFIX}.task`]: input.taskName }),
      [`${TASK_TRACE_LABEL_PREFIX}.processes.listed`]: processRows.size,
      [`${TASK_TRACE_LABEL_PREFIX}.processes.killed`]: killed.length,
      [`${TASK_TRACE_LABEL_PREFIX}.processes.killed.executable_names`]: killed.map(
        (killResult) => killResult.command,
      ),
    }),
  );
  yield* reportKilledProcesses(reportedResults);
  if (input.claimUnknownOwnershipToReport !== null) {
    yield* reportUnknownOwnership(
      listedProcesses,
      findPossibleRunProcesses(listedProcesses, unreadableProcesses, [
        ...childProcessIds,
        ...adoptedProcessIds,
        ...(input.adoptingProcessId === null ? [] : [input.adoptingProcessId]),
        ...processIds,
      ]),
      input.claimUnknownOwnershipToReport,
    );
  }
  return findEndedProcessGroups(checkedGroups);
});

export const forgetEndedProcessGroups =
  (startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>) =>
  (endedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>) =>
    Effect.sync(() =>
      [...endedProcessGroups].flatMap(([processGroupId, endedGroup]) =>
        moveTrackedProcessGroup(
          startedProcessGroups,
          processGroupId,
          TrackedProcessGroupEvent.cases.End.make({ endedGroup }),
        ) === "share"
          ? [processGroupId]
          : [],
      ),
    );

export const killTaskProcessGroups = Effect.fn(function* (
  startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
  taskName: string,
  claimProcessesToReport: Parameters<
    typeof terminateSelectedProcesses
  >[0]["claimProcessesToReport"],
  runMarker: typeof RunMarker.Type | null,
  listProcessesNow: typeof listProcesses = listProcesses,
  sendSignalNow: typeof sendSignal = sendSignal,
) {
  const probedGroups = yield* probeProcessGroups(
    taskName,
    new Map(
      [...startedProcessGroups].filter(([, startedGroup]) => startedGroup.taskName === taskName),
    ),
    sendSignalNow,
  );
  const endedProcessGroups = findEndedProcessGroups(probedGroups);
  const liveGroups = probedGroups.filter((group) =>
    ProcessGroupStage.guards.ProbedAlive(group.stage),
  );
  if (liveGroups.length === 0) {
    return endedProcessGroups;
  }
  const terminatedProcessGroups = yield* terminateSelectedProcesses({
    startedProcessGroups,
    probedGroups: liveGroups,
    ownsProcess: (ownerTaskName) => ownerTaskName === taskName,
    taskName,
    sessionId: null,
    runMarker,
    killsMarkedProcesses: false,
    claimUnknownOwnershipToReport: null,
    adoptingProcessId: null,
    listProcessesNow,
    sendSignalNow,
    claimProcessesToReport,
  });
  return new Map([...endedProcessGroups, ...terminatedProcessGroups]);
});

export const killAllProcessGroups = Effect.fn(function* (
  startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
  claimProcessesToReport: Parameters<
    typeof terminateSelectedProcesses
  >[0]["claimProcessesToReport"],
  sessionId: number | null,
  runMarker: typeof RunMarker.Type | null,
  adoptingProcessId: number | null,
  claimUnknownOwnershipToReport: Parameters<typeof claimUnknownOwnership>[1],
) {
  const probedGroups = yield* probeProcessGroups(null, startedProcessGroups, sendSignal);
  const terminatedProcessGroups = yield* terminateSelectedProcesses({
    startedProcessGroups,
    probedGroups: probedGroups.filter((group) => ProcessGroupStage.guards.ProbedAlive(group.stage)),
    ownsProcess: () => true,
    taskName: null,
    sessionId,
    runMarker,
    killsMarkedProcesses: true,
    claimUnknownOwnershipToReport,
    adoptingProcessId,
    listProcessesNow: listProcesses,
    sendSignalNow: sendSignal,
    claimProcessesToReport,
  });
  return new Map([...findEndedProcessGroups(probedGroups), ...terminatedProcessGroups]);
});
