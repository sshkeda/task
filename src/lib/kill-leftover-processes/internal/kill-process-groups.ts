import { Effect } from "effect";

import { processIdentityKey } from "../../../shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { printLine } from "../../../shared/print-to-standard-error.ts";
import type { RunMarker } from "../../../shared/run-marker.ts";
import { findDescendantProcessIds, findSessionProcessIds } from "./find-process-ids.ts";
import { isStillListedAs, listProcesses } from "./list-processes.ts";
import { findMarkedProcesses } from "./mark-run-processes.ts";
import { ProcessNotKillable } from "./process-cleanup-errors.ts";
import { sendSignal, signalUnlessReplaced } from "./send-signal.ts";
import { findStartedGroupWitness } from "./started-process-groups.ts";
import type { startedProcessGroup } from "./started-process-groups.ts";

const reportKilledProcesses = (
  reportedResults: readonly {
    readonly processId: number;
    readonly command: string;
    readonly ownerTaskName: string | null;
    readonly result: "EPERM" | "ESRCH" | "sent";
  }[],
) =>
  Effect.gen(function* () {
    for (const killResult of reportedResults) {
      const startedBy = `started by ${killResult.ownerTaskName === null ? "no known task" : `"${killResult.ownerTaskName}"`}`;
      yield* killResult.result === "sent"
        ? printLine("Info", `killed ${killResult.processId} (${killResult.command}), ${startedBy}`)
        : printLine(
            "Error",
            formatFailureLines(
              new ProcessNotKillable({
                processId: killResult.processId,
                command: killResult.command,
                ownerTaskName: killResult.ownerTaskName,
              }),
              null,
            ),
          );
      yield* Effect.logInfo(
        killResult.result === "sent" ? "process killed" : "process could not be killed",
      ).pipe(
        Effect.annotateLogs({
          "process.pid": killResult.processId,
          "process.executable.name": killResult.command,
          ...(killResult.ownerTaskName === null
            ? {}
            : { [`${TASK_TRACE_LABEL_PREFIX}.task`]: killResult.ownerTaskName }),
        }),
      );
    }
  });

const findLiveProcessGroups = Effect.fn("findLiveProcessGroups")(function* (
  taskName: string | null,
  selectedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
) {
  const liveProcessGroups = new Map<number, ReturnType<typeof startedProcessGroup>>();
  const endedProcessGroupIds = new Set<number>();
  for (const [processGroupId, startedGroup] of selectedProcessGroups) {
    if ((yield* sendSignal(taskName, -processGroupId, 0)) === "sent") {
      liveProcessGroups.set(processGroupId, startedGroup);
    } else {
      endedProcessGroupIds.add(processGroupId);
    }
  }
  return { liveProcessGroups, endedProcessGroupIds };
});

const terminateSelectedProcesses = Effect.fn("terminateSelectedProcesses")(function* (input: {
  readonly startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>;
  readonly liveProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>;
  readonly ownsProcess: (ownerTaskName: string | null) => boolean;
  readonly taskName: string | null;
  readonly sessionId: number | null;
  readonly runMarker: typeof RunMarker.Type | null;
  readonly adoptingProcessId: number | null;
  readonly listProcessesNow: typeof listProcesses;
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
  const killableGroups = new Map<number, string>();
  const groupWitnesses = new Map<number, Parameters<typeof isStillListedAs>[1]>();
  const endedProcessGroupIds = new Set<number>();
  for (const [group, startedGroup] of input.liveProcessGroups) {
    const witness = findStartedGroupWitness(listedProcesses, group, startedGroup);
    if (witness === null) {
      endedProcessGroupIds.add(group);
    } else {
      killableGroups.set(group, startedGroup.taskName);
      groupWitnesses.set(group, witness);
    }
  }

  const markedProcesses = yield* findMarkedProcesses(input.runMarker, listedProcesses.keys());
  const processIds = [
    ...findDescendantProcessIds(listedProcesses, [...childProcessIds, ...adoptedProcessIds]),
    ...findSessionProcessIds(listedProcesses, input.sessionId),
    ...markedProcesses.keys(),
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
  yield* Effect.annotateCurrentSpan(
    `${TASK_TRACE_LABEL_PREFIX}.processes.listed`,
    processRows.size,
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
      const isGoneOrNotOwned = row === undefined || !input.ownsProcess(ownerTaskName);
      return isGoneOrNotOwned ? [] : [{ processId, row, ownerTaskName }];
    }),
    (leftover) =>
      Effect.gen(function* () {
        const command = yield* leftover.row.readCommandLineUnlessGone;
        return command === null
          ? []
          : [
              {
                processId: leftover.processId,
                startTime: leftover.row.startTime,
                processGroupId: leftover.row.processGroupId,
                command,
                ownerTaskName: leftover.ownerTaskName,
              },
            ];
      }),
  )).flat();
  const groupsStillStarted = yield* Effect.filter([...groupWitnesses], ([, witness]) =>
    isStillListedAs(input.taskName, witness),
  );
  const killResults = yield* Effect.forEach(leftovers, (leftover) =>
    Effect.map(signalUnlessReplaced(input.taskName, leftover, "SIGKILL"), (result) => ({
      ...leftover,
      result,
    })),
  );
  for (const [group] of groupsStillStarted) {
    yield* sendSignal(input.taskName, -group, "SIGKILL");
  }
  for (const group of killableGroups.keys()) {
    endedProcessGroupIds.add(group);
  }
  const signaledResults = killResults.filter((killResult) => killResult.result !== "ESRCH");
  const firstReportedProcessKeys = new Set(
    (yield* input.claimProcessesToReport(
      signaledResults.map((killResult) => ({
        processId: killResult.processId,
        startTime: killResult.startTime,
      })),
    )).map((identity) => processIdentityKey(identity)),
  );
  const reportedResults = signaledResults.filter((killResult) =>
    firstReportedProcessKeys.has(processIdentityKey(killResult)),
  );
  const killed = reportedResults.filter((killResult) => killResult.result === "sent");
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.processes.killed`]: killed.length,
    [`${TASK_TRACE_LABEL_PREFIX}.processes.killed.executable_names`]: killed.map(
      (killResult) => killResult.command,
    ),
  });
  yield* reportKilledProcesses(reportedResults);
  return endedProcessGroupIds;
});

export const forgetEndedProcessGroups =
  (startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>) =>
  (endedProcessGroupIds: ReadonlySet<number>) =>
    Effect.sync(() => {
      for (const endedProcessGroupId of endedProcessGroupIds) {
        startedProcessGroups.delete(endedProcessGroupId);
      }
    });

export const killTaskProcessGroups = Effect.fn("killTaskProcessGroups")(function* (
  startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
  taskName: string,
  claimProcessesToReport: Parameters<
    typeof terminateSelectedProcesses
  >[0]["claimProcessesToReport"],
  listProcessesNow: typeof listProcesses = listProcesses,
) {
  yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName);
  const selectedProcessGroups = new Map(
    [...startedProcessGroups].filter(([, startedGroup]) => startedGroup.taskName === taskName),
  );
  const { liveProcessGroups, endedProcessGroupIds } = yield* findLiveProcessGroups(
    taskName,
    selectedProcessGroups,
  );
  if (liveProcessGroups.size === 0) {
    return endedProcessGroupIds;
  }
  const terminatedProcessGroupIds = yield* terminateSelectedProcesses({
    startedProcessGroups,
    liveProcessGroups,
    ownsProcess: (ownerTaskName) => ownerTaskName === taskName,
    taskName,
    sessionId: null,
    runMarker: null,
    adoptingProcessId: null,
    listProcessesNow,
    claimProcessesToReport,
  });
  return new Set([...endedProcessGroupIds, ...terminatedProcessGroupIds]);
});

export const killAllProcessGroups = Effect.fn("killAllProcessGroups")(function* (
  startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
  claimProcessesToReport: Parameters<
    typeof terminateSelectedProcesses
  >[0]["claimProcessesToReport"],
  sessionId: number | null,
  runMarker: typeof RunMarker.Type | null,
  adoptingProcessId: number | null,
) {
  const { liveProcessGroups, endedProcessGroupIds } = yield* findLiveProcessGroups(
    null,
    startedProcessGroups,
  );
  const terminatedProcessGroupIds = yield* terminateSelectedProcesses({
    startedProcessGroups,
    liveProcessGroups,
    ownsProcess: () => true,
    taskName: null,
    sessionId,
    runMarker,
    adoptingProcessId,
    listProcessesNow: listProcesses,
    claimProcessesToReport,
  });
  return new Set([...endedProcessGroupIds, ...terminatedProcessGroupIds]);
});
