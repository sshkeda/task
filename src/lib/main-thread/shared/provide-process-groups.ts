import { Context, Effect, Layer } from "effect";

import { makeProcessReportClaimer } from "../../../shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import {
  forgetEndedProcessGroups,
  killAllProcessGroups,
  killTaskProcessGroups,
  listProcesses,
  recordLeaderExit,
  recordStartedProcessGroup,
} from "../../kill-leftover-processes/index.ts";
import type { startedProcessGroup } from "../../kill-leftover-processes/index.ts";
import {
  claimUnknownOwnershipInTheRun,
  processGroupsSharedWithTheSupervisor,
  shareProcessGroups,
} from "../../supervise-the-run/index.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";

export const makeProcessGroupsListingWith = (
  {
    ownsSession,
    runMarker,
    runProcessId,
  }: Pick<
    Context.Service.Shape<typeof RunRootTaskConfig>,
    "ownsSession" | "runMarker" | "runProcessId"
  >,
  listProcessesNow: typeof listProcesses,
) => {
  const supervisorAdoptsOrphans = ownsSession && process.platform === "linux";
  const startedProcessGroups = ownsSession
    ? processGroupsSharedWithTheSupervisor
    : new Map<number, ReturnType<typeof startedProcessGroup>>();
  const shareWithTheSupervisor = (processGroupIds: readonly number[]) =>
    ownsSession ? shareProcessGroups(processGroupIds) : Effect.void;
  const shareOnce = (processGroupId: number) => (action: "none" | "share") =>
    action === "share" ? shareWithTheSupervisor([processGroupId]) : Effect.void;
  const forget = (
    endedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>,
  ) =>
    forgetEndedProcessGroups(startedProcessGroups)(endedProcessGroups).pipe(
      Effect.flatMap(shareWithTheSupervisor),
    );
  const { claimProcessesToReport } = makeProcessReportClaimer();
  return {
    claimProcessesToReport,
    record: (processId: number, taskName: string, leaderStartTime: string | null) =>
      Effect.sync(() =>
        recordStartedProcessGroup(startedProcessGroups, processId, taskName, leaderStartTime),
      ).pipe(Effect.flatMap(shareOnce(processId))),
    recordLeaderExit: (
      processId: number,
      taskName: string,
      membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
    ) =>
      Effect.sync(() =>
        recordLeaderExit(startedProcessGroups, processId, taskName, membersAtLeaderExit),
      ).pipe(Effect.flatMap(shareOnce(processId))),
    killForTask: (taskName: string) =>
      killTaskProcessGroups(
        startedProcessGroups,
        taskName,
        claimProcessesToReport,
        runMarker,
        listProcessesNow,
      ).pipe(Effect.flatMap(forget)),
    killAll: (isForced: boolean) => {
      const killsTheSession = isForced && ownsSession;
      return killAllProcessGroups(
        startedProcessGroups,
        claimProcessesToReport,
        killsTheSession ? process.pid : null,
        runMarker,
        supervisorAdoptsOrphans ? runProcessId : null,
        claimUnknownOwnershipInTheRun,
      ).pipe(Effect.flatMap(forget));
    },
  };
};

const makeProcessGroups = Effect.gen(function* () {
  return makeProcessGroupsListingWith(yield* RunRootTaskConfig, listProcesses);
});

export class ProcessGroups extends Context.Service<
  ProcessGroups,
  Effect.Success<typeof makeProcessGroups>
>()(`${TASK_PACKAGE.name}/ProcessGroups`) {}

export const ProcessGroupsLive = Layer.effect(ProcessGroups, makeProcessGroups);
