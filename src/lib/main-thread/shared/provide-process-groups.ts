import { Context, Effect, Layer } from "effect";

import { makeProcessReportClaimer } from "../../../shared/claim-processes-to-report.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import {
  forgetEndedProcessGroups,
  killAllProcessGroups,
  killTaskProcessGroups,
  recordLeaderExit,
  startedProcessGroup,
} from "../../kill-leftover-processes/index.ts";
import {
  processGroupsSharedWithTheSupervisor,
  shareProcessGroups,
} from "../../supervise-the-run/index.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";

const makeProcessGroups = Effect.gen(function* () {
  const { ownsSession, runMarker, runProcessId } = yield* RunRootTaskConfig;
  const supervisorAdoptsOrphans = ownsSession && process.platform === "linux";
  const startedProcessGroups = ownsSession
    ? processGroupsSharedWithTheSupervisor
    : new Map<number, ReturnType<typeof startedProcessGroup>>();
  const shareWithTheSupervisor = (processGroupIds: Iterable<number>) =>
    ownsSession ? shareProcessGroups(processGroupIds) : Effect.void;
  const forget = (endedProcessGroupIds: ReadonlySet<number>) =>
    forgetEndedProcessGroups(startedProcessGroups)(endedProcessGroupIds).pipe(
      Effect.andThen(shareWithTheSupervisor(endedProcessGroupIds)),
    );
  const { claimProcessesToReport } = makeProcessReportClaimer();
  return {
    claimProcessesToReport,
    record: (processId: number, taskName: string, leaderStartTime: string | null) =>
      Effect.sync(() => {
        startedProcessGroups.set(processId, startedProcessGroup(taskName, leaderStartTime, null));
      }).pipe(Effect.andThen(shareWithTheSupervisor([processId]))),
    recordLeaderExit: (processId: number, latestMemberStartTime: string | null) =>
      Effect.sync(() => {
        recordLeaderExit(startedProcessGroups, processId, latestMemberStartTime);
      }).pipe(Effect.andThen(shareWithTheSupervisor([processId]))),
    killForTask: (taskName: string) =>
      killTaskProcessGroups(startedProcessGroups, taskName, claimProcessesToReport).pipe(
        Effect.flatMap(forget),
      ),
    killAll: (isForced: boolean) => {
      const killsTheSession = isForced && ownsSession;
      return killAllProcessGroups(
        startedProcessGroups,
        claimProcessesToReport,
        killsTheSession ? process.pid : null,
        runMarker,
        supervisorAdoptsOrphans ? runProcessId : null,
      ).pipe(Effect.flatMap(forget));
    },
  };
});

export class ProcessGroups extends Context.Service<
  ProcessGroups,
  Effect.Success<typeof makeProcessGroups>
>()(`${TASK_PACKAGE.name}/ProcessGroups`) {}

export const ProcessGroupsLive = Layer.effect(ProcessGroups, makeProcessGroups);
