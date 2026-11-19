import { Effect } from "effect";

import { processIdentityKey } from "../../../shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import type { RunMarker } from "../../../shared/run-marker.ts";
import { lifeStageOf, ProcessGroupStage } from "./define-process-group-stage.ts";
import type { ListedIdentity } from "./define-process-group-stage.ts";
import {
  moveTrackedProcessGroupStage,
  trackedGroupOf,
  TrackedProcessGroupEvent,
  trackedStageOf,
  UNREGISTERED_GENERATION,
} from "./define-tracked-process-group-stage.ts";
import { listProcesses } from "./list-processes.ts";
import { findMarkedProcesses } from "./mark-run-processes.ts";
import { sendSignal } from "./send-signal.ts";

export const startedProcessGroup = (
  generation: number,
  taskName: string,
  leaderStartTime: string | null,
  membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[] | null,
) => ({ generation, taskName, leaderStartTime, membersAtLeaderExit });

const registrations = { latestGeneration: UNREGISTERED_GENERATION };

const registerNextGeneration = () => {
  registrations.latestGeneration += 1;
  return registrations.latestGeneration;
};

export const moveTrackedProcessGroup = (
  startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>,
  processGroupId: number,
  event: typeof TrackedProcessGroupEvent.Type,
) => {
  const [next, action] = moveTrackedProcessGroupStage(
    trackedStageOf(startedProcessGroups.get(processGroupId)),
    event,
  );
  if (action === "none") {
    return action;
  }
  const nextGroup = trackedGroupOf(next);
  if (nextGroup === null) {
    startedProcessGroups.delete(processGroupId);
  } else {
    startedProcessGroups.set(processGroupId, nextGroup);
  }
  return action;
};

export const recordStartedProcessGroup = (
  startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>,
  processId: number,
  taskName: string,
  leaderStartTime: string | null,
) =>
  moveTrackedProcessGroup(
    startedProcessGroups,
    processId,
    TrackedProcessGroupEvent.cases.Start.make({
      generation: registerNextGeneration(),
      taskName,
      leaderStartTime,
    }),
  );

export const recordLeaderExit = (
  startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>,
  processId: number,
  taskName: string,
  membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
) =>
  moveTrackedProcessGroup(
    startedProcessGroups,
    processId,
    TrackedProcessGroupEvent.cases.ExitLeader.make({ taskName, membersAtLeaderExit }),
  );

const findLiveMembers = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  processGroupId: number,
) =>
  [...listedProcesses].flatMap(([processId, listedProcess]) => {
    const isLiveMember = listedProcess.processGroupId === processGroupId && !listedProcess.hasEnded;
    return isLiveMember ? [{ processId, startTime: listedProcess.startTime }] : [];
  });

export const findStartedGroupWitness = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  processGroupId: number,
  startedGroup: Pick<
    ReturnType<typeof startedProcessGroup>,
    "leaderStartTime" | "membersAtLeaderExit" | "taskName"
  >,
  markedIdentities: ReadonlyMap<string, string | null>,
): typeof ListedIdentity.Type | null => {
  const lifeStage = lifeStageOf(startedGroup);
  if (!ProcessGroupStage.guards.LeaderExited(lifeStage)) {
    const leader = listedProcesses.get(processGroupId);
    const isTheLeaderTaskStarted =
      leader?.processGroupId === processGroupId &&
      leader.startTime === startedGroup.leaderStartTime;
    return isTheLeaderTaskStarted
      ? { processId: processGroupId, processGroupId, startTime: leader.startTime }
      : null;
  }
  const recordedMemberKeys = new Set(
    lifeStage.membersAtLeaderExit.map((member) => processIdentityKey(member)),
  );
  const witness = findLiveMembers(listedProcesses, processGroupId).find((member) => {
    const memberKey = processIdentityKey(member);
    return (
      recordedMemberKeys.has(memberKey) || markedIdentities.get(memberKey) === startedGroup.taskName
    );
  });
  return witness === undefined ? null : { ...witness, processGroupId };
};

export const findStartedGroupWitnessNow = (
  taskName: string | null,
  processGroupId: number,
  startedGroup: Pick<
    ReturnType<typeof startedProcessGroup>,
    "leaderStartTime" | "membersAtLeaderExit" | "taskName"
  >,
  runMarker: typeof RunMarker.Type | null,
) =>
  Effect.gen(function* () {
    const listedProcesses = yield* listProcesses(taskName);
    const { markedIdentities } = yield* findMarkedProcesses(
      runMarker,
      new Map(
        [...listedProcesses].filter(
          ([, listedProcess]) => listedProcess.processGroupId === processGroupId,
        ),
      ),
    );
    return findStartedGroupWitness(listedProcesses, processGroupId, startedGroup, markedIdentities);
  });

export const listMembersLeftByLeader = (
  taskName: string | null,
  processGroupId: number,
  membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
) => {
  const recordedMemberKeys = new Set(
    membersAtLeaderExit.map((member) => processIdentityKey(member)),
  );
  return listProcesses(taskName).pipe(
    Effect.flatMap((listedProcesses) =>
      Effect.forEach(
        findLiveMembers(listedProcesses, processGroupId).filter((member) =>
          recordedMemberKeys.has(processIdentityKey(member)),
        ),
        (member) =>
          Effect.gen(function* () {
            const listedProcess = listedProcesses.get(member.processId);
            const command =
              listedProcess === undefined ? null : yield* listedProcess.readCommandLineUnlessGone;
            return command === null ? [] : [{ processId: member.processId, command }];
          }),
      ),
    ),
    Effect.map((members) => members.flat()),
  );
};

export const readMembersAtLeaderExit = (taskName: string | null, processGroupId: number) =>
  sendSignal(taskName, -processGroupId, 0).pipe(
    Effect.flatMap((result) =>
      result === "ESRCH"
        ? Effect.succeed([])
        : listProcesses(taskName).pipe(
            Effect.map((listedProcesses) => findLiveMembers(listedProcesses, processGroupId)),
          ),
    ),
  );
