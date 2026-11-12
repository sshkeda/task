import { Effect } from "effect";

import {
  lifeStageOf,
  moveProcessGroupStage,
  ProcessGroupEvent,
  ProcessGroupStage,
} from "./define-process-group-stage.ts";
import type { ListedIdentity } from "./define-process-group-stage.ts";
import { listProcesses } from "./list-processes.ts";
import { sendSignal } from "./send-signal.ts";

export const startedProcessGroup = (
  taskName: string,
  leaderStartTime: string | null,
  latestMemberStartTimeAtLeaderExit: string | null,
) => ({ taskName, leaderStartTime, latestMemberStartTimeAtLeaderExit });

export const recordLeaderExit = (
  startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>,
  processId: number,
  latestMemberStartTime: string | null,
) => {
  const startedGroup = startedProcessGroups.get(processId);
  if (startedGroup === undefined) {
    return;
  }
  const [next] = moveProcessGroupStage(
    lifeStageOf(startedGroup),
    ProcessGroupEvent.cases.ExitLeader.make({ latestMemberStartTime }),
  );
  if (ProcessGroupStage.guards.LeaderExited(next)) {
    startedProcessGroups.set(
      processId,
      startedProcessGroup(
        startedGroup.taskName,
        startedGroup.leaderStartTime,
        next.latestMemberStartTime,
      ),
    );
  } else {
    startedProcessGroups.delete(processId);
  }
};

const MICROSECONDS_PER_SECOND = 1_000_000n;

const orderOfStartTime = (startTime: string) => {
  const [seconds = "0", microseconds] = startTime.split(".");
  return microseconds === undefined
    ? BigInt(seconds)
    : BigInt(seconds) * MICROSECONDS_PER_SECOND + BigInt(microseconds);
};

const findLatestMemberStartTime = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  processGroupId: number,
) =>
  [...listedProcesses.values()]
    .filter(
      (listedProcess) => listedProcess.processGroupId === processGroupId && !listedProcess.hasEnded,
    )
    .map((listedProcess) => listedProcess.startTime)
    .toSorted((left, right) => Number(orderOfStartTime(left) - orderOfStartTime(right)))
    .at(-1) ?? null;

export const findStartedGroupWitness = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  processGroupId: number,
  startedGroup: Omit<ReturnType<typeof startedProcessGroup>, "taskName">,
): typeof ListedIdentity.Type | null => {
  const { leaderStartTime } = startedGroup;
  const lifeStage = lifeStageOf(startedGroup);
  if (!ProcessGroupStage.guards.LeaderExited(lifeStage)) {
    const leader = listedProcesses.get(processGroupId);
    const isTheLeaderTaskStarted =
      leader?.processGroupId === processGroupId && leader.startTime === leaderStartTime;
    return isTheLeaderTaskStarted
      ? { processId: processGroupId, processGroupId, startTime: leader.startTime }
      : null;
  }
  const witness = [...listedProcesses].find(
    ([, listedProcess]) =>
      listedProcess.processGroupId === processGroupId &&
      !listedProcess.hasEnded &&
      orderOfStartTime(listedProcess.startTime) <=
        orderOfStartTime(lifeStage.latestMemberStartTime) &&
      (leaderStartTime === null ||
        orderOfStartTime(listedProcess.startTime) >= orderOfStartTime(leaderStartTime)),
  );
  return witness === undefined
    ? null
    : { processId: witness[0], processGroupId, startTime: witness[1].startTime };
};

export const findStartedGroupWitnessNow = (
  taskName: string | null,
  processGroupId: number,
  startedGroup: Omit<ReturnType<typeof startedProcessGroup>, "taskName">,
) =>
  listProcesses(taskName).pipe(
    Effect.map((listedProcesses) =>
      findStartedGroupWitness(listedProcesses, processGroupId, startedGroup),
    ),
  );

export const readLatestMemberStartTime = (taskName: string | null, processGroupId: number) =>
  sendSignal(taskName, -processGroupId, 0).pipe(
    Effect.flatMap((result) =>
      result === "ESRCH"
        ? Effect.succeed(null)
        : listProcesses(taskName).pipe(
            Effect.map((listedProcesses) =>
              findLatestMemberStartTime(listedProcesses, processGroupId),
            ),
          ),
    ),
  );
