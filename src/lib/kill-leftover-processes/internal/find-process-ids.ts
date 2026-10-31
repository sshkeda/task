import type { Effect } from "effect";

import type { listProcesses } from "./list-processes.ts";

export const findDescendantProcessIds = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  processIdsAtThisDepth: readonly number[],
  collectedProcessIds: readonly number[] = [],
): readonly number[] => {
  if (processIdsAtThisDepth.length === 0) {
    return collectedProcessIds;
  }
  const collectedThroughThisDepth = [...collectedProcessIds, ...processIdsAtThisDepth];
  return findDescendantProcessIds(
    listedProcesses,
    [...listedProcesses]
      .filter(
        ([processId, listedProcess]) =>
          processIdsAtThisDepth.includes(listedProcess.parentProcessId) &&
          !collectedThroughThisDepth.includes(processId),
      )
      .map(([processId]) => processId),
    collectedThroughThisDepth,
  );
};

export const findSessionProcessIds = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  sessionId: number | null,
) =>
  [...listedProcesses]
    .filter(
      ([processId, listedProcess]) =>
        listedProcess.sessionId === sessionId && processId !== process.pid,
    )
    .map(([processId]) => processId);
