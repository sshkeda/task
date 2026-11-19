import { Effect } from "effect";

import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { processIdentityKey } from "../../../shared/claim-processes-to-report.ts";
import type { listProcesses } from "./list-processes.ts";
import { ProcessOwnershipUnknown } from "./process-cleanup-errors.ts";

const ORPHAN_ADOPTING_PROCESS_ID = 1;

const findAncestorProcessIds = (
  listedProcesses: ReadonlyMap<number, { readonly parentProcessId: number }>,
  processId: number,
  found: ReadonlySet<number> = new Set(),
): ReadonlySet<number> => {
  const parentProcessId = listedProcesses.get(processId)?.parentProcessId;
  const isNewAncestor = parentProcessId !== undefined && !found.has(parentProcessId);
  return isNewAncestor
    ? findAncestorProcessIds(listedProcesses, parentProcessId, new Set([...found, parentProcessId]))
    : found;
};

export const findPossibleRunProcesses = (
  listedProcesses: ReadonlyMap<number, { readonly parentProcessId: number }>,
  unreadableProcesses: ReadonlyMap<number, unknown>,
  knownRunProcessIds: Iterable<number>,
): ReadonlyMap<number, unknown> => {
  const runProcessIds = new Set([process.pid, ...knownRunProcessIds]);
  const adoptingProcessIds = new Set([
    ORPHAN_ADOPTING_PROCESS_ID,
    ...findAncestorProcessIds(listedProcesses, process.pid),
  ]);
  const unknownProcessIds = [...unreadableProcesses.keys()].filter(
    (processId) => !runProcessIds.has(processId),
  );
  const growPossible = (possible: ReadonlySet<number>): ReadonlySet<number> => {
    const grown = new Set(
      unknownProcessIds.filter((processId) => {
        const parentProcessId = listedProcesses.get(processId)?.parentProcessId;
        const parentMayHaveStartedIt =
          parentProcessId === undefined ||
          runProcessIds.has(parentProcessId) ||
          adoptingProcessIds.has(parentProcessId) ||
          possible.has(parentProcessId);
        return parentMayHaveStartedIt;
      }),
    );
    return grown.size === possible.size ? possible : growPossible(grown);
  };
  const possible = growPossible(new Set());
  return new Map([...unreadableProcesses].filter(([processId]) => possible.has(processId)));
};

export const describeUnknownOwnership = (
  listedProcesses: Effect.Success<ReturnType<typeof listProcesses>>,
  possibleRunProcesses: ReadonlyMap<number, unknown>,
) =>
  Effect.map(
    Effect.forEach([...possibleRunProcesses], ([processId, cause]) =>
      Effect.gen(function* () {
        const listedProcess = listedProcesses.get(processId);
        if (listedProcess === undefined) {
          return [];
        }
        const command = yield* listedProcess.readCommandLineUnlessGone;
        return command === null
          ? []
          : [
              new ProcessOwnershipUnknown({
                processId,
                startTime: listedProcess.startTime,
                command,
                cause,
              }),
            ];
      }),
    ),
    (described) => described.flat(),
  );

const keepClaimedUnknownOwnership = (
  unknownOwnerships: readonly ProcessOwnershipUnknown[],
  claimed: readonly (typeof ProcessIdentity.Type)[],
) => {
  const claimedKeys = new Set(claimed.map((identity) => processIdentityKey(identity)));
  return unknownOwnerships.filter((unknownOwnership) =>
    claimedKeys.has(processIdentityKey(unknownOwnership)),
  );
};

export const claimUnknownOwnership = (
  unknownOwnerships: readonly ProcessOwnershipUnknown[],
  claimProcessesToReport: (
    processes: readonly (typeof ProcessIdentity.Type)[],
  ) => Effect.Effect<readonly (typeof ProcessIdentity.Type)[]>,
) =>
  Effect.map(
    claimProcessesToReport(
      unknownOwnerships.map(({ processId, startTime }) => ({ processId, startTime })),
    ),
    (claimed) => keepClaimedUnknownOwnership(unknownOwnerships, claimed),
  );
