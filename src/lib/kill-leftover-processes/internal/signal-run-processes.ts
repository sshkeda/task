import { Effect } from "effect";

import type { ProcessNotSignaled } from "../../../shared/process-errors.ts";
import type { RunMarker } from "../../../shared/run-marker.ts";
import { findDescendantProcessIds, findSessionProcessIds } from "./find-process-ids.ts";
import { listProcesses } from "./list-processes.ts";
import { findMarkedProcesses } from "./mark-run-processes.ts";
import { signalUnlessReplaced } from "./send-signal.ts";
import { findStartedGroupWitness } from "./started-process-groups.ts";
import type { startedProcessGroup } from "./started-process-groups.ts";

export const signalRunProcesses = Effect.fn("signalRunProcesses")(function* (
  run: {
    readonly sessionId: number;
    readonly descendantsOfProcessId: number | null;
    readonly startedProcessGroups: ReadonlyMap<number, ReturnType<typeof startedProcessGroup>>;
    readonly runMarker: typeof RunMarker.Type | null;
  },
  signal: Exclude<ProcessNotSignaled["signal"], 0>,
  listProcessesNow: typeof listProcesses = listProcesses,
) {
  const listedProcesses = yield* listProcessesNow(null);
  const startedProcessGroupIds = new Set(
    [...run.startedProcessGroups].flatMap(([processGroupId, startedGroup]) =>
      findStartedGroupWitness(listedProcesses, processGroupId, startedGroup) === null
        ? []
        : [processGroupId],
    ),
  );
  const processIds = new Set([
    ...findSessionProcessIds(listedProcesses, run.sessionId),
    ...(run.descendantsOfProcessId === null
      ? []
      : findDescendantProcessIds(listedProcesses, [run.descendantsOfProcessId]).slice(1)),
    ...(yield* findMarkedProcesses(run.runMarker, listedProcesses.keys())).keys(),
    ...[...listedProcesses]
      .filter(([, listedProcess]) => startedProcessGroupIds.has(listedProcess.processGroupId))
      .map(([processId]) => processId),
  ]);
  yield* Effect.forEach(
    [...processIds].flatMap((processId) => {
      const listedProcess = listedProcesses.get(processId);
      return listedProcess?.hasEnded === false ? [{ ...listedProcess, processId }] : [];
    }),
    (listedProcess) => signalUnlessReplaced(null, listedProcess, signal),
    { discard: true },
  );
});
