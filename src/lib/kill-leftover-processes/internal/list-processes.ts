import { Effect } from "effect";

import {
  listProcessesFromProcDirectory,
  readProcessFromProcDirectory,
} from "./list-linux-processes-from-proc.ts";
import {
  listProcessesWithSysctl,
  readProcessWithSysctl,
} from "./list-macos-processes-with-sysctl.ts";

export const listProcesses = (taskName: string | null) =>
  Effect.gen(function* () {
    const listedProcesses =
      process.platform === "linux"
        ? yield* listProcessesFromProcDirectory(taskName)
        : yield* listProcessesWithSysctl(taskName);
    const listedProcessesOfEitherPlatform: ReadonlyMap<
      number,
      typeof listedProcesses extends ReadonlyMap<number, infer ListedProcess>
        ? ListedProcess
        : never
    > = listedProcesses;
    return listedProcessesOfEitherPlatform;
  });

export const readProcessUnlessGone = (taskName: string | null, processId: number) =>
  Effect.gen(function* () {
    return process.platform === "linux"
      ? yield* readProcessFromProcDirectory(taskName, processId)
      : yield* readProcessWithSysctl(taskName, processId);
  });

export const isStillListedAs = (
  taskName: string | null,
  identity: {
    readonly processId: number;
    readonly processGroupId: number;
    readonly startTime: string;
  },
) =>
  readProcessUnlessGone(taskName, identity.processId).pipe(
    Effect.map(
      (listedNow) =>
        listedNow !== null &&
        listedNow.startTime === identity.startTime &&
        listedNow.processGroupId === identity.processGroupId,
    ),
  );

export const processGroupHasOnlyEndedMembers = Effect.fn("processGroupHasOnlyEndedMembers")(
  function* (taskName: string | null, processGroupId: number) {
    const listedProcesses = yield* listProcesses(taskName);
    return [...listedProcesses.values()]
      .filter((listedProcess) => listedProcess.processGroupId === processGroupId)
      .every((listedProcess) => listedProcess.hasEnded);
  },
);
