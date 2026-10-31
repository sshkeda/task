import { Effect } from "effect";

import type { startedProcessGroup } from "../../kill-leftover-processes/index.ts";
import { callTheSupervisor } from "./talk-to-the-supervisor.ts";

export const processGroupsSharedWithTheSupervisor = new Map<
  number,
  ReturnType<typeof startedProcessGroup>
>();

export const shareProcessGroups = (processGroupIds: Iterable<number>) =>
  callTheSupervisor((supervisor) =>
    Effect.forEach(
      processGroupIds,
      (processGroupId) =>
        supervisor.ShareProcessGroup(
          {
            processGroupId,
            startedGroup: processGroupsSharedWithTheSupervisor.get(processGroupId) ?? null,
          },
          { discard: true },
        ),
      { discard: true },
    ),
  );
