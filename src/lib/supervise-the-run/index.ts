import { Ref } from "effect";

import { supervisorProcessId, supervisorRunMarker } from "./internal/watch-the-supervisor.ts";

export { isRootTaskFileThere } from "./internal/check-root-task-file.ts";
export { reportReady } from "./internal/report-ready.ts";
export {
  processGroupsSharedWithTheSupervisor,
  shareProcessGroups,
} from "./internal/share-process-groups.ts";
export { superviseRun } from "./internal/supervise-run.ts";

export const readSupervisorProcessId = Ref.get(supervisorProcessId);

export const readSupervisorRunMarker = Ref.get(supervisorRunMarker);
