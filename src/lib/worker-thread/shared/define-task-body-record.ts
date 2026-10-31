import { Option } from "effect";
import type { Cause } from "effect";

import type { startedProcessGroup } from "../../kill-leftover-processes/index.ts";

export const makeTaskBodyRecord = () => ({
  logs: new Array<string>(),
  startedProcessGroups: new Map<number, ReturnType<typeof startedProcessGroup>>(),
  startedAtMilliseconds: 0,
  causeReportedBeforeCleanup: Option.none<Cause.Cause<unknown>>(),
});
