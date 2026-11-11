import { Option } from "effect";
import type { Cause } from "effect";

import type { startedProcessGroup } from "../../kill-leftover-processes/index.ts";

export const makeTaskBodyRecord = () => ({
  startedProcessGroups: new Map<number, ReturnType<typeof startedProcessGroup>>(),
  startedAtMilliseconds: 0,
  returnedBeforeCleanup: false,
  causeReportedBeforeCleanup: Option.none<Cause.Cause<unknown>>(),
});
