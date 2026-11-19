import { Array as Arrays } from "effect";

import type { RootTaskGroup } from "../../../shared/root-task-group.ts";

export const decideNoTaskStarted = (run: {
  readonly madeTaskNames: readonly string[];
  readonly startedTaskNames: readonly string[];
  readonly rootTaskGroup: Pick<typeof RootTaskGroup.Type, "name" | "taskNames"> | null;
  readonly runAlreadyFailed: boolean;
}): Arrays.NonEmptyReadonlyArray<string> | null => {
  const { madeTaskNames, rootTaskGroup } = run;
  const isRootTaskGroupGivenNoTasks =
    rootTaskGroup !== null && !Arrays.isReadonlyArrayNonEmpty(rootTaskGroup.taskNames);
  const startedTaskNames = run.startedTaskNames.filter(
    (taskName) => taskName !== rootTaskGroup?.name,
  );
  if (run.runAlreadyFailed) {
    return null;
  }
  if (isRootTaskGroupGivenNoTasks) {
    return null;
  }
  if (Arrays.isReadonlyArrayNonEmpty(startedTaskNames)) {
    return null;
  }
  return Arrays.isReadonlyArrayNonEmpty(madeTaskNames) ? madeTaskNames : null;
};
