import { Context, Layer, Ref } from "effect";

import { labelLine } from "../../../shared/label-lines.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";

export const formatRunLine = (runDirectoryPath: string) =>
  labelLine("run", null, `${runDirectoryPath}/`);

export class RunDirectory extends Context.Service<
  RunDirectory,
  Ref.Ref<{ readonly path: string; readonly runStartedAtMilliseconds: number } | null>
>()(`${TASK_PACKAGE.name}/RunDirectory`) {}

export const RunDirectoryLive = Layer.effect(
  RunDirectory,
  Ref.make<{ readonly path: string; readonly runStartedAtMilliseconds: number } | null>(null),
);
