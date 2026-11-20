import { Clock, Duration, Effect, Ref } from "effect";

import { labelLine, nameTask } from "../../../shared/label-lines.ts";
import { formatSeconds } from "../shared/format-task-lines.ts";
import { cutToCodeUnits } from "./cut-to-code-units.ts";

export const QUIET_MILLISECONDS_BEFORE_A_RUNNING_LINE = 60_000;

const MOST_NAMED_RUNNING_TASKS = 3;

const MOST_CODE_UNITS_OF_A_RUNNING_TASK_NAME = 60;

export const formatRunningLine = (run: {
  readonly isStopping: boolean;
  readonly ownWork: readonly { readonly taskName: string; readonly milliseconds: number }[];
  readonly endedTaskCount: number;
  readonly runMilliseconds: number;
}) => {
  const hasNoTaskToName = run.isStopping || run.ownWork.length === 0;
  if (hasNoTaskToName) {
    return null;
  }
  const longestFirst = run.ownWork.toSorted(
    (left, right) => right.milliseconds - left.milliseconds,
  );
  const named = longestFirst
    .slice(0, MOST_NAMED_RUNNING_TASKS)
    .map(
      ({ taskName, milliseconds }) =>
        `${nameTask(cutToCodeUnits(taskName, MOST_CODE_UNITS_OF_A_RUNNING_TASK_NAME))} ${formatSeconds(milliseconds)}`,
    );
  const unnamedCount = longestFirst.length - named.length;
  const done = `(${run.endedTaskCount} ${run.endedTaskCount === 1 ? "task" : "tasks"} done)`;
  return labelLine(
    "running",
    null,
    `${[...named, ...(unnamedCount === 0 ? [] : [`and ${unnamedCount} more`]), done].join(" ")}, ${formatSeconds(run.runMilliseconds)}`,
  );
};

export const printRunningLinesWhileQuiet = (options: {
  readonly readLastWrite: Effect.Effect<{
    readonly atMilliseconds: number;
    readonly isLineOpen: boolean;
    readonly awaitNextWrite: Effect.Effect<void>;
  }>;
  readonly readTerminalCommands: Effect.Effect<{
    readonly isAnyRunning: boolean;
    readonly lastEndedAtMilliseconds: number;
    readonly awaitChange: Effect.Effect<void>;
  }>;
  readonly describe: (nowMilliseconds: number) => Effect.Effect<string | null>;
  readonly print: (line: string) => Effect.Effect<void>;
}) =>
  Effect.gen(function* () {
    const lastCheckedAt = yield* Ref.make(yield* Clock.currentTimeMillis);
    const checkOnceQuietLongEnough = Effect.gen(function* () {
      const lastWrite = yield* options.readLastWrite;
      const terminalCommands = yield* options.readTerminalCommands;
      if (lastWrite.isLineOpen) {
        yield* lastWrite.awaitNextWrite;
        return;
      }
      if (terminalCommands.isAnyRunning) {
        yield* terminalCommands.awaitChange;
        return;
      }
      const quietSince = Math.max(
        lastWrite.atMilliseconds,
        terminalCommands.lastEndedAtMilliseconds,
        yield* Ref.get(lastCheckedAt),
      );
      const nowMilliseconds = yield* Clock.currentTimeMillis;
      const quietLeftMilliseconds =
        quietSince + QUIET_MILLISECONDS_BEFORE_A_RUNNING_LINE - nowMilliseconds;
      if (quietLeftMilliseconds > 0) {
        yield* Effect.raceFirst(lastWrite.awaitNextWrite, terminalCommands.awaitChange).pipe(
          Effect.timeoutOption(Duration.millis(quietLeftMilliseconds)),
        );
        return;
      }
      yield* Ref.set(lastCheckedAt, nowMilliseconds);
      const line = yield* options.describe(nowMilliseconds);
      if (line !== null) {
        yield* options.print(line);
      }
    });
    return yield* Effect.forever(checkOnceQuietLongEnough);
  });
