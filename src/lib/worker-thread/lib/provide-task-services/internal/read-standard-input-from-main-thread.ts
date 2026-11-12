import { Cause, Effect, Stdio, Stream } from "effect";
import type { Array as Arrays, Pull } from "effect";
import { systemError } from "effect/PlatformError";
import type { PlatformError } from "effect/PlatformError";

import { StandardInputRead } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import { StandardInputReadOutsideRootTask } from "../../../../../shared/task-errors.ts";
import type { WorkerState } from "../../../shared/provide-worker-state.ts";

const pullOfStandardInputAnswer = (
  read: typeof StandardInputRead.Type,
): Pull.Pull<Arrays.NonEmptyReadonlyArray<Uint8Array>, PlatformError> =>
  StandardInputRead.match(read, {
    Chunks: ({ chunks }) => Effect.succeed(chunks),
    Ended: () => Cause.done(),
    Failed: ({ reason }) =>
      Effect.fail(
        systemError({ _tag: "Unknown", module: "Stdio", method: "stdin", description: reason }),
      ),
    Refused: ({ rootTaskName, taskName }) =>
      Effect.die(new StandardInputReadOutsideRootTask({ rootTaskName, taskName })),
  });

const readNextChunksFromMainThread = (taskName: string, workerState: WorkerState["Service"]) =>
  workerState.requestStandardInputRead(taskName).pipe(Effect.flatMap(pullOfStandardInputAnswer));

export const refuseStandardInputOutsideRootTask = (startMessage: typeof StartTaskFiber.Type) =>
  startMessage.taskName === startMessage.standardInput.rootTaskName
    ? null
    : new StandardInputReadOutsideRootTask({
        taskName: startMessage.taskName,
        rootTaskName: startMessage.standardInput.rootTaskName,
      });

const makeTaskStdio = (
  startMessage: typeof StartTaskFiber.Type,
  workerState: WorkerState["Service"],
) =>
  Effect.gen(function* () {
    const runtimeStdio = yield* Stdio.Stdio;
    const refusal = refuseStandardInputOutsideRootTask(startMessage);
    return Stdio.make({
      ...runtimeStdio,
      stdinIsTerminal: Effect.succeed(startMessage.standardInput.isTerminal),
      stdin:
        refusal === null
          ? Stream.fromPull(
              Effect.succeed(readNextChunksFromMainThread(startMessage.taskName, workerState)),
            )
          : Stream.die(refusal),
    });
  });

export const provideTaskStdio =
  (startMessage: typeof StartTaskFiber.Type, workerState: WorkerState["Service"]) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    Effect.provideServiceEffect(effect, Stdio.Stdio, makeTaskStdio(startMessage, workerState));
