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
): Pull.Pull<Arrays.NonEmptyReadonlyArray<Uint8Array>, PlatformError> => {
  if (StandardInputRead.guards.Chunks(read)) {
    return Effect.succeed(read.chunks);
  }
  if (StandardInputRead.guards.Ended(read)) {
    return Cause.done();
  }
  return Effect.fail(
    systemError({ _tag: "Unknown", module: "Stdio", method: "stdin", description: read.reason }),
  );
};

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
