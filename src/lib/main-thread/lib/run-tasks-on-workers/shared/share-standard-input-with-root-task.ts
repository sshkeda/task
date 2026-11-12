import { Cause, Effect, FiberSet, Pull, Ref, Scope, Semaphore, Stdio, Stream } from "effect";

import { InternalInvariantBroken } from "../../../../../shared/invariant-errors.ts";
import { describeCauseMessage } from "../../../../../shared/report-errors.ts";
import type { StandardInputRead } from "../../../../../shared/rpc.ts";
import { RunRootTaskConfig } from "../../../shared/provide-run-root-task-config.ts";
import {
  moveStandardInputStage,
  StandardInputAction,
  StandardInputEvent,
  StandardInputStage,
} from "./define-standard-input-stage.ts";

export const makeRootTaskStandardInput = Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  const rootTaskName = (yield* RunRootTaskConfig).rootTaskName;
  const isTerminal = yield* stdio.stdinIsTerminal;
  const runScope = yield* Effect.scope;
  const pullStandardInputChunks = yield* Effect.cached(
    Stream.toPull(stdio.stdin).pipe(Scope.provide(runScope)),
  );
  const stage = yield* Ref.make<typeof StandardInputStage.Type>(
    StandardInputStage.cases.NotOpened.make({}),
  );
  const oneReadAtATime = yield* Semaphore.make(1);
  const readFibers = yield* FiberSet.make();
  const moveStage = (event: typeof StandardInputEvent.Type) =>
    Ref.modify(stage, (current) => {
      const [next, action] = moveStandardInputStage(current, event);
      return [action, next] as const;
    });
  const pullNextChunks = pullStandardInputChunks.pipe(
    Effect.flatMap((pull) =>
      Pull.matchEffect(pull, {
        onSuccess: (chunks) =>
          Effect.succeed(StandardInputEvent.cases.ReceiveChunks.make({ chunks })),
        onDone: () => Effect.succeed(StandardInputEvent.cases.ReceiveEnd.make({})),
        onFailure: (cause) =>
          Effect.succeed(
            StandardInputEvent.cases.ReceiveFailure.make({
              reason: describeCauseMessage(Cause.squash(cause)),
            }),
          ),
      }),
    ),
    Effect.flatMap(moveStage),
  );
  const act = (
    action: typeof StandardInputAction.Type,
  ): Effect.Effect<typeof StandardInputRead.Type> =>
    StandardInputAction.match(action, {
      Answer: ({ read }) => Effect.succeed(read),
      DieOutOfOrder: () =>
        Effect.die(
          new InternalInvariantBroken({
            area: "standard input",
            invariant: "the main thread pulls standard input for one read at a time",
          }),
        ),
      Pull: () => Effect.flatMap(pullNextChunks, act),
    });
  const readNextChunks = (taskName: string) =>
    moveStage(StandardInputEvent.cases.Read.make({ rootTaskName, taskName })).pipe(
      Effect.flatMap(act),
      oneReadAtATime.withPermits(1),
    );
  return {
    standardInput: { isTerminal, rootTaskName },
    answerNextRead: <Failure, Services>(
      taskName: string,
      answer: (read: typeof StandardInputRead.Type) => Effect.Effect<void, Failure, Services>,
    ) => FiberSet.run(readFibers, Effect.flatMap(readNextChunks(taskName), answer)),
  };
});
