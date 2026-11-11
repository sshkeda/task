import { Cause, Effect, FiberSet, Pull, Scope, Stdio, Stream } from "effect";

import { describeCauseMessage } from "../../../../../shared/report-errors.ts";
import { StandardInputRead } from "../../../../../shared/rpc.ts";
import { RunRootTaskConfig } from "../../../shared/provide-run-root-task-config.ts";

export const makeRootTaskStandardInput = Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  const rootTaskName = (yield* RunRootTaskConfig).rootTaskName;
  const isTerminal = yield* stdio.stdinIsTerminal;
  const runScope = yield* Effect.scope;
  const pullStandardInputChunks = yield* Effect.cached(
    Stream.toPull(stdio.stdin).pipe(Scope.provide(runScope)),
  );
  const readFibers = yield* FiberSet.make();
  const readNextChunks = pullStandardInputChunks.pipe(
    Effect.flatMap((pull) =>
      Pull.matchEffect(pull, {
        onSuccess: (chunks) => Effect.succeed(StandardInputRead.cases.Chunks.make({ chunks })),
        onDone: () => Effect.succeed(StandardInputRead.cases.Ended.make({})),
        onFailure: (cause) =>
          Effect.succeed(
            StandardInputRead.cases.Failed.make({
              reason: describeCauseMessage(Cause.squash(cause)),
            }),
          ),
      }),
    ),
  );
  return {
    standardInput: { isTerminal, rootTaskName },
    answerNextRead: <Failure, Services>(
      answer: (read: typeof StandardInputRead.Type) => Effect.Effect<void, Failure, Services>,
    ) => FiberSet.run(readFibers, Effect.flatMap(readNextChunks, answer)),
  };
});
