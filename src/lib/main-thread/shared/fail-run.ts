import { Cause, Effect } from "effect";

import { StopState } from "./provide-stop-state.ts";
import { reportTaskFailure } from "./report-task-failure.ts";

export const failTask = Effect.fn("failTask")(function* (
  failure: Effect.Success<StopState["Service"]["awaitFailure"]>,
  markFailed: (taskName: string) => Effect.Effect<void>,
) {
  if (failure.taskName === null) {
    return;
  }
  yield* markFailed(failure.taskName);
  yield* reportTaskFailure(failure.taskName, Cause.fail(failure));
}, Effect.uninterruptible);

export const failRun = Effect.fn("failRun")(function* (
  failure: Parameters<typeof failTask>[0],
  markFailed: Parameters<typeof failTask>[1],
) {
  if (yield* (yield* StopState).recordFirstFailure(failure)) {
    yield* failTask(failure, markFailed);
  }
}, Effect.uninterruptible);

export const failRunOnFailure =
  (markFailed: Parameters<typeof failTask>[1]) =>
  <Success, Failure extends Parameters<typeof failTask>[0], Services>(
    effect: Effect.Effect<Success, Failure, Services>,
  ) =>
    effect.pipe(
      Effect.matchEffect({
        onFailure: (failure) => failRun(failure, markFailed),
        onSuccess: (success) => Effect.succeed(success),
      }),
    );
