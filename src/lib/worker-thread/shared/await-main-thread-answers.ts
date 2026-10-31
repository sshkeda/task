import { Deferred, Effect, Ref, Semaphore } from "effect";
import type { Fiber } from "effect";
import type { RpcClient, RpcClientError } from "effect/rpc";

import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { TaskEvent } from "../../../shared/rpc.ts";
import type { MainThreadRpcs, StandardInputRead } from "../../../shared/rpc.ts";
import { isAwaitedTaskFailed } from "../../../shared/task-errors.ts";

const dieUnlessAnswered = dieOnInvariantFailure(
  "worker protocol",
  "the main thread answers each request the worker makes",
);

export const makeMainThreadAnswers = Effect.fn("makeMainThreadAnswers")(function* (
  mainThread: RpcClient.FromGroup<typeof MainThreadRpcs, RpcClientError.RpcClientError>,
  reportToMainThread: (event: typeof TaskEvent.Type) => Effect.Effect<void>,
  forkInWorkerScope: (effect: Effect.Effect<void>) => Effect.Effect<Fiber.Fiber<void>>,
) {
  const unclaimedStandardInputRead = yield* Ref.make<Deferred.Deferred<
    typeof StandardInputRead.Type
  > | null>(null);
  const oneStandardInputReadAtATime = Semaphore.makeUnsafe(1);
  const sendOrReuseStandardInputRequest = (taskName: string) =>
    Effect.gen(function* () {
      const unclaimed = yield* Ref.get(unclaimedStandardInputRead);
      if (unclaimed !== null) {
        return unclaimed;
      }
      const readDeferred = Deferred.makeUnsafe<typeof StandardInputRead.Type>();
      yield* Ref.set(unclaimedStandardInputRead, readDeferred);
      yield* forkInWorkerScope(
        mainThread.ReadStandardInput({ taskName }).pipe(
          dieUnlessAnswered,
          Effect.flatMap((read) => Deferred.succeed(readDeferred, read)),
          Effect.asVoid,
        ),
      );
      return readDeferred;
    });
  return {
    requestStandardInputRead: (taskName: string) =>
      Effect.uninterruptibleMask((restore) =>
        sendOrReuseStandardInputRequest(taskName).pipe(
          Effect.flatMap((readDeferred) => restore(Deferred.await(readDeferred))),
          Effect.tap(() => Ref.set(unclaimedStandardInputRead, null)),
        ),
      ).pipe(oneStandardInputReadAtATime.withPermits(1)),
    requestDependencyTaskResult: (dependencyTaskName: string, requestingTaskName: string) =>
      mainThread.RequestTask({ taskName: requestingTaskName, dependencyTaskName }).pipe(
        Effect.catchTag("RpcClientError", (error) => dieUnlessAnswered(Effect.fail(error))),
        Effect.map((answered) => answered.value),
        Effect.tapError((failure) =>
          isAwaitedTaskFailed(failure)
            ? reportToMainThread(
                TaskEvent.cases.TaskFiberReceivedAwaitedFailure.make({
                  taskName: requestingTaskName,
                  awaitedTaskName: failure.awaitedTaskName,
                }),
              )
            : Effect.void,
        ),
      ),
    reportStartedProcessAndAwaitRecord: (
      taskName: string,
      processId: number,
      startTime: string | null,
    ) =>
      mainThread.RecordStartedProcess({ taskName, processId, startTime }).pipe(dieUnlessAnswered),
    claimProcessesToReport: (
      taskName: string,
      processes: readonly (typeof ProcessIdentity.Type)[],
    ) => mainThread.ClaimProcessReports({ taskName, processes }).pipe(dieUnlessAnswered),
  };
});
