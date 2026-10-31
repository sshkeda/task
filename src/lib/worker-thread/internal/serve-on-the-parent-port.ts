import { parentPort } from "node:worker_threads";

import { Deferred, Effect, Exit, FiberSet, Layer } from "effect";
import { WorkerRunner } from "effect/workers";
import { WorkerError, WorkerSpawnError } from "effect/workers/WorkerError";

export const ParentPortWorkerRunnerLive = Layer.succeed(WorkerRunner.WorkerRunnerPlatform)({
  start: <Outgoing, Incoming>(): Effect.Effect<
    WorkerRunner.WorkerRunner<Outgoing, Incoming>,
    WorkerError
  > =>
    Effect.gen(function* () {
      const port = yield* Effect.fromNullishOr(parentPort).pipe(
        Effect.mapError(
          () =>
            new WorkerError({
              reason: new WorkerSpawnError({ message: "not running in a worker thread" }),
            }),
        ),
      );
      const sendUnsafe = (_portId: number, message: Outgoing) => {
        port.postMessage([1, message]);
      };
      return {
        run: <Success, Failure, Services>(
          handler: (
            portId: number,
            message: Incoming,
          ) => Effect.Effect<Success, Failure, Services> | void,
        ) =>
          Effect.gen(function* () {
            const runInWorkerScope = yield* FiberSet.makeRuntime<Services, Success, Failure>();
            const closed = yield* Deferred.make<void>();
            const onMessage = (message: WorkerRunner.PlatformMessage<Incoming>) => {
              if (message[0] === 1) {
                Deferred.doneUnsafe(closed, Exit.void);
                return;
              }
              const handled = handler(0, message[1]);
              if (Effect.isEffect(handled)) {
                runInWorkerScope(handled);
              }
            };
            yield* Effect.acquireRelease(
              Effect.sync(() => {
                port.on("message", onMessage);
                port.postMessage([0]);
              }),
              () =>
                Effect.sync(() => {
                  port.off("message", onMessage);
                }),
            );
            yield* Deferred.await(closed);
          }).pipe(Effect.scoped),
        send: (portId: number, message: Outgoing) =>
          Effect.sync(() => {
            sendUnsafe(portId, message);
          }),
        sendUnsafe,
      };
    }),
});
