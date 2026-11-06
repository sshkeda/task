import { Deferred, Duration, Effect, Layer, Option, Schema, Stream } from "effect";
import type { Queue } from "effect";

import { findUserLocationInStack } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  printLine,
  standardErrorClosedDeferred,
  standardErrorWriteFailedDeferred,
} from "../../../shared/print-to-standard-error.ts";
import {
  laterRunCallDeferred,
  runningRootTaskDeferred,
} from "../../../shared/signal-run-called-twice.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { RunCalledTwice } from "../../../shared/task-errors.ts";
import { StopRequested } from "../shared/main-thread-errors.ts";
import { RunRootTaskConfig } from "../shared/provide-run-root-task-config.ts";
import { StopState } from "../shared/provide-stop-state.ts";

const STOPPING_LINES = {
  SIGINT: "stopping, press ctrl-c again to force",
  SIGTERM: "stopping, send SIGTERM or press ctrl-c again to force",
} as const;

const GRACE_AFTER_STANDARD_ERROR_CLOSED = Duration.seconds(1);

const isStopRequested = Schema.is(StopRequested);

const requestStopOnSignal = Effect.fn("requestStopOnSignal")(function* (
  signal: (typeof STOP_SIGNALS)[number],
) {
  yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.signal`, signal);
  const shutdown = yield* StopState;
  if (signal === "SIGHUP") {
    return yield* shutdown.forceStop(new StopRequested({ signal }));
  }
  yield* Effect.gen(function* () {
    if (yield* shutdown.requestStop(signal)) {
      yield* printLine("Info", STOPPING_LINES[signal]);
    }
  }).pipe(shutdown.holdTaskLogsDuring);
}, Effect.uninterruptible);

export const stopRequestsLayer = (signals: Queue.Dequeue<(typeof STOP_SIGNALS)[number]>) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      yield* Stream.fromQueue(signals).pipe(
        Stream.runForEach(requestStopOnSignal),
        Effect.interruptible,
        Effect.forkScoped,
      );
      const shutdown = yield* StopState;
      const forceStopKeepingTheStopSignal = shutdown.outcome.pipe(
        Effect.flatMap((outcome) =>
          shutdown.forceStop(
            isStopRequested(outcome) ? outcome : new StopRequested({ signal: "SIGPIPE" }),
          ),
        ),
      );
      yield* Deferred.await(standardErrorWriteFailedDeferred).pipe(
        Effect.flatMap(shutdown.forceStop),
        Effect.interruptible,
        Effect.forkScoped,
      );
      yield* Deferred.await(standardErrorClosedDeferred).pipe(
        Effect.andThen(shutdown.isStopping),
        Effect.flatMap((isStopping) =>
          isStopping ? Effect.void : Effect.asVoid(shutdown.requestStop("SIGPIPE")),
        ),
        Effect.andThen(
          shutdown.awaitForcedStop.pipe(Effect.timeoutOption(GRACE_AFTER_STANDARD_ERROR_CLOSED)),
        ),
        Effect.flatMap((forcedStop) =>
          Option.isSome(forcedStop) ? Effect.void : forceStopKeepingTheStopSignal,
        ),
        Effect.interruptible,
        Effect.forkScoped,
      );
    }),
  );

export const runCalledTwiceLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const shutdown = yield* StopState;
    yield* RunRootTaskConfig;
    yield* Effect.all([
      Deferred.await(runningRootTaskDeferred),
      Deferred.await(laterRunCallDeferred),
    ]).pipe(
      Effect.map(
        ([runningCall, laterCall]) =>
          new RunCalledTwice({
            runningRootTaskName: runningCall.rootTaskName,
            runningRootTaskLocation:
              runningCall.stack === undefined
                ? undefined
                : findUserLocationInStack(runningCall.stack),
            laterRootTaskName: laterCall.rootTaskName,
            location:
              laterCall.stack === undefined ? undefined : findUserLocationInStack(laterCall.stack),
          }),
      ),
      Effect.flatMap(shutdown.recordFirstFailure),
      Effect.interruptible,
      Effect.forkScoped,
    );
  }),
);
