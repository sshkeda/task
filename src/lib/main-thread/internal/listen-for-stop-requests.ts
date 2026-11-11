import { Deferred, Duration, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import type { Queue } from "effect";

import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  printLine,
  standardErrorClosedDeferred,
  standardErrorWriteFailedDeferred,
} from "../../../shared/print-to-standard-error.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { StopRequested } from "../shared/main-thread-errors.ts";
import { StopState } from "../shared/provide-stop-state.ts";

const STOPPING_LINES = {
  SIGHUP: "stopping, send SIGTERM or press ctrl-c to force",
  SIGINT: "stopping, press ctrl-c again to force",
  SIGTERM: "stopping, send SIGTERM or press ctrl-c again to force",
} as const;

const GRACE_AFTER_STANDARD_ERROR_CLOSED = Duration.seconds(1);

const isStopRequested = Schema.is(StopRequested);

const requestStopOnSignal = Effect.fn("requestStopOnSignal")(function* (
  signal: (typeof STOP_SIGNALS)[number],
  hangUpHeard: Deferred.Deferred<void>,
) {
  yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.signal`, signal);
  const shutdown = yield* StopState;
  if (signal === "SIGHUP") {
    yield* Deferred.done(hangUpHeard, Exit.void);
  }
  yield* Effect.gen(function* () {
    if (yield* shutdown.requestStop(signal)) {
      yield* printLine("Info", STOPPING_LINES[signal]);
    }
  }).pipe(shutdown.holdTaskLogsDuring);
}, Effect.uninterruptible);

const stopOnClosedTerminal = (hangUpHeard: Deferred.Deferred<void>) =>
  Effect.gen(function* () {
    const shutdown = yield* StopState;
    yield* Deferred.done(hangUpHeard, Exit.void);
    yield* shutdown.requestStop("SIGHUP");
  }).pipe(Effect.uninterruptible);

const stopOnClosedPipe = (hangUpHeard: Deferred.Deferred<void>) =>
  Effect.gen(function* () {
    const shutdown = yield* StopState;
    if (yield* Deferred.isDone(hangUpHeard)) {
      return;
    }
    if (!(yield* shutdown.isStopping)) {
      yield* shutdown.requestStop("SIGPIPE");
    }
    const forcedOrHungUp = yield* shutdown.awaitForcedStop.pipe(
      Effect.asVoid,
      Effect.raceFirst(Deferred.await(hangUpHeard)),
      Effect.timeoutOption(GRACE_AFTER_STANDARD_ERROR_CLOSED),
    );
    if (Option.isSome(forcedOrHungUp)) {
      return;
    }
    const outcome = yield* shutdown.outcome;
    yield* shutdown.forceStop(
      isStopRequested(outcome) ? outcome : new StopRequested({ signal: "SIGPIPE" }),
    );
  });

export const stopRequestsLayer = (signals: Queue.Dequeue<(typeof STOP_SIGNALS)[number]>) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const hangUpHeard = yield* Deferred.make<void>();
      yield* Stream.fromQueue(signals).pipe(
        Stream.runForEach((signal) => requestStopOnSignal(signal, hangUpHeard)),
        Effect.interruptible,
        Effect.forkScoped,
      );
      const shutdown = yield* StopState;
      yield* Deferred.await(standardErrorWriteFailedDeferred).pipe(
        Effect.flatMap(shutdown.forceStop),
        Effect.interruptible,
        Effect.forkScoped,
      );
      yield* Deferred.await(standardErrorClosedDeferred).pipe(
        Effect.flatMap((closed) =>
          closed === "terminal" ? stopOnClosedTerminal(hangUpHeard) : stopOnClosedPipe(hangUpHeard),
        ),
        Effect.interruptible,
        Effect.forkScoped,
      );
    }),
  );
