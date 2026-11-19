import { Deferred, Duration, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import type { Queue } from "effect";

import { labelLine } from "../../../shared/label-lines.ts";
import {
  printLine,
  standardErrorClosedDeferred,
  standardErrorWriteFailedDeferred,
} from "../../../shared/print-to-standard-error.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { StopRequested } from "../shared/main-thread-errors.ts";
import { StopState } from "../shared/provide-stop-state.ts";

const STOPPING_LINES = {
  SIGHUP: labelLine("stopping", null, "send SIGTERM or press ctrl-c to force"),
  SIGINT: labelLine("stopping", null, "press ctrl-c again to force"),
  SIGTERM: labelLine("stopping", null, "send SIGTERM or press ctrl-c again to force"),
} as const;

const GRACE_AFTER_STANDARD_ERROR_CLOSED = Duration.seconds(1);

const isStopRequested = Schema.is(StopRequested);

const requestStopOnSignal = Effect.fn(function* (
  signal: (typeof STOP_SIGNALS)[number],
  hangUpHeard: Deferred.Deferred<void>,
) {
  const shutdown = yield* StopState;
  if (signal === "SIGHUP") {
    yield* Deferred.done(hangUpHeard, Exit.void);
  }
  yield* Effect.gen(function* () {
    if (yield* shutdown.requestStop(signal)) {
      yield* printLine(STOPPING_LINES[signal]);
    }
  }).pipe(shutdown.holdTaskLogsDuring, shutdown.holdStopAnnouncementsDuring);
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
