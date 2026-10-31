import { Duration, Effect, Queue, Record } from "effect";

export const STOP_SIGNAL_NUMBERS = { SIGHUP: 1, SIGINT: 2, SIGTERM: 15 } as const;

export const STOP_SIGNALS = Record.keys(STOP_SIGNAL_NUMBERS);

export const STOP_GRACE_PERIOD = Duration.fromInputUnsafe("5 seconds");

export const STOP_REASON_NUMBERS = { ...STOP_SIGNAL_NUMBERS, SIGPIPE: 13 } as const;

export const STOP_REASONS = Record.keys(STOP_REASON_NUMBERS);

const SHELL_EXIT_CODE_BASE_FOR_SIGNALS = 128;

export const exitCodeAfterStopSignal = (signal: (typeof STOP_REASONS)[number]) =>
  SHELL_EXIT_CODE_BASE_FOR_SIGNALS + STOP_REASON_NUMBERS[signal];

export const listenToSignals = <Signal extends NodeJS.Signals>(signals: readonly Signal[]) =>
  Effect.gen(function* () {
    const received = yield* Queue.unbounded<Signal>();
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        signals.map((signal) => {
          const offer = () => {
            Queue.offerUnsafe(received, signal);
          };
          process.on(signal, offer);
          return () => {
            process.off(signal, offer);
          };
        }),
      ),
      (removeListeners) =>
        Effect.sync(() => {
          for (const remove of removeListeners) {
            remove();
          }
        }),
    );
    return received;
  });
