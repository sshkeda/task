import { Clock, Deferred, Effect, Exit, Ref } from "effect";

const terminalCommandsRef = Ref.makeUnsafe({
  runningCount: 0,
  lastEndedAtMilliseconds: 0,
  nextChange: Deferred.makeUnsafe<void>(),
});

const changeRunningCount = (change: -1 | 1) =>
  Effect.gen(function* () {
    const nowMilliseconds = yield* Clock.currentTimeMillis;
    const nextChange = yield* Deferred.make<void>();
    const changed = yield* Ref.modify(terminalCommandsRef, (current) => [
      current.nextChange,
      {
        runningCount: current.runningCount + change,
        lastEndedAtMilliseconds: change === 1 ? current.lastEndedAtMilliseconds : nowMilliseconds,
        nextChange,
      },
    ]);
    yield* Deferred.done(changed, Exit.void);
  });

export const whileACommandHasTheTerminal = <Success, Failure, Services>(
  running: Effect.Effect<Success, Failure, Services>,
) =>
  Effect.acquireUseRelease(
    changeRunningCount(1),
    () => running,
    () => changeRunningCount(-1),
  );

export const readTerminalCommands = Ref.get(terminalCommandsRef).pipe(
  Effect.map(({ runningCount, lastEndedAtMilliseconds, nextChange }) => ({
    isAnyRunning: runningCount > 0,
    lastEndedAtMilliseconds,
    awaitChange: Deferred.await(nextChange),
  })),
);
