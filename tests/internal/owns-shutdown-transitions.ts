import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Layer } from "effect";

import { TaskCleanupTimedOut } from "../../src/lib/main-thread/shared/main-thread-errors.ts";
import { StopState, StopStateLive } from "../../src/lib/main-thread/shared/provide-stop-state.ts";

export const firstFailure = Task.make({
  name: "shutdown records only the first failure and gates new work",
  run: Effect.gen(function* () {
    const shutdown = yield* StopState;
    expect(yield* shutdown.isStopping).toBe(false);
    expect(yield* shutdown.hasFailed).toBe(false);
    expect(yield* shutdown.outcome).toBe(null);
    const failures = ["first", "second"].map(
      (taskName) => new TaskCleanupTimedOut({ taskName, cleanupMilliseconds: 1000 }),
    );
    const recorded = yield* Effect.forEach(failures, shutdown.recordFirstFailure, {
      concurrency: "unbounded",
    });
    expect(recorded.filter(Boolean).length).toBe(1);
    const winner = yield* Effect.fromNullishOr(failures[recorded.indexOf(true)]);
    expect(yield* shutdown.awaitFailure).toBe(winner);
    expect(yield* shutdown.outcome).toBe(winner);
    expect(yield* shutdown.isStopping).toBe(true);
    expect(yield* shutdown.begin).toBe(true);
    expect(yield* shutdown.hasFailed).toBe(true);
  }).pipe((checkingShutdown) =>
    Effect.scopedWith((scope) =>
      Layer.buildWithScope(StopStateLive, scope).pipe(
        Effect.flatMap((stopState) => Effect.provideContext(checkingShutdown, stopState)),
      ),
    ),
  ),
});

export const signals = Task.make({
  name: "shutdown escalates a second signal and preserves the first forced-stop signal",
  run: Effect.gen(function* () {
    const shutdown = yield* StopState;
    expect(yield* shutdown.requestStop("SIGINT")).toBe(true);
    expect((yield* shutdown.awaitFailure)._tag).toBe("StopRequested");
    expect(yield* shutdown.begin).toBe(true);
    expect(yield* shutdown.hasFailed).toBe(true);
    expect(yield* shutdown.requestStop("SIGTERM")).toBe(false);
    const forcedStop = yield* shutdown.awaitForcedStop;
    expect(forcedStop).toMatchObject({ signal: "SIGTERM" });
    yield* shutdown.requestStop("SIGINT");
    expect(yield* shutdown.outcome).toBe(forcedStop);
    expect(yield* shutdown.awaitForcedStop).toBe(forcedStop);
  }).pipe((checkingShutdown) =>
    Effect.scopedWith((scope) =>
      Layer.buildWithScope(StopStateLive, scope).pipe(
        Effect.flatMap((stopState) => Effect.provideContext(checkingShutdown, stopState)),
      ),
    ),
  ),
});

export const normalCleanup = Task.make({
  name: "a signal during normal shutdown forces cleanup and outranks a later failure",
  run: Effect.gen(function* () {
    const shutdown = yield* StopState;
    expect(yield* shutdown.begin).toBe(false);
    expect(yield* shutdown.hasFailed).toBe(false);
    expect(yield* shutdown.isStopping).toBe(true);
    expect(yield* shutdown.outcome).toBe(null);
    yield* shutdown.requestStop("SIGINT");
    const forcedStop = yield* shutdown.awaitForcedStop;
    expect(forcedStop).toMatchObject({ signal: "SIGINT" });
    expect(
      yield* shutdown.recordFirstFailure(
        new TaskCleanupTimedOut({ taskName: "cleanup", cleanupMilliseconds: 1000 }),
      ),
    ).toBe(true);
    expect(yield* shutdown.outcome).toBe(forcedStop);
  }).pipe((checkingShutdown) =>
    Effect.scopedWith((scope) =>
      Layer.buildWithScope(StopStateLive, scope).pipe(
        Effect.flatMap((stopState) => Effect.provideContext(checkingShutdown, stopState)),
      ),
    ),
  ),
});
