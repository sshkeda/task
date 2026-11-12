import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Layer, Schema } from "effect";

import {
  moveTaskStopStage,
  TaskStopEvent,
  TaskStopStage,
} from "../../src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-stop-stage.ts";
import {
  moveRunStopStage,
  RunStopEvent,
  RunStopStage,
} from "../../src/lib/main-thread/shared/define-run-stop-stage.ts";
import type { RunFailure } from "../../src/lib/main-thread/shared/define-run-stop-stage.ts";
import {
  StopRequested,
  TaskCleanupTimedOut,
} from "../../src/lib/main-thread/shared/main-thread-errors.ts";
import { StopState, StopStateLive } from "../../src/lib/main-thread/shared/provide-stop-state.ts";
import { TaskTimedOut } from "../../src/shared/task-errors.ts";

const LIMIT_MILLISECONDS = 1000;

const cleanupTimedOut = new TaskCleanupTimedOut({
  taskName: "first",
  cleanupMilliseconds: LIMIT_MILLISECONDS,
});

const timedOut = new TaskTimedOut({
  taskName: "later",
  timeoutMilliseconds: LIMIT_MILLISECONDS,
});

const isStopRequested = Schema.is(StopRequested);

const withStopState = <Success, Failure>(
  checkingShutdown: Effect.Effect<Success, Failure, StopState>,
) =>
  Effect.scopedWith((scope) =>
    Layer.buildWithScope(StopStateLive, scope).pipe(
      Effect.flatMap((stopState) => Effect.provideContext(checkingShutdown, stopState)),
    ),
  );

export const recordsFirstFailure = Task.make({
  name: "shutdown records only the first failure and gates new work",
  run: Effect.gen(function* () {
    const shutdown = yield* StopState;
    expect(yield* shutdown.isStopping).toBe(false);
    expect(yield* shutdown.hasFailed).toBe(false);
    expect(yield* shutdown.outcome).toBe(null);
    const failures = ["first", "second"].map(
      (taskName) => new TaskCleanupTimedOut({ taskName, cleanupMilliseconds: LIMIT_MILLISECONDS }),
    );
    const recorded = yield* Effect.forEach(failures, shutdown.recordFirstFailure, {
      concurrency: "unbounded",
    });
    expect(recorded.filter(Boolean).length).toBe(1);
    const winner = yield* Effect.fromNullishOr(failures[recorded.indexOf(true)]);
    expect(yield* shutdown.awaitFailure).toBe(winner);
    expect(yield* shutdown.outcome).toBe(winner);
    expect(yield* shutdown.isStopping).toBe(true);
    yield* shutdown.begin;
    expect(yield* shutdown.hasFailed).toBe(true);
    expect(yield* shutdown.outcome).toBe(winner);
  }).pipe(withStopState),
});

export const signals = Task.make({
  name: "shutdown escalates a second signal, never a SIGHUP, and keeps the forcing signal as the outcome",
  run: Effect.gen(function* () {
    const shutdown = yield* StopState;
    expect(yield* shutdown.requestStop("SIGINT")).toBe(true);
    expect(yield* shutdown.awaitFailure).toMatchObject({ signal: "SIGINT" });
    yield* shutdown.begin;
    expect(yield* shutdown.requestStop("SIGHUP")).toBe(false);
    expect(yield* shutdown.isForced).toBe(false);
    expect(yield* shutdown.requestStop("SIGTERM")).toBe(false);
    yield* shutdown.awaitForcedStop;
    yield* shutdown.requestStop("SIGINT");
    expect(yield* shutdown.outcome).toMatchObject({ signal: "SIGTERM" });
  }).pipe(withStopState),
});

export const normalEnding = Task.make({
  name: "a signal while a passed run ends normally is a first stop that becomes the outcome without forcing",
  run: Effect.gen(function* () {
    const shutdown = yield* StopState;
    yield* shutdown.begin;
    expect(yield* shutdown.isStopping).toBe(true);
    expect(yield* shutdown.hasFailed).toBe(false);
    expect(yield* shutdown.outcome).toBe(null);
    expect(yield* shutdown.requestStop("SIGINT")).toBe(true);
    expect(yield* shutdown.awaitFailure).toMatchObject({ signal: "SIGINT" });
    expect(yield* shutdown.isForced).toBe(false);
    expect(yield* shutdown.recordFirstFailure(timedOut)).toBe(false);
    expect(yield* shutdown.outcome).toMatchObject({ signal: "SIGINT" });
  }).pipe(withStopState),
});

const describeFailure = (failure: typeof RunFailure.Type) =>
  isStopRequested(failure) ? failure.signal : failure._tag;

const describeStage = (stage: typeof RunStopStage.Type) =>
  [
    stage._tag,
    ...RunStopStage.match(stage, {
      Running: () => [],
      Failing: ({ firstFailure }) => [firstFailure],
      StoppingOnSignal: ({ firstFailure }) => [firstFailure],
      EndingNormally: () => [],
      Forced: ({ firstFailure, outcome }) => [firstFailure, outcome],
    }).map((failure) => describeFailure(failure)),
  ].join(" ");

const describeEvent = RunStopEvent.match({
  BeginEnding: () => "BeginEnding",
  Fail: ({ failure }) => `Fail ${describeFailure(failure)}`,
  Force: ({ failure }) => `Force ${describeFailure(failure)}`,
  Signal: ({ signal }) => `Signal ${signal}`,
});

const stages = [
  RunStopStage.cases.Running.make({}),
  RunStopStage.cases.EndingNormally.make({}),
  RunStopStage.cases.Failing.make({ firstFailure: cleanupTimedOut }),
  RunStopStage.cases.StoppingOnSignal.make({
    firstFailure: new StopRequested({ signal: "SIGINT" }),
  }),
  RunStopStage.cases.StoppingOnSignal.make({ firstFailure: cleanupTimedOut }),
  RunStopStage.cases.Forced.make({
    firstFailure: new StopRequested({ signal: "SIGINT" }),
    outcome: new StopRequested({ signal: "SIGTERM" }),
  }),
];

const events = [
  RunStopEvent.cases.BeginEnding.make({}),
  RunStopEvent.cases.Fail.make({ failure: timedOut }),
  RunStopEvent.cases.Force.make({ failure: timedOut }),
  RunStopEvent.cases.Force.make({ failure: new StopRequested({ signal: "SIGPIPE" }) }),
  RunStopEvent.cases.Signal.make({ signal: "SIGHUP" }),
  RunStopEvent.cases.Signal.make({ signal: "SIGINT" }),
];

const EXPECTED_MOVES = `
Running | BeginEnding -> EndingNormally | none
Running | Fail TaskTimedOut -> Failing TaskTimedOut | reportFailure
Running | Force TaskTimedOut -> Forced TaskTimedOut TaskTimedOut | none
Running | Force SIGPIPE -> Forced SIGPIPE SIGPIPE | none
Running | Signal SIGHUP -> StoppingOnSignal SIGHUP | announceStop
Running | Signal SIGINT -> StoppingOnSignal SIGINT | announceStop
EndingNormally | BeginEnding -> EndingNormally | none
EndingNormally | Fail TaskTimedOut -> Failing TaskTimedOut | reportFailure
EndingNormally | Force TaskTimedOut -> Forced TaskTimedOut TaskTimedOut | none
EndingNormally | Force SIGPIPE -> Forced SIGPIPE SIGPIPE | none
EndingNormally | Signal SIGHUP -> StoppingOnSignal SIGHUP | announceStop
EndingNormally | Signal SIGINT -> StoppingOnSignal SIGINT | announceStop
Failing TaskCleanupTimedOut | BeginEnding -> Failing TaskCleanupTimedOut | none
Failing TaskCleanupTimedOut | Fail TaskTimedOut -> Failing TaskCleanupTimedOut | none
Failing TaskCleanupTimedOut | Force TaskTimedOut -> Forced TaskCleanupTimedOut TaskTimedOut | none
Failing TaskCleanupTimedOut | Force SIGPIPE -> Forced TaskCleanupTimedOut TaskCleanupTimedOut | none
Failing TaskCleanupTimedOut | Signal SIGHUP -> StoppingOnSignal TaskCleanupTimedOut | announceStop
Failing TaskCleanupTimedOut | Signal SIGINT -> StoppingOnSignal TaskCleanupTimedOut | announceStop
StoppingOnSignal SIGINT | BeginEnding -> StoppingOnSignal SIGINT | none
StoppingOnSignal SIGINT | Fail TaskTimedOut -> StoppingOnSignal SIGINT | none
StoppingOnSignal SIGINT | Force TaskTimedOut -> Forced SIGINT TaskTimedOut | none
StoppingOnSignal SIGINT | Force SIGPIPE -> Forced SIGINT SIGPIPE | none
StoppingOnSignal SIGINT | Signal SIGHUP -> StoppingOnSignal SIGINT | none
StoppingOnSignal SIGINT | Signal SIGINT -> Forced SIGINT SIGINT | none
StoppingOnSignal TaskCleanupTimedOut | BeginEnding -> StoppingOnSignal TaskCleanupTimedOut | none
StoppingOnSignal TaskCleanupTimedOut | Fail TaskTimedOut -> StoppingOnSignal TaskCleanupTimedOut | none
StoppingOnSignal TaskCleanupTimedOut | Force TaskTimedOut -> Forced TaskCleanupTimedOut TaskTimedOut | none
StoppingOnSignal TaskCleanupTimedOut | Force SIGPIPE -> Forced TaskCleanupTimedOut TaskCleanupTimedOut | none
StoppingOnSignal TaskCleanupTimedOut | Signal SIGHUP -> StoppingOnSignal TaskCleanupTimedOut | none
StoppingOnSignal TaskCleanupTimedOut | Signal SIGINT -> Forced TaskCleanupTimedOut TaskCleanupTimedOut | none
Forced SIGINT SIGTERM | BeginEnding -> Forced SIGINT SIGTERM | none
Forced SIGINT SIGTERM | Fail TaskTimedOut -> Forced SIGINT SIGTERM | none
Forced SIGINT SIGTERM | Force TaskTimedOut -> Forced SIGINT SIGTERM | none
Forced SIGINT SIGTERM | Force SIGPIPE -> Forced SIGINT SIGTERM | none
Forced SIGINT SIGTERM | Signal SIGHUP -> Forced SIGINT SIGTERM | none
Forced SIGINT SIGTERM | Signal SIGINT -> Forced SIGINT SIGTERM | none
`;

export const everyMove = Task.make({
  name: "the run stop stage moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = stages.flatMap((stage) =>
      events.map((event) => {
        const [next, action] = moveRunStopStage(stage, event);
        return `${describeStage(stage)} | ${describeEvent(event)} -> ${describeStage(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_MOVES);
  }),
});

const FIRST_STOP_MILLISECONDS = 1000;

const LATER_STOP_MILLISECONDS = 2000;

const firstStop = { stopRequestedAtMilliseconds: FIRST_STOP_MILLISECONDS };

const describeTaskStopStage = (stage: typeof TaskStopStage.Type) =>
  [
    stage._tag,
    ...TaskStopStage.match(stage, {
      NotAsked: () => [],
      FailedBeforeStop: () => [],
      StopSent: ({ stopRequestedAtMilliseconds }) => [stopRequestedAtMilliseconds],
      StoppedBeforeBody: ({ stopRequestedAtMilliseconds }) => [stopRequestedAtMilliseconds],
      FailedBeforeCleanup: ({ stopRequestedAtMilliseconds }) => [stopRequestedAtMilliseconds],
      CleaningUp: ({ stopRequestedAtMilliseconds }) => [stopRequestedAtMilliseconds],
      CleanupNotStarted: () => [],
      CleanupTimedOut: ({ watchesWorker }) => [watchesWorker ? "watching" : "terminating"],
    }),
  ].join(" ");

const describeTaskStopEvent = TaskStopEvent.match({
  EndRun: () => "EndRun",
  FailBeforeCleanup: () => "FailBeforeCleanup",
  PassLimit: () => "PassLimit",
  RequestStop: ({ stopRequestedAtMilliseconds }) => `RequestStop ${stopRequestedAtMilliseconds}`,
  StartCleanup: () => "StartCleanup",
  StopBeforeBody: () => "StopBeforeBody",
});

const taskStopStages = [
  TaskStopStage.cases.NotAsked.make({}),
  TaskStopStage.cases.FailedBeforeStop.make({}),
  TaskStopStage.cases.StopSent.make(firstStop),
  TaskStopStage.cases.StoppedBeforeBody.make(firstStop),
  TaskStopStage.cases.FailedBeforeCleanup.make(firstStop),
  TaskStopStage.cases.CleaningUp.make(firstStop),
  TaskStopStage.cases.CleanupNotStarted.make({}),
  TaskStopStage.cases.CleanupTimedOut.make({ watchesWorker: false }),
  TaskStopStage.cases.CleanupTimedOut.make({ watchesWorker: true }),
];

const taskStopEvents = [
  TaskStopEvent.cases.EndRun.make({}),
  TaskStopEvent.cases.FailBeforeCleanup.make({}),
  TaskStopEvent.cases.PassLimit.make({}),
  TaskStopEvent.cases.RequestStop.make({ stopRequestedAtMilliseconds: LATER_STOP_MILLISECONDS }),
  TaskStopEvent.cases.StartCleanup.make({}),
  TaskStopEvent.cases.StopBeforeBody.make({}),
];

const EXPECTED_TASK_STOP_MOVES = `
NotAsked | EndRun -> NotAsked | none
NotAsked | FailBeforeCleanup -> FailedBeforeStop | none
NotAsked | PassLimit -> NotAsked | none
NotAsked | RequestStop 2000 -> StopSent 2000 | sendStop
NotAsked | StartCleanup -> NotAsked | none
NotAsked | StopBeforeBody -> NotAsked | none
FailedBeforeStop | EndRun -> FailedBeforeStop | interruptRequests
FailedBeforeStop | FailBeforeCleanup -> FailedBeforeStop | none
FailedBeforeStop | PassLimit -> FailedBeforeStop | none
FailedBeforeStop | RequestStop 2000 -> FailedBeforeCleanup 2000 | sendStop
FailedBeforeStop | StartCleanup -> FailedBeforeStop | none
FailedBeforeStop | StopBeforeBody -> FailedBeforeStop | none
StopSent 1000 | EndRun -> StopSent 1000 | none
StopSent 1000 | FailBeforeCleanup -> FailedBeforeCleanup 1000 | none
StopSent 1000 | PassLimit -> CleanupNotStarted | failNotAnswered
StopSent 1000 | RequestStop 2000 -> StopSent 1000 | none
StopSent 1000 | StartCleanup -> CleaningUp 1000 | none
StopSent 1000 | StopBeforeBody -> StoppedBeforeBody 1000 | none
StoppedBeforeBody 1000 | EndRun -> StoppedBeforeBody 1000 | none
StoppedBeforeBody 1000 | FailBeforeCleanup -> StoppedBeforeBody 1000 | none
StoppedBeforeBody 1000 | PassLimit -> CleanupNotStarted | failStillLoading
StoppedBeforeBody 1000 | RequestStop 2000 -> StoppedBeforeBody 1000 | none
StoppedBeforeBody 1000 | StartCleanup -> StoppedBeforeBody 1000 | none
StoppedBeforeBody 1000 | StopBeforeBody -> StoppedBeforeBody 1000 | none
FailedBeforeCleanup 1000 | EndRun -> FailedBeforeCleanup 1000 | interruptRequests
FailedBeforeCleanup 1000 | FailBeforeCleanup -> FailedBeforeCleanup 1000 | none
FailedBeforeCleanup 1000 | PassLimit -> CleanupTimedOut terminating | failTimedOutNotAnswered
FailedBeforeCleanup 1000 | RequestStop 2000 -> FailedBeforeCleanup 1000 | none
FailedBeforeCleanup 1000 | StartCleanup -> CleaningUp 1000 | none
FailedBeforeCleanup 1000 | StopBeforeBody -> FailedBeforeCleanup 1000 | none
CleaningUp 1000 | EndRun -> CleaningUp 1000 | interruptRequests
CleaningUp 1000 | FailBeforeCleanup -> CleaningUp 1000 | none
CleaningUp 1000 | PassLimit -> CleanupTimedOut watching | failTimedOutAndWatch
CleaningUp 1000 | RequestStop 2000 -> CleaningUp 1000 | none
CleaningUp 1000 | StartCleanup -> CleaningUp 1000 | none
CleaningUp 1000 | StopBeforeBody -> CleaningUp 1000 | none
CleanupNotStarted | EndRun -> CleanupNotStarted | none
CleanupNotStarted | FailBeforeCleanup -> CleanupNotStarted | none
CleanupNotStarted | PassLimit -> CleanupNotStarted | none
CleanupNotStarted | RequestStop 2000 -> CleanupNotStarted | none
CleanupNotStarted | StartCleanup -> CleanupNotStarted | none
CleanupNotStarted | StopBeforeBody -> CleanupNotStarted | none
CleanupTimedOut terminating | EndRun -> CleanupTimedOut terminating | interruptRequests
CleanupTimedOut terminating | FailBeforeCleanup -> CleanupTimedOut terminating | none
CleanupTimedOut terminating | PassLimit -> CleanupTimedOut terminating | none
CleanupTimedOut terminating | RequestStop 2000 -> CleanupTimedOut terminating | none
CleanupTimedOut terminating | StartCleanup -> CleanupTimedOut terminating | none
CleanupTimedOut terminating | StopBeforeBody -> CleanupTimedOut terminating | none
CleanupTimedOut watching | EndRun -> CleanupTimedOut watching | interruptRequests
CleanupTimedOut watching | FailBeforeCleanup -> CleanupTimedOut watching | none
CleanupTimedOut watching | PassLimit -> CleanupTimedOut watching | watch
CleanupTimedOut watching | RequestStop 2000 -> CleanupTimedOut watching | none
CleanupTimedOut watching | StartCleanup -> CleanupTimedOut watching | none
CleanupTimedOut watching | StopBeforeBody -> CleanupTimedOut watching | none
`;

export const everyTaskStopMove = Task.make({
  name: "the task stop stage moves as its transition table says for every stage and event",
  run: Effect.sync(() => {
    const moves = taskStopStages.flatMap((stage) =>
      taskStopEvents.map((event) => {
        const [next, action] = moveTaskStopStage(stage, event);
        return `${describeTaskStopStage(stage)} | ${describeTaskStopEvent(event)} -> ${describeTaskStopStage(next)} | ${action}`;
      }),
    );
    expect(`\n${moves.join("\n")}\n`).toBe(EXPECTED_TASK_STOP_MOVES);
  }),
});
