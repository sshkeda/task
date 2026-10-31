import { isatty } from "node:tty";

import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunServices from "@effect/platform-bun/BunServices";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  flow,
  Layer,
  LogLevel,
  Option,
  Queue,
  Runtime,
  Schema,
  Stdio,
  Stream,
} from "effect";
import type { Cause } from "effect";
import type { PlatformError } from "effect/PlatformError";

import { readStackAbove } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import {
  isImportingTaskModule,
  isInsideTaskBody,
  isInsideTaskWorkerThread,
} from "../../../shared/mark-task-worker-thread-contexts.ts";
import {
  printLine,
  StandardErrorWriteFailed,
  standardErrorClosedDeferred,
  standardErrorLinesDeferred,
  standardErrorWriteFailedDeferred,
} from "../../../shared/print-to-standard-error.ts";
import { bunWatchModeFlag } from "../../../shared/read-environment-config.ts";
import type { StandardErrorLine } from "../../../shared/rpc.ts";
import { saveStandardStreamFlags } from "../../../shared/save-standard-stream-flags.ts";
import {
  laterRunCallDeferred,
  runningRootTaskDeferred,
} from "../../../shared/signal-run-called-twice.ts";
import {
  exitCodeAfterStopSignal,
  listenToSignals,
  STOP_SIGNALS,
} from "../../../shared/stop-signals.ts";
import {
  Exited,
  RunCalledDuringImport,
  RunCalledInBunWatchMode,
  RunCalledInStandaloneExecutable,
  RunCalledInTaskBody,
} from "../../../shared/task-errors.ts";
import type { Task } from "./make-tasks.ts";

const STANDARD_ERROR_FILE_DESCRIPTOR = 2;

const READER_WAIT_AFTER_A_STOP_SIGNAL = Duration.seconds(1);

class StandardErrorNotRead extends Schema.TaggedError<StandardErrorNotRead>()(
  "StandardErrorNotRead",
  { waitedMilliseconds: Schema.Finite },
) {
  public override get message() {
    return `nobody read the last lines on standard error within ${this.waitedMilliseconds}ms after a stop signal, so task exited without them`;
  }

  public override get [Runtime.errorReported]() {
    return false;
  }
}

const findErrnoCode = flow(
  Schema.decodeUnknownOption(Schema.Struct({ cause: Schema.Struct({ code: Schema.String }) })),
  Option.map((reason) => reason.cause.code),
  Option.getOrNull,
);

const readerHasGone = (errnoCode: string | null) =>
  errnoCode === "EPIPE" || (errnoCode === "EIO" && isatty(STANDARD_ERROR_FILE_DESCRIPTOR));

const recordWriteFailure = (failure: PlatformError) =>
  Effect.sync(() => {
    const errnoCode = findErrnoCode(failure.reason);
    if (readerHasGone(errnoCode)) {
      return Deferred.doneUnsafe(standardErrorClosedDeferred, Exit.void);
    }
    return Deferred.doneUnsafe(
      standardErrorWriteFailedDeferred,
      Exit.succeed(new StandardErrorWriteFailed({ errnoCode, reason: failure.message })),
    );
  });

const writeLines = (
  stdio: Stdio.Stdio,
  lines: Queue.Queue<typeof StandardErrorLine.Type, Cause.Done>,
  minimumLogLevel: LogLevel.LogLevel,
) =>
  Stream.fromQueue(lines).pipe(
    Stream.filter((line) => LogLevel.isGreaterThanOrEqualTo(line.logLevel, minimumLogLevel)),
    Stream.map((line) => line.text),
    Stream.run(stdio.stderr()),
    Effect.catchTag("PlatformError", (error) =>
      recordWriteFailure(error).pipe(Effect.andThen(Queue.end(lines))),
    ),
    Effect.asVoid,
  );

const exitCodeAfterWriting = <Ended extends NodeJS.Signals | number>(ended: Ended) =>
  Effect.gen(function* () {
    if (Deferred.isDoneUnsafe(standardErrorWriteFailedDeferred)) {
      return yield* yield* Deferred.await(standardErrorWriteFailedDeferred);
    }
    const readerLeftAfterTheRun = ended === 0 && Deferred.isDoneUnsafe(standardErrorClosedDeferred);
    return readerLeftAfterTheRun ? exitCodeAfterStopSignal("SIGPIPE") : ended;
  });

const printLinesDuring = <Ended extends NodeJS.Signals | number, Failure, Services>(
  program: Effect.Effect<Ended, Failure, Services>,
  minimumLogLevel: LogLevel.LogLevel,
) =>
  Effect.gen(function* () {
    const stdio = yield* Stdio.Stdio;
    const stopSignals = yield* listenToSignals(STOP_SIGNALS);
    const lines = yield* Queue.unbounded<typeof StandardErrorLine.Type, Cause.Done>();
    yield* Deferred.succeed(standardErrorLinesDeferred, lines);
    const writer = yield* writeLines(stdio, lines, minimumLogLevel).pipe(
      Effect.interruptible,
      Effect.forkScoped,
    );
    const ended = yield* program;
    yield* Queue.end(lines);
    const linesWritten = Fiber.join(writer);
    yield* Effect.raceFirst(
      linesWritten,
      Queue.take(stopSignals).pipe(
        Effect.andThen(
          linesWritten.pipe(
            Effect.timeoutOrElse({
              duration: READER_WAIT_AFTER_A_STOP_SIGNAL,
              orElse: () =>
                Effect.fail(
                  new StandardErrorNotRead({
                    waitedMilliseconds: Duration.toMillis(READER_WAIT_AFTER_A_STOP_SIGNAL),
                  }),
                ),
            }),
          ),
        ),
      ),
    );
    return yield* exitCodeAfterWriting(ended);
  }).pipe(Effect.scoped);

const restoreStandardStreamFlagsAfter = <Ended, Failure, Services>(
  program: Effect.Effect<Ended, Failure, Services>,
) =>
  Effect.gen(function* () {
    const restoreStandardStreamFlags = yield* saveStandardStreamFlags;
    return yield* program.pipe(Effect.tap(() => restoreStandardStreamFlags));
  }).pipe(Effect.scoped);

const MINIMUM_LOG_LEVEL_OF_LOG_OPTION = {
  info: "Info",
  error: "Error",
} as const satisfies Record<string, LogLevel.LogLevel>;

const runMain = Runtime.makeRunMain(({ fiber, teardown }) => {
  fiber.addObserver((taskExit) => {
    teardown(taskExit, (exitCode) => {
      process.exit(exitCode);
    });
  });
});

export const main = (
  rootTask: Task,
  options: { readonly log?: keyof typeof MINIMUM_LOG_LEVEL_OF_LOG_OPTION } = {},
) => {
  if (isInsideTaskWorkerThread()) {
    const isCalledWhileImportingTaskModule = !isInsideTaskBody() && isImportingTaskModule();
    if (isCalledWhileImportingTaskModule) {
      throw new RunCalledDuringImport();
    }
    throw new RunCalledInTaskBody({ rootTaskName: rootTask.name });
  }
  const runCall = Exit.succeed({
    rootTaskName: rootTask.name,
    stack: readStackAbove(main),
  });
  const isFirstRunCall = Deferred.doneUnsafe(runningRootTaskDeferred, runCall);
  if (!isFirstRunCall) {
    Deferred.doneUnsafe(laterRunCallDeferred, runCall);
    return;
  }
  runMain(
    Effect.gen(function* () {
      const signals = yield* listenToSignals(STOP_SIGNALS);
      if (Bun.isStandaloneExecutable) {
        yield* printLine(
          "Error",
          formatFailureLines(
            new RunCalledInStandaloneExecutable({ rootTaskName: rootTask.name }),
            null,
          ),
        );
        return 1;
      }
      const watchModeFlag = yield* bunWatchModeFlag;
      if (Option.isSome(watchModeFlag)) {
        yield* printLine(
          "Error",
          formatFailureLines(new RunCalledInBunWatchMode({ flag: watchModeFlag.value }), null),
        );
        return 1;
      }
      const supervision = yield* Effect.promise(() => import("../../supervise-the-run/index.ts"));
      const supervisorProcessId = yield* supervision.readSupervisorProcessId;
      const superviseThisRun =
        supervisorProcessId === null && (yield* supervision.isRootTaskFileThere);
      if (superviseThisRun) {
        return yield* supervision.superviseRun(signals);
      }
      if (supervisorProcessId !== null) {
        yield* supervision.reportReady;
      }
      const mainThread = yield* Effect.promise(() => import("../../main-thread/index.ts"));
      return yield* mainThread.runRootTaskFiber.pipe(
        Effect.provide(mainThread.MainThreadLive(rootTask, signals, supervisorProcessId)),
        mainThread.exitCodeOfTheRun,
        Effect.provide(mainThread.StopStateLive),
      );
    }).pipe(
      Effect.catchTags({
        RootTaskFileNotChecked: (failure) =>
          printLine("Error", formatFailureLines(failure, null)).pipe(Effect.as(1)),
        SupervisorSocketNotOpened: (failure) =>
          printLine("Error", formatFailureLines(failure, null)).pipe(Effect.as(1)),
      }),
      Effect.scoped,
      (program) =>
        printLinesDuring(program, MINIMUM_LOG_LEVEL_OF_LOG_OPTION[options.log ?? "info"]),
      Effect.provide(Layer.mergeAll(BunServices.layer, BunCrypto.layerSecureRandom)),
      restoreStandardStreamFlagsAfter,
      Effect.uninterruptible,
      Effect.flatMap((ended) =>
        typeof ended === "number"
          ? Effect.void.pipe(
              Effect.filterOrFail(
                () => ended === 0,
                () => new Exited({ code: ended }),
              ),
            )
          : Effect.sync(() => process.kill(process.pid, ended)).pipe(
              Effect.andThen(Effect.fail(new Exited({ code: 1 }))),
            ),
      ),
    ),
  );
};
