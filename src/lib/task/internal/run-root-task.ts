import { isatty } from "node:tty";

import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunServices from "@effect/platform-bun/BunServices";
import {
  Array as Arrays,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  flow,
  LogLevel,
  Option,
  Path,
  Queue,
  Runtime,
  Schema,
  Stdio,
  Stream,
} from "effect";
import type { Cause } from "effect";
import { constVoid } from "effect/Function";
import type { PlatformError } from "effect/PlatformError";

import { setProjectDirectoryPath } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { formatCauseLines, formatFailureLines } from "../../../shared/format-error-lines.ts";
import { isInsideTaskWorkerThread } from "../../../shared/mark-task-worker-thread-contexts.ts";
import {
  StandardErrorWriteFailed,
  standardErrorClosedDeferred,
  standardErrorLinesDeferred,
  standardErrorWriteFailedDeferred,
} from "../../../shared/print-to-standard-error.ts";
import {
  bunEvaluationFlag,
  bunWatchModeFlag,
  commandLineArguments,
  readLogLevelName,
  takeRunArgument,
  workingDirectoryPath,
} from "../../../shared/read-environment-config.ts";
import type { LOG_LEVEL_NAMES } from "../../../shared/read-environment-config.ts";
import {
  findDefinitionNotGiven,
  filePathOfLocation,
  isListMadeByThisCopy,
  listTasksMade,
} from "../../../shared/record-tasks-made.ts";
import type { RootTaskGroup } from "../../../shared/root-task-group.ts";
import type { StandardErrorLine } from "../../../shared/rpc.ts";
import { saveStandardStreamFlags } from "../../../shared/save-standard-stream-flags.ts";
import {
  exitCodeAfterStopSignal,
  JOB_CONTROL_SIGNALS,
  listenToSignals,
  STOP_SIGNALS,
} from "../../../shared/stop-signals.ts";
import { Exited } from "../../../shared/task-errors.ts";
import { checkDeclaredFlags, decodeTaskFlags, readCommandLine } from "./read-command-line-flags.ts";

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

const readerHasGone = (errnoCode: string | null, isTerminal: boolean) =>
  errnoCode === "EPIPE" || (errnoCode === "EIO" && isTerminal);

const recordWriteFailure = (failure: PlatformError, isTerminal: boolean) =>
  Effect.sync(() => {
    const errnoCode = findErrnoCode(failure.reason);
    if (readerHasGone(errnoCode, isTerminal)) {
      return Deferred.doneUnsafe(
        standardErrorClosedDeferred,
        Exit.succeed(errnoCode === "EPIPE" ? "pipe" : "terminal"),
      );
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
  isTerminal: boolean,
) =>
  Stream.fromQueue(lines).pipe(
    Stream.filter((line) => LogLevel.isGreaterThanOrEqualTo(line.logLevel, minimumLogLevel)),
    Stream.map((line) => line.text),
    Stream.run(stdio.stderr()),
    Effect.catchTag("PlatformError", (error) =>
      recordWriteFailure(error, isTerminal).pipe(Effect.andThen(Queue.end(lines))),
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
    const writer = yield* writeLines(
      stdio,
      lines,
      minimumLogLevel,
      isatty(STANDARD_ERROR_FILE_DESCRIPTOR),
    ).pipe(Effect.interruptible, Effect.forkScoped);
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

const RUN_PROCESS_FILE_PATH = Bun.fileURLToPath(import.meta.resolve("./start-the-run.ts"));

const isTaskOwnThread = Bun.main === RUN_PROCESS_FILE_PATH || isInsideTaskWorkerThread();

const printFailure = (error: Parameters<typeof formatFailureLines>[0]) =>
  Stdio.Stdio.pipe(
    Effect.flatMap((stdio) =>
      Stream.make(`${formatFailureLines(error, null)}\n`).pipe(Stream.run(stdio.stderr())),
    ),
    Effect.ignore,
  );

const runTheRootTask = (
  rootTask: {
    readonly name: string;
    readonly group: typeof RootTaskGroup.Type | null;
    readonly checksForgottenTasks: boolean;
    readonly flags: Readonly<Record<string, string>>;
  },
  log: (typeof LOG_LEVEL_NAMES)[number],
) =>
  Effect.gen(function* () {
    const signals = yield* listenToSignals([...STOP_SIGNALS, ...JOB_CONTROL_SIGNALS]);
    const supervision = yield* Effect.promise(() => import("../../supervise-the-run/index.ts"));
    return yield* supervision
      .superviseRun(signals, RUN_PROCESS_FILE_PATH, {
        rootTaskName: rootTask.name,
        rootTaskGroup: rootTask.group,
        checksForgottenTasks: rootTask.checksForgottenTasks,
        flags: rootTask.flags,
        scriptFilePath: Bun.main,
        log,
      })
      .pipe(Effect.catch((error) => printFailure(error).pipe(Effect.as(1))));
  });

const superviseTheRunInThisProcess = (
  rootTask: Parameters<typeof runTheRootTask>[0],
  log: (typeof LOG_LEVEL_NAMES)[number],
) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    yield* setProjectDirectoryPath(path.dirname(Bun.main));
    return yield* runTheRootTask(rootTask, log);
  }).pipe(Effect.scoped);

const startSupervision = Runtime.makeRunMain(constVoid);

const superviseTheRun = (
  rootTask: Parameters<typeof runTheRootTask>[0],
  log: (typeof LOG_LEVEL_NAMES)[number],
) =>
  Effect.callback<NodeJS.Signals | number>((resume) => {
    startSupervision(
      superviseTheRunInThisProcess(rootTask, log).pipe(
        Effect.provide([BunServices.layer, BunCrypto.layerSecureRandom]),
        Effect.uninterruptible,
        Effect.exit,
        Effect.flatMap((ended) =>
          Effect.sync(() => {
            resume(ended);
          }),
        ),
      ),
    );
  });

const exitAs = (ended: NodeJS.Signals | number) => {
  if (ended === 0) {
    return Effect.void;
  }
  if (typeof ended !== "number") {
    return Effect.sync(() => process.kill(process.pid, ended)).pipe(
      Effect.andThen(Effect.fail(new Exited({ code: 1 }))),
    );
  }
  return Effect.fail(new Exited({ code: ended }));
};

const startMainRun = Runtime.makeRunMain(({ fiber, teardown }) => {
  fiber.addObserver((ended) => {
    teardown(ended, (exitCode) => {
      process.exit(exitCode);
    });
  });
});

const printCause = (cause: Cause.Cause<never>) =>
  Stdio.Stdio.pipe(
    Effect.flatMap((stdio) =>
      Stream.make(`${formatCauseLines(cause, null)}\n`).pipe(Stream.run(stdio.stderr())),
    ),
    Effect.ignore,
  );

const isMadeByTheEntryFile = (location: string | undefined) =>
  Effect.gen(function* () {
    if (location === undefined) {
      return false;
    }
    const madeByFilePath = filePathOfLocation(location);
    if (madeByFilePath === Bun.main) {
      return true;
    }
    const fileSystem = yield* FileSystem.FileSystem;
    const entryFilePath = yield* fileSystem
      .realPath(Bun.main)
      .pipe(Effect.catchTag("PlatformError", () => Effect.succeed(Bun.main)));
    return madeByFilePath === entryFilePath;
  });

const startTheDefaultExportOnceLoaded = Effect.gen(function* () {
  yield* Effect.yieldNow;
  const entryFile = yield* Effect.promise((): Promise<unknown> => import(Bun.main)).pipe(
    Effect.exit,
    Effect.map(Exit.getSuccess),
  );
  const madeTasks = listTasksMade().flatMap(([, tasksMadeWithName]) =>
    tasksMadeWithName.map(({ task, location }) => ({
      task,
      location,
      definitionNotGiven: findDefinitionNotGiven(task),
    })),
  );
  const tasksMadeByTheEntryFile = yield* Effect.filter(madeTasks, (madeTask) =>
    isMadeByTheEntryFile(madeTask.location),
  );
  const givenArguments = yield* commandLineArguments;
  const commandLine = yield* checkDeclaredFlags(madeTasks.map((madeTask) => madeTask.task)).pipe(
    Effect.flatMap((flatFlags) =>
      readCommandLine(givenArguments, flatFlags).pipe(
        Effect.map((read) => ({ ...read, flatFlags })),
      ),
    ),
    Effect.result,
  );
  const path = yield* Path.Path;
  const { decideEntryStart, EntryStart } = yield* Effect.promise(
    () => import("./decide-entry-start.ts"),
  );
  const start = decideEntryStart({
    entryFile,
    entryFilePath: Bun.main,
    madeTasks,
    makesATask: Arrays.isArrayNonEmpty(tasksMadeByTheEntryFile),
    isListMadeHere: isListMadeByThisCopy,
    commandLine,
    entryCommand: ["bun", path.relative(yield* workingDirectoryPath, Bun.main)],
    isStandaloneExecutable: Bun.isStandaloneExecutable,
    watchModeFlag: yield* bunWatchModeFlag,
    evaluationFlag: yield* bunEvaluationFlag,
  });
  return yield* EntryStart.match(start, {
    NotStarted: () => Effect.succeed(null),
    Refused: ({ refusal }) => printFailure(refusal).pipe(Effect.as(1)),
    HelpAsked: ({ text }) =>
      Stdio.Stdio.pipe(
        Effect.flatMap((stdio) => Stream.make(text).pipe(Stream.run(stdio.stdout()))),
        Effect.as(0),
      ),
    Supervised: ({ listedTasks, ...root }) =>
      Effect.gen(function* () {
        yield* Effect.forEach(
          listedTasks,
          (listedTask) =>
            listedTask.flags === null
              ? Effect.void
              : decodeTaskFlags(listedTask.name, listedTask.flags, root.flags),
          { discard: true },
        );
        return yield* superviseTheRun(root, yield* readLogLevelName());
      }),
  });
}).pipe(
  Effect.catch((error) => printFailure(error).pipe(Effect.as(1))),
  Effect.catchCause((cause) => printCause(cause).pipe(Effect.as(1))),
  Effect.provide(BunServices.layer),
  restoreStandardStreamFlagsAfter,
  Effect.flatMap((ended) =>
    Effect.sync(() => {
      if (ended !== null) {
        startMainRun(exitAs(ended));
      }
    }),
  ),
);

export const startTheDefaultExport = () => {
  if (!isTaskOwnThread) {
    Effect.runFork(startTheDefaultExportOnceLoaded);
  }
};

export const startTheRun = () => {
  runMain(
    Effect.gen(function* () {
      const runArgument = yield* takeRunArgument;
      const { log } = runArgument;
      return yield* Effect.gen(function* () {
        const signals = yield* listenToSignals(STOP_SIGNALS);
        const signalsToAcknowledge = yield* listenToSignals(STOP_SIGNALS);
        const supervision = yield* Effect.promise(() => import("../../supervise-the-run/index.ts"));
        const supervisorProcessId = yield* supervision.readSupervisorProcessId;
        if (supervisorProcessId !== null) {
          yield* supervision.acknowledgeStops(signalsToAcknowledge);
          yield* supervision.reportReady;
        }
        const mainThread = yield* Effect.promise(() => import("../../main-thread/index.ts"));
        return yield* mainThread.runRootTaskFiber.pipe(
          Effect.provide(mainThread.MainThreadLive(runArgument, signals, supervisorProcessId)),
          mainThread.exitCodeOfTheRun,
          Effect.provide(mainThread.StopStateLive),
        );
      }).pipe(Effect.scoped, (program) =>
        printLinesDuring(program, MINIMUM_LOG_LEVEL_OF_LOG_OPTION[log]),
      );
    }).pipe(
      Effect.provide([BunServices.layer, BunCrypto.layerSecureRandom]),
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
