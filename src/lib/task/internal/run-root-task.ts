import { isatty } from "node:tty";
import { inspect } from "node:util";

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
  Predicate,
  Queue,
  Runtime,
  Schema,
  Stdio,
  Stream,
} from "effect";
import type { Cause } from "effect";
import { constVoid } from "effect/Function";
import type { PlatformError } from "effect/PlatformError";

import type { LogLevelInvalid } from "../../../shared/environment-config-errors.ts";
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
  findTasksMadeWithName,
  isMadeByThisCopy,
  listTasksMade,
} from "../../../shared/record-tasks-made.ts";
import { TASK_GROUP_KEY, TaskGroup, TaskListNotRecorded } from "../../../shared/root-task-group.ts";
import type { StandardErrorLine } from "../../../shared/rpc.ts";
import { saveStandardStreamFlags } from "../../../shared/save-standard-stream-flags.ts";
import {
  exitCodeAfterStopSignal,
  listenToSignals,
  STOP_SIGNALS,
} from "../../../shared/stop-signals.ts";
import {
  DefaultExportNotRunnable,
  Exited,
  RunArgumentsNotAccepted,
  RunCalledInBunEvaluation,
  RunCalledInBunWatchMode,
  RunCalledInStandaloneExecutable,
  StartTaskNotMade,
  TaskDefinitionNotGiven,
  TaskListNotGiven,
  TaskNameNotString,
} from "../../../shared/task-errors.ts";
import { TaskMadeByAnotherCopy } from "../../errors/index.ts";
import type {
  FlagGivenTwice,
  FlagInvalid,
  FlagMissing,
  FlagNotFlat,
  FlagNotRead,
  FlagReadTwoWays,
  FlagReserved,
  FlagValueMissing,
} from "./flag-errors.ts";
import type { Task } from "./make-tasks.ts";
import {
  checkDeclaredFlags,
  decodeTaskFlags,
  describeAcceptedFlags,
  readCommandLine,
} from "./read-command-line-flags.ts";

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
    readonly listedTasks: readonly Task[];
    readonly group: Effect.Success<typeof takeRunArgument>["rootTaskGroup"];
    readonly checksForgottenTasks: boolean;
    readonly flags: Readonly<Record<string, string>>;
  },
  log: (typeof LOG_LEVEL_NAMES)[number],
) =>
  Effect.gen(function* () {
    const signals = yield* listenToSignals(STOP_SIGNALS);
    if (Bun.isStandaloneExecutable) {
      yield* printFailure(new RunCalledInStandaloneExecutable({ rootTaskName: rootTask.name }));
      return 1;
    }
    const watchModeFlag = yield* bunWatchModeFlag;
    if (Option.isSome(watchModeFlag)) {
      yield* printFailure(new RunCalledInBunWatchMode({ flag: watchModeFlag.value }));
      return yield* Effect.sync(() => process.exit(1));
    }
    const evaluationFlag = yield* bunEvaluationFlag;
    if (Option.isSome(evaluationFlag)) {
      yield* printFailure(new RunCalledInBunEvaluation({ flag: evaluationFlag.value }));
      return 1;
    }
    const taskFromAnotherCopy = rootTask.listedTasks.find(
      (listedTask) => !isMadeByThisCopy(listedTask),
    );
    if (taskFromAnotherCopy !== undefined) {
      yield* printFailure(
        new TaskMadeByAnotherCopy({
          taskName: taskFromAnotherCopy.name,
          requestedTaskName: taskFromAnotherCopy.name,
        }),
      );
      return 1;
    }
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
        restoreStandardStreamFlagsAfter,
        Effect.uninterruptible,
        Effect.exit,
        Effect.flatMap((ended) =>
          Effect.sync(() => {
            resume(ended);
          }),
        ),
      ),
    );
  }).pipe(
    Effect.flatMap((ended) => {
      if (ended === 0) {
        return Effect.void;
      }
      if (typeof ended !== "number") {
        return Effect.sync(() => process.kill(process.pid, ended)).pipe(
          Effect.andThen(Effect.fail(new Exited({ code: 1 }))),
        );
      }
      return Effect.fail(new Exited({ code: ended }));
    }),
  );

const refuseTheRun = (
  refusal:
    | DefaultExportNotRunnable
    | FlagGivenTwice
    | FlagInvalid
    | FlagMissing
    | FlagNotFlat
    | FlagNotRead
    | FlagReadTwoWays
    | FlagReserved
    | FlagValueMissing
    | LogLevelInvalid
    | RunArgumentsNotAccepted
    | StartTaskNotMade
    | TaskDefinitionNotGiven
    | TaskListNotGiven
    | TaskNameNotString,
) => printFailure(refusal).pipe(Effect.andThen(Effect.fail(new Exited({ code: 1 }))));

const startMainRun = Runtime.makeRunMain(({ fiber, teardown }) => {
  fiber.addObserver((ended) => {
    teardown(ended, (exitCode) => {
      process.exit(exitCode);
    });
  });
});

const DefaultExportGroup = Schema.Struct({
  default: Schema.Struct({ [TASK_GROUP_KEY]: TaskGroup }),
});

const decodeDefaultExportGroup = Schema.decodeUnknownOption(DefaultExportGroup);

const decodeDefaultListNotRecorded = Schema.decodeUnknownOption(
  Schema.Struct({ default: Schema.Struct({ [TASK_GROUP_KEY]: TaskListNotRecorded }) }),
);

const FUNCTION_OF_LIST_MODE = { all: "Task.all", allSettled: "Task.allSettled" } as const;

const groupRoot = (taskGroup: typeof TaskGroup.Type) => {
  const name = Bun.main.slice(Bun.main.lastIndexOf("/") + 1);
  return {
    name,
    listedTasks: taskGroup.taskNames.flatMap((taskName) =>
      findTasksMadeWithName(taskName).slice(0, 1),
    ),
    group: { name, ...taskGroup },
    checksForgottenTasks: true,
  };
};

const SHELL_SAFE_WORD = /^[\w%+,./:=@-]+$/u;

const quoteForShell = (word: string) =>
  SHELL_SAFE_WORD.test(word) ? word : `'${word.replaceAll("'", String.raw`'\''`)}'`;

const printAcceptedFlags = (
  startTaskNames: Parameters<typeof describeAcceptedFlags>[1],
  flatFlags: Parameters<typeof describeAcceptedFlags>[2],
  pickedTaskNames: readonly string[],
) =>
  Effect.sync(() => {
    startMainRun(
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const stdio = yield* Stdio.Stdio;
        const command = [
          "bun",
          path.relative(yield* workingDirectoryPath, Bun.main),
          ...pickedTaskNames,
        ]
          .map((word) => quoteForShell(word))
          .join(" ");
        yield* Stream.make(describeAcceptedFlags(command, startTaskNames, flatFlags)).pipe(
          Stream.run(stdio.stdout()),
        );
      }).pipe(Effect.provide(BunServices.layer)),
    );
  });

const refuseTheDefaultExport = (refusal: Parameters<typeof refuseTheRun>[0]) =>
  Effect.sync(() => {
    startMainRun(refuseTheRun(refusal).pipe(Effect.provide(BunServices.layer)));
  });

const refuseAfterAStartupDefect = (cause: Cause.Cause<never>) =>
  Effect.sync(() => {
    startMainRun(
      Stdio.Stdio.pipe(
        Effect.flatMap((stdio) =>
          Stream.make(`${formatCauseLines(cause, null)}\n`).pipe(Stream.run(stdio.stderr())),
        ),
        Effect.ignore,
        Effect.andThen(Effect.fail(new Exited({ code: 1 }))),
        Effect.provide(BunServices.layer),
      ),
    );
  });

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
  const entryFileLoaded = yield* Effect.promise((): Promise<unknown> => import(Bun.main)).pipe(
    Effect.exit,
  );
  if (Exit.isFailure(entryFileLoaded)) {
    return;
  }
  const entryFile = entryFileLoaded.value;
  const hasDefaultExport = Predicate.hasProperty(entryFile, "default");
  const defaultExport = hasDefaultExport ? entryFile.default : null;
  const madeTasks = listTasksMade().flatMap(([, tasksMadeWithName]) => tasksMadeWithName);
  const defaultTaskMade = madeTasks.find((madeTask) => madeTask.task === defaultExport);
  const defaultTask = defaultTaskMade?.task;
  const defaultDefinitionNotGiven =
    defaultTask === undefined ? null : findDefinitionNotGiven(defaultTask);
  if (defaultDefinitionNotGiven !== null) {
    return yield* new TaskDefinitionNotGiven({
      given: defaultDefinitionNotGiven.given,
      location: defaultDefinitionNotGiven.location ?? Bun.main,
    });
  }
  const defaultTaskName: unknown = defaultTask?.name;
  const isDefaultTaskUnnamed =
    defaultTaskMade !== undefined && !Predicate.isString(defaultTaskName);
  if (isDefaultTaskUnnamed) {
    return yield* new TaskNameNotString({
      location: defaultTaskMade.location ?? Bun.main,
      nameText: inspect(defaultTaskName),
    });
  }
  const defaultGroup = decodeDefaultExportGroup(entryFile);
  const isDefaultExportRunnable = defaultTask !== undefined || Option.isSome(defaultGroup);
  const isUnreadableGroup =
    !isDefaultExportRunnable && Predicate.hasProperty(defaultExport, TASK_GROUP_KEY);
  const listNotRecorded = decodeDefaultListNotRecorded(entryFile);
  if (Option.isSome(listNotRecorded)) {
    const { mode, given } = listNotRecorded.value.default[TASK_GROUP_KEY];
    return yield* new TaskListNotGiven({
      calledFunction: FUNCTION_OF_LIST_MODE[mode],
      given,
      location: Bun.main,
    });
  }
  if (isUnreadableGroup) {
    return yield* new DefaultExportNotRunnable({
      location: Bun.main,
      defaultExport: "unreadable list",
      tasksInFile: null,
    });
  }
  if (!isDefaultExportRunnable) {
    const tasksMadeByTheEntryFile = yield* Effect.filter(madeTasks, (madeTask) =>
      isMadeByTheEntryFile(madeTask.location),
    ).pipe(Effect.provide(BunServices.layer));
    const exportedValues: readonly unknown[] = Predicate.isObject(entryFile)
      ? Object.values(entryFile)
      : [];
    const exportsATask = madeTasks.some((madeTask) => exportedValues.includes(madeTask.task));
    const makesATask = Arrays.isArrayNonEmpty(tasksMadeByTheEntryFile);
    const hasATask = makesATask || exportsATask;
    if (hasATask) {
      yield* refuseTheDefaultExport(
        new DefaultExportNotRunnable({
          location: Bun.main,
          defaultExport: hasDefaultExport ? "not a task" : "missing",
          tasksInFile: makesATask ? "makes" : "exports",
        }),
      );
    }
    return;
  }
  const flatFlags = yield* checkDeclaredFlags(madeTasks.map((madeTask) => madeTask.task));
  const commandLine = yield* readCommandLine(yield* commandLineArguments, flatFlags);
  const givenWords = [...commandLine.words];
  const isOneTaskName =
    givenWords.length === 1 &&
    givenWords.every((givenWord) => givenWord.trim() !== "" && !givenWord.startsWith("-"));
  const areArgumentsRefused = Arrays.isArrayNonEmpty(givenWords) && !isOneTaskName;
  if (areArgumentsRefused) {
    return yield* new RunArgumentsNotAccepted({ givenArguments: givenWords });
  }
  const startTaskName = givenWords.at(0);
  const startTask =
    startTaskName === undefined ? undefined : findTasksMadeWithName(startTaskName).at(0);
  const isStartTaskMissing = startTaskName !== undefined && startTask === undefined;
  if (isStartTaskMissing) {
    return yield* new StartTaskNotMade({ startTaskName });
  }
  const defaultRoot =
    defaultTask === undefined
      ? Option.match(defaultGroup, {
          onNone: () => null,
          onSome: (decodedGroup) => groupRoot(decodedGroup.default[TASK_GROUP_KEY]),
        })
      : {
          name: defaultTask.name,
          listedTasks: [defaultTask],
          group: null,
          checksForgottenTasks: true,
        };
  const startRoot =
    startTask === undefined
      ? defaultRoot
      : {
          name: startTask.name,
          listedTasks: [startTask],
          group: null,
          checksForgottenTasks: false,
        };
  if (startRoot === null) {
    return;
  }
  if (commandLine.asksForHelp) {
    return yield* printAcceptedFlags(
      startRoot.listedTasks.map((listedTask) => listedTask.name),
      flatFlags,
      startTask === undefined ? [] : [startTask.name],
    );
  }
  yield* Effect.forEach(
    startRoot.listedTasks,
    (listedTask) =>
      listedTask.flags === null
        ? Effect.void
        : decodeTaskFlags(listedTask.name, listedTask.flags, commandLine.values),
    { discard: true },
  );
  const logLevelName = yield* readLogLevelName();
  yield* Effect.sync(() => {
    startMainRun(superviseTheRun({ ...startRoot, flags: commandLine.values }, logLevelName));
  });
}).pipe(Effect.catch(refuseTheDefaultExport), Effect.catchCause(refuseAfterAStartupDefect));

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
        const supervision = yield* Effect.promise(() => import("../../supervise-the-run/index.ts"));
        const supervisorProcessId = yield* supervision.readSupervisorProcessId;
        if (supervisorProcessId !== null) {
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
