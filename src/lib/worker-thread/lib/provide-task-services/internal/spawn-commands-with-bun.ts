import {
  Array as Arrays,
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Option,
  PlatformError,
  Predicate,
  Record,
  Redacted,
  Sink,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import type { CommandLine } from "effect/process";

import type { ProcessIdentity } from "../../../../../shared/claim-processes-to-report.ts";
import {
  CommandOptionsRefused,
  CommandPipelineRefused,
  RedactedNonStringRefused,
  RedactedValueWithNulRefused,
  WorkingDirectoryUnusable,
} from "../../../../../shared/command-errors.ts";
import type { CommandRefusal } from "../../../../../shared/command-errors.ts";
import { describeFailureLines } from "../../../../../shared/format-error-lines.ts";
import { markKilledBySignal } from "../../../../../shared/killed-by-signal.ts";
import { TaskLines } from "../../../../../shared/mark-task-worker-thread-contexts.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import {
  environmentEveryCommandSees,
  RUN_MARKERS_VARIABLE_NAME,
} from "../../../../../shared/read-environment-config.ts";
import { CauseWithoutFailure } from "../../../../../shared/report-errors.ts";
import { markCommandOfTask } from "../../../../../shared/run-marker.ts";
import { failuresOfCause } from "../../../../../shared/select-cause-reasons.ts";
import {
  makeStandardInputRunner,
  markStandardInputClosedByCommand,
} from "../../../../../shared/standard-input-streams.ts";
import { STOP_GRACE_PERIOD } from "../../../../../shared/stop-signals.ts";
import type { StandardInputReadOutsideRootTask } from "../../../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import {
  findStartedGroupWitnessNow,
  listMembersLeftByLeader,
  processGroupHasOnlyEndedMembers,
  readMembersAtLeaderExit,
} from "../../../../kill-leftover-processes/index.ts";
import { WorkerState } from "../../../shared/provide-worker-state.ts";
import { ProcessGroupNotSignaledByWorker } from "../../../shared/worker-thread-errors.ts";

const MODULE = "ChildProcess";

const reportWarning = (text: string) => TaskLines.use((taskLines) => taskLines.printWarning(text));

const OUTPUT_NAMES = ["stdout", "stderr"] as const;

const OUTPUT_FLUSH_AFTER_LEADER_EXIT_MILLISECONDS = 100;

const OUTPUT_FLUSH_AFTER_LEADER_EXIT = Duration.millis(OUTPUT_FLUSH_AFTER_LEADER_EXIT_MILLISECONDS);

export const formatProgramNames = (command: ChildProcess.Command): string =>
  ChildProcess.isStandardCommand(command)
    ? command.command.slice(command.command.lastIndexOf("/") + 1)
    : `${formatProgramNames(command.left)} | ${formatProgramNames(command.right)}`;

const refuseUnsupportedCommand = (unsupportedCommand: typeof CommandRefusal.Type) =>
  new PlatformError.PlatformError(
    new PlatformError.BadArgument({
      module: MODULE,
      method: "spawn",
      description: unsupportedCommand.message,
      cause: unsupportedCommand,
    }),
  );

const toPlatformError = (thrown: {
  readonly method: string;
  readonly command: string;
  readonly step?: string;
  readonly cause: unknown;
}) => {
  const { cause, step } = thrown;
  const isNotFound =
    Predicate.hasProperty(cause, "code") && (cause.code === "ENOENT" || cause.code === "ESRCH");
  return PlatformError.systemError({
    _tag: isNotFound ? "NotFound" : "Unknown",
    module: MODULE,
    method: thrown.method,
    pathOrDescriptor: thrown.command,
    description: step === undefined ? String(cause) : `${step} failed with ${String(cause)}`,
    cause,
  });
};

const reveal = <Plain extends string | undefined>(word: Plain | Redacted.Redacted) =>
  Redacted.isRedacted(word) ? Redacted.value(word) : word;

const holdsNonString = (word: CommandLine.Word | undefined) =>
  Redacted.isRedacted(word) && !Predicate.isString(Redacted.value(word));

const holdsNul = (word: CommandLine.Word | undefined) =>
  Redacted.isRedacted(word) && !holdsNonString(word) && Redacted.value(word).includes("\0");

const isBrokenPipe = (failure: PlatformError.PlatformError) =>
  Predicate.hasProperty(failure.cause, "code") && failure.cause.code === "EPIPE";

const describeExit = (
  subprocess: Bun.Subprocess,
  membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
) => ({
  code: subprocess.exitCode,
  signal: subprocess.signalCode,
  usage: subprocess.resourceUsage(),
  membersAtLeaderExit,
});

const UNUSABLE_WORKING_DIRECTORIES = {
  missing: { reasonTag: "NotFound", whatIsWrong: "doesn't exist" },
  notADirectory: { reasonTag: "BadResource", whatIsWrong: "is not a directory" },
} as const;

const findUnusableWorkingDirectory = (workingDirectory: string) =>
  FileSystem.FileSystem.use((fileSystem) => fileSystem.stat(workingDirectory)).pipe(
    Effect.map((workingDirectoryInformation) =>
      workingDirectoryInformation.type === "Directory"
        ? null
        : {
            unusable: UNUSABLE_WORKING_DIRECTORIES.notADirectory,
            found: workingDirectoryInformation.type,
          },
    ),
    Effect.catchReason("PlatformError", "NotFound", (notFound) =>
      Effect.succeed({ unusable: UNUSABLE_WORKING_DIRECTORIES.missing, found: notFound }),
    ),
  );

const makeWorkingDirectoryFailure = (
  programName: string,
  runDirectoryPath: string,
  workingDirectory: string,
  unusable: (typeof UNUSABLE_WORKING_DIRECTORIES)[keyof typeof UNUSABLE_WORKING_DIRECTORIES],
  cause: PlatformError.PlatformError | PlatformError.SystemError | string,
) => {
  const unusableWorkingDirectory = new WorkingDirectoryUnusable({
    workingDirectory: workingDirectory.startsWith(`${runDirectoryPath}/`)
      ? workingDirectory.slice(runDirectoryPath.length + 1)
      : workingDirectory,
    whatIsWrong: unusable.whatIsWrong,
    cause,
  });
  return PlatformError.systemError({
    _tag: unusable.reasonTag,
    module: MODULE,
    method: "spawn",
    pathOrDescriptor: programName,
    description: unusableWorkingDirectory.message,
    cause: unusableWorkingDirectory,
  });
};

export const checkWorkingDirectoryBeforeSpawn = (
  programName: string,
  runDirectoryPath: string,
  workingDirectory: string,
) =>
  findUnusableWorkingDirectory(workingDirectory).pipe(
    Effect.flatMap((found) =>
      found === null
        ? Effect.void
        : Effect.fail(
            makeWorkingDirectoryFailure(
              programName,
              runDirectoryPath,
              workingDirectory,
              found.unusable,
              found.found,
            ),
          ),
    ),
  );

export const recheckWorkingDirectoryAfterSpawnFailure = (
  programName: string,
  runDirectoryPath: string,
  workingDirectory: string,
  spawnFailure: PlatformError.PlatformError,
) =>
  findUnusableWorkingDirectory(workingDirectory).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.fail(spawnFailure),
      onSuccess: (found) =>
        Effect.fail(
          found === null
            ? spawnFailure
            : makeWorkingDirectoryFailure(
                programName,
                runDirectoryPath,
                workingDirectory,
                found.unusable,
                spawnFailure,
              ),
        ),
    }),
  );

export const spawnWithBun = Effect.fn(function* (
  command: ChildProcess.Command,
  taskName: string,
  standardInputRefusal: StandardInputReadOutsideRootTask | null,
  runDirectoryPath: string,
  cleanupDuration: Duration.Duration,
  leaderExitHandlers: {
    readonly onLeaderExited: (
      processId: number,
      membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
    ) => Effect.Effect<void>;
    readonly killMembersLeftByLeader: (
      processId: number,
      membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
    ) => Effect.Effect<void, { readonly message: string }>;
  },
) {
  if (!ChildProcess.isStandardCommand(command)) {
    return yield* refuseUnsupportedCommand(
      new CommandPipelineRefused({ programNames: formatProgramNames(command) }),
    );
  }
  const workerState = yield* WorkerState;
  const programName = formatProgramNames(command);
  const spawnOptions = command.options;
  const readForceKillAfter = (killOptions: ChildProcess.KillOptions | undefined) =>
    Duration.fromInputUnsafe(killOptions?.forceKillAfter ?? cleanupDuration);
  const readLimitAfterLeaderExit = () => {
    const forceKillAfter = readForceKillAfter(spawnOptions);
    return Duration.isFinite(forceKillAfter) ? forceKillAfter : STOP_GRACE_PERIOD;
  };
  const unsupported = [
    spawnOptions.shell !== undefined && "shell",
    spawnOptions.detached === false && "detached: false",
    spawnOptions.additionalFds !== undefined && "additionalFds",
    ...(["stdin", "stdout", "stderr"] as const).map((name) => {
      const option = spawnOptions[name];
      const stream = Predicate.hasProperty(option, "stream") ? option.stream : option;
      return stream === "overlapped"
        ? `${name}: "overlapped"`
        : Sink.isSink(stream) && `${name} as a Sink`;
    }),
  ].filter((unsupportedOption) => Predicate.isString(unsupportedOption));
  if (Arrays.isArrayNonEmpty(unsupported)) {
    return yield* refuseUnsupportedCommand(
      new CommandOptionsRefused({ programName, optionNames: unsupported }),
    );
  }

  const namesOfRedactedValues = (isRefused: (word: CommandLine.Word | undefined) => boolean) => [
    ...command.args.flatMap((word, index) =>
      isRefused(word) ? [`argument ${String(index + 1)}`] : [],
    ),
    ...Object.entries(spawnOptions.env ?? {}).flatMap(([name, value]) =>
      isRefused(value) ? [`the environment variable ${name}`] : [],
    ),
  ];
  const redactedNonStrings = namesOfRedactedValues(holdsNonString);
  if (Arrays.isArrayNonEmpty(redactedNonStrings)) {
    return yield* refuseUnsupportedCommand(
      new RedactedNonStringRefused({ programName, valueNames: redactedNonStrings }),
    );
  }
  const redactedValuesWithNul = namesOfRedactedValues(holdsNul);
  if (Arrays.isArrayNonEmpty(redactedValuesWithNul)) {
    return yield* refuseUnsupportedCommand(
      new RedactedValueWithNulRefused({ programName, valueNames: redactedValuesWithNul }),
    );
  }

  const standardInputOption = Predicate.hasProperty(spawnOptions.stdin, "stream")
    ? spawnOptions.stdin.stream
    : (spawnOptions.stdin ?? "pipe");
  const endStandardInputOnDone = Predicate.hasProperty(spawnOptions.stdin, "stream")
    ? (spawnOptions.stdin.endOnDone ?? true)
    : true;
  const standardStreamOption = (name: "stderr" | "stdin" | "stdout") => {
    const option = spawnOptions[name];
    const stream = Predicate.hasProperty(option, "stream") ? option.stream : option;
    const isPassedThrough = stream === "inherit" || stream === "ignore";
    return isPassedThrough ? stream : "pipe";
  };
  const inheritsRefusedStandardInput =
    standardStreamOption("stdin") === "inherit" && standardInputRefusal !== null;
  if (inheritsRefusedStandardInput) {
    return yield* Effect.die(standardInputRefusal);
  }
  const outputReleased = {
    stdout: Deferred.makeUnsafe<void>(),
    stderr: Deferred.makeUnsafe<void>(),
  };
  const outputMustEndAtMilliseconds = {
    stdout: Deferred.makeUnsafe<number>(),
    stderr: Deferred.makeUnsafe<number>(),
  };
  const outputMayEndWhenIdle = Deferred.makeUnsafe<void>();
  const outputMayBeHeldOutsideGroup = Deferred.makeUnsafe<void>();
  const outputHeldOutsideGroupReported = Deferred.makeUnsafe<void>();
  const exitedDeferred = Deferred.makeUnsafe<
    ReturnType<typeof describeExit>,
    PlatformError.PlatformError
  >();
  const listingFailureToPlatformError =
    (method: string) => (listingFailure: { readonly message: string }) =>
      PlatformError.systemError({
        _tag: "Unknown",
        module: MODULE,
        method,
        pathOrDescriptor: programName,
        description: listingFailure.message,
        cause: listingFailure,
      });
  const signalProcessGroup = (subprocess: Bun.Subprocess, signal: ChildProcess.Signal) =>
    Effect.try({
      try: () => process.kill(-subprocess.pid, signal),
      catch: (cause) => toPlatformError({ method: "kill", command: programName, cause }),
    }).pipe(
      Effect.as(true),
      Effect.catchTag("PlatformError", (error) => {
        const permissionDenied =
          Predicate.hasProperty(error.cause, "code") && error.cause.code === "EPERM";
        return permissionDenied
          ? processGroupHasOnlyEndedMembers(null, subprocess.pid).pipe(
              workerState.provideRuntimeServices,
              Effect.mapError(listingFailureToPlatformError("kill")),
              Effect.flatMap((ended) => (ended ? Effect.succeed(false) : Effect.fail(error))),
            )
          : Effect.fail(error);
      }),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(false)),
    );
  const signalMembersLeftByLeader = (
    subprocess: Bun.Subprocess,
    signal: ChildProcess.Signal,
    membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
  ) =>
    findStartedGroupWitnessNow(
      null,
      subprocess.pid,
      { leaderStartTime: null, membersAtLeaderExit, taskName },
      workerState.runMarker,
    ).pipe(
      workerState.provideRuntimeServices,
      Effect.mapError(listingFailureToPlatformError("kill")),
      Effect.flatMap((witness) =>
        witness === null ? Effect.succeed(false) : signalProcessGroup(subprocess, signal),
      ),
    );
  const signalProcessGroupUnlessReused = (
    subprocess: Bun.Subprocess,
    signal: ChildProcess.Signal,
  ) =>
    Effect.gen(function* () {
      if (!Deferred.isDoneUnsafe(exitedDeferred)) {
        return yield* signalProcessGroup(subprocess, signal);
      }
      const processExit = yield* Effect.exit(Deferred.await(exitedDeferred));
      const membersAtLeaderExit = Exit.isFailure(processExit)
        ? []
        : processExit.value.membersAtLeaderExit;
      if (membersAtLeaderExit.length === 0) {
        return false;
      }
      return yield* signalMembersLeftByLeader(subprocess, signal, membersAtLeaderExit);
    });

  const reportProcessCleanupFailure =
    (processId: number) =>
    <Success, Failure, Services>(cleanup: Effect.Effect<Success, Failure, Services>) =>
      cleanup.pipe(
        Effect.matchEffect({
          onFailure: (error) =>
            TaskLines.use((taskLines) =>
              taskLines.printError(
                describeFailureLines(
                  new ProcessGroupNotSignaledByWorker({
                    processGroupId: processId,
                    programName,
                    cause: error,
                  }),
                ),
              ),
            ).pipe(
              Effect.annotateLogs(
                `${TASK_TRACE_LABEL_PREFIX}.cause`,
                Cause.pretty(Cause.die(error)),
              ),
            ),
          onSuccess: (success) => Effect.succeed(success),
        }),
      );

  const terminate = (
    subprocess: Bun.Subprocess,
    killOptions: ChildProcess.KillOptions | undefined,
  ) =>
    Effect.gen(function* () {
      const signal = killOptions?.killSignal ?? "SIGTERM";
      const forceKillAfter = readForceKillAfter(killOptions);
      yield* signalProcessGroupUnlessReused(subprocess, signal);
      yield* signalProcessGroupUnlessReused(subprocess, "SIGCONT");
      const ended = yield* Deferred.await(exitedDeferred).pipe(
        Effect.timeoutOption(forceKillAfter),
      );
      if (Option.isNone(ended)) {
        yield* reportWarning(
          `${programName} did not exit within ${Duration.format(forceKillAfter)} of ${signal}, so ${TASK_PACKAGE.name} sent SIGKILL to its process group`,
        );
        yield* signalProcessGroupUnlessReused(subprocess, "SIGKILL");
      }
      yield* Deferred.await(exitedDeferred);
    });

  const endOutputOnceIdle = Deferred.done(outputMayEndWhenIdle, Exit.void).pipe(Effect.asVoid);

  const outputStillOpen = `${programName} exited, but its output was still open ${Duration.format(OUTPUT_FLUSH_AFTER_LEADER_EXIT)} later`;
  const describeOutputHeldByAnotherProcess = () =>
    `${outputStillOpen}, and no process it left in its process group is still running, so another process holds it; ${TASK_PACKAGE.name} waits up to ${Duration.format(readLimitAfterLeaderExit())} for it to close`;

  const endOutputHeldOutsideGroupOnceIdle = Deferred.done(
    outputMayBeHeldOutsideGroup,
    Exit.void,
  ).pipe(Effect.andThen(endOutputOnceIdle));

  const reportOutputHeldOutsideGroupOnce = Deferred.isDone(outputMayBeHeldOutsideGroup).pipe(
    Effect.flatMap((mayBeHeld) =>
      mayBeHeld ? Deferred.done(outputHeldOutsideGroupReported, Exit.void) : Effect.succeed(false),
    ),
    Effect.flatMap((isFirstReport) =>
      isFirstReport ? reportWarning(describeOutputHeldByAnotherProcess()) : Effect.void,
    ),
  );

  const awaitOutputReleased = (subprocess: Bun.Subprocess) =>
    Effect.forEach(
      OUTPUT_NAMES.filter((name) => subprocess[name] instanceof ReadableStream),
      (name) => Deferred.await(outputReleased[name]),
      { discard: true },
    );

  const reportOutputHeldPastLimit = (
    subprocess: Bun.Subprocess,
    membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
    signal: ChildProcess.Signal,
  ) =>
    listMembersLeftByLeader(null, subprocess.pid, membersAtLeaderExit).pipe(
      workerState.provideRuntimeServices,
      Effect.mapError(listingFailureToPlatformError("kill")),
      Effect.flatMap((members) =>
        reportWarning(
          Arrays.isArrayNonEmpty(members)
            ? `${outputStillOpen}, so ${TASK_PACKAGE.name} sent ${signal} to the processes it left in its process group: ${members.map((member) => `${member.processId} (${member.command})`).join(", ")}`
            : describeOutputHeldByAnotherProcess(),
        ),
      ),
    );

  const endMembersLeftByLeader = (
    subprocess: Bun.Subprocess,
    membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
  ) => {
    const signal = spawnOptions.killSignal ?? "SIGTERM";
    const leaderWasKilled = subprocess.signalCode !== null;
    return (
      leaderWasKilled
        ? Effect.succeed(true)
        : awaitOutputReleased(subprocess).pipe(
            Effect.timeoutOption(OUTPUT_FLUSH_AFTER_LEADER_EXIT),
            Effect.map(Option.isSome),
          )
    ).pipe(
      Effect.flatMap((released) =>
        released ? Effect.void : reportOutputHeldPastLimit(subprocess, membersAtLeaderExit, signal),
      ),
      Effect.andThen(signalMembersLeftByLeader(subprocess, signal, membersAtLeaderExit)),
      Effect.andThen(signalMembersLeftByLeader(subprocess, "SIGCONT", membersAtLeaderExit)),
      Effect.andThen(
        awaitOutputReleased(subprocess).pipe(
          Effect.timeoutOption(readLimitAfterLeaderExit()),
          Effect.flatMap((released) =>
            Option.isSome(released)
              ? Effect.void
              : leaderExitHandlers
                  .killMembersLeftByLeader(subprocess.pid, membersAtLeaderExit)
                  .pipe(Effect.andThen(endOutputOnceIdle)),
          ),
        ),
      ),
      reportProcessCleanupFailure(subprocess.pid),
      workerState.forkInWorkerScope,
    );
  };

  const extendsEnvironmentEveryCommandSees =
    spawnOptions.extendEnv === true || spawnOptions.env === undefined;
  const declaredEnvironment = extendsEnvironmentEveryCommandSees
    ? { ...(yield* environmentEveryCommandSees), ...spawnOptions.env }
    : spawnOptions.env;
  const { runMarker } = workerState;
  const environment =
    runMarker === null
      ? declaredEnvironment
      : {
          ...declaredEnvironment,
          [RUN_MARKERS_VARIABLE_NAME]: markCommandOfTask(runMarker, taskName),
        };
  if (spawnOptions.cwd !== undefined) {
    yield* checkWorkingDirectoryBeforeSpawn(programName, runDirectoryPath, spawnOptions.cwd).pipe(
      workerState.provideRuntimeServices,
    );
  }
  const child = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        Bun.spawn([command.command, ...command.args.map(reveal)], {
          ...(spawnOptions.cwd === undefined ? {} : { cwd: spawnOptions.cwd }),
          env: Record.map(environment, reveal),
          stdio: [
            standardStreamOption("stdin"),
            standardStreamOption("stdout"),
            standardStreamOption("stderr"),
            ...(runMarker === null ? [] : [runMarker.fileDescriptor]),
          ],
          detached: true,
        }),
      catch: (cause) => toPlatformError({ method: "spawn", command: programName, cause }),
    }).pipe(
      Effect.catchTag("PlatformError", (spawnFailure) =>
        spawnOptions.cwd === undefined
          ? Effect.fail(spawnFailure)
          : recheckWorkingDirectoryAfterSpawnFailure(
              programName,
              runDirectoryPath,
              spawnOptions.cwd,
              spawnFailure,
            ).pipe(workerState.provideRuntimeServices),
      ),
      Effect.tap((subprocess) =>
        Deferred.complete(
          exitedDeferred,
          Effect.tryPromise({
            try: () => subprocess.exited,
            catch: (cause) => toPlatformError({ method: "exited", command: programName, cause }),
          }).pipe(
            Effect.andThen(() =>
              readMembersAtLeaderExit(null, subprocess.pid).pipe(
                workerState.provideRuntimeServices,
                Effect.mapError(listingFailureToPlatformError("exited")),
              ),
            ),
            Effect.tap((membersAtLeaderExit) =>
              leaderExitHandlers.onLeaderExited(subprocess.pid, membersAtLeaderExit),
            ),
            Effect.tap((membersAtLeaderExit) =>
              membersAtLeaderExit.length === 0
                ? endOutputHeldOutsideGroupOnceIdle
                : endMembersLeftByLeader(subprocess, membersAtLeaderExit),
            ),
            Effect.flatMap((membersAtLeaderExit) =>
              Effect.try({
                try: () => describeExit(subprocess, membersAtLeaderExit),
                catch: (cause) =>
                  toPlatformError({ method: "resourceUsage", command: programName, cause }),
              }),
            ),
          ),
        ).pipe(workerState.forkInWorkerScope),
      ),
    ),
    (subprocess) =>
      Deferred.isDone(exitedDeferred).pipe(
        Effect.flatMap((done) =>
          done
            ? signalProcessGroupUnlessReused(subprocess, spawnOptions.killSignal ?? "SIGTERM")
            : terminate(subprocess, spawnOptions),
        ),
        reportProcessCleanupFailure(subprocess.pid),
        Effect.andThen(
          Effect.forEach(
            [subprocess.stdout, subprocess.stderr],
            (readable) => {
              const isPipeWithoutReader = readable instanceof ReadableStream && !readable.locked;
              return isPipeWithoutReader ? Effect.promise(() => readable.cancel()) : Effect.void;
            },
            { discard: true },
          ),
        ),
      ),
  );

  const readOutput = (
    readable: ReadableStream<Uint8Array> | number | undefined,
    name: (typeof OUTPUT_NAMES)[number],
  ) =>
    readable instanceof ReadableStream
      ? Stream.fromReadableStream({
          evaluate: () => readable,
          releaseLockOnEnd: true,
          onError: (cause) =>
            toPlatformError({ method: `fromReadable(${name})`, command: programName, cause }),
        }).pipe(
          (output) =>
            Stream.transformPull(output, (pull) =>
              Effect.succeed(
                Effect.gen(function* () {
                  const reading = yield* Effect.forkChild(pull);
                  const mayEnd =
                    Deferred.isDoneUnsafe(outputMayEndWhenIdle) ||
                    (yield* Effect.raceFirst(
                      Fiber.await(reading).pipe(Effect.as(false)),
                      Deferred.await(outputMayEndWhenIdle).pipe(Effect.as(true)),
                    ));
                  if (!mayEnd) {
                    return yield* Fiber.join(reading);
                  }
                  const nowMilliseconds = yield* Clock.currentTimeMillis;
                  yield* Deferred.done(
                    outputMustEndAtMilliseconds[name],
                    Exit.succeed(nowMilliseconds + Duration.toMillis(readLimitAfterLeaderExit())),
                  );
                  const mustEndAtMilliseconds = yield* Deferred.await(
                    outputMustEndAtMilliseconds[name],
                  );
                  const readSoon = yield* Fiber.join(reading).pipe(
                    Effect.timeoutOption(
                      Duration.millis(
                        Math.max(
                          Math.min(
                            OUTPUT_FLUSH_AFTER_LEADER_EXIT_MILLISECONDS,
                            mustEndAtMilliseconds - nowMilliseconds,
                          ),
                          0,
                        ),
                      ),
                    ),
                  );
                  if (Option.isSome(readSoon)) {
                    return readSoon.value;
                  }
                  yield* reportOutputHeldOutsideGroupOnce;
                  const millisecondsLeft = mustEndAtMilliseconds - (yield* Clock.currentTimeMillis);
                  const read = yield* Fiber.join(reading).pipe(
                    Effect.timeoutOption(Duration.millis(Math.max(millisecondsLeft, 0))),
                  );
                  if (Option.isSome(read)) {
                    return read.value;
                  }
                  yield* Fiber.interrupt(reading);
                  return yield* Cause.done();
                }),
              ),
            ),
          Stream.ensuring(Deferred.done(outputReleased[name], Exit.void)),
        )
      : Stream.empty;
  const standardOutput = readOutput(child.stdout, "stdout");
  const standardError = readOutput(child.stderr, "stderr");

  const standardInputWriter = child.stdin;
  const writeStandardInput = (
    method: string,
    step: string,
    write: () => unknown,
    brokenPipeEndsTheInput: boolean,
  ) =>
    Effect.tryPromise({
      try: () => Promise.resolve(write()),
      catch: (cause) => toPlatformError({ method, command: programName, step, cause }),
    }).pipe(
      Effect.as(true),
      Effect.catchTag("PlatformError", (failure) => {
        const brokenPipeEndedTheInput = brokenPipeEndsTheInput && isBrokenPipe(failure);
        return brokenPipeEndedTheInput ? Effect.succeed(false) : Effect.fail(failure);
      }),
      Effect.mapError((failure) =>
        isBrokenPipe(failure) ? markStandardInputClosedByCommand(failure) : failure,
      ),
    );
  const hasNoStandardInputPipe =
    standardInputWriter === undefined || typeof standardInputWriter === "number";
  const makeStandardInputSink = (closedByCommand: "ends the input" | "fails") =>
    hasNoStandardInputPipe
      ? Sink.drain
      : Sink.forEachWhile((chunk: Uint8Array) =>
          writeStandardInput(
            "fromWritable(stdin)",
            "writing standard input",
            () => standardInputWriter.write(chunk),
            closedByCommand === "ends the input",
          ),
        ).pipe(
          Sink.onExit((sinkExit) =>
            endStandardInputOnDone
              ? writeStandardInput(
                  "end(stdin)",
                  "ending standard input",
                  () => standardInputWriter.end(),
                  closedByCommand === "ends the input" || Exit.isFailure(sinkExit),
                )
              : Effect.void,
          ),
        );
  const standardInput = makeStandardInputRunner();
  const standardInputFiber = Stream.isStream(standardInputOption)
    ? yield* standardInput
        .run(Stream.run(standardInputOption, makeStandardInputSink("ends the input")))
        .pipe(
          Effect.onError((cause) => {
            const stoppedOnRequest =
              Cause.hasInterruptsOnly(cause) && !standardInput.interruptedItself();
            return stoppedOnRequest
              ? Effect.void
              : terminate(child, spawnOptions).pipe(reportProcessCleanupFailure(child.pid));
          }),
          Effect.forkScoped,
        )
    : undefined;
  const failIfStandardInputStreamFailed =
    standardInputFiber === undefined
      ? Effect.void
      : Fiber.interrupt(standardInputFiber).pipe(
          Effect.andThen(Fiber.await(standardInputFiber)),
          Effect.flatMap((standardInputExit) => {
            if (Exit.isSuccess(standardInputExit)) {
              return Effect.void;
            }
            if (!Cause.hasInterruptsOnly(standardInputExit.cause)) {
              return Effect.failCause(failuresOfCause(standardInputExit.cause));
            }
            return standardInput.interruptedItself()
              ? Effect.die(
                  new CauseWithoutFailure({ details: Cause.pretty(standardInputExit.cause) }),
                )
              : Effect.void;
          }),
        );

  const handle = ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(child.pid),
    exitCode: Deferred.await(exitedDeferred).pipe(
      Effect.tap(() => failIfStandardInputStreamFailed),
      Effect.flatMap((processExit) =>
        processExit.code === null
          ? Effect.fail(
              markKilledBySignal(
                PlatformError.systemError({
                  _tag: "Unknown",
                  module: MODULE,
                  method: "exitCode",
                  pathOrDescriptor: programName,
                  description: `process interrupted by signal ${processExit.signal}`,
                }),
                String(processExit.signal),
              ),
            )
          : Effect.succeed(ChildProcessSpawner.ExitCode(processExit.code)),
      ),
    ),
    isRunning: Effect.map(Deferred.isDone(exitedDeferred), (done) => !done),
    kill: (killOptions) => terminate(child, killOptions),
    stdin: makeStandardInputSink("fails"),
    stdout: standardOutput,
    stderr: standardError,
    all: Stream.merge(standardOutput, standardError),
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
    unref: Effect.sync(() => {
      child.unref();
      return Effect.sync(() => {
        child.ref();
      });
    }),
  });
  return { handle, exited: Deferred.await(exitedDeferred) };
});
