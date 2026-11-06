import {
  Array as Arrays,
  Cause,
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

import {
  CommandOptionsRefused,
  CommandPipelineRefused,
  RedactedValueWithNulRefused,
} from "../../../../../shared/command-errors.ts";
import type { CommandRefusal } from "../../../../../shared/command-errors.ts";
import { formatFailureLines } from "../../../../../shared/format-error-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import {
  environmentEveryCommandSees,
  RUN_MARKERS_VARIABLE_NAME,
} from "../../../../../shared/read-environment-config.ts";
import { CauseWithoutFailure } from "../../../../../shared/report-errors.ts";
import { failuresOfCause } from "../../../../../shared/select-cause-reasons.ts";
import {
  makeStandardInputRunner,
  markStandardInputClosedByCommand,
} from "../../../../../shared/standard-input-streams.ts";
import type { StandardInputReadOutsideRootTask } from "../../../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import {
  findStartedGroupWitnessNow,
  processGroupHasOnlyEndedMembers,
  readLatestMemberStartTime,
} from "../../../../kill-leftover-processes/index.ts";
import { WorkerState } from "../../../shared/provide-worker-state.ts";
import { ProcessGroupNotSignaledByWorker } from "../../../shared/worker-thread-errors.ts";

const MODULE = "ChildProcess";

const OUTPUT_NAMES = ["stdout", "stderr"] as const;

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

const toPlatformError =
  (method: string, command: string, step?: string) => (thrown: Cause.UnknownError) => {
    const isNotFound =
      Predicate.hasProperty(thrown.cause, "code") &&
      (thrown.cause.code === "ENOENT" || thrown.cause.code === "ESRCH");
    return PlatformError.systemError({
      _tag: isNotFound ? "NotFound" : "Unknown",
      module: MODULE,
      method,
      pathOrDescriptor: command,
      description:
        step === undefined ? String(thrown.cause) : `${step} failed with ${String(thrown.cause)}`,
      cause: thrown.cause,
    });
  };

const reveal = <Plain extends string | undefined>(word: Plain | Redacted.Redacted) =>
  Redacted.isRedacted(word) ? Redacted.value(word) : word;

const holdsNul = (word: CommandLine.Word | undefined) =>
  Redacted.isRedacted(word) && Redacted.value(word).includes("\0");

const isBrokenPipe = (failure: PlatformError.PlatformError) =>
  Predicate.hasProperty(failure.cause, "code") && failure.cause.code === "EPIPE";

const describeExit = (subprocess: Bun.Subprocess, latestMemberStartTime: string | null) => ({
  code: subprocess.exitCode,
  signal: subprocess.signalCode,
  usage: subprocess.resourceUsage(),
  latestMemberStartTime,
});

export const spawnWithBun = Effect.fn("spawnWithBun")(function* (
  command: ChildProcess.Command,
  standardInputRefusal: StandardInputReadOutsideRootTask | null,
  packageDirectoryPath: string,
  cleanupDuration: Duration.Duration,
  leaderExitHandlers: {
    readonly onLeaderExited: (
      processId: number,
      latestMemberStartTime: string | null,
    ) => Effect.Effect<void>;
    readonly killMembersLeftByLeader: (
      processId: number,
      latestMemberStartTime: string,
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

  const redactedValuesWithNul = [
    ...command.args.flatMap((word, index) =>
      holdsNul(word) ? [`argument ${String(index + 1)}`] : [],
    ),
    ...Object.entries(spawnOptions.env ?? {}).flatMap(([name, value]) =>
      holdsNul(value) ? [`the environment variable ${name}`] : [],
    ),
  ];
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
    Effect.try(() => process.kill(-subprocess.pid, signal)).pipe(
      Effect.as(true),
      Effect.catchTag("UnknownError", (error) => {
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
      Effect.mapError((error) =>
        PlatformError.isPlatformError(error) ? error : toPlatformError("kill", programName)(error),
      ),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(false)),
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
      const leftNoMembers =
        Exit.isFailure(processExit) || processExit.value.latestMemberStartTime === null;
      if (leftNoMembers) {
        return false;
      }
      const witness = yield* findStartedGroupWitnessNow(null, subprocess.pid, {
        leaderStartTime: null,
        latestMemberStartTimeAtLeaderExit: processExit.value.latestMemberStartTime,
      }).pipe(
        workerState.provideRuntimeServices,
        Effect.mapError(listingFailureToPlatformError("kill")),
      );
      return witness === null ? false : yield* signalProcessGroup(subprocess, signal);
    });

  const reportProcessCleanupFailure =
    (processId: number) =>
    <Success, Failure, Services>(cleanup: Effect.Effect<Success, Failure, Services>) =>
      cleanup.pipe(
        Effect.matchEffect({
          onFailure: (error) =>
            Effect.logWarning(
              formatFailureLines(
                new ProcessGroupNotSignaledByWorker({
                  processGroupId: processId,
                  programName,
                  cause: error,
                }),
                null,
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
        yield* Effect.logWarning(
          `${programName} did not exit within ${Duration.format(forceKillAfter)} of ${signal}, so ${TASK_PACKAGE.name} sent SIGKILL to its process group`,
        );
        yield* signalProcessGroupUnlessReused(subprocess, "SIGKILL");
      }
      yield* Deferred.await(exitedDeferred);
    });

  const endMembersLeftByLeader = (
    subprocess: Bun.Subprocess,
    latestMemberStartTimeAtLeaderExit: string,
  ) =>
    signalProcessGroup(subprocess, spawnOptions.killSignal ?? "SIGTERM").pipe(
      Effect.andThen(
        Effect.forEach(
          OUTPUT_NAMES.filter((name) => subprocess[name] instanceof ReadableStream),
          (name) => Deferred.await(outputReleased[name]),
          { discard: true },
        ).pipe(
          Effect.timeoutOption(readForceKillAfter(spawnOptions)),
          Effect.flatMap((released) =>
            Option.isSome(released)
              ? Effect.void
              : leaderExitHandlers.killMembersLeftByLeader(
                  subprocess.pid,
                  latestMemberStartTimeAtLeaderExit,
                ),
          ),
          reportProcessCleanupFailure(subprocess.pid),
          workerState.forkInWorkerScope,
        ),
      ),
    );

  const describeWorkingDirectory = (workingDirectory: string) =>
    workingDirectory.startsWith(`${packageDirectoryPath}/`)
      ? workingDirectory.slice(packageDirectoryPath.length + 1)
      : workingDirectory;
  const blameMissingWorkingDirectory = (
    workingDirectory: string,
    spawnFailure: PlatformError.PlatformError,
  ) => {
    const workingDirectoryFailure = (_tag: "BadResource" | "NotFound", whatIsWrong: string) =>
      PlatformError.systemError({
        _tag,
        module: MODULE,
        method: "spawn",
        pathOrDescriptor: programName,
        description: `the working directory ${describeWorkingDirectory(workingDirectory)} ${whatIsWrong}`,
        cause: spawnFailure.reason.cause,
      });
    return FileSystem.FileSystem.use((fileSystem) => fileSystem.stat(workingDirectory)).pipe(
      workerState.provideRuntimeServices,
      Effect.map((workingDirectoryInformation) =>
        workingDirectoryInformation.type === "Directory"
          ? spawnFailure
          : workingDirectoryFailure("BadResource", "is not a directory"),
      ),
      Effect.catchReason("PlatformError", "NotFound", () =>
        Effect.succeed(workingDirectoryFailure("NotFound", "doesn't exist")),
      ),
      Effect.flatMap(Effect.fail),
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
      : { ...declaredEnvironment, [RUN_MARKERS_VARIABLE_NAME]: runMarker.environmentValue };
  const child = yield* Effect.acquireRelease(
    Effect.try(() =>
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
    ).pipe(
      Effect.mapError(toPlatformError("spawn", programName)),
      Effect.catchTag("PlatformError", (error) =>
        spawnOptions.cwd === undefined
          ? Effect.fail(error)
          : blameMissingWorkingDirectory(spawnOptions.cwd, error),
      ),
      Effect.tap((subprocess) =>
        Deferred.complete(
          exitedDeferred,
          Effect.tryPromise(() => subprocess.exited).pipe(
            Effect.mapError(toPlatformError("exited", programName)),
            Effect.andThen(() =>
              readLatestMemberStartTime(null, subprocess.pid).pipe(
                workerState.provideRuntimeServices,
                Effect.mapError(listingFailureToPlatformError("exited")),
              ),
            ),
            Effect.tap((latestMemberStartTime) =>
              leaderExitHandlers.onLeaderExited(subprocess.pid, latestMemberStartTime),
            ),
            Effect.tap((latestMemberStartTime) =>
              latestMemberStartTime === null
                ? Effect.void
                : endMembersLeftByLeader(subprocess, latestMemberStartTime).pipe(
                    reportProcessCleanupFailure(subprocess.pid),
                  ),
            ),
            Effect.flatMap((latestMemberStartTime) =>
              Effect.try(() => describeExit(subprocess, latestMemberStartTime)).pipe(
                Effect.mapError(toPlatformError("resourceUsage", programName)),
              ),
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
            toPlatformError(`fromReadable(${name})`, programName)(new Cause.UnknownError(cause)),
        }).pipe(Stream.ensuring(Deferred.done(outputReleased[name], Exit.void)))
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
    Effect.tryPromise(() => Promise.resolve(write())).pipe(
      Effect.mapError(toPlatformError(method, programName, step)),
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
              PlatformError.systemError({
                _tag: "Unknown",
                module: MODULE,
                method: "exitCode",
                pathOrDescriptor: programName,
                description: `process interrupted by signal ${processExit.signal}`,
              }),
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
