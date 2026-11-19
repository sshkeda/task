import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Redacted,
  Ref,
  Scope,
  Semaphore,
  Stream,
  Struct,
} from "effect";
import type { PlatformError } from "effect/PlatformError";
import * as PlatformErrors from "effect/PlatformError";
import { ChildProcess } from "effect/process";

import { CommandOutputTooLong } from "../../../shared/command-errors.ts";
import { CommandRecapFiles } from "../../../shared/command-recap-file.ts";
import { findCallingUserLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { splitOutputPieces } from "../../../shared/label-output.ts";
import type { PRINTED_TO } from "../../../shared/label-output.ts";
import { TaskLines } from "../../../shared/mark-task-worker-thread-contexts.ts";
import { recapNestedRunIn } from "../../../shared/nested-recap-lines.ts";
import { takeOutputStreamId } from "../../../shared/output-stream-ids.ts";
import {
  environmentEveryCommandSees,
  RECAP_FILE_VARIABLE_NAME,
} from "../../../shared/read-environment-config.ts";
import { CauseWithoutFailure } from "../../../shared/report-errors.ts";
import { TerminalCommandEnded, TerminalCommands } from "../../../shared/run-in-terminal.ts";
import { failureValuesOfCause } from "../../../shared/select-cause-reasons.ts";
import {
  isStandardInputClosedByCommand,
  makeStandardInputRunner,
} from "../../../shared/standard-input-streams.ts";
import { Tasks } from "../../../shared/task-services.ts";
import {
  CapturedCommandFailed,
  CommandFailed,
  CommandNotStarted,
  CommandStandardInputFailed,
} from "../../errors/index.ts";
import { CommandEnd, decideCommandEnd } from "./decide-command-end.ts";
import { trustOutputModeName } from "./trust-output-mode-names.ts";

const printAndCapture = (
  output: Stream.Stream<Uint8Array, PlatformError>,
  streamId: number,
  printedTo: (typeof PRINTED_TO)[number],
) =>
  Effect.gen(function* () {
    const texts: string[] = [];
    return yield* splitOutputPieces(
      Stream.decodeText(output).pipe(
        Stream.tap((text) =>
          Effect.sync(() => {
            texts.push(text);
          }),
        ),
      ),
    ).pipe(
      Stream.runForEachArray((pieces) =>
        TaskLines.use((taskLines) => taskLines.printStreamOutput(streamId, pieces, printedTo)),
      ),
      Effect.as(texts),
    );
  });

const joinOutput = (
  texts: readonly string[],
  command: string,
  outputName: CommandOutputTooLong["outputName"],
) =>
  Effect.try({
    try: () => texts.join(""),
    catch: () => new CommandOutputTooLong({ command, outputName }),
  }).pipe(Effect.catch((error) => Effect.die(error)));

const OUTPUT_MODES = {
  logged: {
    readOutput: printAndCapture,
    returnOutput: (standardOutput: string) => standardOutput,
    makeFailure: (
      failure: ConstructorParameters<typeof CommandFailed>[0],
      nestedRecapLines?: readonly string[],
    ) =>
      nestedRecapLines === undefined
        ? new CommandFailed(failure)
        : recapNestedRunIn(new CommandFailed(failure), nestedRecapLines),
  },
  secret: {
    readOutput: (output: Stream.Stream<Uint8Array, PlatformError>) =>
      Stream.runCollect(Stream.decodeText(output)),
    returnOutput: (standardOutput: string) => Redacted.make(standardOutput),
    makeFailure: (failure: ConstructorParameters<typeof CommandFailed>[0]) =>
      new CapturedCommandFailed({
        command: failure.command,
        code: failure.code,
        signal: failure.signal ?? null,
        standardOutput: Redacted.make(failure.standardOutput),
        standardError: Redacted.make(failure.standardError),
        ...(failure.location === undefined ? {} : { location: failure.location }),
      }),
  },
  terminal: {
    readOutput: (output: Stream.Stream<Uint8Array, PlatformError>) =>
      Stream.runDrain(output).pipe(Effect.as([])),
    returnOutput: (): void => undefined,
    makeFailure: (failure: ConstructorParameters<typeof CommandFailed>[0]) =>
      new CommandFailed({ ...failure, stdio: "inherit" }),
  },
};

const outputModes: {
  readonly [Output in keyof typeof OUTPUT_MODES]: {
    readonly readOutput: (typeof OUTPUT_MODES)["logged"]["readOutput"];
    readonly returnOutput: (
      standardOutput: string,
    ) => ReturnType<(typeof OUTPUT_MODES)[Output]["returnOutput"]>;
    readonly makeFailure: (
      failure: ConstructorParameters<typeof CommandFailed>[0],
      nestedRecapLines?: readonly string[],
    ) => ReturnType<(typeof OUTPUT_MODES)[Output]["makeFailure"]>;
  };
} = OUTPUT_MODES;

const causeOfStandardInputFailures = (failures: readonly CommandStandardInputFailed[]) =>
  Cause.fromReasons(failures.map((failure) => Cause.makeFailReason(failure)));

const terminalTurns = Semaphore.makeUnsafe(1);

export const exec = <
  Secret extends boolean = false,
  Stdio extends "inherit" | undefined = undefined,
>(
  name: string,
  commandArguments: readonly string[],
  ...[commandOptions = {}]: [Stdio] extends ["inherit"]
    ? [
        commandOptions: {
          readonly cwd?: string | undefined;
          readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
          readonly stdio: Stdio;
          readonly print?: never;
          readonly secret?: never;
        },
      ]
    : [Secret] extends [true]
      ? [
          commandOptions: {
            readonly cwd?: string | undefined;
            readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
            readonly stdin?: Stream.Stream<Uint8Array, PlatformError> | undefined;
            readonly secret: Secret;
            readonly print?: never;
            readonly stdio?: undefined extends Stdio ? undefined : Stdio;
          },
        ]
      : [
          commandOptions?: {
            readonly cwd?: string | undefined;
            readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
            readonly stdin?: Stream.Stream<Uint8Array, PlatformError> | undefined;
            readonly stdio?: undefined extends Stdio ? undefined : Stdio;
          } & (
            | { readonly secret?: Secret; readonly print?: undefined }
            | { readonly secret?: undefined; readonly print: true }
          ),
        ]
) => {
  const location = findCallingUserLocation();
  const calledAt = location === undefined ? {} : { location };
  return Tasks.use(() => {
    const options: {
      readonly cwd?: string | undefined;
      readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
      readonly stdin?: Stream.Stream<Uint8Array, PlatformError> | undefined;
      readonly secret?: Secret | undefined;
      readonly print?: true | undefined;
      readonly stdio?: Stdio | (undefined extends Stdio ? false : Stdio);
    } = commandOptions;
    const command = name.slice(name.lastIndexOf("/") + 1);
    const readCommandEnvironment = environmentEveryCommandSees.pipe(
      Effect.map((inherited) =>
        Struct.omit({ ...inherited, ...options.env }, [RECAP_FILE_VARIABLE_NAME]),
      ),
    );
    const runInTerminal = Effect.gen(function* () {
      const outputMode = outputModes[yield* trustOutputModeName(options)];
      const terminalCommands = yield* TerminalCommands;
      const ended = yield* terminalCommands({
        command: name,
        args: commandArguments,
        env: Object.fromEntries(
          Object.entries(yield* readCommandEnvironment).map(([variableName, value]) => [
            variableName,
            Redacted.isRedacted(value) ? Redacted.value(value) : value,
          ]),
        ),
        cwd: options.cwd ?? ".",
      }).pipe(
        Effect.mapError(
          (failure) => new CommandNotStarted({ command, cause: failure, ...calledAt }),
        ),
      );
      if (TerminalCommandEnded.guards.Signaled(ended)) {
        return yield* PlatformErrors.systemError({
          _tag: "Unknown",
          module: "ChildProcess",
          method: "exitCode",
          pathOrDescriptor: command,
          description: `process interrupted by signal ${ended.signal}`,
        });
      }
      if (ended.code === 0) {
        return outputMode.returnOutput("");
      }
      return yield* outputMode.makeFailure({
        command,
        code: ended.code,
        standardOutput: "",
        standardError: "",
        ...calledAt,
      });
    });
    const runCommandPrintingTo = (printedTo: (typeof PRINTED_TO)[number]) =>
      Effect.scoped(
        Effect.gen(function* () {
          const outputModeName = yield* trustOutputModeName(options);
          const outputMode = outputModes[outputModeName];
          const holdsOutput = outputModeName === "logged" && printedTo === "held";
          const recapFiles = Option.getOrNull(yield* Effect.serviceOption(CommandRecapFiles));
          const canHandRecapFile = outputModeName === "logged" && recapFiles !== null;
          const recapPath = canHandRecapFile ? yield* recapFiles.makeCommandRecapPath : null;
          const outputScope = yield* Scope.fork(yield* Effect.scope);
          const commandReleased = yield* Deferred.make<void>();
          const child = yield* ChildProcess.make(name, commandArguments, {
            ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
            env: {
              ...(yield* readCommandEnvironment),
              ...(recapPath === null ? {} : { [RECAP_FILE_VARIABLE_NAME]: recapPath }),
            },
            stdin: options.stdin === undefined ? "ignore" : "pipe",
            stderr: "pipe",
          }).pipe(
            Effect.mapError(
              (failure) => new CommandNotStarted({ command, cause: failure, ...calledAt }),
            ),
          );
          const standardInput = makeStandardInputRunner();
          const standardInputFiber = yield* Effect.forkScoped(
            options.stdin === undefined
              ? Effect.void
              : standardInput.run(Stream.run(options.stdin, child.stdin)),
          );
          const streamIds = [yield* takeOutputStreamId, yield* takeOutputStreamId] as const;
          const readUntilCommandReleased = (
            output: Stream.Stream<Uint8Array, PlatformError>,
            streamId: number,
            outputName: CommandOutputTooLong["outputName"],
          ) =>
            outputMode
              .readOutput(
                output.pipe(Stream.interruptWhen(Deferred.await(commandReleased))),
                streamId,
                printedTo,
              )
              .pipe(Effect.flatMap((texts) => joinOutput(texts, command, outputName)));
          const outputFiber = yield* Effect.forkIn(
            Effect.all(
              [
                readUntilCommandReleased(child.stdout, streamIds[0], "standard output"),
                readUntilCommandReleased(child.stderr, streamIds[1], "standard error"),
              ],
              { concurrency: "unbounded" },
            ),
            outputScope,
          );
          const taskLines = yield* TaskLines;
          const hasPassed = yield* Ref.make(false);
          yield* Scope.addFinalizer(
            outputScope,
            Deferred.done(commandReleased, Exit.void).pipe(
              Effect.andThen(Fiber.await(outputFiber)),
              Effect.andThen(
                holdsOutput
                  ? Effect.flatMap(Ref.get(hasPassed), (passed) =>
                      taskLines.settleHeldOutput(streamIds, passed ? "passed" : "failed"),
                    )
                  : Effect.void,
              ),
            ),
          );
          const describeStandardInputFailures = (
            standardInputExit: Exit.Exit<unknown, unknown>,
          ) => {
            if (Exit.isSuccess(standardInputExit)) {
              return [];
            }
            if (Cause.hasInterruptsOnly(standardInputExit.cause)) {
              return standardInput.interruptedItself()
                ? [new CauseWithoutFailure({ details: Cause.pretty(standardInputExit.cause) })]
                : [];
            }
            return failureValuesOfCause(standardInputExit.cause).filter(
              (failureValue) =>
                !(
                  PlatformErrors.isPlatformError(failureValue) &&
                  isStandardInputClosedByCommand(failureValue)
                ),
            );
          };
          const exitBeforeStandardInputFailed = yield* Effect.raceFirst(
            Effect.raceFirst(
              Effect.asSome(Effect.exit(child.exitCode)),
              Fiber.await(standardInputFiber).pipe(
                Effect.flatMap((standardInputExit) =>
                  describeStandardInputFailures(standardInputExit).length > 0
                    ? Effect.succeedNone
                    : Effect.never,
                ),
              ),
            ),
            Fiber.join(outputFiber).pipe(Effect.andThen(Effect.never)),
          );
          const exitCodeExit = yield* Option.match(exitBeforeStandardInputFailed, {
            onNone: () => child.kill().pipe(Effect.andThen(Effect.exit(child.exitCode))),
            onSome: Effect.succeed,
          });
          yield* Fiber.interrupt(standardInputFiber);
          const standardInputFailures = describeStandardInputFailures(
            yield* Fiber.await(standardInputFiber),
          ).map(
            (failureValue) =>
              new CommandStandardInputFailed({ command, cause: failureValue, ...calledAt }),
          );
          const failWithOutput = (
            code: number | null,
            signal: string | null,
            failures: readonly CommandStandardInputFailed[],
          ) =>
            Effect.gen(function* () {
              const [standardOutput, standardError] = yield* Fiber.join(outputFiber);
              const hasRecapPathAfterExit =
                code !== null && recapPath !== null && recapFiles !== null;
              const nestedRecapLines = hasRecapPathAfterExit
                ? yield* recapFiles.readCommandRecap(recapPath)
                : undefined;
              return yield* Effect.failCause(
                Cause.combine(
                  Cause.fail(
                    outputMode.makeFailure(
                      { command, code, signal, standardOutput, standardError, ...calledAt },
                      nestedRecapLines,
                    ),
                  ),
                  causeOfStandardInputFailures(failures),
                ),
              );
            });
          return yield* CommandEnd.match(decideCommandEnd(exitCodeExit, standardInputFailures), {
            Passed: () =>
              Fiber.join(outputFiber).pipe(
                Effect.tap(() => Ref.set(hasPassed, true)),
                Effect.map(([standardOutput]) => outputMode.returnOutput(standardOutput)),
              ),
            ExitedWithCode: ({ code, standardInputFailures: failures }) =>
              failWithOutput(code, null, failures),
            KilledBySignal: ({ signal }) => failWithOutput(null, signal, []),
            ExitFailedWithoutSignal: ({ cause }) =>
              Fiber.await(outputFiber).pipe(Effect.andThen(Effect.failCause(cause))),
            StandardInputFailed: ({ standardInputFailures: failures }) =>
              Effect.failCause(causeOfStandardInputFailures(failures)),
          });
        }),
      );
    if (options.stdio === "inherit") {
      return terminalTurns.withPermit(runInTerminal);
    }
    return runCommandPrintingTo(options.print === true ? "terminal and file" : "held");
  });
};
