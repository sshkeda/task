import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Redacted,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import type { PlatformError } from "effect/PlatformError";
import * as PlatformErrors from "effect/PlatformError";
import { ChildProcess } from "effect/process";

import { CommandOutputTooLong } from "../../../shared/command-errors.ts";
import { signalThatKilled } from "../../../shared/killed-by-signal.ts";
import { environmentEveryCommandSees } from "../../../shared/read-environment-config.ts";
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
import { trustOutputModeName } from "./trust-output-mode-names.ts";

const logAndCapture = (output: Stream.Stream<Uint8Array, PlatformError>) =>
  Effect.suspend(() => {
    const texts: string[] = [];
    return Stream.decodeText(output).pipe(
      Stream.tap((text) =>
        Effect.sync(() => {
          texts.push(text);
        }),
      ),
      Stream.splitLines,
      Stream.runForEachArray((lines) => Effect.log(lines.join("\n"))),
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
    readOutput: logAndCapture,
    returnOutput: (standardOutput: string) => standardOutput,
    makeFailure: (failure: ConstructorParameters<typeof CommandFailed>[0]) =>
      new CommandFailed(failure),
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
      }),
  },
  terminal: {
    readOutput: (output: Stream.Stream<Uint8Array, PlatformError>) =>
      Stream.runDrain(output).pipe(Effect.as([])),
    returnOutput: (): void => undefined,
    makeFailure: (failure: ConstructorParameters<typeof CommandFailed>[0]) =>
      new CommandFailed(failure),
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
    ) => ReturnType<(typeof OUTPUT_MODES)[Output]["makeFailure"]>;
  };
} = OUTPUT_MODES;

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
        },
      ]
    : [Secret] extends [true]
      ? [
          commandOptions: {
            readonly cwd?: string | undefined;
            readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
            readonly stdin?: Stream.Stream<Uint8Array, PlatformError> | undefined;
            readonly secret: Secret;
            readonly stdio?: undefined extends Stdio ? undefined : Stdio;
          },
        ]
      : [
          commandOptions?: {
            readonly cwd?: string | undefined;
            readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
            readonly stdin?: Stream.Stream<Uint8Array, PlatformError> | undefined;
            readonly secret?: Secret;
            readonly stdio?: undefined extends Stdio ? undefined : Stdio;
          },
        ]
) =>
  Tasks.use(() => {
    const options: {
      readonly cwd?: string | undefined;
      readonly env?: Readonly<Record<string, Redacted.Redacted | string>> | undefined;
      readonly stdin?: Stream.Stream<Uint8Array, PlatformError> | undefined;
      readonly secret?: Secret;
      readonly stdio?: Stdio | (undefined extends Stdio ? false : Stdio);
    } = commandOptions;
    const command = name.slice(name.lastIndexOf("/") + 1);
    const runInTerminal = Effect.gen(function* () {
      const outputMode = outputModes[yield* trustOutputModeName(options)];
      const terminalCommands = yield* TerminalCommands;
      const ended = yield* terminalCommands({
        command: name,
        args: commandArguments,
        env: Object.fromEntries(
          Object.entries({ ...(yield* environmentEveryCommandSees), ...options.env }).map(
            ([variableName, value]) => [
              variableName,
              Redacted.isRedacted(value) ? Redacted.value(value) : value,
            ],
          ),
        ),
        cwd: options.cwd ?? ".",
      }).pipe(Effect.mapError((failure) => new CommandNotStarted({ command, cause: failure })));
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
      });
    });
    const runCommand = Effect.scoped(
      Effect.gen(function* () {
        const outputMode = outputModes[yield* trustOutputModeName(options)];
        const outputScope = yield* Scope.fork(yield* Effect.scope);
        const commandReleased = yield* Deferred.make<void>();
        const child = yield* ChildProcess.make(name, commandArguments, {
          ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
          env: { ...(yield* environmentEveryCommandSees), ...options.env },
          stdin: options.stdin === undefined ? "ignore" : "pipe",
          stderr: "pipe",
        }).pipe(Effect.mapError((failure) => new CommandNotStarted({ command, cause: failure })));
        const standardInput = makeStandardInputRunner();
        const standardInputFiber = yield* Effect.forkScoped(
          options.stdin === undefined
            ? Effect.void
            : standardInput.run(Stream.run(options.stdin, child.stdin)),
        );
        const readUntilCommandReleased = (
          output: Stream.Stream<Uint8Array, PlatformError>,
          outputName: CommandOutputTooLong["outputName"],
        ) =>
          outputMode
            .readOutput(output.pipe(Stream.interruptWhen(Deferred.await(commandReleased))))
            .pipe(Effect.flatMap((texts) => joinOutput(texts, command, outputName)));
        const outputFiber = yield* Effect.forkIn(
          Effect.all(
            [
              readUntilCommandReleased(child.stdout, "standard output"),
              readUntilCommandReleased(child.stderr, "standard error"),
            ],
            { concurrency: "unbounded" },
          ),
          outputScope,
        );
        yield* Scope.addFinalizer(
          outputScope,
          Deferred.done(commandReleased, Exit.void).pipe(Effect.andThen(Fiber.await(outputFiber))),
        );
        const describeStandardInputFailures = (standardInputExit: Exit.Exit<unknown, unknown>) => {
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
            Effect.exit(child.exitCode),
            Fiber.await(standardInputFiber).pipe(
              Effect.flatMap((standardInputExit) =>
                describeStandardInputFailures(standardInputExit).length > 0
                  ? Effect.succeed(null)
                  : Effect.never,
              ),
            ),
          ),
          Fiber.join(outputFiber).pipe(Effect.andThen(Effect.never)),
        );
        const exitCodeExit =
          exitBeforeStandardInputFailed ??
          (yield* child.kill().pipe(Effect.andThen(Effect.exit(child.exitCode))));
        yield* Fiber.interrupt(standardInputFiber);
        const standardInputFailures = Cause.fromReasons(
          describeStandardInputFailures(yield* Fiber.await(standardInputFiber)).map(
            (failureValue) =>
              Cause.makeFailReason(
                new CommandStandardInputFailed({ command, cause: failureValue }),
              ),
          ),
        );
        const standardInputFailed = standardInputFailures.reasons.length > 0;
        const killingSignal = Exit.isFailure(exitCodeExit)
          ? Option.match(Cause.findErrorOption(exitCodeExit.cause), {
              onNone: () => null,
              onSome: signalThatKilled,
            })
          : null;
        if (Exit.isFailure(exitCodeExit)) {
          if (standardInputFailed) {
            return yield* Effect.failCause(standardInputFailures);
          }
          if (killingSignal === null) {
            yield* Fiber.await(outputFiber);
            return yield* Effect.failCause(exitCodeExit.cause);
          }
        }
        const code = Exit.isSuccess(exitCodeExit) ? exitCodeExit.value : null;
        if (code === 0) {
          if (standardInputFailed) {
            return yield* Effect.failCause(standardInputFailures);
          }
          const [standardOutput] = yield* Fiber.join(outputFiber);
          return outputMode.returnOutput(standardOutput);
        }
        const [standardOutput, standardError] = yield* Fiber.join(outputFiber);
        return yield* Effect.failCause(
          Cause.combine(
            Cause.fail(
              outputMode.makeFailure({
                command,
                code,
                signal: killingSignal,
                standardOutput,
                standardError,
              }),
            ),
            standardInputFailures,
          ),
        );
      }),
    );
    return options.stdio === "inherit" ? terminalTurns.withPermit(runInTerminal) : runCommand;
  });
