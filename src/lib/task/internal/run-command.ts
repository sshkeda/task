import { Cause, Deferred, Effect, Exit, Fiber, Redacted, Scope, Stream } from "effect";
import type { PlatformError } from "effect/PlatformError";
import * as PlatformErrors from "effect/PlatformError";
import { ChildProcess } from "effect/process";

import { environmentEveryCommandSees } from "../../../shared/read-environment-config.ts";
import { CauseWithoutFailure } from "../../../shared/report-errors.ts";
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
      Effect.map(() => texts.join("")),
    );
  });

const OUTPUT_MODES = {
  logged: {
    readOutput: logAndCapture,
    returnOutput: (standardOutput: string) => standardOutput,
    makeFailure: (failure: ConstructorParameters<typeof CommandFailed>[0]) =>
      new CommandFailed(failure),
  },
  secret: {
    readOutput: (output: Stream.Stream<Uint8Array, PlatformError>) =>
      Stream.mkString(Stream.decodeText(output)),
    returnOutput: (standardOutput: string) => Redacted.make(standardOutput),
    makeFailure: (failure: ConstructorParameters<typeof CommandFailed>[0]) =>
      new CapturedCommandFailed({
        command: failure.command,
        code: failure.code,
        standardOutput: Redacted.make(failure.standardOutput),
        standardError: Redacted.make(failure.standardError),
      }),
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

export const exec = <Secret extends boolean = false>(
  name: string,
  commandArguments: readonly string[],
  ...[commandOptions = {}]: [Secret] extends [true]
    ? [
        commandOptions: {
          readonly cwd?: string;
          readonly env?: Readonly<Record<string, Redacted.Redacted | string>>;
          readonly stdin?: Stream.Stream<Uint8Array, PlatformError>;
          readonly secret: Secret;
        },
      ]
    : [
        commandOptions?: {
          readonly cwd?: string;
          readonly env?: Readonly<Record<string, Redacted.Redacted | string>>;
          readonly stdin?: Stream.Stream<Uint8Array, PlatformError>;
          readonly secret?: Secret;
        },
      ]
) =>
  Tasks.use(() =>
    Effect.scoped(
      Effect.gen(function* () {
        const outputMode = outputModes[yield* trustOutputModeName(commandOptions.secret)];
        const command = name.slice(name.lastIndexOf("/") + 1);
        const outputScope = yield* Scope.fork(yield* Effect.scope);
        const commandReleased = yield* Deferred.make<void>();
        const child = yield* ChildProcess.make(name, commandArguments, {
          ...(commandOptions.cwd === undefined ? {} : { cwd: commandOptions.cwd }),
          env: { ...(yield* environmentEveryCommandSees), ...commandOptions.env },
          stdin: commandOptions.stdin === undefined ? "ignore" : "pipe",
          stderr: "pipe",
        }).pipe(Effect.mapError((failure) => new CommandNotStarted({ command, cause: failure })));
        const standardInput = makeStandardInputRunner();
        const standardInputFiber = yield* Effect.forkScoped(
          commandOptions.stdin === undefined
            ? Effect.void
            : standardInput.run(Stream.run(commandOptions.stdin, child.stdin)),
        );
        const readUntilCommandReleased = (output: Stream.Stream<Uint8Array, PlatformError>) =>
          outputMode.readOutput(output.pipe(Stream.interruptWhen(Deferred.await(commandReleased))));
        const outputFiber = yield* Effect.forkIn(
          Effect.all(
            [readUntilCommandReleased(child.stdout), readUntilCommandReleased(child.stderr)],
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
        if (Exit.isFailure(exitCodeExit)) {
          return yield* standardInputFailed
            ? Effect.failCause(standardInputFailures)
            : Effect.failCause(exitCodeExit.cause);
        }
        if (exitCodeExit.value === 0) {
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
                code: exitCodeExit.value,
                standardOutput,
                standardError,
              }),
            ),
            standardInputFailures,
          ),
        );
      }),
    ),
  );
