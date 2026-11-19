import { Chunk, Effect, Exit, Fiber, FileSystem, Option, Queue, Ref, Scope, Stream } from "effect";
import type { Cause } from "effect";
import { ChildProcess } from "effect/process";

import { failTestCleanup } from "./fail-test-cleanup.ts";
import { findMisprintedErrorLines } from "./find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "./find-repository-directory.ts";
import type { makeProjectUsingTaskFrom } from "./make-projects.ts";
import { ErrorsMisprinted, NeverPrinted } from "./test-errors.ts";

export const TASK_ENVIRONMENT_VARIABLE_PREFIX = "SSHKEDA_TASK";

export const TASK_STATE_DIRECTORY_NAME = ".sshkeda-task";

const readParentEnvironmentWithoutTaskConfig = Effect.sync(() =>
  Object.fromEntries(
    Object.entries(process.env).flatMap(([name, value]) => {
      const isUnsetOrTaskConfig =
        value === undefined || name.startsWith(`${TASK_ENVIRONMENT_VARIABLE_PREFIX}_`);
      return isUnsetOrTaskConfig ? [] : [[name, value]];
    }),
  ),
);

export const makeTaskProcessEnvironment = (
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
  environment: Readonly<Record<string, string>> = {},
) =>
  Effect.gen(function* () {
    return {
      ...(yield* readParentEnvironmentWithoutTaskConfig),
      HOME: project.home,
      BUN_RUNTIME_TRANSPILER_CACHE_PATH: `${yield* REPOSITORY_DIRECTORY}/.scratch/transpiler-cache`,
      [`${TASK_ENVIRONMENT_VARIABLE_PREFIX}_WORKERS`]: String(navigator.hardwareConcurrency),
      ...environment,
    };
  });

const waitForWritten =
  (chunks: Queue.Queue<string, Cause.Done>, captured: Ref.Ref<Chunk.Chunk<string>>) =>
  (wanted: string) =>
    Stream.fromEffectRepeat(Queue.take(chunks)).pipe(
      Stream.scan(
        () => "",
        (written: string, chunk: string) => written + chunk,
      ),
      Stream.filter((written) => written.includes(wanted)),
      Stream.runHead,
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Ref.get(captured).pipe(
              Effect.flatMap((written) =>
                Effect.fail(new NeverPrinted({ wanted, seen: Chunk.join(written, "") })),
              ),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

export const startTaskProcess = (
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
  file: string,
  environment: Readonly<Record<string, string>> = {},
  workingDirectory = project.directory,
  scriptArguments: readonly string[] = [],
  standardInput: ChildProcess.CommandInput = "ignore",
  bunArgumentList: readonly string[] = [],
) =>
  Effect.gen(function* () {
    const child = yield* ChildProcess.make(
      process.execPath,
      [...bunArgumentList, `${project.directory}/${file}`, ...scriptArguments],
      {
        cwd: workingDirectory,
        env: yield* makeTaskProcessEnvironment(project, environment),
        extendEnv: false,
        stdin: standardInput,
        stdout: "pipe",
        stderr: "pipe",
      },
    );

    const fileSystem = yield* FileSystem.FileSystem;
    const capturedOutput = yield* Ref.make(Chunk.empty<string>());
    const capturedErrors = yield* Ref.make(Chunk.empty<string>());
    yield* Scope.addFinalizerExit(project.scope, (scopeExit) =>
      Exit.isSuccess(scopeExit)
        ? Effect.void
        : Effect.gen(function* () {
            const outputFilePath = `${project.directory}/child-${child.pid}-output.txt`;
            yield* fileSystem.writeFileString(
              outputFilePath,
              `stdout:\n${Chunk.join(yield* Ref.get(capturedOutput), "")}\nstderr:\n${Chunk.join(yield* Ref.get(capturedErrors), "")}`,
            );
            yield* Effect.log(
              `kept failure artifacts: output ${outputFilePath}, runs ${project.home}/${TASK_STATE_DIRECTORY_NAME}/runs`,
            );
          }).pipe(failTestCleanup("preserve failed child output and runs")),
    );
    const outputChunks = yield* Queue.unbounded<string, Cause.Done>();
    const output = yield* child.stdout.pipe(
      Stream.decodeText,
      Stream.runForEach((outputChunk) =>
        Ref.update(capturedOutput, Chunk.append(outputChunk)).pipe(
          Effect.andThen(Queue.offer(outputChunks, outputChunk)),
        ),
      ),
      Effect.ensuring(Queue.end(outputChunks)),

      Effect.forkScoped,
    );
    const lines = yield* Queue.unbounded<string, Cause.Done>();
    const printed = yield* Ref.make(Chunk.empty<string>());
    const errorChunks = yield* Queue.unbounded<string, Cause.Done>();
    const errors = yield* child.stderr.pipe(
      Stream.decodeText,
      Stream.tap((errorChunk) =>
        Ref.update(capturedErrors, Chunk.append(errorChunk)).pipe(
          Effect.andThen(Queue.offer(errorChunks, errorChunk)),
        ),
      ),
      Stream.splitLines,
      Stream.runForEach((line) =>
        Ref.update(printed, Chunk.append(line)).pipe(Effect.andThen(Queue.offer(lines, line))),
      ),
      Effect.tapError((failure) => Effect.logError("Failed to read test child stderr", failure)),
      Effect.ensuring(Queue.end(lines).pipe(Effect.andThen(Queue.end(errorChunks)))),
      Effect.forkScoped,
    );

    return {
      processId: child.pid,
      waitForLine: (wanted: string) =>
        Stream.fromEffectRepeat(Queue.take(lines)).pipe(
          Stream.filter((line) => line.includes(wanted)),
          Stream.runHead,
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Ref.get(printed).pipe(
                  Effect.flatMap((printedLines) =>
                    Effect.fail(new NeverPrinted({ wanted, seen: Chunk.join(printedLines, "\n") })),
                  ),
                ),
              onSome: Effect.succeed,
            }),
          ),
        ),
      waitForLines: (wanted: readonly string[]) =>
        Stream.fromEffectRepeat(Queue.take(lines)).pipe(
          Stream.scan(
            () => wanted,
            (missing: readonly string[], line: string) =>
              missing.filter((part) => !line.includes(part)),
          ),
          Stream.filter((missing) => missing.length === 0),
          Stream.runHead,
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Ref.get(printed).pipe(
                  Effect.flatMap((printedLines) =>
                    Effect.fail(
                      new NeverPrinted({
                        wanted: wanted.join(" and "),
                        seen: Chunk.join(printedLines, "\n"),
                      }),
                    ),
                  ),
                ),
              onSome: () => Effect.void,
            }),
          ),
        ),
      waitForStandardOutput: waitForWritten(outputChunks, capturedOutput),
      waitForStandardError: waitForWritten(errorChunks, capturedErrors),
      sendSignal: (name: "HUP" | "INT" | "KILL" | "TERM") =>
        Effect.sync(() => process.kill(child.pid, `SIG${name}`)),
      waitForStandardErrorToClose: Fiber.join(errors).pipe(
        Effect.andThen(Ref.get(printed)),
        Effect.map((printedLines) => Chunk.join(printedLines, "\n")),
      ),
      waitForExit: Effect.gen(function* () {
        const exitCode = Number(yield* child.exitCode);
        yield* Fiber.join(output);
        const standardOutput = Chunk.join(yield* Ref.get(capturedOutput), "");
        yield* Fiber.join(errors);
        const standardError = Chunk.join(yield* Ref.get(printed), "\n");
        const misprinted = findMisprintedErrorLines(standardError);
        if (misprinted.length > 0) {
          return yield* new ErrorsMisprinted({ misprinted });
        }

        return { exitCode, standardOutput, standardError };
      }),
    };
  });

export const runTaskProcess = (...taskProcessArguments: Parameters<typeof startTaskProcess>) =>
  Effect.scoped(
    startTaskProcess(...taskProcessArguments).pipe(
      Effect.flatMap((command) => command.waitForExit),
    ),
  );
