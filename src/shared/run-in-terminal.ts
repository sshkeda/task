import { Context, Duration, Effect, Exit, Option, Predicate, Schema } from "effect";
import { SecureRandom } from "effect/crypto";
import * as PlatformErrors from "effect/PlatformError";

import { STOP_GRACE_PERIOD, STOP_SIGNALS } from "./stop-signals.ts";
import { TASK_PACKAGE } from "./task-package.ts";

export const TerminalCommand = Schema.Struct({
  id: Schema.String,
  command: Schema.String,
  args: Schema.Array(Schema.String),
  env: Schema.Record(Schema.String, Schema.String),
  cwd: Schema.String,
  cleanup: Schema.DurationFromMillis,
});

export const TerminalCommandId = Schema.Struct({ id: Schema.String });

export const TerminalCommandEnded = Schema.TaggedUnion({
  Exited: { code: Schema.Int },
  Signaled: { signal: Schema.String },
  NotStarted: { notFound: Schema.Boolean, description: Schema.String },
});

const GRACE_FOR_A_STOP_AFTER_A_STOP_SIGNAL = Duration.seconds(1);

class TerminalCommandNotSpawned extends Schema.TaggedError<TerminalCommandNotSpawned>()(
  "TerminalCommandNotSpawned",
  { cause: Schema.Defect() },
) {}

const isStopSignal = (signal: string) => STOP_SIGNALS.some((stopSignal) => stopSignal === signal);

const commandsInTerminal = new Map<string, Bun.Subprocess>();

export const isCommandInTerminal = (processId: number) =>
  [...commandsInTerminal.values()].some((subprocess) => subprocess.pid === processId);

const terminate = (subprocess: Bun.Subprocess, cleanup: Duration.Duration) =>
  Effect.sync(() => {
    subprocess.kill("SIGTERM");
  }).pipe(
    Effect.andThen(Effect.promise(() => subprocess.exited).pipe(Effect.timeoutOption(cleanup))),
    Effect.flatMap((exited) =>
      Option.isSome(exited)
        ? Effect.void
        : Effect.sync(() => {
            subprocess.kill("SIGKILL");
          }).pipe(Effect.andThen(Effect.promise(() => subprocess.exited))),
    ),
  );

export const runInTerminal =
  (stopRequested: Effect.Effect<void>, runMarkerFileDescriptor: number | null) =>
  (command: typeof TerminalCommand.Type) =>
    Effect.acquireUseRelease(
      Effect.try({
        try: () =>
          Bun.spawn([command.command, ...command.args], {
            cwd: command.cwd,
            env: command.env,
            stdio: [
              "inherit",
              "inherit",
              "inherit",
              ...(runMarkerFileDescriptor === null ? [] : [runMarkerFileDescriptor]),
            ],
          }),
        catch: (cause) => new TerminalCommandNotSpawned({ cause }),
      }).pipe(
        Effect.tap((subprocess) =>
          Effect.sync(() => {
            commandsInTerminal.set(command.id, subprocess);
          }),
        ),
      ),
      (subprocess) =>
        Effect.promise(() => subprocess.exited).pipe(
          Effect.flatMap((code): Effect.Effect<typeof TerminalCommandEnded.Type> => {
            const signal = subprocess.signalCode;
            if (signal === null) {
              return Effect.succeed(TerminalCommandEnded.cases.Exited.make({ code }));
            }
            const signaled = TerminalCommandEnded.cases.Signaled.make({ signal: String(signal) });
            return isStopSignal(signaled.signal)
              ? stopRequested.pipe(
                  Effect.timeoutOption(GRACE_FOR_A_STOP_AFTER_A_STOP_SIGNAL),
                  Effect.flatMap((stopped) =>
                    Option.isSome(stopped) ? Effect.never : Effect.succeed(signaled),
                  ),
                )
              : Effect.succeed(signaled);
          }),
        ),
      (subprocess, exit) =>
        (Exit.isSuccess(exit) ? Effect.void : terminate(subprocess, command.cleanup)).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              commandsInTerminal.delete(command.id);
            }),
          ),
        ),
    ).pipe(
      Effect.catchTag("TerminalCommandNotSpawned", (error) => {
        const isNotFound =
          Predicate.hasProperty(error.cause, "code") && error.cause.code === "ENOENT";
        return Effect.succeed(
          TerminalCommandEnded.cases.NotStarted.make({
            notFound: isNotFound,
            description: String(error.cause),
          }),
        );
      }),
    );

export const awaitTerminalCommandExit = (command: typeof TerminalCommandId.Type) =>
  Effect.suspend(() => {
    const subprocess = commandsInTerminal.get(command.id);
    return subprocess === undefined
      ? Effect.void
      : Effect.promise(() => subprocess.exited).pipe(Effect.asVoid);
  });

export const failUnlessStarted =
  (commandName: string) => (ended: typeof TerminalCommandEnded.Type) =>
    TerminalCommandEnded.guards.NotStarted(ended)
      ? Effect.fail(
          PlatformErrors.systemError({
            _tag: ended.notFound ? "NotFound" : "Unknown",
            module: "ChildProcess",
            method: "spawn",
            pathOrDescriptor: commandName.slice(commandName.lastIndexOf("/") + 1),
            description: ended.description,
          }),
        )
      : Effect.succeed(ended);

export const TerminalCommands = Context.Reference<
  (
    command: Omit<typeof TerminalCommand.Type, "cleanup" | "id">,
  ) => Effect.Effect<
    Exclude<typeof TerminalCommandEnded.Type, { readonly _tag: "NotStarted" }>,
    PlatformErrors.PlatformError
  >
>(`${TASK_PACKAGE.name}/TerminalCommands`, {
  defaultValue: () => (command) =>
    runInTerminal(
      Effect.never,
      null,
    )({
      ...command,
      id: SecureRandom.uuidV4Unsafe(),
      cleanup: STOP_GRACE_PERIOD,
    }).pipe(Effect.flatMap(failUnlessStarted(command.command))),
});
