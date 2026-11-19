import { Context, Effect, Exit, Option, Predicate, Schema } from "effect";
import type { Duration } from "effect";
import { SecureRandom } from "effect/crypto";
import * as PlatformErrors from "effect/PlatformError";

import { PrintableError, ReportedErrors } from "../lib/errors/index.ts";
import { reportUnderlyingErrors } from "./report-cause.ts";
import { UNTAGGED_FAILURE_TAG } from "./report-errors.ts";
import { countSignalsReceived, STOP_GRACE_PERIOD, STOP_SIGNALS } from "./stop-signals.ts";
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
  NotStarted: { notFound: Schema.Boolean, cause: PrintableError },
});

export class TerminalCommandNotSpawned extends Schema.TaggedError<TerminalCommandNotSpawned>()(
  "TerminalCommandNotSpawned",
  { cause: Schema.Defect() },
) {}

export const notStartedBecauseSpawnThrew = (error: TerminalCommandNotSpawned) =>
  TerminalCommandEnded.cases.NotStarted.make({
    notFound: Predicate.hasProperty(error.cause, "code") && error.cause.code === "ENOENT",
    cause: reportUnderlyingErrors(error, UNTAGGED_FAILURE_TAG)[0],
  });

const commandsInTerminal = new Map<
  string,
  { readonly processId: number; readonly exited: Effect.Effect<unknown> }
>();

export const isCommandInTerminal = (processId: number) =>
  [...commandsInTerminal.values()].some((command) => command.processId === processId);

export const trackCommandInTerminal = (
  id: string,
  processId: number,
  exited: Effect.Effect<unknown>,
) => {
  commandsInTerminal.set(id, { processId, exited });
};

export const forgetCommandInTerminal = (id: string) =>
  Effect.sync(() => {
    commandsInTerminal.delete(id);
  });

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
  (stopRequested: Effect.Effect<void> | null, runMarkerFileDescriptor: number | null) =>
  (command: typeof TerminalCommand.Type) =>
    Effect.acquireUseRelease(
      Effect.try({
        try: () => {
          const signalsReceivedBeforeSpawn = new Map<string, number>(
            STOP_SIGNALS.map((stopSignal) => [stopSignal, countSignalsReceived(stopSignal)]),
          );
          const subprocess = Bun.spawn([command.command, ...command.args], {
            cwd: command.cwd,
            env: command.env,
            stdio: [
              "inherit",
              "inherit",
              "inherit",
              ...(runMarkerFileDescriptor === null ? [] : [runMarkerFileDescriptor]),
            ],
          });
          trackCommandInTerminal(
            command.id,
            subprocess.pid,
            Effect.promise(() => subprocess.exited),
          );
          return { subprocess, signalsReceivedBeforeSpawn };
        },
        catch: (cause) => new TerminalCommandNotSpawned({ cause }),
      }),
      ({ subprocess, signalsReceivedBeforeSpawn }) =>
        Effect.promise(() => subprocess.exited).pipe(
          Effect.flatMap((code): Effect.Effect<typeof TerminalCommandEnded.Type> => {
            const signal = subprocess.signalCode;
            if (signal === null) {
              return Effect.succeed(TerminalCommandEnded.cases.Exited.make({ code }));
            }
            const signaled = TerminalCommandEnded.cases.Signaled.make({ signal: String(signal) });
            const signalsReceivedBefore = signalsReceivedBeforeSpawn.get(signaled.signal);
            const isPartOfARunStop =
              stopRequested !== null &&
              signalsReceivedBefore !== undefined &&
              countSignalsReceived(signaled.signal) > signalsReceivedBefore;
            return isPartOfARunStop
              ? stopRequested.pipe(Effect.andThen(Effect.never))
              : Effect.succeed(signaled);
          }),
        ),
      ({ subprocess }, exit) =>
        (Exit.isSuccess(exit) ? Effect.void : terminate(subprocess, command.cleanup)).pipe(
          Effect.ensuring(forgetCommandInTerminal(command.id)),
        ),
    ).pipe(
      Effect.catchTag("TerminalCommandNotSpawned", (error) =>
        Effect.succeed(notStartedBecauseSpawnThrew(error)),
      ),
    );

export const awaitTerminalCommandExit = (command: typeof TerminalCommandId.Type) =>
  Effect.suspend(() => {
    const tracked = commandsInTerminal.get(command.id);
    return tracked === undefined ? Effect.void : Effect.asVoid(tracked.exited);
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
            cause: new ReportedErrors({ reportedErrors: [ended.cause] }),
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
      null,
      null,
    )({
      ...command,
      id: SecureRandom.uuidV4Unsafe(),
      cleanup: STOP_GRACE_PERIOD,
    }).pipe(Effect.flatMap(failUnlessStarted(command.command))),
});
