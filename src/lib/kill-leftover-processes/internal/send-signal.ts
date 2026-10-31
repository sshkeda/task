import { Effect, Predicate } from "effect";

import { ProcessNotSignaled } from "../../../shared/process-errors.ts";
import { isStillListedAs } from "./list-processes.ts";

export const sendSignal = (
  taskName: string | null,
  processId: number,
  signal: ProcessNotSignaled["signal"],
) =>
  Effect.try(() => process.kill(processId, signal)).pipe(
    Effect.as("sent" as const),
    Effect.catchTag("UnknownError", (error) => {
      const code = Predicate.hasProperty(error.cause, "code") ? error.cause.code : undefined;
      const processIsGoneOrNotOurs = code === "ESRCH" || code === "EPERM";
      return processIsGoneOrNotOurs ? Effect.succeed<"EPERM" | "ESRCH">(code) : Effect.fail(error);
    }),
    Effect.mapError((cause) => new ProcessNotSignaled({ taskName, processId, signal, cause })),
  );

export const signalUnlessReplaced = (
  taskName: string | null,
  listed: Parameters<typeof isStillListedAs>[1],
  signal: ProcessNotSignaled["signal"],
) =>
  isStillListedAs(taskName, listed).pipe(
    Effect.flatMap((isTheListedProcess) =>
      isTheListedProcess
        ? sendSignal(taskName, listed.processId, signal)
        : Effect.succeed("ESRCH" as const),
    ),
  );
