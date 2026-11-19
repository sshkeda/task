import { Task } from "@sshkeda/task";
import { Effect, Schedule } from "effect";

export const isProcessAlive = (processId: number) =>
  Task.exec("ps", ["-o", "stat=", "-p", String(processId)]).pipe(
    Effect.map((output) => {
      const state = output.trim();
      return state !== "" && !state.startsWith("Z") && !state.includes("E");
    }),
    Effect.catchTag("CommandFailed", () => Effect.succeed(false)),
  );

export const waitUntilProcessEnds = (processId: number) =>
  isProcessAlive(processId).pipe(
    Effect.repeat({ until: (alive) => !alive, schedule: Schedule.spaced("20 millis") }),
    Effect.timeout("10 seconds"),
  );
