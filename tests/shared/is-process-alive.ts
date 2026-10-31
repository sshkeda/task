import { Task } from "@sshkeda/task";
import { Effect } from "effect";

export const isProcessAlive = (processId: number) =>
  Task.exec("ps", ["-o", "stat=", "-p", String(processId)]).pipe(
    Effect.map((output) => {
      const state = output.trim();
      return state !== "" && !state.startsWith("Z") && !state.includes("E");
    }),
    Effect.catchTag("CommandFailed", () => Effect.succeed(false)),
  );
