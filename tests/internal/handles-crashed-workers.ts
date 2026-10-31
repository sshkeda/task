import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const exited = Task.make(
  "a worker that exits on its own fails the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const quit = Task.make("quit", Effect.sync(() => process.exit(7)));
if (import.meta.main) Task.main(quit);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[quit] error WorkerExited: its worker exited on its own with code 7",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const exitWithPendingDependencies = Task.make(
  "a worker exit with pending dependencies preserves its failure without secondary dispatch defects",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const quit = Task.make("quit", Effect.sync(() => process.exit(7)));
export const pending = Task.make("pending", Effect.never.pipe(Effect.andThen(Effect.log("pending body ran"))));
export const main = Task.make("main", Task.all([quit, pending]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("its worker exited on its own with code 7");
      expect(ran.standardError).not.toContain("pending body ran");
      const traces = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(traces.map((trace) => trace.text).join("")).not.toContain("NoSuchElementError");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const vanished = Task.make(
  "a worker that exits while its task's finalizers run ends that task's wait at once and fails the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => process.exit(7)));
  return yield* Task.serve("up");
}));
export const main = Task.make("main", Effect.asVoid(server));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[server] closed");
      expect(ran.standardError).toContain(
        "[server] error WorkerExited: its worker exited on its own with code 7",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const thrown = Task.make(
  "an uncaught throw in a worker fails the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  setTimeout(() => { throw new Error("escaped"); }, 0);
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[main] main.ts:4:32: error WorkerExited: its worker stopped on the uncaught error escaped",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
