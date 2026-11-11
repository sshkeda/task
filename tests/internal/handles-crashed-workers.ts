import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const exited = makeTest(
  "a worker that exits on its own fails the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "quit", run: Effect.sync(() => process.exit(7)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[quit] error WorkerExited: its worker exited on its own with code 7",
      );
    }),
  ),
);

export const exitWithPendingDependencies = makeTest(
  "a worker exit with pending dependencies preserves its failure without secondary dispatch defects",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const quit = Task.make({ name: "quit", run: Effect.sync(() => process.exit(7)) });
export const pending = Task.make({ name: "pending", run: Effect.never.pipe(Effect.andThen(Effect.log("pending body ran"))) });
export default Task.make({ name: "main", run: Task.all([quit, pending]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("its worker exited on its own with code 7");
      expect(ran.standardError).not.toContain("pending body ran");
      const traces = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(traces.map((trace) => trace.text).join("")).not.toContain("NoSuchElementError");
    }),
  ),
);

export const vanished = makeTest(
  "a worker that exits while its task's finalizers run ends that task's wait at once and fails the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => process.exit(7)));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[server] closed");
      expect(ran.standardError).toContain(
        "[server] error WorkerExited: its worker exited on its own with code 7",
      );
    }),
  ),
);

export const thrown = makeTest(
  "an uncaught throw in a worker fails the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  setTimeout(() => { throw new Error("escaped"); }, 0);
  yield* Effect.never;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[main] main.ts:4:32: error WorkerExited: its worker stopped on the uncaught error escaped",
      );
    }),
  ),
);
