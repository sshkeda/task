import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
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
      expect(ran.standardError).toMatch(
        /^run: \S+\/\nerror: \[quit\] main\.ts:3:21: WorkerExited: its worker exited on its own with code 7\nerror: \[quit\] its output is in \S+\/1\.output\nended: \[quit\], [\d.]+s, failed\nrecap: \[quit\] 1\.output \| main\.ts:3:21: WorkerExited: its worker exited on its own with code 7\nfailed: \[quit\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
      const traces = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
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
      expect(ran.standardError).toContain("closed: [server]");
      expect(ran.standardError).toMatch(
        /\nrecap: \[server\] 2\.output \| main\.ts:3:28: WorkerExited: its worker exited on its own with code 7\nfailed: \[server\] \(1 of 2 tasks\), [\d.]+s\nrun: \S+\/$/u,
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
      expect(ran.standardError).toMatch(
        /^run: \S+\/\nerror: \[main\] main\.ts:4:32: Error: escaped\nerror: \[main\] its worker stopped on this uncaught error\nerror: \[main\] its output is in \S+\/1\.output\nended: \[main\], [\d.]+s, failed\nrecap: \[main\] 1\.output \| main\.ts:4:32: Error: escaped\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
    }),
  ),
);
