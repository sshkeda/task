import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const EVIDENCE_REJECTED = `import { Schema } from "effect";
export class EvidenceRejected extends Schema.TaggedError<EvidenceRejected>()("EvidenceRejected", { message: Schema.String }) {}`;

const ERROR_LOG_LEVEL = { SSHKEDA_TASK_LOG: "error" } as const;

export const passedRun = makeTest(
  "a run with SSHKEDA_TASK_LOG=error that passes prints nothing and exits 0, and still writes its trace",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const dependency = Task.make({ name: "dependency", run: Effect.log("dependency ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* dependency;
  yield* Effect.log("checking");
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2"]);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(ran.standardError).toBe("");
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(trace?.logs.map((log) => log.body.stringValue)).toContain("checking");
    }),
  ),
);

export const typedFailure = makeTest(
  "a run with SSHKEDA_TASK_LOG=error whose task fails prints only the error line and exits 1",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "evidence-rejected.ts": EVIDENCE_REJECTED,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { EvidenceRejected } from "./evidence-rejected.ts";
export default Task.make({ name: "memo-hook", run: Effect.fail(new EvidenceRejected({ message: "Start: and End: must be nonempty lines" })) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(ran.standardError).toBe(
        "[memo-hook] main.ts:4:68: error EvidenceRejected: Start: and End: must be nonempty lines",
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const hiddenLogs = makeTest(
  "a run with SSHKEDA_TASK_LOG=error hides a body's info and warning logs and its commands' output, and prints its error logs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "evidence-rejected.ts": EVIDENCE_REJECTED,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { EvidenceRejected } from "./evidence-rejected.ts";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("checking");
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2"]);
  yield* Effect.logWarning("careful");
  yield* Effect.logError("broken");
  return yield* new EvidenceRejected({ message: "rejected" });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(ran.standardError).toBe(
        "[main] broken\n[main] main.ts:9:21: error EvidenceRejected: rejected",
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

const FAILED_COMMAND_EXIT_CODE = 3;

export const failedCommandOutput = makeTest(
  "a run with SSHKEDA_TASK_LOG=error prints a failed command's output under its CommandFailed error, since it was hidden as the command wrote it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo checked; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(ran.standardError).toBe(
        `[main] error CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\n[main]   checked\n[main]   broken`,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
    }),
  ),
);

export const refusedTaskDefinitions = makeTest(
  "a run with SSHKEDA_TASK_LOG=error that refuses its task definitions prints only the refusals and exits 1",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "same", run: Effect.void });
export const b = Task.make({ name: "same", run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(ran.standardError).toBe(
        [
          'main.ts:3:21: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make',
          'main.ts:4:23: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make',
        ].join("\n"),
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const refusedBunWatchMode = makeTest(
  "a run with SSHKEDA_TASK_LOG=error under bun --watch still prints RunCalledInBunWatchMode",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        ERROR_LOG_LEVEL,
        project.directory,
        [],
        "ignore",
        ["--watch"],
      );
      expect(ran.standardError).toBe(
        "error RunCalledInBunWatchMode: @sshkeda/task doesn't run under bun --watch",
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);
