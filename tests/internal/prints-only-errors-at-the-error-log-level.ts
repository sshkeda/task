import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const EVIDENCE_REJECTED = `import { Schema } from "effect";
export class EvidenceRejected extends Schema.TaggedError<EvidenceRejected>()("EvidenceRejected", { message: Schema.String }) {}`;

export const passedRun = Task.make(
  'a run with { log: "error" } that passes prints nothing and exits 0, and still writes its trace',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const dependency = Task.make("dependency", Effect.log("dependency ran"));
export const main = Task.make("main", Effect.gen(function* () {
  yield* dependency;
  yield* Effect.log("checking");
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2"]);
}));
if (import.meta.main) Task.main(main, { log: "error" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe("");
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(trace?.logs.map((log) => log.body.stringValue)).toContain("checking");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const typedFailure = Task.make(
  'a run with { log: "error" } whose task fails prints only the error line and exits 1',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "evidence-rejected.ts": EVIDENCE_REJECTED,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { EvidenceRejected } from "./evidence-rejected.ts";
export const main = Task.make("memo-hook", Effect.fail(new EvidenceRejected({ message: "Start: and End: must be nonempty lines" })));
if (import.meta.main) Task.main(main, { log: "error" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe(
        "[memo-hook] main.ts:4:60: error EvidenceRejected: Start: and End: must be nonempty lines",
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const hiddenLogs = Task.make(
  "a run with { log: \"error\" } hides a body's info and warning logs and its commands' output, and prints its error logs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "evidence-rejected.ts": EVIDENCE_REJECTED,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { EvidenceRejected } from "./evidence-rejected.ts";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("checking");
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2"]);
  yield* Effect.logWarning("careful");
  yield* Effect.logError("broken");
  return yield* new EvidenceRejected({ message: "rejected" });
}));
if (import.meta.main) Task.main(main, { log: "error" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe(
        "[main] broken\n[main] main.ts:9:21: error EvidenceRejected: rejected",
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const refusedTaskDefinitions = Task.make(
  'a run with { log: "error" } that refuses its task definitions prints only the refusals and exits 1',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make("same", Effect.void);
export const b = Task.make("same", Effect.void);
if (import.meta.main) Task.main(a, { log: "error" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe(
        [
          'main.ts:3: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make',
          'main.ts:4: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make',
        ].join("\n"),
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const refusedBunWatchMode = Task.make(
  'a run with { log: "error" } under bun --watch still prints RunCalledInBunWatchMode',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.void);
if (import.meta.main) Task.main(main, { log: "error" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {}, project.directory, [], "ignore", [
        "--watch",
      ]);
      expect(ran.standardError).toBe(
        "error RunCalledInBunWatchMode: @sshkeda/task doesn't run under bun --watch",
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
