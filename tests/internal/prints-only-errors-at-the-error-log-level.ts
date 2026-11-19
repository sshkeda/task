import { expect } from "bun:test";

import { Effect } from "effect";

import { read } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import {
  findRunDirectoryPath,
  withRunDirectoryNamed,
  withRunLinePathNamed,
  withSecondsNamed,
} from "../shared/name-output-files.ts";
import { readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { withoutStackFrames } from "../shared/without-stack-frames.ts";

const EVIDENCE_REJECTED = `import { Schema } from "effect";
export class EvidenceRejected extends Schema.TaggedError<EvidenceRejected>()("EvidenceRejected", { message: Schema.String }) {}`;

const ERROR_LOG_LEVEL = { SSHKEDA_TASK_LOG: "error" } as const;

export const passedRun = makeTest(
  "a run with SSHKEDA_TASK_LOG=error that passes prints only its summary and run line and exits 0, and still writes its trace",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const dependency = Task.make({ name: "dependency", run: Effect.log("dependency ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* dependency;
  yield* Effect.log("checking");
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2"], { print: true });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(withSecondsNamed(withRunDirectoryNamed(ran.standardError, project))).toBe(
        "run: <run folder>/\npassed: 2 tasks, <seconds>\nrun: <run folder>/",
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(trace?.logs.map((log) => log.body.stringValue)).toContain("checking");
    }),
  ),
);

export const typedFailure = makeTest(
  "a run with SSHKEDA_TASK_LOG=error whose task fails prints only its error once with its task and location, its recap line, the summary and the run line, and exits 1",
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
      expect(withSecondsNamed(withRunDirectoryNamed(ran.standardError, project))).toBe(
        [
          "run: <run folder>/",
          "error: [memo-hook] main.ts:4:68: EvidenceRejected: Start: and End: must be nonempty lines",
          "error: [memo-hook] its output is in <run folder>/1.output",
          "recap: [memo-hook] 1.output | main.ts:4:68: EvidenceRejected: Start: and End: must be nonempty lines",
          "failed: [memo-hook] (1 of 1 task), <seconds>",
          "run: <run folder>/",
        ].join("\n"),
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const hiddenLogs = makeTest(
  "a run with SSHKEDA_TASK_LOG=error hides a body's info, warning and error logs and its commands' output, and prints its error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "evidence-rejected.ts": EVIDENCE_REJECTED,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { EvidenceRejected } from "./evidence-rejected.ts";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("checking");
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2"], { print: true });
  yield* Effect.logWarning("careful");
  yield* Effect.logError("broken");
  return yield* new EvidenceRejected({ message: "rejected" });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(withSecondsNamed(withRunDirectoryNamed(ran.standardError, project))).toBe(
        [
          "run: <run folder>/",
          "error: [main] main.ts:9:21: EvidenceRejected: rejected",
          "error: [main] its output is in <run folder>/1.output",
          "recap: [main] 1.output | main.ts:9:21: EvidenceRejected: rejected",
          "failed: [main] (1 of 1 task), <seconds>",
          "run: <run folder>/",
        ].join("\n"),
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

const FAILED_COMMAND_EXIT_CODE = 3;

export const failedCommandOutput = makeTest(
  "a run with SSHKEDA_TASK_LOG=error hides a failed command's output as it prints, prints its CommandFailed error with one line naming the task's file, which holds that output, and recaps that output's lines",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo checked; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(withSecondsNamed(withRunLinePathNamed(ran.standardError))).toBe(
        [
          "run: <run folder>/",
          `error: [main] main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
          "error: [main] its output is in <run folder>/1.output",
          `recap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
          "recap: [main] checked",
          "recap: [main] broken",
          "failed: [main] (1 of 1 task), <seconds>",
          "run: <run folder>/",
        ].join("\n"),
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(
        withRunLinePathNamed(
          withSecondsNamed(
            withoutStackFrames(yield* read(`${findRunDirectoryPath(ran.standardError)}/1.output`)),
          ),
          ran.standardError,
        ),
      ).toBe(
        [
          "started: [main], <seconds>, 1.output",
          "[main] checked",
          "[main] broken",
          `error: [main] main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
          "error: [main] its output is in <run folder>/1.output",
          "ended: [main], <seconds>, failed",
          "",
        ].join("\n"),
      );
    }),
  ),
);

export const refusedTaskDefinitions = makeTest(
  "a run with SSHKEDA_TASK_LOG=error that refuses its task definitions prints only each refusal once as an error line, the summary and the run line, and exits 1",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "same", run: Effect.void });
export const b = Task.make({ name: "same", run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(withSecondsNamed(withRunDirectoryNamed(ran.standardError, project))).toBe(
        [
          "run: <run folder>/",
          "error: main.ts:3:21: TaskMadeMoreThanOnce: makes the task [same], which 2 Task.make() calls make",
          "error: main.ts:4:23: TaskMadeMoreThanOnce: makes the task [same], which 2 Task.make() calls make",
          "failed: 0 of 0 tasks, <seconds>",
          "run: <run folder>/",
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
        "error: RunCalledInBunWatchMode: @sshkeda/task doesn't run under bun --watch",
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);
