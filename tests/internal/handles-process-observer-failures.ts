import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const FAILING_CHILD_EXIT_CODE = 3;

const observerFailure = (failureMode: "exited" | "resourceUsage") =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
const originalSpawn = Bun.spawn;
export default Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  Bun.spawn = (...arguments_) => {
    const subprocess = originalSpawn(...arguments_);
    return new Proxy(subprocess, { get(target, property) {
      if (property === "${failureMode}") {
        if (property === "exited") return target.exited.then(() => { throw new Error("observer failed"); });
        return () => { throw new Error("observer failed"); };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    }});
  };
  yield* Effect.addFinalizer(() => Effect.sync(() => { Bun.spawn = originalSpawn; }));
  const child = yield* ChildProcess.make("bun", ["-e", "process.exit(0)"], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
  yield* child.exitCode;
})) });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain("observer failed");
      expect(failedRun.standardError).not.toContain("timed out");
      expect(failedRun.standardError).not.toContain("does not start cleaning up");
    }),
  );

export const rejectedExitObservation = makeTest(
  "a rejected child exit observer fails the command and closes its scope",
  observerFailure("exited"),
);

export const failedResourceObservation = makeTest(
  "a failed child resource observer fails the command and closes its scope",
  observerFailure("resourceUsage"),
);

export const processCleanupFailures = makeTest(
  "worker process cleanup failures are reported while main remains the final cleanup authority",
  Effect.scoped(
    Effect.gen(function* () {
      for (const childExitCode of [0, FAILING_CHILD_EXIT_CODE]) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  const originalKill = process.kill;
  process.kill = (processId, signal) => {
    if (processId < 0 && signal === "SIGTERM") {
      throw Object.assign(new Error("cleanup signal invalid"), { code: "EINVAL" });
    }
    return originalKill(processId, signal);
  };
  yield* Effect.addFinalizer(() => Effect.sync(() => { process.kill = originalKill; }));
  const child = yield* ChildProcess.make("sh", ["-c", "sleep 1000 > /dev/null 2>&1 & exit ${childExitCode}"], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
  yield* child.exitCode;
})) });`,
        });
        const completedRun = yield* runTaskProcess(project, "main.ts");
        expect(completedRun.exitCode).toBe(0);
        expect(completedRun.standardError).toMatch(
          /^\[main\] error ProcessGroupNotSignaledByWorker: worker could not signal process group \d+ of sh because cleanup signal invalid$/mu,
        );
        expect(completedRun.standardError).not.toContain("timed out");
        const traces = yield* readTraces(yield* readTraceDirectory(project, completedRun));
        expect(
          traces.some((trace) =>
            trace.logs.some(
              (log) =>
                log.body.stringValue?.includes("worker could not signal process group") === true,
            ),
          ),
        ).toBe(true);
        expect(
          traces.some((trace) =>
            trace.logs.some((log) =>
              String(readAttribute(log.attributes, "sshkeda_task.cause")).includes(
                "cleanup signal invalid",
              ),
            ),
          ),
        ).toBe(true);
      }
    }),
  ),
);

export const zombieCommandKill = makeTest(
  "killing a command that exited but is not reaped yet succeeds, because its process group holds only a zombie",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Exit } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
const processState = (processId: number) =>
  Bun.spawnSync(["ps", "-o", "stat=", "-p", String(processId)]).stdout.toString().trim();
export default Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("sleep", ["1000"], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
  const stateBeforeKill = yield* Effect.sync(() => {
    process.kill(child.pid, "SIGKILL");
    let state = processState(child.pid);
    while (!state.startsWith("Z")) {
      state = processState(child.pid);
    }
    return state;
  });
  yield* Effect.log(\`command state before kill \${stateBeforeKill.slice(0, 1)}\`);
  const killed = yield* Effect.exit(child.kill());
  yield* Effect.log(\`kill \${Exit.isSuccess(killed) ? "succeeded" : "failed"}\`);
})) });`,
      });
      const completedRun = yield* runTaskProcess(project, "main.ts");
      expect(completedRun.exitCode, completedRun.standardError).toBe(0);
      expect(completedRun.standardError).toContain("[main] command state before kill Z");
      expect(completedRun.standardError).toContain("[main] kill succeeded");
      expect(completedRun.standardError).not.toContain("worker could not signal process group");
    }),
  ),
);

export const processGroupKillFailure = makeTest(
  "a kill whose process group signal fails reports that failure instead of signaling only the command",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("cat", [], { stdout: "ignore", stderr: "ignore" });
  const originalKill = process.kill;
  process.kill = (processId, signal) => {
    if (processId < 0 && signal === "SIGTERM") {
      throw Object.assign(new Error("process group signal denied"), { code: "EPERM" });
    }
    return originalKill(processId, signal);
  };
  const killFailure = yield* child.kill().pipe(
    Effect.flip,
    Effect.ensuring(Effect.sync(() => { process.kill = originalKill; })),
  );
  yield* Effect.log(\`kill refused because \${killFailure.message}\`);
  yield* Effect.log(\`command still running \${yield* child.isRunning}\`);
})) });`,
      });
      const completedRun = yield* runTaskProcess(project, "main.ts");
      expect(completedRun.exitCode, completedRun.standardError).toBe(0);
      expect(completedRun.standardError).toContain("[main] kill refused because");
      expect(completedRun.standardError).toContain("process group signal denied");
      expect(completedRun.standardError).toContain("[main] command still running true");
    }),
  ),
);
