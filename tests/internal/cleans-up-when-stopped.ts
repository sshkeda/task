import { expect } from "bun:test";

import { Effect } from "effect";

import { exists, read } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import {
  EXIT_CODE_AFTER_SIGHUP,
  EXIT_CODE_AFTER_SIGINT,
  EXIT_CODE_AFTER_SIGTERM,
} from "../shared/exit-codes-after-stop-signals.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const CLEANUP_CUT_OFF =
  "error TaskCleanupCutOff: its cleanup was cut off because the stop was forced";

const findCutOffLines = (standardError: string) =>
  standardError.match(/^\[[^\]\n]+\] error TaskCleanupCutOff: .*$/gmu) ?? [];

const STOPPING_AFTER_A_HANGUP = "stopping, send SIGTERM or press ctrl-c to force";

export const interrupt = makeTest(
  "Ctrl-C cancels the run, runs the finalizers, exits 130 and ends with the reason line StopRequested: received SIGINT",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  yield* ChildProcess.make("sleep", ["1000"]);
  yield* Effect.log("sleeping");
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] sleeping");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain(
        "[main] sleeping\nstopping, press ctrl-c again to force",
      );
      expect(exited.standardError).toContain("[main] cleaned up");
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      expect(exited.standardError).toMatch(
        /\nwrote the trace to \S+\nStopRequested: received SIGINT$/u,
      );
      expect(findKilledProcesses(exited.standardError)).toEqual([]);
    }),
  ),
);

export const interruptSharedDependency = makeTest(
  "Ctrl-C stops two tasks waiting on the same task as stopped, not failed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const waitUntilStopped = (taskName: string) => Effect.gen(function* () {
  yield* Effect.log(\`\${taskName} waiting\`);
  yield* Effect.never;
});
export const left = Task.make({ name: "left", run: waitUntilStopped("left") });
export const right = Task.make({ name: "right", run: waitUntilStopped("right") });
export const first = Task.make({ name: "first", run: Effect.gen(function* () {
  yield* left;
}) });
export const second = Task.make({ name: "second", run: Task.all([left, right]) });
export default Task.make({ name: "main", run: Task.allSettled([first, second]) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine(" waiting");
      yield* command.waitForLine(" waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).not.toContain("CauseWithoutFailure");
      expect(exited.standardError).toMatch(
        /^failed: 0 of 5 tasks, 5 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

export const terminate = makeTest(
  "SIGTERM cancels the run, runs the finalizers and exits 143",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  yield* ChildProcess.make("sleep", ["1000"]);
  yield* Effect.log("sleeping");
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] sleeping");
      yield* command.sendSignal("TERM");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGTERM);
      expect(exited.standardError).toContain(
        "[main] sleeping\nstopping, send SIGTERM or press ctrl-c again to force",
      );
      expect(exited.standardError).toContain("[main] cleaned up");
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGTERM$/mu,
      );
      expect(findKilledProcesses(exited.standardError)).toEqual([]);
    }),
  ),
);

export const hangUpServing = makeTest(
  "SIGHUP from a closed terminal stops the run like a first Ctrl-C, so a serving task's command that ignores SIGTERM is killed when its cleanup limit passes, and the run exits 129",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  const child = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo started; exec sleep 1000"]);
  yield* child.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  yield* Effect.log(\`server process \${child.pid}\`);
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
      expect(exited.standardError).toContain(`[main] ready\n${STOPPING_AFTER_A_HANGUP}`);
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGHUP$/mu);
      expect(exited.standardError).toContain("[main] stopped: the run ended before it finished");
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      const serverProcessId = Number(
        /\[server\] server process (?<processId>\d+)/u.exec(exited.standardError)?.[1],
      );
      expect(yield* isProcessAlive(serverProcessId)).toBe(false);
    }),
  ),
);

export const hangUpWithDeclaredCleanup = makeTest(
  "SIGHUP gives a task its declared cleanup limit, so a finalizer that needs longer than the default 5s but less than its 30 seconds finishes, and the run exits 129",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", cleanup: "30 seconds", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("6 seconds").pipe(
    Effect.andThen(Effect.promise(() => Bun.write("released", ""))),
  ));
  yield* Effect.log("holding the lock");
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] holding the lock");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(exited.standardError).not.toContain("TaskCleanupTimedOut");
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGHUP$/mu,
      );
    }),
  ),
);

export const hangUpPastDefaultCleanup = makeTest(
  "SIGHUP while a finalizer outlasts the default 5s cleanup limit reports TaskCleanupTimedOut as a Ctrl-C does, and the run exits 129",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.log("ready").pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sleep("1 minute"))),
});`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("HUP");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
      expect(stopped.standardError).toContain(`[main] ready\n${STOPPING_AFTER_A_HANGUP}`);
      expect(stopped.standardError).toContain(
        "[main] still stopping after 2s; @sshkeda/task kills its commands and stops waiting for it when its 5s cleanup limit passes\n[main] error TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it",
      );
      expect(findCutOffLines(stopped.standardError)).toEqual([]);
    }),
  ),
);

export const hangUpThenInterrupt = makeTest(
  "a Ctrl-C after a SIGHUP forces the stop as a second signal does, cutting off a cleanup that its 30 seconds would have allowed, and the run exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", cleanup: "30 seconds", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("20 seconds").pipe(
    Effect.andThen(Effect.promise(() => Bun.write("released", ""))),
  ));
  yield* Effect.log("holding the lock");
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] holding the lock");
      yield* command.sendSignal("HUP");
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(findCutOffLines(exited.standardError)).toEqual([`[main] ${CLEANUP_CUT_OFF}`]);
      expect(yield* exists(`${project.directory}/released`)).toBe(false);
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGINT$/mu);
    }),
  ),
);

const HOLDING_A_LOCK = (finalizerSleep: string) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", cleanup: "30 seconds", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("${finalizerSleep}").pipe(
    Effect.andThen(Effect.promise(() => Bun.write("released", ""))),
  ));
  yield* Effect.log("holding the lock");
  yield* Effect.never;
}) });`;

export const hangUpTwice = makeTest(
  "a second SIGHUP, as a shell sends when its terminal closes, does not force the stop, so a 3s finalizer still finishes, the stopping line prints once, and the run exits 129",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOLDING_A_LOCK("3 seconds") });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] holding the lock");
      yield* command.sendSignal("HUP");
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* command.sendSignal("HUP");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(countOccurrences(exited.standardError, "stopping, ")).toBe(1);
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGHUP$/mu,
      );
    }),
  ),
);

export const interruptThenHangUp = makeTest(
  "a SIGHUP after a Ctrl-C does not force the stop, so a 3s finalizer still finishes, and the run exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOLDING_A_LOCK("3 seconds") });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] holding the lock");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("stopping, press ctrl-c again to force");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGINT$/mu);
    }),
  ),
);

export const hangUpThenTerminate = makeTest(
  "a SIGTERM after a SIGHUP forces the stop as a second signal does, cutting off a cleanup that its 30 seconds would have allowed, and the run exits 143",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOLDING_A_LOCK("20 seconds") });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] holding the lock");
      yield* command.sendSignal("HUP");
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* command.sendSignal("HUP");
      yield* command.sendSignal("TERM");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGTERM);
      expect(findCutOffLines(exited.standardError)).toEqual([`[main] ${CLEANUP_CUT_OFF}`]);
      expect(yield* exists(`${project.directory}/released`)).toBe(false);
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGTERM$/mu);
    }),
  ),
);

export const forcedDuringSpawnSync = makeTest(
  "a forced stop while a task's worker is blocked in Bun.spawnSync kills the blocking command, reports it and exits promptly",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() =>
  Bun.spawnSync(["sh", "-c", "echo blocking command started >&2; exec sleep 1000"], { stderr: "inherit" }),
) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("blocking command started");
      yield* command.sendSignal("HUP");
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* command.sendSignal("TERM");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGTERM);
      expect(exited.standardError).toContain("[main] stopped: the run ended before it finished");
      expect(findCutOffLines(exited.standardError)).toEqual([`[main] ${CLEANUP_CUT_OFF}`]);
      const killed = findKilledProcesses(exited.standardError);
      expect(killed.map((each) => each.by)).toEqual(["no known task"]);
      expect(killed[0]?.command).toMatch(/^(?:sh|sleep)$/u);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
    }),
  ),
);

export const forcedWhileLoopingOverSpawnSync = makeTest(
  "a forced stop while a worker loops over Bun.spawnSync kills the running command and lets the worker start no other, so the run exits 143 promptly",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  for (let index = 0; index < 6; index++) {
    Bun.spawnSync(["sh", "-c", \`echo started command \${index} >&2; exec sleep 1000.\${process.pid}\${index}\`], { stderr: "inherit" });
  }
}) });`,
      });
      yield* Effect.forEach(
        ["first", "second", "third", "fourth"],
        () =>
          Effect.gen(function* () {
            const command = yield* startTaskProcess(project, "main.ts");
            yield* command.waitForLine("started command 0");
            yield* command.sendSignal("HUP");
            yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
            yield* command.sendSignal("TERM");
            const exited = yield* command.waitForExit;
            expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGTERM);
            expect(exited.standardError).not.toContain("command 1");
            const killed = findKilledProcesses(exited.standardError);
            expect(killed.map((each) => each.by)).toEqual(["no known task"]);
            expect(killed[0]?.command).toMatch(/^(?:sh|sleep)$/u);
          }),
        { discard: true },
      );
    }),
  ),
);

export const outputClosed = makeTest(
  "when standard error closes, as in | head -n 4, the run stops, a command that ignores SIGTERM is killed after a short grace period, and the run exits 141",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const prepare = Task.make({ name: "prepare", run: Effect.log("prepared") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* prepare;
  const command = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo $$ > command-pid; echo started; exec sleep 1000"]);
  yield* command.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  while (!(yield* Effect.promise(() => Bun.file("head-exited").exists()))) {
    yield* Effect.sleep("10 millis");
  }
  yield* Effect.forever(Effect.andThen(Effect.log("serving"), Effect.sleep("1 millis")));
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | while IFS= read -r line; do case "$line" in "["*"] waiting for worker "* | "["*"] loading "*) ;; *) printf '%s\\\\n' "$line" ;; esac; done | { head -n 4; exec 0<&-; touch head-exited; }\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const piped = yield* runTaskProcess(project, "piped.ts");
      expect(piped.standardOutput).toMatch(
        /^\[main\] started\n\[prepare\] started\n\[prepare\] prepared\n\[prepare\] passed in [\d.]+s\n$/u,
      );
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("141");
      const commandProcessId = Number((yield* read(`${project.directory}/command-pid`)).trim());
      expect(yield* isProcessAlive(commandProcessId)).toBe(false);
    }),
  ),
);

export const forcedIgnoresTerm = makeTest(
  "the first Ctrl-C waits on a finalizer stuck on a command that ignores SIGTERM, and a second Ctrl-C kills that command and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const child = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo started; exec sleep 1000"]);
  yield* child.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  yield* Effect.log(\`waiting on \${child.pid}\`);
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] waiting on");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("stopping, press ctrl-c again to force");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      const commandProcessId = Number(
        /\[main\] waiting on (?<processId>\d+)/u.exec(exited.standardError)?.[1],
      );
      expect(findKilledProcesses(exited.standardError)).toEqual([
        { processId: commandProcessId, command: "sleep", by: '"main"' },
      ]);
      expect(yield* isProcessAlive(commandProcessId)).toBe(false);
      expect(findCutOffLines(exited.standardError)).toEqual([`[main] ${CLEANUP_CUT_OFF}`]);
      expect(exited.standardError).toMatch(
        /^failed: main \(1 of 1 task\), [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

export const startingUp = makeTest(
  "a SIGTERM that arrives while task is starting up ends the run before any task starts, and exits 143",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (import.meta.main) {
  const signalOnceTaskListens = () => {
    if (process.listenerCount("SIGTERM") === 0) {
      setImmediate(signalOnceTaskListens);
      return;
    }
    process.kill(process.pid, "SIGTERM");
  };
  setImmediate(signalOnceTaskListens);
}
export default Task.make({ name: "main", run: Effect.promise(() => Bun.write("main-started", "")).pipe(Effect.andThen(Effect.never)) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGTERM);
      const lines =
        /^stopping, send SIGTERM or press ctrl-c again to force\n(?:wrote the trace to (?<traceFile>\S+)\n)?error StopRequested: received SIGTERM\nStopRequested: received SIGTERM$/u.exec(
          exited.standardError,
        );
      expect(lines, exited.standardError).not.toBeNull();
      const { traceFile } = lines?.groups ?? {};
      if (traceFile !== undefined) {
        expect(yield* exists(traceFile)).toBe(true);
      }
      expect(findKilledProcesses(exited.standardError)).toEqual([]);
      expect(yield* isProcessAlive(command.processId)).toBe(false);
      expect(yield* exists(`${project.directory}/main-started`)).toBe(false);
    }),
  ),
);

export const loading = makeTest(
  "Ctrl-C ends the run while a worker is still loading the task files, and the run is still traced",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "loading.ts": `import { isMainThread } from "node:worker_threads";
if (!isMainThread) {
  console.error("worker loading");
  await new Promise(() => {});
}
export const greeting = "hello";`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { greeting } from "./loading.ts";
export default Task.make({ name: "main", run: Effect.log(greeting) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("worker loading");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      expect(yield* isProcessAlive(command.processId)).toBe(false);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      expect(trace?.named("runRootTaskFiber")).toHaveLength(1);
    }),
  ),
);

export const root = makeTest(
  "a serving root holds the command until Ctrl-C, then runs its finalizers and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("main stopped"));
  return yield* Task.serve("up");
}), timeout: "5 seconds", cleanup: "5 seconds" });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("main is serving");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain(
        "stopping, press ctrl-c again to force\n[main] main stopped\n[main] closed",
      );
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

const SERVING_ROOT_THAT_RECORDS_ITS_PROCESS_ID = `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread) await Bun.write("task-pid", String(process.pid));
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("main stopped"));
  return yield* Task.serve("up");
}), timeout: "5 seconds", cleanup: "5 seconds" });`;

const INTERRUPT_ONCE_SERVING_IS_READ_AND_STANDARD_ERROR_CLOSED = `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | { sed '/main is serving/q'; exec 0<&-; kill -INT $(cat task-pid); }\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`;

export const interruptAfterOutputClosed = makeTest(
  "Ctrl-C after standard error closes, when the stopping line is the first write that finds it closed, still exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": SERVING_ROOT_THAT_RECORDS_ITS_PROCESS_ID,
        "piped.ts": INTERRUPT_ONCE_SERVING_IS_READ_AND_STANDARD_ERROR_CLOSED,
      });
      const piped = yield* runTaskProcess(project, "piped.ts");
      expect(piped.standardOutput).toEndWith("main is serving\n");
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("130");
    }),
  ),
);

export const interruptAfterOutputClosedDuringLongCleanup = makeTest(
  "Ctrl-C after standard error closes still exits 130 when the cleanup outlasts the 1s task waits once standard error closed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread) await Bun.write("task-pid", String(process.pid));
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("3 seconds"));
  return yield* Task.serve("up");
}), timeout: "5 seconds", cleanup: "5 seconds" });`,
        "piped.ts": INTERRUPT_ONCE_SERVING_IS_READ_AND_STANDARD_ERROR_CLOSED,
      });
      const piped = yield* runTaskProcess(project, "piped.ts");
      expect(piped.standardOutput).toEndWith("main is serving\n");
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("130");
    }),
  ),
);

export const interruptWithOutputFull = makeTest(
  "Ctrl-C while standard error is a full non-blocking pipe that nobody reads still ends the run, with exit 1 because its last lines were never read",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": SERVING_ROOT_THAT_RECORDS_ITS_PROCESS_ID,
        "full.pl": `use strict; use Fcntl;
pipe(my $reader, my $writer) or die;
fcntl($writer, F_SETFL, fcntl($writer, F_GETFL, 0) | O_NONBLOCK) or die;
my $pid = fork();
if ($pid == 0) { close $reader; open(STDERR, ">&", $writer) or die; exec($ARGV[0], "main.ts") or die; }
while (my $line = <$reader>) { last if $line =~ /main is serving/; }
while (defined syswrite($writer, "x" x 512)) {}
kill "INT", $pid;
waitpid($pid, 0);
print $? >> 8, "\\n";`,
        "full.ts": `const perl = Bun.spawn(["perl", "full.pl", process.execPath], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await perl.exited;`,
      });
      const full = yield* runTaskProcess(project, "full.ts");
      expect(full.standardOutput).toBe("1\n");
    }),
  ),
);

export const interruptWhileTheReaderWaits = makeTest(
  "Ctrl-C while the reader of a full 2>&1 pipe waits ends the run with exit 130 once the reader takes every line, the stopping line included",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread) await Bun.write("task-pid", String(process.pid));
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("main stopped").pipe(
    Effect.andThen(Effect.promise(() => Bun.write("stopped", ""))),
  ));
  yield* Effect.forEach(Array.from({ length: 2000 }, (_, index) => index), (index) => Effect.log(\`\${index} \${"x".repeat(100)}\`), { discard: true });
  yield* Effect.promise(() => Bun.write("logged", ""));
  return yield* Task.serve("up");
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | { until [ -e logged ] || [ -e exit-code ]; do sleep 0.1; done; kill -INT $(cat task-pid) 2>/dev/null; until [ -e stopped ] || [ -e exit-code ]; do sleep 0.1; done; cat > output; }\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      yield* runTaskProcess(project, "piped.ts");
      const output = yield* read(`${project.directory}/output`);
      expect(output).toContain("[main] 1999 ");
      expect(output).toContain("stopping, press ctrl-c again to force\n");
      expect(output).toContain("[main] main stopped\n");
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe(
        String(EXIT_CODE_AFTER_SIGINT),
      );
    }),
  ),
);

export const forced = makeTest(
  "a second Ctrl-C kills what the finalizers are still running and reports it with its task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.scoped(Effect.gen(function* () {
    const child = yield* ChildProcess.make("sleep", ["1000"]);
    yield* Effect.log("cleaning up");
    yield* child.exitCode;
  })));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[server] cleaning up");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      const killed = findKilledProcesses(exited.standardError);
      expect(killed.map((each) => `${each.command} by ${each.by}`)).toEqual(['sleep by "server"']);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
      expect(findCutOffLines(exited.standardError)).toEqual([`[server] ${CLEANUP_CUT_OFF}`]);
      expect(exited.standardError).toMatch(
        /^failed: server \(1 of 2 tasks\), 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

export const forcedAfterFailure = makeTest(
  "a second Ctrl-C while the run cleans up after a task failed kills what the finalizers are still running, but still exits 1 and ends with that failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.scoped(Effect.gen(function* () {
    const child = yield* ChildProcess.make("sleep", ["1000"]);
    yield* Effect.log("cleaning up");
    yield* child.exitCode;
  })));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  return yield* Effect.fail("broken build");
}) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[server] cleaning up");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("stopping, press ctrl-c again to force");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(1);
      const killed = findKilledProcesses(exited.standardError);
      expect(killed.map((each) => `${each.command} by ${each.by}`)).toEqual(['sleep by "server"']);
      expect(exited.standardError).toContain(
        "[main] error TaskBodyFailed: its body failed with broken build\n",
      );
      expect(findCutOffLines(exited.standardError)).toEqual([`[server] ${CLEANUP_CUT_OFF}`]);
      expect(exited.standardError).not.toContain("StopRequested");
      expect(exited.standardError).toEndWith("\nTaskBodyFailed: its body failed with broken build");
    }),
  ),
);

const SERVER_WITH_SLOW_CLEANUP = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaning up").pipe(
    Effect.andThen(Effect.sleep("2 seconds")),
    Effect.andThen(Effect.promise(() => Bun.write("released", ""))),
  ));
  return yield* Task.serve("up");
}) });`;

const stopWhilePassedRunCloses = (
  signal: "HUP" | "INT",
  stoppingLine: string,
  exitCode: number,
  reason: string,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${SERVER_WITH_SLOW_CLEANUP}
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[server] cleaning up");
      yield* command.sendSignal(signal);
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(exitCode);
      expect(exited.standardError).toContain(`[server] cleaning up\n${stoppingLine}`);
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(exited.standardError).toMatch(new RegExp(`^error StopRequested: ${reason}$`, "mu"));
    }),
  );

export const interruptWhilePassedRunCloses = makeTest(
  "one Ctrl-C while a passed run closes its serving tasks is a first stop, so it prints the stopping line, lets a 2s cleanup finish and exits 130",
  stopWhilePassedRunCloses(
    "INT",
    "stopping, press ctrl-c again to force",
    EXIT_CODE_AFTER_SIGINT,
    "received SIGINT",
  ),
);

export const hangUpWhilePassedRunCloses = makeTest(
  "SIGHUP while a passed run closes its serving tasks is a first stop, so it prints the stopping line, lets a 2s cleanup finish and exits 129",
  stopWhilePassedRunCloses(
    "HUP",
    STOPPING_AFTER_A_HANGUP,
    EXIT_CODE_AFTER_SIGHUP,
    "received SIGHUP",
  ),
);

export const interruptAfterFailure = makeTest(
  "one Ctrl-C while the run cleans up after a task failed is a first stop, so it prints the stopping line, lets a 2s cleanup finish and still exits 1 with that failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${SERVER_WITH_SLOW_CLEANUP}
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  return yield* Effect.fail("broken build");
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[server] cleaning up");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(1);
      expect(exited.standardError).toContain(
        "[server] cleaning up\nstopping, press ctrl-c again to force",
      );
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(exited.standardError).not.toContain("StopRequested");
      expect(exited.standardError).toEndWith("\nTaskBodyFailed: its body failed with broken build");
    }),
  ),
);

export const forcedStuck = makeTest(
  "a second Ctrl-C while a worker is stuck in synchronous code ends the run without waiting for that worker, not even the exit grace",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.scoped(Effect.gen(function* () {
    yield* ChildProcess.make("sleep", ["1000"]);
    yield* Effect.log("cleaning up");
    yield* Effect.sync(() => { while (true) {} });
  })));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[server] cleaning up");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(
        findKilledProcesses(exited.standardError).map((each) => `${each.command} by ${each.by}`),
      ).toEqual(['sleep by "server"']);
      expect(findCutOffLines(exited.standardError)).toEqual([`[server] ${CLEANUP_CUT_OFF}`]);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      const workerShutdownLogs = (trace?.logs ?? [])
        .map((log) => String(log.body.stringValue))
        .filter((body) => body.startsWith("worker terminated"));
      expect(workerShutdownLogs).toContain(
        "worker terminated without waiting for it to exit because the stop was forced",
      );
      expect(workerShutdownLogs.filter((body) => body.includes("did not exit within"))).toEqual([]);
    }),
  ),
);

export const forcedCutsOffCleanup = makeTest(
  "a second Ctrl-C while a task's finalizer is still running names that task as cut off, counts it as failed in the summary, records it in the trace and still exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const deploy = Task.make({ name: "deploy", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.gen(function* () {
    yield* Effect.promise(() => Bun.write("cleanup-started", ""));
    yield* Effect.log("releasing lock");
    yield* Effect.sleep("1 minute");
    yield* Effect.promise(() => Bun.write("cleanup-finished", ""));
  }));
  yield* Effect.log("deploying");
  yield* Effect.never;
}) });
export default deploy;`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[deploy] deploying");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[deploy] releasing lock");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/cleanup-started`)).toBe(true);
      expect(yield* exists(`${project.directory}/cleanup-finished`)).toBe(false);
      expect(findCutOffLines(exited.standardError)).toEqual([`[deploy] ${CLEANUP_CUT_OFF}`]);
      expect(exited.standardError).toMatch(
        /^\[deploy\] error TaskCleanupCutOff: its cleanup was cut off because the stop was forced\n(?:.*\n)*failed: deploy \(1 of 1 task\), [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      expect(
        (trace?.logs ?? [])
          .map((log) => String(log.body.stringValue))
          .filter((body) => body.includes("TaskCleanupCutOff")),
      ).toEqual([`failed: ${CLEANUP_CUT_OFF}`]);
    }),
  ),
);

export const forcedSkipsFinishedCleanup = makeTest(
  "a second Ctrl-C names the task whose cleanup is still running as cut off but not a task that already finished its cleanup",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync } from "node:fs";
import { Effect, Schedule } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.gen(function* () {
    yield* Effect.promise(() => Bun.write("slow-cleanup-started", ""));
    yield* Effect.sleep("1 minute");
    yield* Effect.promise(() => Bun.write("slow-cleanup-finished", ""));
  }));
  yield* Effect.log("slow running");
  yield* Effect.never;
}) });
export const quick = Task.make({ name: "quick", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => existsSync("slow-cleanup-started")).pipe(
    Effect.repeat({ until: (started) => started, schedule: Schedule.spaced("10 millis") }),
    Effect.andThen(Effect.promise(() => Bun.write("quick-cleanup-finished", ""))),
  ));
  yield* Effect.log("quick running");
  yield* Effect.never;
}) });
export default Task.make({ name: "main", run: Task.all([slow, quick]) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine(" running");
      yield* command.waitForLine(" running");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[quick] stopped: the run ended before it finished");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/quick-cleanup-finished`)).toBe(true);
      expect(yield* exists(`${project.directory}/slow-cleanup-finished`)).toBe(false);
      const cutOffLines = findCutOffLines(exited.standardError);
      expect(cutOffLines).toContain(`[slow] ${CLEANUP_CUT_OFF}`);
      expect(cutOffLines.filter((line) => line.startsWith("[quick]"))).toEqual([]);
    }),
  ),
);

export const forcedCutsOffStuckBody = makeTest(
  "a second Ctrl-C while a task's body is stuck in synchronous code, so its cleanup never started, names that task as cut off and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make({ name: "spin", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.promise(() => Bun.write("cleanup-finished", "")));
  yield* Effect.log("spinning");
  yield* Effect.sync(() => { while (true) {} });
}) });
export default spin;`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[spin] spinning");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("stopping, press ctrl-c again to force");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/cleanup-finished`)).toBe(false);
      expect(findCutOffLines(exited.standardError)).toEqual([`[spin] ${CLEANUP_CUT_OFF}`]);
      expect(exited.standardError).not.toContain("TaskCleanupNotStarted");
      expect(exited.standardError).toMatch(
        /^failed: spin \(1 of 1 task\), [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

const BODY_STOPS = [
  {
    stoppedBy: "timeout",
    options: ', timeout: "3 seconds", cleanup: "5 seconds"',
    exitCode: 1,
    timeoutLines: ["[main] error TaskTimedOut: timed out after 3s"],
  },
  { stoppedBy: "Ctrl-C", options: "", exitCode: 130, timeoutLines: [] },
] as const;

const ensuringCleanup = (cleanup: string) =>
  `Effect.log("body started").pipe(Effect.andThen(Effect.never), Effect.ensuring(${cleanup}))`;

const scopeCleanup = (cleanup: string) =>
  `Effect.addFinalizer(() => ${cleanup}).pipe(Effect.andThen(Effect.log("body started")), Effect.andThen(Effect.never))`;

const stopBody = (body: string, cleanupLines: readonly string[]) =>
  Effect.forEach(
    BODY_STOPS,
    ({ stoppedBy, options, exitCode, timeoutLines }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class CloseFailed extends Data.TaggedError("CloseFailed") {}
export default Task.make({ name: "main", run: ${body}${options} });`,
          });
          const command = yield* startTaskProcess(project, "main.ts");
          yield* command.waitForLine("[main] body started");
          if (stoppedBy === "Ctrl-C") {
            yield* command.sendSignal("INT");
          }
          const exited = yield* command.waitForExit;
          expect(exited.exitCode, `${stoppedBy}\n${exited.standardError}`).toBe(exitCode);
          const errorLines = (exited.standardError.match(/^\[main\] .*error .*$/gmu) ?? []).map(
            (line) => line.replace(/^\[main\] \S+: error /u, "[main] error "),
          );
          expect(errorLines, `${stoppedBy}: ${body}`).toEqual([...timeoutLines, ...cleanupLines]);
        }),
      ),
    { concurrency: "unbounded", discard: true },
  );

export const bodyCleanupFailed = makeTest(
  "a task body cleanup that fails or dies while the task times out or is stopped fails the task and names the error",
  Effect.all(
    [
      stopBody(ensuringCleanup("Effect.fail(new CloseFailed())"), [
        "[main] error CloseFailed: CloseFailed",
      ]),
      stopBody(ensuringCleanup('Effect.die(new Error("close crashed"))'), [
        "[main] error TaskBodyFailed: its body failed with close crashed",
      ]),
      stopBody(scopeCleanup('Effect.die(new Error("scope close crashed"))'), [
        "[main] error TaskBodyFailed: its body failed with scope close crashed",
      ]),
      stopBody(
        `${scopeCleanup('Effect.die(new Error("scope close crashed"))')}.pipe(Effect.ensuring(Effect.fail(new CloseFailed())))`,
        [
          "[main] error CloseFailed: CloseFailed",
          "[main] error TaskCleanupFailed: its cleanup failed with scope close crashed",
        ],
      ),
    ],
    { concurrency: "unbounded", discard: true },
  ),
);

export const bodyCleanupSucceeded = makeTest(
  "a task body cleanup that succeeds while the task times out or is stopped adds no error",
  stopBody(`${scopeCleanup("Effect.void")}.pipe(Effect.ensuring(Effect.void))`, []),
);

export const stillStoppingWithCleanupLimit = makeTest(
  "a stopped task whose cleanup outlasts the notice says its commands are killed and the run stops waiting at its cleanup limit, then fails when the limit passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.log("ready").pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sleep("1 minute"))),
  cleanup: "4 seconds",
});`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toContain(
        "[main] still stopping after 2s; @sshkeda/task kills its commands and stops waiting for it when its 4s cleanup limit passes\n[main] error TaskCleanupTimedOut: its cleanup did not finish within 4s, so the run stopped waiting for it",
      );
    }),
  ),
);

export const cleanupFinishesShortlyAfterItsLimit = makeTest(
  "a finalizer that outlasts its cleanup limit by less than the 2s a worker gets to exit still finishes after TaskCleanupTimedOut is printed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.gen(function* () {
      yield* Effect.sleep("2 seconds");
      yield* Effect.promise(() => Bun.write("cleanup-finished", ""));
      yield* Effect.log("released the lock");
    }));
    yield* Effect.log("ready");
    yield* Effect.never;
  }),
  cleanup: "1 second",
});`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode, stopped.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toMatch(
        /^\[main\] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n(?:.*\n)*\[main\] released the lock\n(?:.*\n)*failed: main \(1 of 1 task\), [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      expect(yield* exists(`${project.directory}/cleanup-finished`)).toBe(true);
    }),
  ),
);

export const cleanupEndedWithItsWorker = makeTest(
  "a finalizer that outlasts its cleanup limit by more than the 2s a worker gets to exit is ended with its worker, so what it would do later never happens",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.gen(function* () {
      yield* Effect.sleep("1 minute");
      yield* Effect.promise(() => Bun.write("cleanup-finished", ""));
      yield* Effect.log("released the lock");
    }));
    yield* Effect.log("ready");
    yield* Effect.never;
  }),
  cleanup: "1 second",
});`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode, stopped.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toContain(
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
      );
      expect(stopped.standardError).not.toContain("released the lock");
      expect(yield* exists(`${project.directory}/cleanup-finished`)).toBe(false);
    }),
  ),
);

export const cleanupFinishesWhileAnotherTaskStops = makeTest(
  "a finalizer past its cleanup limit keeps running while the run waits for a serving task to stop, and finishes long after TaskCleanupTimedOut is printed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const quick = Task.make({
  name: "quick",
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.gen(function* () {
      yield* Effect.sleep("4 seconds");
      yield* Effect.promise(() => Bun.write("quick-cleanup-finished", ""));
      yield* Effect.log("released the lock");
    }));
    yield* Effect.log("quick ready");
    yield* Effect.never;
  }),
  cleanup: "1 second",
});
export const patient = Task.make({
  name: "patient",
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.sleep("6 seconds").pipe(Effect.andThen(Effect.log("drained"))));
    return yield* Task.serve("up");
  }),
  cleanup: "30 seconds",
});
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* patient;
  yield* quick;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[quick] quick ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode, stopped.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toMatch(
        /^\[quick\] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n(?:.*\n)*\[quick\] released the lock\n(?:.*\n)*\[patient\] drained\n(?:.*\n)*failed: quick \(1 of 3 tasks\), 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      expect(yield* exists(`${project.directory}/quick-cleanup-finished`)).toBe(true);
    }),
  ),
);

export const timedOutCleanupLimitOnce = makeTest(
  "a timed-out task whose cleanup outlasts its limit says it is still stopping and fails its cleanup once",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.log("ready").pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sleep("1 minute"))),
  timeout: "1 second",
  cleanup: "3 seconds",
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        "[main] error TaskTimedOut: timed out after 1s\n[main] still stopping after 2s; @sshkeda/task kills its commands and stops waiting for it when its 3s cleanup limit passes\n[main] error TaskCleanupTimedOut: its cleanup did not finish within 3s, so the run stopped waiting for it\n",
      );
      expect(countOccurrences(ran.standardError, "[main] still stopping"), ran.standardError).toBe(
        1,
      );
      expect(
        countOccurrences(ran.standardError, "[main] error TaskCleanupTimedOut"),
        ran.standardError,
      ).toBe(1);
    }),
  ),
);

export const stoppedCommandIgnoresTerm = makeTest(
  "stopping a task whose command ignores SIGTERM prints that it is still stopping, then SIGKILLs the command when the task's cleanup limit passes, so the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync } from "node:fs";
import { Effect, Schedule } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const stubborn = Task.make({ name: "stubborn", run: Task.exec("sh", ["-c", "trap 'echo got TERM' TERM; touch started; while true; do sleep 1; done"]), cleanup: "4 seconds" });
export const failing = Task.make({ name: "failing", run: Effect.gen(function* () {
  yield* Effect.sync(() => existsSync("started")).pipe(
    Effect.repeat({ until: (started) => started, schedule: Schedule.spaced("50 millis") }),
  );
  return yield* Effect.fail(new TaskError({ message: "lint failed" }));
}) });
export default Task.make({ name: "main", run: Task.all([stubborn, failing]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[stubborn] still stopping after 2s; @sshkeda/task kills its commands and stops waiting for it when its 4s cleanup limit passes",
      );
      expect(ran.standardError).not.toContain("did not exit within 5s");
      expect(ran.standardError).toMatch(/^failed: \S.*, [\d.]+s$/mu);
    }),
  ),
);

export const suspendedCommandGetsTerm = makeTest(
  "stopping a task whose command was suspended with SIGSTOP continues it, so it handles SIGTERM and exits instead of waiting out the task's cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const suspended = Task.make({ name: "suspended", run: Task.exec("sh", ["-c", "trap 'echo got TERM; exit 0' TERM; echo $$ > pid; kill -STOP $$; while true; do sleep 1; done"]), cleanup: "4 seconds" });
export const failing = Task.make({ name: "failing", run: Effect.gen(function* () {
  yield* Task.exec("sh", ["-c", "until [ -s pid ] && ps -o stat= -p $(cat pid) | grep -q T; do sleep 0.05; done"]);
  return yield* Effect.fail(new TaskError({ message: "lint failed" }));
}) });
export default Task.make({ name: "main", run: Task.all([suspended, failing]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[suspended] got TERM\n");
      expect(ran.standardError).not.toContain("still stopping");
      expect(ran.standardError).not.toContain("did not exit within");
      expect(ran.standardError).toMatch(/^failed: failing \(1 of 3 tasks\), 1 stopped, [\d.]+s$/mu);
    }),
  ),
);

export const stoppedCommandGetsCleanupLimit = makeTest(
  "a command stopped by its task's timeout gets the task's whole cleanup limit, not 5s, to exit after SIGTERM",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", run: Task.exec("sh", ["-c", "trap 'echo got TERM; sleep 6; echo graceful done; exit 0' TERM; while true; do sleep 0.1; done"]), timeout: "1 second", cleanup: "20 seconds" });
export default Task.make({ name: "main", run: Task.allSettled([slow]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[slow] got TERM");
      expect(ran.standardError).toContain("[slow] graceful done");
      expect(ran.standardError).not.toContain("sent SIGKILL");
      expect(ran.standardError).not.toContain("TaskCleanupTimedOut");
    }),
  ),
);

const COMMAND_STOPS = [
  { stoppedBy: "its timeout", serverTimeout: "1 second", mainTask: "server" },
  {
    stoppedBy: "another task's failure",
    serverTimeout: "1 minute",
    mainTask: "Task.all([server, failing])",
  },
] as const;

const stopCommandThatSavesOnTerm = ({
  stoppedBy,
  serverTimeout,
  mainTask,
}: (typeof COMMAND_STOPS)[number]) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync } from "node:fs";
import { Effect, Schedule } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const server = Task.make({ name: "server", run: Task.exec("sh", ["-c", "trap 'sleep 0.5; echo stopping >&2; echo saved > saved.txt; exit 0' TERM; touch started; while true; do sleep 0.1; done"]), timeout: "${serverTimeout}", cleanup: "10 seconds" });
export const failing = Task.make({ name: "failing", run: Effect.gen(function* () {
  yield* Effect.sync(() => existsSync("started")).pipe(
    Effect.repeat({ until: (started) => started, schedule: Schedule.spaced("50 millis") }),
  );
  return yield* Effect.fail(new TaskError({ message: "lint failed" }));
}) });
export default Task.make({ name: "main", run: ${mainTask} });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, stoppedBy).toBe(1);
      expect(yield* read(`${project.directory}/saved.txt`), stoppedBy).toBe("saved\n");
    }),
  );

export const stoppedCommandCleansUp = makeTest(
  "a command stopped by its task's timeout or by another task's failure can still write to its output, so its SIGTERM handler runs to the end",
  Effect.forEach(COMMAND_STOPS, stopCommandThatSavesOnTerm, {
    concurrency: "unbounded",
    discard: true,
  }),
);

export const timeoutWithoutCleanup = makeTest(
  "a task with timeout but no cleanup runs, whether it is awaited or the root",
  Effect.forEach(
    ['export default Task.make({ name: "main", run: Effect.asVoid(e2e) });', "export default e2e;"],
    (rootTaskLines) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const e2e = Task.make({ name: "e2e", run: Effect.log("ran"), timeout: "1 minute" });
${rootTaskLines}`,
          });
          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode, ran.standardError).toBe(0);
          expect(ran.standardError).toContain("[e2e] ran");
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
);

export const timeoutWithDefaultCleanup = makeTest(
  "a task that times out with timeout but no cleanup gets a 5s cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.never.pipe(Effect.ensuring(Effect.sleep("1 minute"))),
  timeout: "1 second",
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain(
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it",
      );
    }),
  ),
);

export const defaultCleanupLimit = makeTest(
  "a stopped task that sets neither timeout nor cleanup gets a 5s cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.log("ready").pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sleep("1 minute"))),
});`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toContain(
        "[main] still stopping after 2s; @sshkeda/task kills its commands and stops waiting for it when its 5s cleanup limit passes\n[main] error TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it",
      );
    }),
  ),
);
