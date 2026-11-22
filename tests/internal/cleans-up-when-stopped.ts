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
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const endedLinesOf = (standardError: string, taskName: string) =>
  standardError.split("\n").filter((line) => line.startsWith(`ended: [${taskName}], `));

const CLEANUP_CUT_OFF = "TaskCleanupCutOff: its cleanup was cut off because the stop was forced";

const findCutOffLines = (standardError: string) =>
  standardError.match(/^error: \[[^\]\n]+\] \S+: TaskCleanupCutOff: .*$/gmu) ?? [];

const STOPPING_AFTER_A_HANGUP = "stopping: send SIGTERM or press ctrl-c to force";

const STOPPED_LINE = "stopped: the run ended before it finished";

const ERROR_AND_RECAP_LINES = 2;

const STILL_STOPPING = "still stopping";

const RUN_ENDED_EVENT = "run ended before it finished";

const STOP_EVENTS_PAST_THE_CLEANUP_LIMIT = ["stop requested", "cleanup limit passed"] as const;

const STOP_EVENTS_WITHIN_THE_CLEANUP_LIMIT = ["stop requested", "stop finished"] as const;

export const interrupt = makeTest(
  "Ctrl-C cancels the run, runs the finalizers, exits 130 and ends with the stopped summary and the run line",
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
        "[main] sleeping\nstopping: press ctrl-c again to force",
      );
      expect(exited.standardError).toContain("[main] cleaned up");
      expect(exited.standardError).toMatch(
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
        /\nstopped: \[left\] \[right\] \[first\] and 2 more \(5 of 5 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
    }),
  ),
);

export const interruptAfterAPassedTask = makeTest(
  "Ctrl-C after one task passed sums the run up as stopped and counts only the tasks it stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const quick = Task.make({ name: "quick", run: Effect.void });
export const waiting = Task.make({ name: "waiting", run: Effect.andThen(Effect.log("waiting"), Effect.never) });
export default Task.make({ name: "main", run: Effect.andThen(quick, waiting) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[waiting] waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toMatch(
        /\nstopped: \[waiting\] \[main\] \(2 of 3 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(exited.standardError).not.toContain("failed:");
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
        "[main] sleeping\nstopping: send SIGTERM or press ctrl-c again to force",
      );
      expect(exited.standardError).toContain("[main] cleaned up");
      expect(exited.standardError).toMatch(
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
      expect(exited.standardError).toMatch(
        /\nfailed: \[server\] \(1 of 2 tasks\), 1 stopped, [\d.]+s\nrun: \S+\/$/u,
      );
      expect(exited.standardError).not.toContain(STOPPED_LINE);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      expect(trace?.findTaskEventNames("task", "main")).toContain(RUN_ENDED_EVENT);
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
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
      expect(stopped.standardError).toContain(
        `[main] ready\n${STOPPING_AFTER_A_HANGUP}\nerror: [main] main.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it\nended: [main], `,
      );
      expect(stopped.standardError).toMatch(
        /\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(stopped.standardError).not.toContain(STILL_STOPPING);
      expect(findCutOffLines(stopped.standardError)).toEqual([]);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, stopped));
      expect(trace?.findTaskEventNames("cleanup", "main")).toEqual([
        ...STOP_EVENTS_PAST_THE_CLEANUP_LIMIT,
      ]);
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
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [main] main.ts:3:21: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(yield* exists(`${project.directory}/released`)).toBe(false);
      expect(exited.standardError).toMatch(
        /\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
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
      expect(countOccurrences(exited.standardError, "stopping: ")).toBe(1);
      expect(exited.standardError).toMatch(
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
      yield* command.waitForLine("stopping: press ctrl-c again to force");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(exited.standardError).toMatch(
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
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
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [main] main.ts:3:21: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(yield* exists(`${project.directory}/released`)).toBe(false);
      expect(exited.standardError).toMatch(
        /\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
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
      expect(exited.standardError).not.toContain("[main] stopped:");
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [main] main.ts:3:21: ${CLEANUP_CUT_OFF}`,
      ]);
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
  "when standard error closes, as in | head -n 2, the run stops, a command that ignores SIGTERM is killed after a short grace period, and the run exits 141",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const prepare = Task.make({ name: "prepare", run: Effect.log("prepared") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const command = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo $$ > command-pid; echo started; exec sleep 1000"]);
  yield* command.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  yield* prepare;
  while (!(yield* Effect.promise(() => Bun.file("head-exited").exists()))) {
    yield* Effect.sleep("10 millis");
  }
  yield* Effect.forever(Effect.andThen(Effect.log("serving"), Effect.sleep("1 millis")));
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | { head -n 2; exec 0<&-; touch head-exited; }\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const piped = yield* runTaskProcess(project, "piped.ts");
      expect(piped.standardOutput).toMatch(/^run: \S+\/\n\[prepare\] prepared\n$/u);
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
      yield* command.waitForLine("stopping: press ctrl-c again to force");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      const commandProcessId = Number(
        /\[main\] waiting on (?<processId>\d+)/u.exec(exited.standardError)?.[1],
      );
      expect(findKilledProcesses(exited.standardError)).toEqual([
        { processId: commandProcessId, command: "sleep", by: "[main]" },
      ]);
      expect(yield* isProcessAlive(commandProcessId)).toBe(false);
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [main] main.ts:4:21: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(exited.standardError).toMatch(
        /\nerror: \[main\] main\.ts:4:21: TaskCleanupCutOff: its cleanup was cut off because the stop was forced\n(?:killed: \[main\] \d+ \(sleep\)\n)?(?:ended: \[main\], [\d.]+s, (?:stopped|failed)\n)?(?:killed: \[main\] \d+ \(sleep\)\n)?failed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(endedLinesOf(exited.standardError, "main"), exited.standardError).toHaveLength(1);
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
        /^(?:stopping: send SIGTERM or press ctrl-c again to force|run: (?<firstRunDirectory>\S+)\/\nstopping: send SIGTERM or press ctrl-c again to force\nstopped: 0 of 0 tasks, [\d.]+s\nrun: (?<lastRunDirectory>\S+)\/)$/u.exec(
          exited.standardError,
        );
      expect(lines, exited.standardError).not.toBeNull();
      const { firstRunDirectory, lastRunDirectory } = lines?.groups ?? {};
      expect(lastRunDirectory, exited.standardError).toBe(firstRunDirectory);
      if (firstRunDirectory !== undefined) {
        expect(yield* exists(`${firstRunDirectory}/trace.jsonl`)).toBe(true);
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
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(yield* isProcessAlive(command.processId)).toBe(false);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      expect(trace?.withRole("run")).toHaveLength(1);
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
      yield* command.waitForLine("serving: [main], ");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).not.toContain("main is serving");
      expect(exited.standardError).toContain(
        "stopping: press ctrl-c again to force\n[main] main stopped\nclosed: [main]",
      );
      expect(exited.standardError).toMatch(
        /\nstopped: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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

const INTERRUPT_ONCE_SERVING_IS_READ_AND_STANDARD_ERROR_CLOSED = `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | { sed '/^serving: /q'; exec 0<&-; kill -INT $(cat task-pid); }\`], { stdio: ["ignore", "inherit", "inherit"] });
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
      expect(piped.standardOutput).toMatch(/^serving: \[main\], [\d.]+s\n$/mu);
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
      expect(piped.standardOutput).toMatch(/^serving: \[main\], [\d.]+s\n$/mu);
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
while (my $line = <$reader>) { last if $line =~ /^serving: /; }
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

const HELD_BACK_LINE_COUNT = 64;

export const interruptAfterAFailureWhileOutputIsHeldBack = makeTest(
  "one Ctrl-C after a task failed, while its cleanup's console output is held back because nobody reads standard error, still ends the run without a second signal",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const line = "x".repeat(1_048_576) + "\\n";
export default Task.make({ name: "main", cleanup: "200 millis", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.promise(async () => {
    for (let written = 1; written <= ${HELD_BACK_LINE_COUNT}; written++) {
      await new Promise<void>((resolve) => process.stdout.write(line, () => resolve()));
      writeFileSync("written", String(written));
    }
  }));
  return yield* Effect.fail(new Error("boom"));
}) });`,
        "unread.pl": `use strict;
pipe(my $reader, my $writer) or die;
my $pid = fork();
if ($pid == 0) { close $reader; open(STDERR, ">&", $writer) or die; exec($ARGV[0], "main.ts") or die; }
close $writer;
while (my $line = <$reader>) { last if $line =~ /^error: /; }
select(undef, undef, undef, 0.05) until -s "written";
my ($seen, $same) = ("", 0);
while ($same < 10) {
  select(undef, undef, undef, 0.2);
  open(my $file, "<", "written") or die;
  my $now = <$file> // "";
  close $file;
  if ($now eq $seen) { $same++; } else { ($seen, $same) = ($now, 0); }
}
open(my $stalled, ">", "written-when-stopped") or die;
print $stalled $seen;
close $stalled;
kill "INT", $pid;
local $SIG{ALRM} = sub { kill "KILL", $pid; };
alarm 20;
waitpid($pid, 0);
print $? & 127 ? "killed\\n" : ($? >> 8) . "\\n";`,
        "unread.ts": `const perl = Bun.spawn(["perl", "unread.pl", process.execPath], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await perl.exited;`,
      });
      const unread = yield* runTaskProcess(project, "unread.ts");
      expect(Number(yield* read(`${project.directory}/written-when-stopped`))).toBeLessThan(
        HELD_BACK_LINE_COUNT,
      );
      expect(unread.standardOutput).toBe("1\n");
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
      expect(output).toContain("stopping: press ctrl-c again to force\n");
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
      expect(killed.map((each) => `${each.command} by ${each.by}`)).toEqual(["sleep by [server]"]);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [server] main.ts:4:28: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(exited.standardError).toMatch(
        /\nfailed: \[server\] \(1 of 2 tasks\), 1 stopped, [\d.]+s\nrun: \S+\/$/u,
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
      yield* command.waitForLine("stopping: press ctrl-c again to force");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(1);
      const killed = findKilledProcesses(exited.standardError);
      expect(killed.map((each) => `${each.command} by ${each.by}`)).toEqual(["sleep by [server]"]);
      expect(exited.standardError).toContain(
        "\nerror: [main] main.ts:12:21: UntaggedFailure: broken build\n",
      );
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [server] main.ts:4:28: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(exited.standardError).not.toContain("StopRequested");
      expect(exited.standardError).toMatch(
        /\nfailed: \[server\] \[main\] \(2 of 2 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
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

const stopWhilePassedRunCloses = (signal: "HUP" | "INT", stoppingLine: string, exitCode: number) =>
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
      expect(exited.standardError).toMatch(/\nstopped: 0 of 2 tasks, [\d.]+s\nrun: \S+\/$/u);
    }),
  );

export const interruptWhilePassedRunCloses = makeTest(
  "one Ctrl-C while a passed run closes its serving tasks is a first stop, so it prints the stopping line, lets a 2s cleanup finish and exits 130",
  stopWhilePassedRunCloses("INT", "stopping: press ctrl-c again to force", EXIT_CODE_AFTER_SIGINT),
);

export const hangUpWhilePassedRunCloses = makeTest(
  "SIGHUP while a passed run closes its serving tasks is a first stop, so it prints the stopping line, lets a 2s cleanup finish and exits 129",
  stopWhilePassedRunCloses("HUP", STOPPING_AFTER_A_HANGUP, EXIT_CODE_AFTER_SIGHUP),
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
        "[server] cleaning up\nstopping: press ctrl-c again to force",
      );
      expect(yield* exists(`${project.directory}/released`)).toBe(true);
      expect(findCutOffLines(exited.standardError)).toEqual([]);
      expect(exited.standardError).not.toContain("StopRequested");
      expect(exited.standardError).toContain(
        "\nerror: [main] main.ts:10:21: UntaggedFailure: broken build\n",
      );
      expect(exited.standardError).toMatch(
        /\nfailed: \[main\] \(1 of 2 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
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
      ).toEqual(["sleep by [server]"]);
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [server] main.ts:4:28: ${CLEANUP_CUT_OFF}`,
      ]);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
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
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [deploy] main.ts:3:28: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(exited.standardError).not.toContain("[deploy] stopped:");
      expect(exited.standardError).toMatch(
        /\nerror: \[deploy\] main\.ts:3:28: TaskCleanupCutOff: its cleanup was cut off because the stop was forced\n(?:ended: \[deploy\], [\d.]+s, (?:stopped|failed)\n)?failed: \[deploy\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(endedLinesOf(exited.standardError, "deploy"), exited.standardError).toHaveLength(1);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      expect(
        (trace?.logs ?? [])
          .map((log) => String(log.body.stringValue))
          .filter((body) => body.includes("TaskCleanupCutOff")),
      ).toEqual([`failed: error ${CLEANUP_CUT_OFF}`]);
    }),
  ),
);

const FORCED_STOP_TASK_NAMES = ["deploy", "main", "watch"] as const;

export const forcedStopEndsEachTaskOnce = makeTest(
  "a second Ctrl-C prints a cut-off line only for the task still in its cleanup, and each task's span records that the run ended before it finished",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const deploy = Task.make({ name: "deploy", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.andThen(Effect.log("releasing lock"), Effect.sleep("1 minute")));
  yield* Effect.log("deploying");
  yield* Effect.never;
}) });
export const watch = Task.make({ name: "watch", run: Effect.never });
export default Task.make({ name: "main", run: Task.all([deploy, watch]) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[deploy] deploying");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[deploy] releasing lock");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      const cutOffLines = [...findCutOffLines(exited.standardError)];
      expect(cutOffLines).toContain(`error: [deploy] main.ts:3:28: ${CLEANUP_CUT_OFF}`);
      expect(cutOffLines).toEqual([...new Set(cutOffLines)]);
      expect(exited.standardError).not.toContain(STOPPED_LINE);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      expect(
        FORCED_STOP_TASK_NAMES.map((taskName) =>
          countOccurrences(
            trace?.findTaskEventNames("task", taskName).join("\n") ?? "",
            RUN_ENDED_EVENT,
          ),
        ),
      ).toEqual(FORCED_STOP_TASK_NAMES.map(() => 1));
    }),
  ),
);

export const timedOutCleanupEndsOnce = makeTest(
  "a stopped task whose cleanup runs past its limit prints TaskCleanupTimedOut once, the other tasks print no end line, and each stop span records how its stop ended",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const deploy = Task.make({ name: "deploy", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("1 minute"));
  yield* Effect.log("deploying");
  yield* Effect.never;
}), cleanup: "1 second" });
export const watch = Task.make({ name: "watch", run: Effect.never });
export default Task.make({ name: "main", run: Task.all([deploy, watch]) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[deploy] deploying");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(
        countOccurrences(
          exited.standardError,
          "error: [deploy] main.ts:3:28: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
        ),
      ).toBe(1);
      expect(exited.standardError).not.toContain(STOPPED_LINE);
      expect(exited.standardError).not.toContain(STILL_STOPPING);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      expect(trace?.findTaskEventNames("cleanup", "deploy")).toEqual([
        ...STOP_EVENTS_PAST_THE_CLEANUP_LIMIT,
      ]);
      expect(trace?.findTaskEventNames("cleanup", "watch")).toEqual([
        ...STOP_EVENTS_WITHIN_THE_CLEANUP_LIMIT,
      ]);
      expect(trace?.findTaskEventNames("task", "watch")).toContain(RUN_ENDED_EVENT);
      expect(trace?.findTaskEventNames("task", "main")).toContain(RUN_ENDED_EVENT);
    }),
  ),
);

export const forcedSkipsFinishedCleanup = makeTest(
  "a second Ctrl-C names the task whose cleanup is still running as cut off but not a task that already finished its cleanup",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync } from "node:fs";
import { Effect, Schedule, Schema } from "effect";
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
export const quick = Task.make({ name: "quick", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => existsSync("slow-cleanup-started")).pipe(
    Effect.repeat({ until: (started) => started, schedule: Schedule.spaced("10 millis") }),
    Effect.andThen(Effect.promise(() => Bun.write("quick-cleanup-finished", ""))),
  ));
  yield* Effect.log("quick running");
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Task.all([slow, quick]) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine(" running");
      yield* command.waitForLine(" running");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("closed: [quick]");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/quick-cleanup-finished`)).toBe(true);
      expect(yield* exists(`${project.directory}/slow-cleanup-finished`)).toBe(false);
      const cutOffLines = findCutOffLines(exited.standardError);
      expect(cutOffLines).toContain(`error: [slow] main.ts:4:26: ${CLEANUP_CUT_OFF}`);
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
      yield* command.waitForLine("stopping: press ctrl-c again to force");
      yield* command.sendSignal("INT");

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* exists(`${project.directory}/cleanup-finished`)).toBe(false);
      expect(findCutOffLines(exited.standardError)).toEqual([
        `error: [spin] main.ts:3:26: ${CLEANUP_CUT_OFF}`,
      ]);
      expect(exited.standardError).not.toContain("TaskCleanupNotStarted");
      expect(exited.standardError).toMatch(
        /\nerror: \[spin\] main\.ts:3:26: TaskCleanupCutOff: its cleanup was cut off because the stop was forced\n(?:ended: \[spin\], [\d.]+s, (?:stopped|failed)\n)?failed: \[spin\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(endedLinesOf(exited.standardError, "spin"), exited.standardError).toHaveLength(1);
    }),
  ),
);

const BODY_STOPS = [
  {
    stoppedBy: "timeout",
    options: ', timeout: "3 seconds", cleanup: "5 seconds"',
    exitCode: 1,
    timeoutLines: ["error: [main] TaskTimedOut: timed out after 3s"],
    timeoutRecapLines: [
      "error: [main] TaskTimedOut: timed out after 3s",
      "recap: [main] no command was running",
    ],
  },
  { stoppedBy: "Ctrl-C", options: "", exitCode: 130, timeoutLines: [], timeoutRecapLines: [] },
] as const;

const ensuringCleanup = (cleanup: string) =>
  `Effect.log("body started").pipe(Effect.andThen(Effect.never), Effect.ensuring(${cleanup}))`;

const scopeCleanup = (cleanup: string) =>
  `Effect.addFinalizer(() => ${cleanup}).pipe(Effect.andThen(Effect.log("body started")), Effect.andThen(Effect.never))`;

const stopBody = (body: string, cleanupLines: readonly string[]) =>
  Effect.forEach(
    BODY_STOPS,
    ({ stoppedBy, options, exitCode, timeoutLines, timeoutRecapLines }) =>
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
          const errorLines = (
            exited.standardError.match(/^error: \[main\] \S+: \w+: .*$/gmu) ?? []
          ).map((line) => line.replace(/^error: \[main\] \S+: /u, "error: [main] "));
          expect(errorLines, `${stoppedBy}: ${body}`).toEqual([...timeoutLines, ...cleanupLines]);
          const recapLines = (exited.standardError.match(/^recap: .*$/gmu) ?? []).map((line) =>
            line.replace(/^recap: \[main\] 1\.output \| \S+: /u, "error: [main] "),
          );
          const recapOfTheLatestError: readonly string[] =
            cleanupLines.length === 0 ? timeoutRecapLines : cleanupLines.slice(-1);
          expect(recapLines, `${stoppedBy}: ${body}`).toEqual(
            timeoutLines.length === 0 ? [] : [...recapOfTheLatestError],
          );
        }),
      ),
    { concurrency: "unbounded", discard: true },
  );

export const bodyCleanupFailed = makeTest(
  "a task body cleanup that fails or dies while the task times out or is stopped fails the task and names the error",
  Effect.all(
    [
      stopBody(ensuringCleanup("Effect.fail(new CloseFailed())"), [
        "error: [main] CloseFailed: CloseFailed",
      ]),
      stopBody(ensuringCleanup('Effect.die(new Error("close crashed"))'), [
        "error: [main] Error: close crashed",
      ]),
      stopBody(scopeCleanup('Effect.die(new Error("scope close crashed"))'), [
        "error: [main] Error: scope close crashed",
      ]),
      stopBody(
        `${scopeCleanup('Effect.die(new Error("scope close crashed"))')}.pipe(Effect.ensuring(Effect.fail(new CloseFailed())))`,
        ["error: [main] CloseFailed: CloseFailed", "error: [main] Error: scope close crashed"],
      ),
    ],
    { concurrency: "unbounded", discard: true },
  ),
);

export const bodyCleanupSucceeded = makeTest(
  "a task body cleanup that succeeds while the task times out or is stopped adds no error",
  stopBody(`${scopeCleanup("Effect.void")}.pipe(Effect.ensuring(Effect.void))`, []),
);

const CLEANUP_MILLISECONDS_ATTRIBUTE = "sshkeda_task.cleanup.milliseconds";

const CLEANUP_LIMIT_MILLISECONDS = 4000;

const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

const SLEEP_TOLERANCE_MILLISECONDS = 100;

export const stopPastCleanupLimit = makeTest(
  "a stopped task whose cleanup outlasts its limit prints no progress line before it fails when the limit passes, and its stop span records the request with the limit and the limit passing",
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
        "stopping: press ctrl-c again to force\nerror: [main] main.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 4s, so the run stopped waiting for it\nended: [main], ",
      );
      expect(stopped.standardError).toMatch(
        /\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(stopped.standardError).not.toContain(STILL_STOPPING);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, stopped));
      const stopEvents = trace?.findTaskSpan("cleanup", "main")?.events ?? [];
      expect(
        stopEvents.map((event) => [
          event.name,
          readAttribute(event.attributes, CLEANUP_MILLISECONDS_ATTRIBUTE),
        ]),
      ).toEqual([
        ["stop requested", CLEANUP_LIMIT_MILLISECONDS],
        ["cleanup limit passed", undefined],
      ]);
      const [requestedAt, limitPassedAt] = stopEvents.map((event) =>
        Number(BigInt(event.timeUnixNano) / NANOSECONDS_PER_MILLISECOND),
      );
      expect(Number(limitPassedAt) - Number(requestedAt)).toBeGreaterThanOrEqual(
        CLEANUP_LIMIT_MILLISECONDS - SLEEP_TOLERANCE_MILLISECONDS,
      );
    }),
  ),
);

export const cleanupFinishesShortlyAfterItsLimit = makeTest(
  "a finalizer that outlasts its cleanup limit by less than the 2s a worker gets to exit still finishes after its task fails with TaskCleanupTimedOut",
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
        /\nerror: \[main\] main\.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n(?:.*\n)*\[main\] released the lock\n(?:.*\n)*failed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
        "\nerror: [main] main.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
      );
      expect(stopped.standardError).not.toContain("released the lock");
      expect(yield* exists(`${project.directory}/cleanup-finished`)).toBe(false);
    }),
  ),
);

export const cleanupFinishesWhileAnotherTaskStops = makeTest(
  "a finalizer past its cleanup limit keeps running while the run waits for a serving task to stop, and finishes long after its task fails with TaskCleanupTimedOut",
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
        /\nerror: \[quick\] main\.ts:3:27: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n(?:.*\n)*\[quick\] released the lock\n(?:.*\n)*\[patient\] drained\n(?:.*\n)*failed: \[quick\] \(1 of 3 tasks\), 2 stopped, [\d.]+s\nrun: \S+\/$/u,
      );
      expect(yield* exists(`${project.directory}/quick-cleanup-finished`)).toBe(true);
    }),
  ),
);

export const timedOutCleanupLimitOnce = makeTest(
  "a timed-out task whose cleanup outlasts its limit prints no progress line and fails its cleanup once",
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
      expect(ran.standardError).toMatch(
        /\nerror: \[main\] main\.ts:3:21: TaskTimedOut: timed out after 1s\nerror: \[main\] no command was running\nerror: \[main\] main\.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 3s, so the run stopped waiting for it\nerror: \[main\] its output is in \S+\/1\.output\nended: \[main\], [\d.]+s, failed\nrecap: \[main\] 1\.output \| main\.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 3s, so the run stopped waiting for it\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(ran.standardError).not.toContain(STILL_STOPPING);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const stopEventNames = trace?.findTaskEventNames("cleanup", "main") ?? [];
      expect(stopEventNames).toContain("stop requested");
      expect(countOccurrences(stopEventNames.join("\n"), "cleanup limit passed")).toBe(1);
      expect(countOccurrences(ran.standardError, "TaskCleanupTimedOut"), ran.standardError).toBe(
        ERROR_AND_RECAP_LINES,
      );
    }),
  ),
);

export const stoppedCommandIgnoresTerm = makeTest(
  "stopping a task whose command ignores SIGTERM prints no progress line, then SIGKILLs the command when the task's cleanup limit passes, so the run ends",
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
      expect(ran.standardError).not.toContain(STILL_STOPPING);
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
export const suspended = Task.make({ name: "suspended", run: Task.exec("sh", ["-c", "trap 'echo got TERM; exit 0' TERM; echo $$ > pid; kill -STOP $$; while true; do sleep 1; done"], { print: true }), cleanup: "4 seconds" });
export const failing = Task.make({ name: "failing", run: Effect.gen(function* () {
  yield* Task.exec("sh", ["-c", "until [ -s pid ] && ps -o stat= -p $(cat pid) | grep -q T; do sleep 0.05; done"]);
  return yield* Effect.fail(new TaskError({ message: "lint failed" }));
}) });
export default Task.make({ name: "main", run: Task.all([suspended, failing]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[suspended] got TERM\n");
      expect(ran.standardError).not.toContain(STILL_STOPPING);
      expect(ran.standardError).not.toContain("did not exit within");
      expect(ran.standardError).toMatch(
        /^failed: \[failing\] \(1 of 3 tasks\), 1 stopped, [\d.]+s$/mu,
      );
    }),
  ),
);

export const stoppedCommandGetsCleanupLimit = makeTest(
  "a command stopped by its task's timeout gets the task's whole cleanup limit, not 5s, to exit after SIGTERM",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", run: Task.exec("sh", ["-c", "trap 'echo got TERM; sleep 6; echo graceful done; exit 0' TERM; while true; do sleep 0.1; done"], { print: true }), timeout: "1 second", cleanup: "20 seconds" });
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
      expect(ran.standardError).toMatch(
        /^run: \S+\/\nerror: \[main\] main\.ts:3:21: TaskTimedOut: timed out after 1s\nerror: \[main\] no command was running\nerror: \[main\] main\.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it\nerror: \[main\] its output is in \S+\/1\.output\nended: \[main\], [\d.]+s, failed\nrecap: \[main\] 1\.output \| main\.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
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
        "stopping: press ctrl-c again to force\nerror: [main] main.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it\nended: [main], ",
      );
      expect(stopped.standardError).toMatch(
        /\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
    }),
  ),
);
