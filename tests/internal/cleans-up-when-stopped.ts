import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { exists, read } from "../shared/access-files.ts";
import {
  EXIT_CODE_AFTER_SIGHUP,
  EXIT_CODE_AFTER_SIGINT,
  EXIT_CODE_AFTER_SIGTERM,
} from "../shared/exit-codes-after-stop-signals.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

export const interrupt = Task.make(
  "Ctrl-C cancels the run, runs the finalizers, exits 130 and ends with the reason line StopRequested: received SIGINT",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  yield* ChildProcess.make("sleep", ["1000"]);
  yield* Effect.log("sleeping");
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
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
        /^failed: 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      expect(exited.standardError).toMatch(
        /\nwrote the trace to \S+\nStopRequested: received SIGINT$/u,
      );
      expect(findKilledProcesses(exited.standardError)).toEqual([]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const terminate = Task.make(
  "SIGTERM cancels the run, runs the finalizers and exits 143",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  yield* ChildProcess.make("sleep", ["1000"]);
  yield* Effect.log("sleeping");
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
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
        /^failed: 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGTERM$/mu,
      );
      expect(findKilledProcesses(exited.standardError)).toEqual([]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const hangUpServing = Task.make(
  "SIGHUP from a closed terminal stops the run like a second Ctrl-C, killing a serving task's command even when it ignores SIGTERM, and exits 129",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", Effect.gen(function* () {
  const child = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo started; exec sleep 1000"]);
  yield* child.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  yield* Effect.log(\`server process \${child.pid}\`);
  return yield* Task.serve("up");
}));
export const main = Task.make("main", Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGHUP$/mu);
      expect(exited.standardError).toContain("[main] stopped: the run ended before it finished");
      const serverProcessId = Number(
        /\[server\] server process (?<processId>\d+)/u.exec(exited.standardError)?.[1],
      );
      expect(yield* isProcessAlive(serverProcessId)).toBe(false);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const forcedDuringSpawnSync = Task.make(
  "a forced stop while a task's worker is blocked in Bun.spawnSync kills the blocking command, reports it and exits promptly",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.sync(() =>
  Bun.spawnSync(["sh", "-c", "echo blocking command started >&2; exec sleep 1000"], { stderr: "inherit" }),
));
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("blocking command started");
      yield* command.sendSignal("HUP");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
      expect(exited.standardError).toContain("[main] stopped: the run ended before it finished");
      const killed = findKilledProcesses(exited.standardError);
      expect(killed.map((each) => each.by)).toEqual(["no known task"]);
      expect(killed[0]?.command).toMatch(/^(?:sh|sleep)$/u);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const forcedWhileLoopingOverSpawnSync = Task.make(
  "a forced stop while a worker loops over Bun.spawnSync kills the running command and lets the worker start no other, so the run exits 129 promptly",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.sync(() => {
  for (let index = 0; index < 6; index++) {
    Bun.spawnSync(["sh", "-c", \`echo started command \${index} >&2; exec sleep 1000.\${process.pid}\${index}\`], { stderr: "inherit" });
  }
}));
if (import.meta.main) Task.main(main);`,
      });
      yield* Effect.forEach(
        ["first", "second", "third", "fourth"],
        () =>
          Effect.gen(function* () {
            const command = yield* startTaskProcess(project, "main.ts");
            yield* command.waitForLine("started command 0");
            yield* command.sendSignal("HUP");
            const exited = yield* command.waitForExit;
            expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGHUP);
            expect(exited.standardError).not.toContain("command 1");
            const killed = findKilledProcesses(exited.standardError);
            expect(killed.map((each) => each.by)).toEqual(["no known task"]);
            expect(killed[0]?.command).toMatch(/^(?:sh|sleep)$/u);
          }),
        { discard: true },
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const outputClosed = Task.make(
  "when standard error closes, as in | head -n 4, the run stops, a command that ignores SIGTERM is killed after a short grace period, and the run exits 141",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const prepare = Task.make("prepare", Effect.log("prepared"));
export const main = Task.make("main", Effect.gen(function* () {
  yield* prepare;
  const command = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo $$ > command-pid; echo started; exec sleep 1000"]);
  yield* command.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  while (!(yield* Effect.promise(() => Bun.file("head-exited").exists()))) {
    yield* Effect.sleep("10 millis");
  }
  yield* Effect.forever(Effect.andThen(Effect.log("serving"), Effect.sleep("1 millis")));
}));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const forcedIgnoresTerm = Task.make(
  "the first Ctrl-C waits on a finalizer stuck on a command that ignores SIGTERM, and a second Ctrl-C kills that command and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  const child = yield* ChildProcess.make("sh", ["-c", "trap '' TERM; echo started; exec sleep 1000"]);
  yield* child.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  yield* Effect.log(\`waiting on \${child.pid}\`);
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
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
      expect(exited.standardError).toMatch(
        /^failed: 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const startingUp = Task.make(
  "a SIGTERM that arrives while task is starting up ends the run before any task starts, and exits 143",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.never);
if (import.meta.main) {
  Task.main(main);
  process.kill(process.pid, "SIGTERM");
}`,
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
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const loading = Task.make(
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
export const main = Task.make("main", Effect.log(greeting));
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("worker loading");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toMatch(
        /^failed: 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      expect(yield* isProcessAlive(command.processId)).toBe(false);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      expect(trace?.named("runRootTaskFiber")).toHaveLength(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const root = Task.make(
  "a serving root holds the command until Ctrl-C, then runs its finalizers and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("main stopped"));
  return yield* Task.serve("up");
}), { timeout: "5 seconds", cleanup: "5 seconds" });
if (import.meta.main) Task.main(main);`,
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
        /^failed: 1 task, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const SERVING_ROOT_THAT_RECORDS_ITS_PROCESS_ID = `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread) await Bun.write("task-pid", String(process.pid));
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("main stopped"));
  return yield* Task.serve("up");
}), { timeout: "5 seconds", cleanup: "5 seconds" });
if (import.meta.main) Task.main(main);`;

export const interruptAfterOutputClosed = Task.make(
  "Ctrl-C after standard error closes, when the stopping line is the first write that finds it closed, still exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": SERVING_ROOT_THAT_RECORDS_ITS_PROCESS_ID,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | { sed '/main is serving/q'; exec 0<&-; kill -INT $(cat task-pid); }\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const piped = yield* runTaskProcess(project, "piped.ts");
      expect(piped.standardOutput).toEndWith("main is serving\n");
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("130");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const interruptWithOutputFull = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const interruptWhileTheReaderWaits = Task.make(
  "Ctrl-C while the reader of a full 2>&1 pipe waits ends the run with exit 130 once the reader takes every line, the stopping line included",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread) await Bun.write("task-pid", String(process.pid));
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("main stopped").pipe(
    Effect.andThen(Effect.promise(() => Bun.write("stopped", ""))),
  ));
  yield* Effect.forEach(Array.from({ length: 2000 }, (_, index) => index), (index) => Effect.log(\`\${index} \${"x".repeat(100)}\`), { discard: true });
  yield* Effect.promise(() => Bun.write("logged", ""));
  return yield* Task.serve("up");
}));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const forced = Task.make(
  "a second Ctrl-C kills what the finalizers are still running and reports it with its task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.scoped(Effect.gen(function* () {
    const child = yield* ChildProcess.make("sleep", ["1000"]);
    yield* Effect.log("cleaning up");
    yield* child.exitCode;
  })));
  return yield* Task.serve("up");
}));
export const main = Task.make("main", Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
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
      expect(exited.standardError).toMatch(
        /^failed: 2 tasks, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const forcedStuck = Task.make(
  "a second Ctrl-C while a worker is stuck in synchronous code ends the run without waiting for that worker, not even the exit grace",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.scoped(Effect.gen(function* () {
    yield* ChildProcess.make("sleep", ["1000"]);
    yield* Effect.log("cleaning up");
    yield* Effect.sync(() => { while (true) {} });
  })));
  return yield* Task.serve("up");
}));
export const main = Task.make("main", Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const BODY_STOPS = [
  {
    stoppedBy: "timeout",
    options: ', { timeout: "3 seconds", cleanup: "5 seconds" }',
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
export const main = Task.make("main", ${body}${options});
if (import.meta.main) Task.main(main);`,
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

export const bodyCleanupFailed = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyCleanupSucceeded = Task.make(
  "a task body cleanup that succeeds while the task times out or is stopped adds no error",
  stopBody(`${scopeCleanup("Effect.void")}.pipe(Effect.ensuring(Effect.void))`, []),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const stillStoppingWithCleanupLimit = Task.make(
  "a stopped task whose cleanup outlasts the notice says its cleanup limit will end it, then fails when the limit passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make(
  "main",
  Effect.log("ready").pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sleep("1 minute"))),
  { cleanup: "4 seconds" },
);
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toContain(
        "[main] still stopping after 2s; @sshkeda/task ends it when its 4s cleanup limit passes\n[main] error TaskCleanupTimedOut: its cleanup did not finish within 4s",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const stoppedCommandIgnoresTerm = Task.make(
  "stopping a task whose command ignores SIGTERM prints that it is still stopping, then SIGKILLs the command when the task's cleanup limit passes, so the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync } from "node:fs";
import { Effect, Schedule } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const stubborn = Task.make("stubborn", Task.exec("sh", ["-c", "trap 'echo got TERM' TERM; touch started; while true; do sleep 1; done"]), { cleanup: "4 seconds" });
export const failing = Task.make("failing", Effect.gen(function* () {
  yield* Effect.sync(() => existsSync("started")).pipe(
    Effect.repeat({ until: (started) => started, schedule: Schedule.spaced("50 millis") }),
  );
  return yield* Effect.fail(new TaskError({ message: "lint failed" }));
}));
export const main = Task.make("main", Task.all([stubborn, failing]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[stubborn] still stopping after 2s; @sshkeda/task ends it when its 4s cleanup limit passes",
      );
      expect(ran.standardError).not.toContain("did not exit within 5s");
      expect(ran.standardError).toMatch(/^failed: \S.*, [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const stoppedCommandGetsCleanupLimit = Task.make(
  "a command stopped by its task's timeout gets the task's whole cleanup limit, not 5s, to exit after SIGTERM",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const slow = Task.make("slow", Task.exec("sh", ["-c", "trap 'echo got TERM; sleep 6; echo graceful done; exit 0' TERM; while true; do sleep 0.1; done"]), { timeout: "1 second", cleanup: "20 seconds" });
export const main = Task.make("main", Task.allSettled([slow]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[slow] got TERM");
      expect(ran.standardError).toContain("[slow] graceful done");
      expect(ran.standardError).not.toContain("sent SIGKILL");
      expect(ran.standardError).not.toContain("TaskCleanupTimedOut");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
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
export const server = Task.make("server", Task.exec("sh", ["-c", "trap 'sleep 0.5; echo stopping >&2; echo saved > saved.txt; exit 0' TERM; touch started; while true; do sleep 0.1; done"]), { timeout: "${serverTimeout}", cleanup: "10 seconds" });
export const failing = Task.make("failing", Effect.gen(function* () {
  yield* Effect.sync(() => existsSync("started")).pipe(
    Effect.repeat({ until: (started) => started, schedule: Schedule.spaced("50 millis") }),
  );
  return yield* Effect.fail(new TaskError({ message: "lint failed" }));
}));
export const main = Task.make("main", ${mainTask});
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, stoppedBy).toBe(1);
      expect(yield* read(`${project.directory}/saved.txt`), stoppedBy).toBe("saved\n");
    }),
  );

export const stoppedCommandCleansUp = Task.make(
  "a command stopped by its task's timeout or by another task's failure can still write to its output, so its SIGTERM handler runs to the end",
  Effect.forEach(COMMAND_STOPS, stopCommandThatSavesOnTerm, {
    concurrency: "unbounded",
    discard: true,
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const timeoutWithoutCleanup = Task.make(
  "a task with timeout but no cleanup runs, whether it is awaited or the root",
  Effect.forEach(
    [
      'export const main = Task.make("main", Effect.asVoid(e2e));\nif (import.meta.main) Task.main(main);',
      "if (import.meta.main) Task.main(e2e);",
    ],
    (rootTaskLines) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const e2e = Task.make("e2e", Effect.log("ran"), { timeout: "1 minute" });
${rootTaskLines}`,
          });
          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode, ran.standardError).toBe(0);
          expect(ran.standardError).toContain("[e2e] ran");
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const timeoutWithDefaultCleanup = Task.make(
  "a task that times out with timeout but no cleanup gets a 5s cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make(
  "main",
  Effect.never.pipe(Effect.ensuring(Effect.sleep("1 minute"))),
  { timeout: "1 second" },
);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain(
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 5s",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const defaultCleanupLimit = Task.make(
  "a stopped task that sets neither timeout nor cleanup gets a 5s cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make(
  "main",
  Effect.log("ready").pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sleep("1 minute"))),
);
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const stopped = yield* command.waitForExit;
      expect(stopped.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(stopped.standardError).toContain(
        "[main] still stopping after 2s; @sshkeda/task ends it when its 5s cleanup limit passes\n[main] error TaskCleanupTimedOut: its cleanup did not finish within 5s",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
