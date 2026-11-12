import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Queue, Ref, Schedule, Schema, Stream } from "effect";

import { exists, read, write } from "../shared/access-files.ts";
import {
  EXIT_CODE_AFTER_SIGHUP,
  EXIT_CODE_AFTER_SIGINT,
  EXIT_CODE_AFTER_SIGTERM,
} from "../shared/exit-codes-after-stop-signals.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import {
  makeTaskProcessEnvironment,
  runTaskProcess,
  startTaskProcess,
} from "../shared/run-task-processes.ts";
import { startTerminalCommand } from "../shared/start-terminal-commands.ts";

const RUN_AND_SPAWNED_PROCESS_ID_COUNT = 2;

const ORPHAN_HOLDING_SPAWN_SYNC_OUTPUT = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => Bun.spawnSync(["sh", "-c", "(sleep 1000 2>/dev/null & echo orphan $! $$ >&2); exit 0"], { stderr: "inherit" })) });`;

const SPAWNING_SERVER = (label: string) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => Bun.spawn(["sleep", "1000"]).pid).pipe(
  Effect.flatMap((processId) => Effect.log(\`run \${process.pid} ${label} \${processId}\`)),
  Effect.andThen(Effect.never),
) });`;

const RUNNING_A_COMMAND = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log(\`run \${process.pid}\`).pipe(
  Effect.andThen(Task.exec("sh", ["-c", "echo command $$; exec sleep 1000"])),
) });`;

const RELEASING_IN_STEPS = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const releaseStep = (step: number) => Effect.sleep("1200 millis").pipe(
  Effect.andThen(Effect.promise(() => Bun.write(\`released-\${step}\`, ""))),
);
export default Task.make({ name: "main", cleanup: "30 seconds", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.forEach([1, 2, 3, 4, 5], releaseStep, { discard: true }));
  yield* Effect.log(\`run \${process.pid} supervised by \${process.ppid}\`);
  yield* Task.exec("sh", ["-c", "echo command $$; exec sleep 1000"]);
}) });`;

const RELEASE_STEPS = ["1", "2", "3", "4", "5"] as const;

const STOPPING_AFTER_A_HANGUP = "stopping, send SIGTERM or press ctrl-c to force";

const SLOW_LOADING_SERVER = `console.error(\`loading \${process.pid}\`);
await Bun.sleep(1000);
${SPAWNING_SERVER("spawned")}`;

const RECORDING_STOPS_UNTIL_GO = `import { appendFileSync, existsSync } from "node:fs";
const isRunProcess = Bun.main.endsWith("start-the-run.ts");
if (Bun.isMainThread) {
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
      if (isRunProcess) {
        appendFileSync("received", \`\${signal}\\n\`);
      } else {
        console.error(\`supervisor got \${signal}\`);
      }
    });
  }
  if (isRunProcess) {
    console.error(\`starting \${process.pid}\`);
    while (!existsSync("go")) {
      Bun.sleepSync(50);
    }
  }
}`;

const REFUSING_TO_CONTINUE = `if (import.meta.main) {
  const kill = process.kill.bind(process);
  process.kill = (processId, signal) => {
    if (signal === "SIGCONT" && processId !== process.pid) {
      throw Object.assign(new Error("continue refused"), { code: "EINVAL" });
    }
    return kill(processId, signal);
  };
}
${SPAWNING_SERVER("spawned")}`;

const LAUNCHER = (
  scriptArguments: string,
) => `const child = Bun.spawn([process.execPath, ${scriptArguments}], { stdio: ["ignore", "inherit", "inherit"] });
console.error(\`started \${child.pid}\`);
await child.exited;
console.error(\`exited \${child.exitCode} \${child.signalCode}\`);`;

const parseProcessIds = (line: string) =>
  Array.from(line.matchAll(/\b\d+\b/gu), ([processId]) => Number(processId));

const decodeProcessId = Schema.decodeUnknownEffect(
  Schema.TupleWithRest(Schema.Tuple([Schema.Finite]), [Schema.Finite]),
);

const decodeProcessIdPair = Schema.decodeUnknownEffect(
  Schema.TupleWithRest(Schema.Tuple([Schema.Finite, Schema.Finite]), [Schema.Finite]),
);

const killIfAlive = (processId: number) =>
  isProcessAlive(processId).pipe(
    Effect.flatMap((alive) =>
      alive ? Task.exec("kill", ["-KILL", String(processId)]) : Effect.void,
    ),
    failTestCleanup(`kill test process ${processId}`),
  );

const killWhenDone = (processIds: readonly number[]) =>
  Effect.forEach(processIds, (processId) => Effect.addFinalizer(() => killIfAlive(processId)), {
    discard: true,
  });

const waitForEachLine = (
  command: Effect.Success<ReturnType<typeof startTaskProcess>>,
  prefixes: readonly string[],
) =>
  Stream.fromEffectRepeat(command.waitForLine("")).pipe(
    Stream.scan(
      () => new Map<string, readonly number[]>(),
      (lines, line) => {
        const prefix = prefixes.find((wanted) => line.startsWith(wanted));
        return prefix === undefined
          ? lines
          : new Map([...lines, [prefix, parseProcessIds(line.slice(prefix.length))]]);
      },
    ),
    Stream.filter((lines) => lines.size === prefixes.length),
    Stream.runHead,
    Effect.flatMap(Effect.fromOption),
    Effect.tap((lines) => killWhenDone([...lines.values()].flat())),
  );

const pollProcessState = (processId: number, until: (state: string) => boolean) =>
  Task.exec("ps", ["-o", "stat=", "-p", String(processId)]).pipe(
    Effect.map((state) => state.trim()),
    Effect.catchTag("CommandFailed", () => Effect.succeed("")),
    Effect.repeat({ until, schedule: Schedule.spaced("50 millis") }),
    Effect.timeout("10 seconds"),
  );

const isStopped = (state: string) => state.startsWith("T");

const waitUntilEnded = (processId: number) =>
  pollProcessState(processId, (state) => state === "" || state.startsWith("Z"));

const startOnTerminal = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  command: readonly string[],
) =>
  Effect.gen(function* () {
    const chunks = yield* Queue.unbounded<string>();
    const screen = yield* Ref.make("");
    const decoder = new TextDecoder();
    const environment = yield* makeTaskProcessEnvironment(project, { PS1: "$ ", ENV: "" });
    const started = yield* startTerminalCommand(command, {
      workingDirectory: project.directory,
      environment,
      terminal: {
        data: (_terminal, data) => {
          Queue.offerUnsafe(chunks, decoder.decode(data, { stream: true }));
        },
      },
    });
    const takeUntilShown = (wanted: RegExp) =>
      Stream.fromEffectRepeat(Queue.take(chunks)).pipe(
        Stream.mapEffect((chunk) => Ref.updateAndGet(screen, (text) => text + chunk)),
        Stream.filter((text) => wanted.test(text)),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
    return {
      type: (text: string) => Effect.sync(() => started.terminal?.write(text)),
      waitFor: (wanted: RegExp) =>
        Ref.get(screen).pipe(
          Effect.filterOrElse(
            (text) => wanted.test(text),
            () => takeUntilShown(wanted),
          ),
          Effect.flatMap((text) => Effect.fromNullishOr(wanted.exec(text)?.[0])),
        ),
      close: Effect.sync(() => started.terminal?.close()),
      exited: Effect.promise(() => started.exited),
      screen: Ref.get(screen),
    };
  });

export const orphanHoldingSpawnSync = makeTest(
  "a forced stop while Bun.spawnSync waits on an orphan its command left holding the output kills and reports that orphan, and the run exits 143",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": ORPHAN_HOLDING_SPAWN_SYNC_OUTPUT });
      const command = yield* startTaskProcess(project, "main.ts");
      const [orphan, shell] = yield* decodeProcessIdPair(
        (yield* waitForEachLine(command, ["orphan"])).get("orphan"),
      );
      yield* waitUntilEnded(shell);
      yield* command.sendSignal("HUP");
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* command.sendSignal("TERM");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGTERM);
      expect(findKilledProcesses(exited.standardError).map((each) => each.processId)).toEqual([
        orphan,
      ]);
      expect(yield* isProcessAlive(orphan)).toBe(false);
    }),
  ),
);

export const sameLauncher = makeTest(
  "a forced stop of one run leaves running another run and a sleep started by the same launcher in the same process group",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "a.ts": ORPHAN_HOLDING_SPAWN_SYNC_OUTPUT,
        "b.ts": SPAWNING_SERVER("spawned"),
        "launch.ts": `const start = (file: string) => Bun.spawn(["sh", "-c", \`"$0" \${file} & echo started \${file} $! >&2; wait $!\`, process.execPath], { stdio: ["ignore", "inherit", "inherit"] });
const a = start("a.ts");
const b = start("b.ts");
const unrelated = Bun.spawn(["sleep", "1000"]);
console.error(\`unrelated \${unrelated.pid}\`);
console.error(\`a exited \${await a.exited}\`);
console.error(\`b exited \${await b.exited}\`);
unrelated.kill("SIGKILL");
await unrelated.exited;`,
      });
      const command = yield* startTaskProcess(project, "launch.ts");
      const lines = yield* waitForEachLine(command, [
        "started a.ts",
        "started b.ts",
        "unrelated",
        "orphan",
        "[main] run",
      ]);
      const [runA] = yield* decodeProcessId(lines.get("started a.ts"));
      const [runB] = yield* decodeProcessId(lines.get("started b.ts"));
      const [unrelated] = yield* decodeProcessId(lines.get("unrelated"));
      const [orphan] = yield* decodeProcessId(lines.get("orphan"));
      const [childB, commandB] = yield* decodeProcessIdPair(lines.get("[main] run"));
      yield* Effect.sync(() => process.kill(runA, "SIGHUP"));
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* Effect.sync(() => process.kill(runA, "SIGTERM"));
      yield* command.waitForLine("a exited 143");
      expect(yield* isProcessAlive(orphan)).toBe(false);
      for (const survivor of [runB, childB, commandB, unrelated]) {
        expect(yield* isProcessAlive(survivor)).toBe(true);
      }
      yield* Effect.sync(() => process.kill(runB, "SIGTERM"));
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toContain("b exited 143");
      const killed = findKilledProcesses(exited.standardError).map((each) => each.processId);
      expect(killed).toContain(orphan);
      expect(killed).toContain(commandB);
      for (const processId of [runA, runB, childB, commandB, unrelated, orphan]) {
        expect(yield* isProcessAlive(processId)).toBe(false);
      }
    }),
  ),
);

export const terminalInterrupt = makeTest(
  "pressing Ctrl-C twice in a terminal while Bun.spawnSync waits on an orphan kills and reports that orphan, and the run exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": ORPHAN_HOLDING_SPAWN_SYNC_OUTPUT });
      const terminal = yield* startOnTerminal(project, [process.execPath, "main.ts"]);
      const [orphan, shell] = yield* decodeProcessIdPair(
        parseProcessIds(yield* terminal.waitFor(/orphan \d+ \d+\r\n/u)),
      );
      yield* killWhenDone([orphan]);
      yield* waitUntilEnded(shell);
      yield* terminal.type("\u0003");
      yield* terminal.waitFor(/stopping, press ctrl-c again to force/u);
      yield* terminal.type("\u0003");
      expect(yield* terminal.exited).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(yield* terminal.waitFor(/StopRequested: received SIGINT\r\n$/u)).toBeTruthy();
      expect(yield* terminal.screen).toContain(`killed ${orphan} (`);
      expect(yield* isProcessAlive(orphan)).toBe(false);
    }),
  ),
);

export const terminalSuspend = makeTest(
  "Ctrl-Z in a terminal stops the run and the processes it started, and fg resumes them",
  Effect.scoped(
    Effect.gen(function* () {
      yield* terminalInterrupt;
      const project = yield* makeProject({ "main.ts": SPAWNING_SERVER("spawned") });
      const terminal = yield* startOnTerminal(project, ["sh", "-i"]);
      yield* terminal.type(`"${process.execPath}" main.ts\n`);
      const started = parseProcessIds(yield* terminal.waitFor(/\[main\] run \d+ spawned \d+\r\n/u));
      yield* killWhenDone(started);
      expect(started).toHaveLength(RUN_AND_SPAWNED_PROCESS_ID_COUNT);
      yield* terminal.type("\u001A");
      for (const processId of started) {
        yield* pollProcessState(processId, isStopped);
      }
      yield* terminal.type("fg\n");
      for (const processId of started) {
        yield* pollProcessState(processId, (state) => !isStopped(state));
      }
      yield* terminal.type("\u0003");
      yield* terminal.waitFor(/StopRequested: received SIGINT\r\n/u);
      yield* terminal.type('echo "exit $?"\n');
      yield* terminal.waitFor(/exit 130\r\n/u);
      yield* terminal.type("exit\n");
      yield* terminal.exited;
      for (const processId of started) {
        yield* waitUntilEnded(processId);
      }
    }),
  ),
);

export const stopsInOrderBeforeReady = makeTest(
  "SIGINT then SIGTERM to a run whose process is still starting are held until it is ready, and the run never receives SIGTERM before SIGINT",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "bunfig.toml": 'preload = ["./record-stops.ts"]\n',
        "record-stops.ts": RECORDING_STOPS_UNTIL_GO,
        "main.ts": SPAWNING_SERVER("spawned"),
      });
      const command = yield* startTaskProcess(project, "main.ts");
      const [starting] = yield* decodeProcessId(
        parseProcessIds(yield* command.waitForLine("starting")),
      );
      yield* killWhenDone([starting]);
      yield* command.sendSignal("INT");
      yield* command.waitForLine("supervisor got SIGINT");
      yield* command.sendSignal("TERM");
      yield* command.waitForLine("supervisor got SIGTERM");
      yield* write(`${project.directory}/go`, "");
      const exited = yield* command.waitForExit;
      const received = yield* read(`${project.directory}/received`);
      const endedOnSigterm = exited.exitCode === EXIT_CODE_AFTER_SIGTERM;
      expect(received).toBe(endedOnSigterm ? "SIGINT\nSIGTERM\n" : received);
      expect(["SIGINT\n", "SIGINT\nSIGTERM\n"]).toContain(received);
      expect(endedOnSigterm ? EXIT_CODE_AFTER_SIGTERM : EXIT_CODE_AFTER_SIGINT).toBe(
        exited.exitCode,
      );
      expect(exited.standardError).toEndWith(
        `StopRequested: received ${endedOnSigterm ? "SIGTERM" : "SIGINT"}`,
      );
      yield* waitUntilEnded(starting);
    }),
  ),
);

export const continueRefused = makeTest(
  "a supervisor that fails to continue the run's processes after SIGTSTP and SIGCONT prints the failure, exits 1 and kills the run's stopped processes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": REFUSING_TO_CONTINUE });
      const command = yield* startTaskProcess(project, "main.ts");
      const started = parseProcessIds(yield* command.waitForLine("[main] run"));
      yield* killWhenDone(started);
      expect(started).toHaveLength(RUN_AND_SPAWNED_PROCESS_ID_COUNT);
      yield* Effect.sync(() => process.kill(command.processId, "SIGTSTP"));
      for (const processId of [...started, command.processId]) {
        yield* pollProcessState(processId, isStopped);
      }
      yield* Effect.sync(() => process.kill(command.processId, "SIGCONT"));
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("ProcessNotSignaled");
      for (const processId of started) {
        yield* waitUntilEnded(processId);
      }
    }),
  ),
);

const expectEveryReleaseStep = (project: Effect.Success<ReturnType<typeof makeProject>>) =>
  Effect.gen(function* () {
    const released = yield* Effect.forEach(RELEASE_STEPS, (step) =>
      exists(`${project.directory}/released-${step}`),
    );
    expect(released.every((isReleased) => isReleased)).toBe(true);
  });

export const terminalClosed = makeTest(
  "closing the terminal of a run stops it like a first Ctrl-C, so every step of a 6s finalizer within its 30 seconds runs, its command ends, and the run exits 129 leaving no process alive",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": RELEASING_IN_STEPS });
      const terminal = yield* startOnTerminal(project, [process.execPath, "main.ts"]);
      const [run] = yield* decodeProcessId(
        parseProcessIds(yield* terminal.waitFor(/\[main\] run \d+ supervised by \d+\r\n/u)),
      );
      const [commandProcessId] = yield* decodeProcessId(
        parseProcessIds(yield* terminal.waitFor(/\[main\] command \d+\r\n/u)),
      );
      yield* killWhenDone([run, commandProcessId]);
      yield* terminal.close;
      expect(yield* terminal.exited).toBe(EXIT_CODE_AFTER_SIGHUP);
      yield* expectEveryReleaseStep(project);
      yield* waitUntilEnded(commandProcessId);
      yield* waitUntilEnded(run);
    }),
  ),
);

export const terminalClosedUnderShell = makeTest(
  "closing the terminal of an interactive bash that runs bun main.ts, which hangs the run up twice, once from the terminal and once from bash, still lets every step of a 6s finalizer within its 30 seconds run, and leaves no process alive",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": RELEASING_IN_STEPS });
      const terminal = yield* startOnTerminal(project, ["bash", "--norc", "-i"]);
      yield* terminal.type(`"${process.execPath}" main.ts\n`);
      const [run, supervisor] = yield* decodeProcessIdPair(
        parseProcessIds(yield* terminal.waitFor(/\[main\] run \d+ supervised by \d+\r\n/u)),
      );
      const [commandProcessId] = yield* decodeProcessId(
        parseProcessIds(yield* terminal.waitFor(/\[main\] command \d+\r\n/u)),
      );
      yield* killWhenDone([run, supervisor, commandProcessId]);
      yield* terminal.close;
      yield* terminal.exited;
      yield* waitUntilEnded(run);
      yield* waitUntilEnded(supervisor);
      yield* waitUntilEnded(commandProcessId);
      yield* expectEveryReleaseStep(project);
    }),
  ),
);

export const terminalClosedUnderPipe = makeTest(
  "closing the terminal of bun main.ts 2>&1 | cat, which ends cat and the run's standard error with it, still lets every step of a 6s finalizer within its 30 seconds run, and leaves no process alive",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": RELEASING_IN_STEPS });
      const terminal = yield* startOnTerminal(project, [
        "sh",
        "-c",
        `"${process.execPath}" main.ts 2>&1 | cat`,
      ]);
      const [run, supervisor] = yield* decodeProcessIdPair(
        parseProcessIds(yield* terminal.waitFor(/\[main\] run \d+ supervised by \d+\r\n/u)),
      );
      const [commandProcessId] = yield* decodeProcessId(
        parseProcessIds(yield* terminal.waitFor(/\[main\] command \d+\r\n/u)),
      );
      yield* killWhenDone([run, supervisor, commandProcessId]);
      yield* terminal.close;
      yield* terminal.exited;
      yield* waitUntilEnded(run);
      yield* waitUntilEnded(supervisor);
      yield* waitUntilEnded(commandProcessId);
      yield* expectEveryReleaseStep(project);
    }),
  ),
);

export const pipedIntoCat = makeTest(
  "a forced stop of bun main.ts | cat kills the run's orphan but not cat, which ends with the run's output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": ORPHAN_HOLDING_SPAWN_SYNC_OUTPUT,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ "$0" main.ts & echo run $! >&2; wait $!; echo $? > exit-code; } 2>&1 | cat >&2\`, process.execPath], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const command = yield* startTaskProcess(project, "piped.ts");
      const lines = yield* waitForEachLine(command, ["run", "orphan"]);
      const [run] = yield* decodeProcessId(lines.get("run"));
      const [orphan, shell] = yield* decodeProcessIdPair(lines.get("orphan"));
      yield* waitUntilEnded(shell);
      yield* Effect.sync(() => process.kill(run, "SIGHUP"));
      yield* command.waitForLine(STOPPING_AFTER_A_HANGUP);
      yield* Effect.sync(() => process.kill(run, "SIGTERM"));
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("143");
      expect(exited.standardError).toEndWith("StopRequested: received SIGTERM");
      expect(findKilledProcesses(exited.standardError).map((each) => each.processId)).toEqual([
        orphan,
      ]);
      expect(yield* isProcessAlive(orphan)).toBe(false);
    }),
  ),
);

export const exitSignal = makeTest(
  "a run whose process is killed by a signal ends the process its launcher started with that signal",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => process.kill(process.pid, "SIGUSR2")) });`,
        "launch.ts": LAUNCHER(`"main.ts"`),
      });
      const exited = yield* startTaskProcess(project, "launch.ts").pipe(
        Effect.flatMap((command) => command.waitForExit),
      );
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toEndWith("exited null SIGUSR2");
    }),
  ),
);

export const stuckLoadingTheRootFile = makeTest(
  "a SIGTERM to a run stuck loading its root file ends the run instead of waiting for it to finish loading",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (existsSync("loaded")) {
  console.error("stuck loading");
  await new Promise(() => {});
}
writeFileSync("loaded", "");
export default Task.make({ name: "main", run: Effect.void });`,
        "launch.ts": LAUNCHER(`"main.ts"`),
      });
      const command = yield* startTaskProcess(project, "launch.ts");
      const lines = yield* waitForEachLine(command, ["started", "stuck loading"]);
      const [launched] = yield* decodeProcessId(lines.get("started"));
      yield* Effect.sync(() => process.kill(launched, "SIGTERM"));
      const exited = yield* command.waitForExit;
      expect(exited.standardError).toEndWith("exited 143 null");
    }),
  ),
);

export const launcherKilled = makeTest(
  "SIGKILL to the process its launcher started ends the run and every process it started",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": SPAWNING_SERVER("spawned"),
        "launch.ts": LAUNCHER(`"main.ts"`),
      });
      const command = yield* startTaskProcess(project, "launch.ts");
      const lines = yield* waitForEachLine(command, ["started", "[main] run"]);
      const [launched] = yield* decodeProcessId(lines.get("started"));
      yield* Effect.sync(() => process.kill(launched, "SIGKILL"));
      const exited = yield* command.waitForExit;
      expect(exited.standardError).toEndWith("exited null SIGKILL");
      for (const processId of lines.get("[main] run") ?? []) {
        yield* waitUntilEnded(processId);
      }
    }),
  ),
);

export const runProcessKilled = makeTest(
  "SIGKILL to the run's process while a task's command runs makes the supervisor end that command",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": RUNNING_A_COMMAND,
        "launch.ts": LAUNCHER(`"main.ts"`),
      });
      const command = yield* startTaskProcess(project, "launch.ts");
      const lines = yield* waitForEachLine(command, ["[main] run", "[main] command"]);
      const [runProcess] = yield* decodeProcessId(lines.get("[main] run"));
      const [commandProcess] = yield* decodeProcessId(lines.get("[main] command"));
      yield* Effect.sync(() => process.kill(runProcess, "SIGKILL"));
      const exited = yield* command.waitForExit;
      expect(exited.standardError).toEndWith("exited null SIGKILL");
      yield* waitUntilEnded(commandProcess);
    }),
  ),
);

export const supervisorKilled = makeTest(
  "SIGKILL to the supervisor while a task's command runs ends the run's process and that command",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": RUNNING_A_COMMAND });
      const command = yield* startTaskProcess(project, "main.ts");
      const lines = yield* waitForEachLine(command, ["[main] run", "[main] command"]);
      const [runProcess] = yield* decodeProcessId(lines.get("[main] run"));
      const [commandProcess] = yield* decodeProcessId(lines.get("[main] command"));
      yield* command.sendSignal("KILL");
      yield* waitUntilEnded(runProcess);
      yield* waitUntilEnded(commandProcess);
    }),
  ),
);

export const launcherKilledWhileLoading = makeTest(
  "SIGKILL to the process its launcher started, while the run is still loading its root file, ends the run before it starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": SLOW_LOADING_SERVER,
        "launch.ts": LAUNCHER(`"main.ts"`),
      });
      const command = yield* startTaskProcess(project, "launch.ts");
      const [launched] = yield* decodeProcessId(
        parseProcessIds(yield* command.waitForLine("started")),
      );
      const [loading] = yield* Stream.fromEffectRepeat(command.waitForLine("loading")).pipe(
        Stream.map(parseProcessIds),
        Stream.filter(([processId]) => processId !== launched),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
        Effect.flatMap(decodeProcessId),
      );
      yield* killWhenDone([loading]);
      yield* Effect.sync(() => process.kill(launched, "SIGKILL"));
      yield* waitUntilEnded(loading);
      const exited = yield* command.waitForExit;
      expect(exited.standardError).toEndWith("exited null SIGKILL");
      expect(exited.standardError).not.toContain("[main]");
    }),
  ),
);

export const forcedStopWhileLoading = makeTest(
  "SIGHUP while the run is still loading its root file stops the run with exit code 129 without waiting for the file to load",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (existsSync("loaded")) {
  console.error(\`loading \${process.pid}\`);
  await Bun.sleep(1_000_000);
}
writeFileSync("loaded", "");
export default Task.make({ name: "main", run: Effect.log("ran") });`,
        "launch.ts": LAUNCHER(`"main.ts"`),
      });
      const command = yield* startTaskProcess(project, "launch.ts");
      const [launched] = yield* decodeProcessId(
        parseProcessIds(yield* command.waitForLine("started")),
      );
      const [loading] = yield* decodeProcessId(
        parseProcessIds(yield* command.waitForLine("loading")),
      );
      yield* killWhenDone([loading]);
      yield* Effect.sync(() => process.kill(launched, "SIGHUP"));
      const exited = yield* command.waitForExit;
      expect(exited.standardError).toEndWith("exited 129 null");
      expect(exited.standardError).not.toContain("[main]");
      yield* waitUntilEnded(loading);
    }),
  ),
);

export const startedProcessEnvironment = makeTest(
  "a task sees the Bun arguments the run was started with, and a process it starts with Bun.spawn gets the run's environment",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.error(\`arguments \${process.execArgv.join(" ")}.\`);
  const printed = Bun.spawnSync(["env"]).stdout.toString();
  console.error(printed.trim().split("\\n").map((variable) => \`environment \${variable}\`).join("\\n"));
}) });`,
      });
      const exited = yield* runTaskProcess(project, "main.ts");
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toContain("arguments .\n");
      const printed = exited.standardError
        .split("\n")
        .filter((line) => line.startsWith("environment "))
        .map((line) => line.slice("environment ".length));
      const environment = yield* makeTaskProcessEnvironment(project);
      expect(printed.toSorted()).toEqual(
        Object.entries(environment)
          .map(([name, value]) => `${name}=${value}`)
          .toSorted(),
      );
    }),
  ),
);

export const workingDirectoryFlag = makeTest(
  "a run started with bun --cwd and a path relative to where it started runs and passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "app/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
        "separate.ts": LAUNCHER(`"--cwd", "app", "main.ts"`),
        "joined.ts": LAUNCHER(`"--cwd=app", "main.ts"`),
        "titled.ts": LAUNCHER(`"--title", "--cwd", "--smol", "app/main.ts"`),
      });
      for (const launcher of ["separate.ts", "joined.ts", "titled.ts"]) {
        const exited = yield* runTaskProcess(project, launcher);
        expect(exited.standardError, launcher).toContain("[main] ran\n");
        expect(exited.standardError, launcher).toEndWith("exited 0 null");
      }
    }),
  ),
);

export const nestedRun = makeTest(
  "a run that a task starts with Bun.spawn and the run's own Bun arguments runs and passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  const nested = Bun.spawnSync([process.execPath, ...process.execArgv, "nested.ts"], { stderr: "inherit" });
  console.error(\`nested exited \${nested.exitCode}\`);
}) });`,
        "nested.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const nested = Task.make({ name: "nested", run: Effect.log(\`ran \${process.pid}\`) });
export default nested;`,
      });
      const exited = yield* runTaskProcess(project, "main.ts");
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toContain("[nested] ran ");
      expect(exited.standardError).toContain("nested exited 0");
    }),
  ),
);

export const nestedRunFromWorker = makeTest(
  "a run that a Worker of the root task file starts with the Worker's Bun arguments supervises itself",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (import.meta.main) {
  const worker = new Worker(new URL("./spawn.ts", import.meta.url));
  console.error(await new Promise((resolve) => { worker.onmessage = (event) => resolve(event.data); }));
  worker.terminate();
}
export default Task.make({ name: "main", run: Effect.void });`,
        "spawn.ts": `const nested = Bun.spawnSync([process.execPath, ...process.execArgv, "nested.ts"], { stderr: "pipe" });
postMessage(\`spawned by \${process.pid}: \${nested.stderr.toString().match(/\\[nested\\] ppid \\d+/)?.[0]}\`);`,
        "nested.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "nested", run: Effect.log(\`ppid \${process.ppid}\`) });`,
      });
      const exited = yield* runTaskProcess(project, "main.ts");
      expect(exited.exitCode).toBe(0);
      const spawned = exited.standardError
        .split("\n")
        .filter((line) => line.startsWith("spawned by "))
        .map((line) => parseProcessIds(line));
      expect(spawned).toHaveLength(1);
      for (const [spawner, nestedParent] of spawned) {
        expect(nestedParent).toBeNumber();
        expect(nestedParent).not.toBe(spawner);
      }
    }),
  ),
);
