import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Deferred, Effect, FileSystem, Schedule, Schema, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { RpcTest } from "effect/rpc";

import {
  forgetEndedProcessGroups,
  killTaskProcessGroups,
  recordLeaderExit,
  signalRunProcesses,
  startedProcessGroup,
} from "../../src/lib/kill-leftover-processes/index.ts";
import type { TrackedProcessGroup } from "../../src/lib/kill-leftover-processes/index.ts";
import { listProcesses } from "../../src/lib/kill-leftover-processes/internal/list-processes.ts";
import { createRunMarker } from "../../src/lib/kill-leftover-processes/internal/mark-run-processes.ts";
import { makeProcessGroupsListingWith } from "../../src/lib/main-thread/shared/provide-process-groups.ts";
import { processGroupsSharedWithTheSupervisor } from "../../src/lib/supervise-the-run/index.ts";
import {
  SupervisorRpcs,
  supervisorConnectionDeferred,
} from "../../src/lib/supervise-the-run/internal/talk-to-the-supervisor.ts";
import { makeProcessReportClaimer } from "../../src/shared/claim-processes-to-report.ts";
import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";
import { RUN_MARKERS_VARIABLE_NAME } from "../../src/shared/read-environment-config.ts";
import { markCommandOfTask } from "../../src/shared/run-marker.ts";
import { exists, read, write } from "../shared/access-files.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive, waitUntilProcessEnds } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readShellExecutableName } from "../shared/read-shell-executable-name.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const SHELL_AND_SLEEP_PROCESS_COUNT = 2;

const REGISTRATION = 1;

export const hung = makeTest(
  "a task that starts a command and hangs is stopped by its timeout, which runs its finalizers and stops the command's whole tree",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
export const hang = Task.make({ name: "hang", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  const child = yield* ChildProcess.make("sh", ["-c", "sleep 1000 & echo $$ $! > pids; wait"]);
  yield* child.exitCode;
}), timeout: "5 seconds", cleanup: "5 seconds" });
export default hang;`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[hang] cleaned up");
      expect(ran.standardError).toMatch(
        /^run: \S+\/\nerror: \[hang\] main\.ts:4:26: TaskTimedOut: timed out after 5s\n\[hang\] cleaned up\nerror: \[hang\] its output is in \S+\/1\.output\nended: \[hang\], [\d.]+s, failed\nrecap: \[hang\] 1\.output \| main\.ts:4:26: TaskTimedOut: timed out after 5s\nfailed: \[hang\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(findKilledProcesses(ran.standardError)).toEqual([]);
      const processIds = (yield* read(`${project.directory}/pids`)).trim().split(" ");
      expect(processIds).toHaveLength(SHELL_AND_SLEEP_PROCESS_COUNT);
      for (const processId of processIds) {
        expect(yield* isProcessAlive(Number(processId))).toBe(false);
      }
    }),
  ),
);

const LEAKING_SHELL_COMMAND =
  "sleep 1000 & echo forked $! >&2; while [ ! -e exec-seen ]; do sleep 0.05; done; echo started; wait";

const LEAKING_ROOT = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => Bun.spawn(["sh", "-c", "${LEAKING_SHELL_COMMAND}"], { stdout: "pipe" })).pipe(
  Effect.flatMap((child) => Effect.promise(() => child.stdout.getReader().read()).pipe(Effect.as(child.pid))),
  Effect.flatMap((processId) => Effect.log(\`spawned \${processId}\`)),
) });`;

const runUntilTheForkedChildIsSleep = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  environment: Readonly<Record<string, string>> = {},
) =>
  Effect.gen(function* () {
    const command = yield* startTaskProcess(project, "main.ts", environment);
    const forked = (yield* command.waitForLine("forked ")).slice("forked ".length).trim();
    yield* Task.exec("ps", ["-o", "comm=", "-p", forked]).pipe(
      Effect.repeat({
        until: (name) => name.trim().endsWith("sleep"),
        schedule: Schedule.spaced("20 millis"),
      }),
      Effect.timeout("10 seconds"),
    );
    yield* write(`${project.directory}/exec-seen`, "");
    return yield* command.waitForExit;
  });

export const leaked = makeTest(
  "a process left running at the end, started outside ChildProcess, is killed and reported with the processes it started",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": LEAKING_ROOT,
      });
      const ran = yield* runUntilTheForkedChildIsSleep(project);
      expect(ran.exitCode).toBe(0);
      const processId = Number(/\[main\] spawned (?<processId>\d+)/u.exec(ran.standardError)?.[1]);
      const killed = findKilledProcesses(ran.standardError);
      expect(killed.map((each) => `${each.command} by ${each.by}`)).toEqual([
        `${yield* readShellExecutableName} by no known task`,
        "sleep by no known task",
      ]);
      expect(killed[0]?.processId).toBe(processId);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
    }),
  ),
);

export const withoutPgrep = makeTest(
  "a run finds, kills and reports leftover processes without pgrep or ps on PATH",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": LEAKING_ROOT,
      });
      const commandDirectoryPath = `${project.directory}/bin`;
      yield* fileSystem.makeDirectory(commandDirectoryPath);
      for (const commandName of ["sh", "sleep"]) {
        const commandPath = yield* Effect.fromNullishOr(Bun.which(commandName));
        yield* fileSystem.symlink(commandPath, `${commandDirectoryPath}/${commandName}`);
      }
      expect(Bun.which("pgrep", { PATH: commandDirectoryPath })).toBeNull();
      expect(Bun.which("ps", { PATH: commandDirectoryPath })).toBeNull();
      const ran = yield* runUntilTheForkedChildIsSleep(project, { PATH: commandDirectoryPath });
      expect(ran.exitCode).toBe(0);
      const processId = Number(/\[main\] spawned (?<processId>\d+)/u.exec(ran.standardError)?.[1]);
      const killed = findKilledProcesses(ran.standardError);
      expect(killed.map((each) => `${each.command} by ${each.by}`)).toEqual([
        `${yield* readShellExecutableName} by no known task`,
        "sleep by no known task",
      ]);
      expect(killed[0]?.processId).toBe(processId);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
    }),
  ),
);

export const churning = makeTest(
  "a leftover process whose children start and exit while task lists processes is still killed and reported, and the run passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => Bun.spawn(["sh", "-c", "echo started; while true; do true & true & true & wait; done"], { stdout: "pipe" })).pipe(
  Effect.flatMap((child) => Effect.promise(() => child.stdout.getReader().read()).pipe(Effect.as(child.pid))),
  Effect.flatMap((processId) => Effect.log(\`spawned \${processId}\`)),
) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const processId = Number(/\[main\] spawned (?<processId>\d+)/u.exec(ran.standardError)?.[1]);
      expect(findKilledProcesses(ran.standardError)).toContainEqual({
        processId,
        command: yield* readShellExecutableName,
        by: "no known task",
      });
      expect(yield* isProcessAlive(processId)).toBe(false);
    }),
  ),
);

const orphanCommand = (file: string) =>
  `ChildProcess.make("sh", ["-c", "trap '' TERM; sleep 1000 > /dev/null 2>&1 & until ps -o command= -p $! | grep -qx 'sleep 1000'; do :; done; echo $$ $(ps -o pgid= -p $$) $! $(ps -o pgid= -p $!) > ${file}"]).pipe(Effect.flatMap((child) => child.exitCode))`;

export const orphaned = makeTest(
  "a grandchild that ignores SIGTERM, left in a task command's process group after the command exits, is found through that group, killed and reported with that task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
export const server = Task.make({ name: "server", run: ${orphanCommand("server-pids")}.pipe(Effect.andThen(Task.serve("up"))) });
export default Task.make({ name: "main", run: Effect.andThen(server, ${orphanCommand("main-pids")}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const expected = yield* Effect.forEach(["server", "main"], (name) =>
        Effect.gen(function* () {
          const [shell, shellGroup, sleep, sleepGroup] = yield* Schema.decodeUnknownEffect(
            Schema.Tuple([
              Schema.FiniteFromString,
              Schema.FiniteFromString,
              Schema.FiniteFromString,
              Schema.FiniteFromString,
            ]),
          )((yield* read(`${project.directory}/${name}-pids`)).trim().split(/\s+/u));
          expect([shellGroup, sleepGroup]).toEqual([shell, shell]);
          expect(yield* isProcessAlive(sleep)).toBe(false);
          const logged = trace?.logs.find(
            (log) =>
              log.body.stringValue === "process killed" &&
              readAttribute(log.attributes, "process.pid") === sleep,
          );
          expect(readAttribute(logged?.attributes ?? [], "sshkeda_task.task")).toBe(name);
          return { processId: sleep, command: "sleep", by: `[${name}]` };
        }),
      );
      expect(
        findKilledProcesses(ran.standardError).toSorted((left, right) =>
          left.by.localeCompare(right.by),
        ),
      ).toEqual(expected.toSorted((left, right) => left.by.localeCompare(right.by)));
    }),
  ),
);

export const killedAfterLeaderExit = makeTest(
  "a process that ignores SIGTERM and holds a command's output after the command exits is killed by the worker, reported once and traced with its task, and its kill is never printed as a task log line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.asVoid(Task.exec("sh", ["-c", "trap '' TERM; sleep 600 & echo $! > pid; printf finished"])) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const sleep = yield* Schema.decodeEffect(Schema.FiniteFromString)(
        (yield* read(`${project.directory}/pid`)).trim(),
      );
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: sleep, command: "sleep", by: "[main]" },
      ]);
      expect(ran.standardError).not.toContain("process killed");
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const logged = trace?.logs.find(
        (log) =>
          log.body.stringValue === "process killed" &&
          readAttribute(log.attributes, "process.pid") === sleep,
      );
      expect(readAttribute(logged?.attributes ?? [], "sshkeda_task.task")).toBe("main");
    }),
  ),
  "30 seconds",
);

const FAKE_SECRET = "FAKE-SECRET-123";

export const argumentsKeptPrivate = makeTest(
  "a leftover process is reported and traced by its process id and program name, and no argument of it or of a command a task starts reaches the output or the state directory",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: ChildProcess.make("sh", ["-c", "trap '' TERM; perl -e 'sleep 300' -- --token=${FAKE_SECRET} > /dev/null 2>&1 & until ps -o comm= -p $! | grep -q perl; do :; done; echo $! > leftover-pid"]).pipe(Effect.flatMap((child) => child.exitCode)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const perl = Number((yield* read(`${project.directory}/leftover-pid`)).trim());
      expect(ran.standardError).toContain(`killed: [main] ${perl} (perl)`);
      expect(yield* isProcessAlive(perl)).toBe(false);
      const stateDirectory = `${project.home}/${TASK_STATE_DIRECTORY_NAME}`;
      const stateFiles = yield* Effect.filter(
        (yield* fileSystem.readDirectory(stateDirectory, { recursive: true })).map(
          (entry) => `${stateDirectory}/${entry}`,
        ),
        (filePath) =>
          Effect.map(fileSystem.stat(filePath), (information) => information.type === "File"),
      );
      expect(stateFiles.some((filePath) => filePath.endsWith("/trace.jsonl"))).toBe(true);
      const filesHoldingSecret = yield* Effect.filter(stateFiles, (filePath) =>
        Effect.map(fileSystem.readFile(filePath), (bytes) =>
          Buffer.from(bytes).includes(FAKE_SECRET),
        ),
      );
      expect({
        standardOutput: ran.standardOutput.includes(FAKE_SECRET),
        standardError: ran.standardError.includes(FAKE_SECRET),
        filesHoldingSecret,
      }).toEqual({ standardOutput: false, standardError: false, filesHoldingSecret: [] });
    }),
  ),
);

export const endedTask = makeTest(
  "a process a task leaves running is killed and reported when that task ends, before the task that yielded it goes on",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const leaver = Task.make({ name: "leaver", run: ChildProcess.make("sh", ["-c", "trap '' TERM; sleep 1000 > /dev/null 2>&1 & until ps -o command= -p $! | grep -qx 'sleep 1000'; do :; done; echo $! > leaver-pid"]).pipe(Effect.flatMap((child) => child.exitCode)) });
export default Task.make({ name: "main", run: Effect.andThen(leaver, Effect.log("after leaver")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const sleep = Number((yield* read(`${project.directory}/leaver-pid`)).trim());
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: sleep, command: "sleep", by: "[leaver]" },
      ]);
      expect(ran.standardError.indexOf(`killed: [leaver] ${sleep} `)).toBeLessThan(
        ran.standardError.indexOf("[main] after leaver"),
      );
      expect(yield* isProcessAlive(sleep)).toBe(false);
    }),
  ),
);

export const jobControl = makeTest(
  "a process a task's command starts in its own process group with set -m is killed and reported with that task when the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("bash", ["-c", "set -m; ${process.execPath} -e 'setInterval(() => {}, 1000)' > /dev/null 2>&1 & echo $! > job-pid"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const job = (yield* read(`${project.directory}/job-pid`)).trim();
      yield* Effect.addFinalizer(() =>
        isProcessAlive(Number(job)).pipe(
          Effect.flatMap((alive) => (alive ? Task.exec("kill", ["-KILL", job]) : Effect.void)),
          failTestCleanup("kill leftover test process"),
        ),
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findKilledProcesses(ran.standardError), ran.standardError).toEqual([
        { processId: Number(job), command: "bun", by: "[main]" },
      ]);
      expect(yield* isProcessAlive(Number(job))).toBe(false);
    }),
  ),
);

export const escaped = makeTest(
  "task never kills its own process group, so a grandchild of a Bun.spawn process whose parent exited is left running on macOS, while on Linux, where the supervisor adopts it, it is killed and reported",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.promise(() => Bun.spawn(["sh", "-c", "sleep 1000 > /dev/null 2>&1 & echo $! $PPID > pid"]).exited) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      const ran = yield* command.waitForExit;
      const [sleep, run] = yield* Schema.decodeUnknownEffect(
        Schema.Tuple([Schema.NonEmptyString, Schema.NonEmptyString]),
      )((yield* read(`${project.directory}/pid`)).trim().split(" "));
      yield* Effect.addFinalizer(() =>
        isProcessAlive(Number(sleep)).pipe(
          Effect.flatMap((alive) => (alive ? Task.exec("kill", ["-KILL", sleep]) : Effect.void)),
          failTestCleanup("kill leftover test process"),
        ),
      );
      const isAdopted = process.platform === "linux";
      expect(ran.exitCode).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toEqual(
        isAdopted ? [{ processId: Number(sleep), command: "sleep", by: "no known task" }] : [],
      );
      expect(yield* isProcessAlive(Number(sleep))).toBe(!isAdopted);
      if (!isAdopted) {
        expect(Number(yield* Task.exec("ps", ["-o", "pgid=", "-p", sleep]))).toBe(Number(run));
      }
    }),
  ),
);

export const nothingStarted = makeTest(
  "a run whose tasks start no process kills and reports nothing",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Task.serve("up") });
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toEqual([]);
      expect(ran.standardError).not.toContain("could not kill");
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        trace?.logs.filter((log) => String(log.body.stringValue).startsWith("process ")),
      ).toEqual([]);
    }),
  ),
);

export const foreign = makeTest(
  "a process task did not start is never killed, even one running the same command as a leftover it kills",
  Effect.scoped(
    Effect.gen(function* () {
      const other = yield* ChildProcess.make("sleep", ["1000"]);
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => Bun.spawn(["sleep", "1000"]).pid).pipe(Effect.flatMap((processId) => Effect.log(\`spawned \${processId}\`))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const leftover = Number(/\[main\] spawned (?<processId>\d+)/u.exec(ran.standardError)?.[1]);
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: leftover, command: "sleep", by: "no known task" },
      ]);
      expect(yield* isProcessAlive(leftover)).toBe(false);
      expect(yield* isProcessAlive(other.pid)).toBe(true);
    }),
  ),
);

export const markerInArguments = makeTest(
  "a process task did not start, which names the run's marker in its arguments but not in its environment, is never killed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo \\"$${RUN_MARKERS_VARIABLE_NAME}\\" > marker.part; mv marker.part marker; until [ -e foreign-started ]; do sleep 0.05; done"]) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* exists(`${project.directory}/marker`).pipe(
        Effect.repeat({ until: (written) => written, schedule: Schedule.spaced("50 millis") }),
        Effect.timeout("10 seconds"),
      );
      const marker = (yield* read(`${project.directory}/marker`)).trim();
      const namingTheMarker = yield* ChildProcess.make(
        "perl",
        ["-e", "sleep 1000", `${RUN_MARKERS_VARIABLE_NAME}=${marker}`],
        { stdout: "ignore", stderr: "ignore" },
      );
      yield* write(`${project.directory}/foreign-started`, "");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toEqual([]);
      expect(yield* isProcessAlive(namingTheMarker.pid)).toBe(true);
    }),
  ),
);

export const ownGroup = makeTest(
  "each command a task starts leads its own process group",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
const report = (file: string) => ChildProcess.make("sh", ["-c", \`echo $$ $(ps -o pgid= -p $$) > \${file}\`]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
export default Task.make({ name: "main", run: Effect.all([report("first"), report("second")], { concurrency: "unbounded" }) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      expect((yield* command.waitForExit).exitCode).toBe(0);
      const groups = yield* Effect.forEach(["first", "second"], (file) =>
        read(`${project.directory}/${file}`).pipe(
          Effect.map((text) =>
            text
              .trim()
              .split(/\s+/u)
              .map((processGroupId) => Number(processGroupId)),
          ),
        ),
      );
      for (const [processId, group] of groups) {
        expect(group).toBe(processId);
        expect(group).not.toBe(command.processId);
      }
      expect(groups[0]?.[1]).not.toBe(groups[1]?.[1]);
    }),
  ),
);

export const spawnedDuringWorkerExit = makeTest(
  "a process a worker starts while the run waits for workers to exit is still killed and reported",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { appendFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.forkDetach(Effect.forever(Effect.sync(() => {
    Bun.spawnSync(["sleep", "0.7"]);
    const child = Bun.spawn(["sh", "-c", "trap '' TERM; exec sleep 1000"], { stdio: ["ignore", "ignore", "ignore"] });
    appendFileSync("pids", \`\${child.pid}\\n\`);
  })));
  yield* Effect.sleep("200 millis");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const processIds = (yield* read(`${project.directory}/pids`)).trim().split("\n");
      yield* Effect.addFinalizer(() =>
        Effect.forEach(
          processIds,
          (processId) =>
            isProcessAlive(Number(processId)).pipe(
              Effect.flatMap((alive) =>
                alive ? Task.exec("kill", ["-KILL", processId]) : Effect.void,
              ),
            ),
          { discard: true },
        ).pipe(failTestCleanup("kill leftover test processes")),
      );
      expect(ran.exitCode).toBe(0);
      expect(processIds.length).toBeGreaterThan(0);
      for (const processId of processIds) {
        expect(yield* isProcessAlive(Number(processId))).toBe(false);
      }
    }),
  ),
);

const killTestProcessWhenDone = (processId: number) =>
  Effect.addFinalizer(() =>
    isProcessAlive(processId).pipe(
      Effect.flatMap((alive) =>
        alive ? Task.exec("kill", ["-KILL", String(processId)]) : Effect.void,
      ),
      failTestCleanup(`kill test process ${processId}`),
    ),
  );

const readStartTimeOfThisProcess = listProcesses(null).pipe(
  Effect.flatMap((listed) => Effect.fromNullishOr(listed.get(process.pid))),
  Effect.map((listed) => listed.startTime),
);

const LEAVE_A_GROUP_WHOSE_LEADER_EXITED =
  'my $leader = fork(); if ($leader == 0) { setpgrp(0, 0); my $member = fork(); if ($member == 0) { open(STDOUT, ">", "/dev/null"); open(STDERR, ">", "/dev/null"); exec("sleep", "1000"); } print "$$ $member"; exit 0; } waitpid($leader, 0);';

const startGroupWhoseLeaderExited = Effect.gen(function* () {
  const [processGroupId, member] = yield* Schema.decodeUnknownEffect(
    Schema.Tuple([Schema.FiniteFromString, Schema.FiniteFromString]),
  )((yield* Task.exec("perl", ["-e", LEAVE_A_GROUP_WHOSE_LEADER_EXITED])).split(" "));
  yield* killTestProcessWhenDone(member);
  const listedMember = yield* Effect.fromNullishOr((yield* listProcesses(null)).get(member));
  expect(listedMember.processGroupId).toBe(processGroupId);
  expect((yield* listProcesses(null)).has(processGroupId)).toBe(false);
  return { processGroupId, member };
});

const listedAsAnEarlierProcess = (processId: number, earlierStartTime: string) =>
  ((taskName: string | null) =>
    listProcesses(taskName).pipe(
      Effect.map(
        (listed) =>
          new Map(
            [...listed].map(([listedProcessId, listedProcess]) => [
              listedProcessId,
              listedProcessId === processId
                ? { ...listedProcess, startTime: earlierStartTime }
                : listedProcess,
            ]),
          ),
      ),
    )) satisfies typeof listProcesses;

export const reusedProcessGroupId = makeTest(
  "a recorded process group ID that an unrelated group, whose leader has exited, now holds is never signaled, whether task saw its own leader exit or not",
  Effect.scoped(
    Effect.gen(function* () {
      const unrelated = yield* startGroupWhoseLeaderExited;
      const earlierStartTime = yield* readStartTimeOfThisProcess;
      const { claimProcessesToReport } = makeProcessReportClaimer();
      for (const membersAtLeaderExit of [
        null,
        [{ processId: process.pid, startTime: earlierStartTime }],
      ]) {
        yield* killTaskProcessGroups(
          new Map([
            [
              unrelated.processGroupId,
              startedProcessGroup(REGISTRATION, "leaver", earlierStartTime, membersAtLeaderExit),
            ],
          ]),
          "leaver",
          claimProcessesToReport,
          null,
        );
        expect(yield* isProcessAlive(unrelated.member)).toBe(true);
      }
    }),
  ),
);

export const reusedProcessGroupIdAfterRecordedMembersEnded = makeTest(
  "a recorded process group ID that an unrelated group now holds is never signaled after the members task saw at its leader's exit have ended, even when the unrelated group's member started between the leader and those members",
  Effect.scoped(
    Effect.gen(function* () {
      const leaderStartTime = yield* readStartTimeOfThisProcess;
      const unrelated = yield* startGroupWhoseLeaderExited;
      const endedMember = yield* ChildProcess.make("sleep", ["1000"]);
      const listedEndedMember = yield* Effect.fromNullishOr(
        (yield* listProcesses(null)).get(endedMember.pid),
      );
      yield* endedMember.kill({ killSignal: "SIGKILL" });
      yield* waitUntilProcessEnds(endedMember.pid);
      yield* killTaskProcessGroups(
        new Map([
          [
            unrelated.processGroupId,
            startedProcessGroup(REGISTRATION, "leaver", leaderStartTime, [
              { processId: endedMember.pid, startTime: listedEndedMember.startTime },
            ]),
          ],
        ]),
        "leaver",
        makeProcessReportClaimer().claimProcessesToReport,
        null,
      );
      expect(yield* isProcessAlive(unrelated.member)).toBe(true);
    }),
  ),
);

export const leaderExitRecordedDuringKill = makeTest(
  "a process group whose leader exit is recorded while a kill of its task looks at the group stays tracked, so the next kill stops the members its leader left",
  Effect.scoped(
    Effect.gen(function* () {
      const group = yield* startGroupWhoseLeaderExited;
      const leaderStartTime = yield* readStartTimeOfThisProcess;
      const listedMember = yield* Effect.fromNullishOr(
        (yield* listProcesses(null)).get(group.member),
      );
      const startedProcessGroups = new Map([
        [group.processGroupId, startedProcessGroup(REGISTRATION, "leaver", leaderStartTime, null)],
      ]);
      const killAndForget = (listProcessesNow: typeof listProcesses) =>
        killTaskProcessGroups(
          startedProcessGroups,
          "leaver",
          makeProcessReportClaimer().claimProcessesToReport,
          null,
          listProcessesNow,
        ).pipe(Effect.flatMap(forgetEndedProcessGroups(startedProcessGroups)));
      yield* killAndForget((taskName) =>
        Effect.sync(() =>
          recordLeaderExit(startedProcessGroups, group.processGroupId, "leaver", [
            { processId: group.member, startTime: listedMember.startTime },
          ]),
        ).pipe(Effect.andThen(listProcesses(taskName))),
      );
      expect(startedProcessGroups.get(group.processGroupId)).toEqual(
        startedProcessGroup(REGISTRATION, "leaver", leaderStartTime, [
          { processId: group.member, startTime: listedMember.startTime },
        ]),
      );
      yield* killAndForget(listProcesses);
      yield* waitUntilProcessEnds(group.member);
      expect(startedProcessGroups.has(group.processGroupId)).toBe(false);
    }),
  ),
);

const supervisorMirror = new Map<number, typeof TrackedProcessGroup.Type>();

const actAsTheSupervisor = Effect.gen(function* () {
  const handlers = yield* SupervisorRpcs.toHandlers({
    ReportReady: () => Effect.void,
    AcknowledgeStop: () => Effect.void,
    ShareProcessGroup: ({ processGroupId, startedGroup }) =>
      Effect.sync(() => {
        if (startedGroup === null) {
          supervisorMirror.delete(processGroupId);
        } else {
          supervisorMirror.set(processGroupId, startedGroup);
        }
      }),
    RunInTerminal: () => Effect.die("the test supervisor runs no terminal commands"),
    AwaitTerminalCommandExit: () => Effect.void,
    ClaimUnknownOwnership: ({ processes }) => Effect.succeed(processes),
    SweepBeforeExit: () => Effect.succeed([]),
  });
  const supervisor = yield* RpcTest.makeClient(SupervisorRpcs).pipe(
    Effect.provideContext(handlers),
  );
  yield* Deferred.succeed(supervisorConnectionDeferred, supervisor);
});

export const supervisorKeepsLeaderExitRecordedDuringKill = makeTest(
  "the supervisor keeps the leader exit the run records while a kill of the task looks at the group, and drops the group once a later kill ends it",
  Effect.scoped(
    Effect.gen(function* () {
      yield* actAsTheSupervisor;
      const group = yield* startGroupWhoseLeaderExited;
      const leaderStartTime = yield* readStartTimeOfThisProcess;
      const listedMember = yield* Effect.fromNullishOr(
        (yield* listProcesses(null)).get(group.member),
      );
      const runConfig = { ownsSession: true, runMarker: null, runProcessId: process.pid };
      const processGroups = makeProcessGroupsListingWith(runConfig, listProcesses);
      const processGroupsRacingALeaderExit = makeProcessGroupsListingWith(runConfig, (taskName) =>
        processGroups
          .recordLeaderExit(group.processGroupId, "leaver", [
            { processId: group.member, startTime: listedMember.startTime },
          ])
          .pipe(Effect.andThen(listProcesses(taskName))),
      );
      yield* processGroups.record(group.processGroupId, "leaver", leaderStartTime);
      yield* processGroupsRacingALeaderExit.killForTask("leaver");
      const tracked = yield* Effect.fromNullishOr(
        processGroupsSharedWithTheSupervisor.get(group.processGroupId),
      );
      expect(tracked.membersAtLeaderExit).toEqual([
        { processId: group.member, startTime: listedMember.startTime },
      ]);
      expect(supervisorMirror.get(group.processGroupId)).toEqual(tracked);
      yield* processGroups.killForTask("leaver");
      yield* waitUntilProcessEnds(group.member);
      expect(processGroupsSharedWithTheSupervisor.has(group.processGroupId)).toBe(false);
      expect(supervisorMirror.has(group.processGroupId)).toBe(false);
    }),
  ),
);

export const reusedProcessId = makeTest(
  "a process at a PID the process list showed for an earlier process task started is never signaled, by PID or by group",
  Effect.scoped(
    Effect.gen(function* () {
      const sleep = yield* ChildProcess.make("sleep", ["1000"]);
      yield* killTestProcessWhenDone(sleep.pid);
      const earlierStartTime = yield* readStartTimeOfThisProcess;
      const { claimProcessesToReport } = makeProcessReportClaimer();
      yield* killTaskProcessGroups(
        new Map([[sleep.pid, startedProcessGroup(REGISTRATION, "leaver", earlierStartTime, null)]]),
        "leaver",
        claimProcessesToReport,
        null,
        listedAsAnEarlierProcess(sleep.pid, earlierStartTime),
      );
      expect(yield* isProcessAlive(sleep.pid)).toBe(true);
    }),
  ),
);

export const reusedRunProcessId = makeTest(
  "the supervisor never signals a process at a PID its process list showed for an earlier process or process group of the run",
  Effect.scoped(
    Effect.gen(function* () {
      const sleep = yield* ChildProcess.make("sleep", ["1000"]);
      yield* killTestProcessWhenDone(sleep.pid);
      const listedSleep = yield* Effect.fromNullishOr((yield* listProcesses(null)).get(sleep.pid));
      expect(listedSleep.sessionId).toBe(sleep.pid);
      const earlierStartTime = yield* readStartTimeOfThisProcess;
      yield* signalRunProcesses(
        {
          sessionId: listedSleep.sessionId,
          descendantsOfProcessId: null,
          startedProcessGroups: new Map([
            [sleep.pid, startedProcessGroup(REGISTRATION, "leaver", earlierStartTime, null)],
          ]),
          runMarker: null,
          sparedProcessId: null,
        },
        "SIGKILL",
        listedAsAnEarlierProcess(sleep.pid, earlierStartTime),
      );
      expect(yield* isProcessAlive(sleep.pid)).toBe(true);
    }),
  ),
);

export const markedChildAfterRecordedMembersEnded = makeTest(
  "a process a task's command group gains after its leader exits is killed and reported when that task ends, even after every member task saw at the leader's exit has ended",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { Effect, Schedule } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
const hasEnded = (processId: number) => Effect.try(() => process.kill(processId, 0)).pipe(Effect.match({ onFailure: () => true, onSuccess: () => false }));
export const leaver = Task.make({ name: "leaver", run: Effect.gen(function* () {
  const leader = yield* ChildProcess.make("sh", ["-c", "(trap '' TERM; until [ -e go ]; do sleep 0.05; done; ${process.execPath} -e 'setInterval(() => {}, 1000)' > /dev/null 2>&1 & echo $! > child-pid.part; mv child-pid.part child-pid) > /dev/null 2>&1 & echo $! > member-pid"]);
  yield* leader.exitCode;
  const member = Number(readFileSync("member-pid", "utf8").trim());
  writeFileSync("go", "");
  yield* Effect.suspend(() => (existsSync("child-pid") ? hasEnded(member) : Effect.succeed(false))).pipe(Effect.repeat({ until: (done) => done, schedule: Schedule.spaced("50 millis") }));
}) });
export default Task.make({ name: "main", run: Effect.andThen(leaver, Effect.log("after leaver")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const child = Number((yield* read(`${project.directory}/child-pid`)).trim());
      yield* killTestProcessWhenDone(child);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: child, command: "bun", by: "[leaver]" },
      ]);
      expect(ran.standardError.indexOf(`killed: [leaver] ${child} `)).toBeLessThan(
        ran.standardError.indexOf("[main] after leaver"),
      );
      expect(yield* isProcessAlive(child)).toBe(false);
    }),
  ),
);

const SPAWN_UNMARKED_MEMBER = `const member = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1000)"], { env: {}, stdio: ["ignore", "ignore", "ignore"] });
console.log(member.pid);
setInterval(() => {}, 1000);`;

export const markerOfAnEarlierProcess = makeTest(
  "the supervisor never signals a process group whose only marked member is listed with the identity of an earlier process at its PID, so the group's other members stay running",
  Effect.scoped(
    Effect.gen(function* () {
      const runMarker = yield* Effect.fromNullishOr(yield* createRunMarker([]));
      const leader = yield* ChildProcess.make(
        "env",
        [
          `${RUN_MARKERS_VARIABLE_NAME}=${markCommandOfTask(runMarker, "leaver")}`,
          process.execPath,
          "-e",
          SPAWN_UNMARKED_MEMBER,
        ],
        { stdout: "pipe", stderr: "ignore" },
      );
      yield* killTestProcessWhenDone(leader.pid);
      const member = Number(
        yield* Effect.fromOption(
          yield* leader.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead),
        ),
      );
      yield* killTestProcessWhenDone(member);
      const earlierStartTime = yield* readStartTimeOfThisProcess;
      yield* signalRunProcesses(
        {
          sessionId: -1,
          descendantsOfProcessId: null,
          startedProcessGroups: new Map([
            [leader.pid, startedProcessGroup(REGISTRATION, "leaver", earlierStartTime, [])],
          ]),
          runMarker,
          sparedProcessId: null,
        },
        "SIGKILL",
        listedAsAnEarlierProcess(leader.pid, earlierStartTime),
      );
      expect(yield* isProcessAlive(member)).toBe(true);
    }),
  ),
);
