import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Schedule, Schema } from "effect";
import { ChildProcess } from "effect/process";

import {
  killTaskProcessGroups,
  signalRunProcesses,
  startedProcessGroup,
} from "../../src/lib/kill-leftover-processes/index.ts";
import { listProcesses } from "../../src/lib/kill-leftover-processes/internal/list-processes.ts";
import { makeProcessReportClaimer } from "../../src/shared/claim-processes-to-report.ts";
import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";
import { read, write } from "../shared/access-files.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const SHELL_AND_SLEEP_PROCESS_COUNT = 2;

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
      expect(ran.standardError).toContain("[hang] error TaskTimedOut: timed out after 5s");
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
        "sh by no known task",
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
        "sh by no known task",
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
        command: "sh",
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
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
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
          return { processId: sleep, command: "sleep", by: `"${name}"` };
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
      expect(ran.standardError).toContain(`killed ${perl} (perl), started by "main"`);
      expect(yield* isProcessAlive(perl)).toBe(false);
      const stateDirectory = `${project.home}/${TASK_STATE_DIRECTORY_NAME}`;
      const stateFiles = yield* Effect.filter(
        (yield* fileSystem.readDirectory(stateDirectory, { recursive: true })).map(
          (entry) => `${stateDirectory}/${entry}`,
        ),
        (filePath) =>
          Effect.map(fileSystem.stat(filePath), (information) => information.type === "File"),
      );
      expect(stateFiles.some((filePath) => filePath.endsWith(".jsonl"))).toBe(true);
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
        { processId: sleep, command: "sleep", by: '"leaver"' },
      ]);
      expect(ran.standardError.indexOf(`killed ${sleep} `)).toBeLessThan(
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
        { processId: Number(job), command: "bun", by: '"main"' },
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
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
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
      for (const latestMemberStartTimeAtLeaderExit of [null, earlierStartTime]) {
        yield* killTaskProcessGroups(
          new Map([
            [
              unrelated.processGroupId,
              startedProcessGroup("leaver", earlierStartTime, latestMemberStartTimeAtLeaderExit),
            ],
          ]),
          "leaver",
          claimProcessesToReport,
        );
        expect(yield* isProcessAlive(unrelated.member)).toBe(true);
      }
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
        new Map([[sleep.pid, startedProcessGroup("leaver", earlierStartTime, null)]]),
        "leaver",
        claimProcessesToReport,
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
            [sleep.pid, startedProcessGroup("leaver", earlierStartTime, null)],
          ]),
          runMarker: null,
        },
        "SIGKILL",
        listedAsAnEarlierProcess(sleep.pid, earlierStartTime),
      );
      expect(yield* isProcessAlive(sleep.pid)).toBe(true);
    }),
  ),
);
