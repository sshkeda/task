import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Schedule } from "effect";

import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";
import { read } from "../shared/access-files.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const ESCAPED_PROCESS_ID_FILE = "escaped-pid";

const FAILED_COMMAND_EXIT_CODE = 3;

const WRITE_PROCESS_ID = `open my $file, ">", "${ESCAPED_PROCESS_ID_FILE}"; print $file $$; close $file;`;

const CLOSE_INHERITED_DESCRIPTORS = "POSIX::close($_) for 3..255;";

const EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP = "if (my $child = fork) { waitpid $child, 0; exit }";

const ESCAPES = {
  ownGroup: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} setpgrp(0, 0); if (fork) { exit } ${WRITE_PROCESS_ID} exec "sleep", "1000"`,
  newSession: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); if (fork) { exit } ${WRITE_PROCESS_ID} exec "sleep", "1000"`,
  newSessionEmptyEnvironment: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
  newSessionClosedDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); ${CLOSE_INHERITED_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "${process.execPath}", "-e", "setInterval(() => {}, 1000)"`,
  ownGroupEmptyEnvironmentClosedDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} setpgrp(0, 0); ${CLOSE_INHERITED_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
  newSessionEmptyEnvironmentClosedDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); ${CLOSE_INHERITED_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
};

const ESCAPED_PROGRAM_NAMES = {
  ownGroup: "sleep",
  newSession: "sleep",
  newSessionEmptyEnvironment: "sleep",
  newSessionClosedDescriptors: "bun",
  ownGroupEmptyEnvironmentClosedDescriptors: "sleep",
  newSessionEmptyEnvironmentClosedDescriptors: "sleep",
} as const;

const ROOT_ENDINGS = {
  passes: "",
  fails: `yield* Task.exec("sh", ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"]);`,
  waits: `yield* Effect.log(\`run \${process.pid}\`);
  yield* Task.exec("sleep", ["1000"]);`,
} as const;

const escapingRoot = (
  escape: keyof typeof ESCAPES,
  ending: keyof typeof ROOT_ENDINGS,
) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Task.exec("perl", ["-MPOSIX", "-e", \`open STDOUT, ">/dev/null"; open STDERR, ">/dev/null"; ${ESCAPES[escape]}\`]);
  yield* Task.exec("sh", ["-c", "until [ -s ${ESCAPED_PROCESS_ID_FILE} ] && [ \\"$(ps -o comm= -p $(cat ${ESCAPED_PROCESS_ID_FILE}))\\" != perl ]; do sleep 0.05; done"]);
  ${ROOT_ENDINGS[ending]}
}));
if (import.meta.main) Task.main(main);`;

const readEscapedProcessId = (directory: string) =>
  Effect.gen(function* () {
    const processId = Number((yield* read(`${directory}/${ESCAPED_PROCESS_ID_FILE}`)).trim());
    yield* Effect.addFinalizer(() =>
      isProcessAlive(processId).pipe(
        Effect.flatMap((alive) =>
          alive ? Task.exec("kill", ["-KILL", String(processId)]) : Effect.void,
        ),
        failTestCleanup(`kill escaped test process ${processId}`),
      ),
    );
    return processId;
  });

const waitUntilEnded = (processId: number) =>
  isProcessAlive(processId).pipe(
    Effect.repeat({ until: (alive) => !alive, schedule: Schedule.spaced("50 millis") }),
    Effect.timeout("10 seconds"),
  );

const killedWhenTheRunEnds = (escape: keyof typeof ESCAPES) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": escapingRoot(escape, "passes") });
      const ran = yield* runTaskProcess(project, "main.ts");
      const escaped = yield* readEscapedProcessId(project.directory);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toContainEqual({
        processId: escaped,
        command: ESCAPED_PROGRAM_NAMES[escape],
        by: "no known task",
      });
      expect(yield* isProcessAlive(escaped)).toBe(false);
    }),
  );

export const ownGroup = Task.make(
  "a process a command double-forks into its own process group is killed and reported when the run ends",
  killedWhenTheRunEnds("ownGroup"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const newSession = Task.make(
  "a process a command double-forks into a new session is killed and reported when the run ends",
  killedWhenTheRunEnds("newSession"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const newSessionEmptyEnvironment = Task.make(
  "a process a command double-forks into a new session and starts with an empty environment is killed and reported when the run ends",
  killedWhenTheRunEnds("newSessionEmptyEnvironment"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const newSessionClosedDescriptors = Task.make(
  "a process a command double-forks into a new session after closing every inherited file descriptor is killed and reported when the run ends",
  killedWhenTheRunEnds("newSessionClosedDescriptors"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failedRun = Task.make(
  "a process a command double-forks into a new session is killed and reported when the run fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": escapingRoot("newSession", "fails") });
      const ran = yield* runTaskProcess(project, "main.ts");
      const escaped = yield* readEscapedProcessId(project.directory);
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(findKilledProcesses(ran.standardError)).toContainEqual({
        processId: escaped,
        command: "sleep",
        by: "no known task",
      });
      expect(yield* isProcessAlive(escaped)).toBe(false);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const killedWhenStopped = (stop: "INT" | "KILL" | "the run's process") =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": escapingRoot("newSession", "waits") });
      const command = yield* startTaskProcess(project, "main.ts");
      const runProcess = Number((yield* command.waitForLine("[main] run ")).split(" ").at(-1));
      const escaped = yield* readEscapedProcessId(project.directory);
      yield* stop === "the run's process"
        ? Effect.sync(() => process.kill(runProcess, "SIGKILL"))
        : command.sendSignal(stop);
      yield* waitUntilEnded(escaped);
    }),
  );

export const interruptedRun = Task.make(
  "a process a command double-forks into a new session is killed when the run is stopped with SIGINT",
  killedWhenStopped("INT"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const runProcessKilled = Task.make(
  "a process a command double-forks into a new session is killed by the supervisor when the run's process gets SIGKILL",
  killedWhenStopped("the run's process"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const supervisorKilled = Task.make(
  "a process a command double-forks into a new session is killed by the run's process when the supervisor gets SIGKILL",
  killedWhenStopped("KILL"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const leftRunningOnMacOs = (escape: keyof typeof ESCAPES) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": escapingRoot(escape, "passes") });
      const ran = yield* runTaskProcess(project, "main.ts");
      const escaped = yield* readEscapedProcessId(project.directory);
      const isAdopted = process.platform === "linux";
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toEqual(
        isAdopted ? [{ processId: escaped, command: "sleep", by: "no known task" }] : [],
      );
      expect(yield* isProcessAlive(escaped)).toBe(!isAdopted);
    }),
  );

export const unmarked = Task.make(
  "a process a command double-forks into a new session, with an empty environment and every inherited file descriptor closed, is killed and reported when the run ends on Linux, which adopts it, and is left running on macOS when its program is part of macOS, as nothing marks it as the run's",
  leftRunningOnMacOs("newSessionEmptyEnvironmentClosedDescriptors"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unmarkedInOwnGroup = Task.make(
  "a process a command double-forks into its own process group, with an empty environment and every inherited file descriptor closed, is killed and reported when the run ends on Linux, which adopts it, and is left running on macOS when its program is part of macOS, as task does not track the sessions of commands",
  leftRunningOnMacOs("ownGroupEmptyEnvironmentClosedDescriptors"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const markerKeptPrivate = Task.make(
  "the marker a command gets reaches neither the run's output nor its state directory, even when the command fails",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "echo $SSHKEDA_TASK_RUN_MARKERS > marker; exit 1"]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const markers = (yield* read(`${project.directory}/marker`)).trim().split(" ");
      expect(markers.every((marker) => marker !== "")).toBe(true);
      const stateDirectory = `${project.home}/${TASK_STATE_DIRECTORY_NAME}`;
      const stateFiles = yield* Effect.filter(
        (yield* fileSystem.readDirectory(stateDirectory, { recursive: true })).map(
          (entry) => `${stateDirectory}/${entry}`,
        ),
        (filePath) =>
          Effect.map(fileSystem.stat(filePath), (information) => information.type === "File"),
      );
      expect(stateFiles.some((filePath) => filePath.endsWith(".jsonl"))).toBe(true);
      const filesHoldingMarker = yield* Effect.filter(stateFiles, (filePath) =>
        Effect.map(fileSystem.readFile(filePath), (bytes) =>
          markers.some((marker) => Buffer.from(bytes).includes(marker)),
        ),
      );
      expect({
        output: markers.some(
          (marker) => ran.standardOutput.includes(marker) || ran.standardError.includes(marker),
        ),
        filesHoldingMarker,
      }).toEqual({ output: false, filesHoldingMarker: [] });
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
