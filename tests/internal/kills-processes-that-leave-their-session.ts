import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Schedule } from "effect";

import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";
import { exists, read } from "../shared/access-files.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive, waitUntilProcessEnds } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath } from "../shared/name-output-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const ESCAPED_PROCESS_ID_FILE = "escaped-pid";

const FAILED_COMMAND_EXIT_CODE = 3;

const WRITE_PROCESS_ID = `open my $file, ">", "${ESCAPED_PROCESS_ID_FILE}"; print $file $$; close $file;`;

const CLOSE_INHERITED_DESCRIPTORS = "POSIX::close($_) for 3..255;";

const DESCRIPTORS_BELOW_THE_MARKER_PIPE = 5000;

const MOVE_THE_MARKER_PIPE_ABOVE_OTHER_DESCRIPTORS = `POSIX::dup2(3, ${DESCRIPTORS_BELOW_THE_MARKER_PIPE}) or die; my $null = POSIX::open("/dev/null", POSIX::O_RDONLY()); POSIX::dup2($null, $_) for 3..${DESCRIPTORS_BELOW_THE_MARKER_PIPE - 1};`;

const EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP = "if (my $child = fork) { waitpid $child, 0; exit }";

const ESCAPES = {
  ownGroup: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} setpgrp(0, 0); if (fork) { exit } ${WRITE_PROCESS_ID} exec "${process.execPath}", "-e", "setInterval(() => {}, 1000)"`,
  newSession: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); if (fork) { exit } ${WRITE_PROCESS_ID} exec "${process.execPath}", "-e", "setInterval(() => {}, 1000)"`,
  newSessionEmptyEnvironment: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
  newSessionClosedDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); ${CLOSE_INHERITED_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "${process.execPath}", "-e", "setInterval(() => {}, 1000)"`,
  ownGroupEmptyEnvironmentClosedDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} setpgrp(0, 0); ${CLOSE_INHERITED_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
  newSessionEmptyEnvironmentClosedDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); ${CLOSE_INHERITED_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
  newSessionEmptyEnvironmentMarkerPipeAfterManyDescriptors: `${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); ${MOVE_THE_MARKER_PIPE_ABOVE_OTHER_DESCRIPTORS} if (fork) { exit } ${WRITE_PROCESS_ID} exec "/usr/bin/env", "-i", "/bin/sleep", "1000"`,
};

const ESCAPED_PROGRAM_NAMES = {
  ownGroup: "bun",
  newSession: "bun",
  newSessionEmptyEnvironment: "sleep",
  newSessionClosedDescriptors: "bun",
  ownGroupEmptyEnvironmentClosedDescriptors: "sleep",
  newSessionEmptyEnvironmentClosedDescriptors: "sleep",
  newSessionEmptyEnvironmentMarkerPipeAfterManyDescriptors: "sleep",
} as const;

const ESCAPED_OWNERS = {
  ownGroup: "[main]",
  newSession: "[main]",
  newSessionEmptyEnvironment: "no known task",
  newSessionClosedDescriptors: "[main]",
  ownGroupEmptyEnvironmentClosedDescriptors: "no known task",
  newSessionEmptyEnvironmentClosedDescriptors: "no known task",
  newSessionEmptyEnvironmentMarkerPipeAfterManyDescriptors: "no known task",
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
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.exec("perl", ["-MPOSIX", "-e", \`open STDOUT, ">/dev/null"; open STDERR, ">/dev/null"; ${ESCAPES[escape]}\`]);
  yield* Task.exec("sh", ["-c", "until [ -s ${ESCAPED_PROCESS_ID_FILE} ] && [ \\"$(ps -o comm= -p $(cat ${ESCAPED_PROCESS_ID_FILE}))\\" != perl ]; do sleep 0.05; done"]);
  ${ROOT_ENDINGS[ending]}
}) });`;

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

const LINE_OF_THE_ROOT_TASK_OR_OF_TASK =
  /^(?:\[main\] |(?:started|ended|error|recap|warning|serving|closed|stopping|killed|passed|failed|stopped|run): )/u;

const expectKilledBeforeTheSummaryAndTheRunLineLast = (
  standardError: string,
  processId: number,
) => {
  const lines = standardError.split("\n");
  const killedLine = lines.findIndex(
    (line) => line.startsWith("killed: ") && line.includes(` ${processId} (`),
  );
  const summaryLine = lines.findIndex((line) => /^(?:passed|failed): /u.test(line));
  expect(
    lines.filter((line) => !LINE_OF_THE_ROOT_TASK_OR_OF_TASK.test(line)),
    standardError,
  ).toEqual([]);
  expect(killedLine, standardError).toBeGreaterThan(-1);
  expect(killedLine, standardError).toBeLessThan(summaryLine);
  expect(lines.slice(summaryLine + 1), standardError).toEqual([
    `run: ${findRunDirectoryPath(standardError)}/`,
  ]);
};

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
        by: ESCAPED_OWNERS[escape],
      });
      expectKilledBeforeTheSummaryAndTheRunLineLast(ran.standardError, escaped);
      yield* waitUntilProcessEnds(escaped);
    }),
  );

export const ownGroup = makeTest(
  "a process a command double-forks into its own process group is killed and reported when the run ends",
  killedWhenTheRunEnds("ownGroup"),
);

export const newSession = makeTest(
  "a process a command double-forks into a new session is killed and reported when the run ends",
  killedWhenTheRunEnds("newSession"),
);

export const newSessionEmptyEnvironment = makeTest(
  "a process a command double-forks into a new session and starts with an empty environment is killed and reported when the run ends",
  killedWhenTheRunEnds("newSessionEmptyEnvironment"),
);

export const newSessionClosedDescriptors = makeTest(
  "a process a command double-forks into a new session after closing every inherited file descriptor is killed and reported when the run ends",
  killedWhenTheRunEnds("newSessionClosedDescriptors"),
);

export const markerPipeAfterManyDescriptors = makeTest(
  "a process a command double-forks into a new session and starts with an empty environment, holding the run's marker pipe after more than 4096 other file descriptors, is killed and reported when the run ends",
  killedWhenTheRunEnds("newSessionEmptyEnvironmentMarkerPipeAfterManyDescriptors"),
);

export const failedRun = makeTest(
  "a process a command double-forks into a new session is killed and reported when the run fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": escapingRoot("newSession", "fails") });
      const ran = yield* runTaskProcess(project, "main.ts");
      const escaped = yield* readEscapedProcessId(project.directory);
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(findKilledProcesses(ran.standardError)).toContainEqual({
        processId: escaped,
        command: ESCAPED_PROGRAM_NAMES.newSession,
        by: ESCAPED_OWNERS.newSession,
      });
      expectKilledBeforeTheSummaryAndTheRunLineLast(ran.standardError, escaped);
      yield* waitUntilProcessEnds(escaped);
    }),
  ),
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
      yield* waitUntilProcessEnds(escaped);
      const standardError = yield* command.waitForStandardErrorToClose;
      expect(
        standardError
          .split("\n")
          .filter((line) => line !== "" && !LINE_OF_THE_ROOT_TASK_OR_OF_TASK.test(line)),
        standardError,
      ).toEqual([]);
    }),
  );

export const interruptedRun = makeTest(
  "a process a command double-forks into a new session is killed when the run is stopped with SIGINT",
  killedWhenStopped("INT"),
);

export const runProcessKilled = makeTest(
  "a process a command double-forks into a new session is killed by the supervisor when the run's process gets SIGKILL",
  killedWhenStopped("the run's process"),
);

export const supervisorKilled = makeTest(
  "a process a command double-forks into a new session is killed by the run's process when the supervisor gets SIGKILL, and every line printed follows the line format",
  killedWhenStopped("KILL"),
);

const WRITING_ITS_PROCESS_ID = `import { renameSync, writeFileSync } from "node:fs";
writeFileSync("${ESCAPED_PROCESS_ID_FILE}.part", String(process.pid));
renameSync("${ESCAPED_PROCESS_ID_FILE}.part", "${ESCAPED_PROCESS_ID_FILE}");
setInterval(() => {}, 1000);`;

export const leftByTerminalCommand = makeTest(
  "a process a terminal command leaves running in the background is killed and reported as started by the root task when the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "left-behind.ts": WRITING_ITS_PROCESS_ID,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.exec("sh", ["-c", '"$0" left-behind.ts >/dev/null 2>&1 &', "${process.execPath}"], { stdio: "inherit" });
  yield* Task.exec("sh", ["-c", "until [ -s ${ESCAPED_PROCESS_ID_FILE} ]; do sleep 0.05; done"]);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const escaped = yield* readEscapedProcessId(project.directory);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: escaped, command: "bun", by: "[main]" },
      ]);
      yield* waitUntilProcessEnds(escaped);
    }),
  ),
);

export const terminalCommandAfterSupervisorKilled = makeTest(
  "a terminal command in its own process group is killed by the run's process when the supervisor gets SIGKILL",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "holds-the-terminal.ts": WRITING_ITS_PROCESS_ID,
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Task.exec(
    "perl",
    ["-e", "setpgrp(0, 0); exec @ARGV", "${process.execPath}", "holds-the-terminal.ts"],
    { stdio: "inherit" },
  ),
});`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* exists(`${project.directory}/${ESCAPED_PROCESS_ID_FILE}`).pipe(
        Effect.repeat({ until: (written) => written, schedule: Schedule.spaced("50 millis") }),
        Effect.timeout("10 seconds"),
      );
      const terminalCommand = yield* readEscapedProcessId(project.directory);
      yield* command.sendSignal("KILL");
      yield* waitUntilProcessEnds(terminalCommand);
    }),
  ),
);

const WRITING_WITHOUT_END = `import { writeSync } from "node:fs";
setInterval(() => {
  try {
    writeSync(1, "still here\\n");
  } catch {}
}, 100);`;

const CLEANUP_LIMIT_MILLISECONDS = 1000;

const SLOWEST_RETURN_MILLISECONDS = 5000;

export const writingWithoutEnd = makeTest(
  "a command returns once its task's cleanup limit has passed since it exited, although a process it left in another session keeps writing to its output, and that process is killed and reported when the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "writes-without-end.ts": WRITING_WITHOUT_END,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  cleanup: ${CLEANUP_LIMIT_MILLISECONDS},
  run: Effect.gen(function* () {
    const startedAt = Date.now();
    yield* Task.exec("perl", ["-MPOSIX", "-e", \`${EXIT_ONCE_THE_CHILD_LEFT_THE_GROUP} POSIX::setsid(); if (fork) { exit } ${WRITE_PROCESS_ID} exec "${process.execPath}", "writes-without-end.ts"\`], { print: true });
    yield* Effect.log(\`returned after \${Date.now() - startedAt}\`);
  }),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const escaped = yield* readEscapedProcessId(project.directory);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] still here");
      const returnedAfter = Number(
        /returned after (?<milliseconds>\d+)/u.exec(ran.standardError)?.[1],
      );
      expect(returnedAfter).toBeGreaterThanOrEqual(CLEANUP_LIMIT_MILLISECONDS);
      expect(returnedAfter).toBeLessThan(SLOWEST_RETURN_MILLISECONDS);
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: escaped, command: "bun", by: "[main]" },
      ]);
      yield* waitUntilProcessEnds(escaped);
    }),
  ),
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
      if (isAdopted) {
        yield* waitUntilProcessEnds(escaped);
      } else {
        expect(yield* isProcessAlive(escaped)).toBe(true);
      }
    }),
  );

export const unmarked = makeTest(
  "a process a command double-forks into a new session, with an empty environment and every inherited file descriptor closed, is killed and reported when the run ends on Linux, which adopts it, and is left running on macOS when its program is part of macOS, as nothing marks it as the run's",
  leftRunningOnMacOs("newSessionEmptyEnvironmentClosedDescriptors"),
);

export const unmarkedInOwnGroup = makeTest(
  "a process a command double-forks into its own process group, with an empty environment and every inherited file descriptor closed, is killed and reported when the run ends on Linux, which adopts it, and is left running on macOS when its program is part of macOS, as task does not track the sessions of commands",
  leftRunningOnMacOs("ownGroupEmptyEnvironmentClosedDescriptors"),
);

export const markerKeptPrivate = makeTest(
  "the marker a command gets reaches neither the run's output nor its state directory, even when the command fails",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo $SSHKEDA_TASK_RUN_MARKERS > marker; exit 1"]) });`,
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
      expect(stateFiles.some((filePath) => filePath.endsWith("/trace.jsonl"))).toBe(true);
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
);
