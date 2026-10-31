import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Exit, FileSystem, Stream } from "effect";
import { ChildProcess } from "effect/process";

import { listProcessesFromProcDirectory } from "../../src/lib/kill-leftover-processes/internal/list-linux-processes-from-proc.ts";
import { listProcesses } from "../../src/lib/kill-leftover-processes/internal/list-processes.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue } from "../shared/failure-as-value.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { makeTaskProcessEnvironment } from "../shared/run-task-processes.ts";

const FIXTURE_PROCESS_ID = 300;

const readProcessGroupIdWithPs = (processId: number) =>
  Task.exec("ps", ["-o", "pgid=", "-p", String(processId)]).pipe(
    Effect.map((text) => Number(text)),
  );

const readFirstLine = <Failure>(standardOutput: Stream.Stream<Uint8Array, Failure>) =>
  standardOutput.pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.runHead,
    Effect.flatMap(Effect.fromOption),
  );

const killWhenDone = (processId: number) =>
  Effect.addFinalizer(() =>
    Task.exec("kill", ["-KILL", String(processId)]).pipe(
      failTestCleanup(`kill test process ${processId}`),
    ),
  );

export const ownProcess = Task.make(
  "the process list shows this process running, with its parent process id, the process group ps reports and the name of the bun running it",
  Effect.gen(function* () {
    const ownListedProcess = yield* Effect.fromNullishOr(
      (yield* listProcesses(null)).get(process.pid),
    );
    expect(ownListedProcess.hasEnded).toBe(false);
    expect(ownListedProcess.parentProcessId).toBe(process.ppid);
    expect(ownListedProcess.processGroupId).toBe(yield* readProcessGroupIdWithPs(process.pid));
    expect(yield* ownListedProcess.readCommandLineUnlessGone).toBe(
      process.execPath.slice(process.execPath.lastIndexOf("/") + 1),
    );
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const ownProcessUnderBunPathWithSpace = Task.make(
  "the process list test of this process passes when the bun running it is in a directory whose name has a space",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repository = yield* REPOSITORY_DIRECTORY;
      const bunDirectory = `${yield* makeScratchDirectory}/Bun Runtimes`;
      yield* fileSystem.makeDirectory(bunDirectory);
      const bunPath = `${bunDirectory}/bun`;
      yield* fileSystem
        .link(process.execPath, bunPath)
        .pipe(
          Effect.catchTag("PlatformError", () => fileSystem.copyFile(process.execPath, bunPath)),
        );
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
import { ownProcess } from "${repository}/tests/internal/lists-processes.ts";
export const main = Task.make("main", ownProcess);
if (import.meta.main) Task.main(main);`,
      });
      const child = yield* ChildProcess.make(bunPath, [`${project.directory}/main.ts`], {
        cwd: project.directory,
        env: yield* makeTaskProcessEnvironment(project),
        extendEnv: false,
        stdout: "ignore",
        stderr: "pipe",
      });
      const standardError = yield* child.stderr.pipe(Stream.decodeText, Stream.mkString);
      expect(Number(yield* child.exitCode), standardError).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const spawnedTree = Task.make(
  "the process list shows a started sh and the perl it starts with their parent process ids, process group and program names",
  Effect.scoped(
    Effect.gen(function* () {
      const shell = yield* ChildProcess.make("sh", [
        "-c",
        `perl -e '$| = 1; print "$$\\n"; sleep 1000' & wait`,
      ]);
      const perlProcessId = Number(yield* readFirstLine(shell.stdout));
      yield* killWhenDone(perlProcessId);
      const listed = yield* listProcesses(null);
      const listedShell = yield* Effect.fromNullishOr(listed.get(shell.pid));
      const listedPerl = yield* Effect.fromNullishOr(listed.get(perlProcessId));
      const shellProcessGroupId = yield* readProcessGroupIdWithPs(shell.pid);
      expect(listedShell.parentProcessId).toBe(process.pid);
      expect(listedPerl.parentProcessId).toBe(shell.pid);
      expect(listedShell.processGroupId).toBe(shellProcessGroupId);
      expect(listedPerl.processGroupId).toBe(shellProcessGroupId);
      expect(listedShell.hasEnded).toBe(false);
      expect(listedPerl.hasEnded).toBe(false);
      expect(yield* listedShell.readCommandLineUnlessGone).toBe("sh");
      expect(yield* listedPerl.readCommandLineUnlessGone).toBe("perl");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const zombie = Task.make(
  "the process list shows a child its parent has not reaped as an ended child of that parent, whose program name reads as gone",
  Effect.scoped(
    Effect.gen(function* () {
      const parent = yield* ChildProcess.make("perl", [
        "-MPOSIX",
        "-e",
        `sigprocmask(SIG_BLOCK, POSIX::SigSet->new(SIGCHLD));
$SIG{CHLD} = sub {};
my $child = fork;
POSIX::_exit(0) if $child == 0;
sigsuspend(POSIX::SigSet->new);
$| = 1;
print "$child\\n";
sleep;`,
      ]);
      const zombieProcessId = Number(yield* readFirstLine(parent.stdout));
      const listedZombie = yield* Effect.fromNullishOr(
        (yield* listProcesses(null)).get(zombieProcessId),
      );
      expect(listedZombie.parentProcessId).toBe(parent.pid);
      expect(listedZombie.hasEnded).toBe(true);
      expect(yield* listedZombie.readCommandLineUnlessGone).toBeNull();
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const emptyFirstArgument = Task.make(
  "a process started with an empty argv[0] is named by its executable, not by its arguments or its environment",
  Effect.scoped(
    Effect.gen(function* () {
      const parent = yield* ChildProcess.make(
        "perl",
        [
          "-e",
          `pipe(my $execed, my $writer) or die;
my $child = fork;
if ($child == 0) { close $execed; exec { "/bin/sleep" } "", "1000"; exit 1 }
close $writer;
my $unused = <$execed>;
$| = 1;
print "$child\\n";
sleep;`,
        ],
        { env: { SECRET_TOKEN: "hunter2" }, extendEnv: true },
      );
      const sleepProcessId = Number(yield* readFirstLine(parent.stdout));
      yield* killWhenDone(sleepProcessId);
      const listedSleep = yield* Effect.fromNullishOr(
        (yield* listProcesses(null)).get(sleepProcessId),
      );
      expect(yield* listedSleep.readCommandLineUnlessGone).toBe("sleep");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const namedByFirstArgument = Task.make(
  "a process is named by the base name of its argv[0], without its arguments",
  Effect.scoped(
    Effect.gen(function* () {
      const perl = yield* ChildProcess.make("perl", [
        "-e",
        `$| = 1; print "$$\\n"; sleep 1000`,
        "\uFEFFleading",
        "inside\uFEFFword",
      ]);
      const perlProcessId = Number(yield* readFirstLine(perl.stdout));
      yield* killWhenDone(perlProcessId);
      const listedPerl = yield* Effect.fromNullishOr(
        (yield* listProcesses(null)).get(perlProcessId),
      );
      expect(yield* listedPerl.readCommandLineUnlessGone).toBe("perl");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const exitedAfterListing = Task.make(
  "a process that exits after the process list shows it reads as gone instead of failing",
  Effect.scoped(
    Effect.gen(function* () {
      const sleep = yield* ChildProcess.make("sleep", ["1000"]);
      const listed = yield* listProcesses(null);
      const listedSleep = yield* Effect.fromNullishOr(listed.get(sleep.pid));
      yield* sleep.kill({ killSignal: "SIGKILL" });
      expect(Exit.isFailure(yield* exitAsValue(sleep.exitCode))).toBe(true);
      expect(yield* listedSleep.readCommandLineUnlessGone).toBeNull();
      expect((yield* listProcesses(null)).has(sleep.pid)).toBe(false);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const linuxByteOrderMarks = Task.make(
  "the Linux process list names a process by its argv[0] with one U+FEFF dropped from its start, and without its other arguments",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const fixtures = yield* makeScratchDirectory;
      yield* fileSystem.writeFileString(
        `${fixtures}/stat`,
        `${FIXTURE_PROCESS_ID} (perl) S 1 ${FIXTURE_PROCESS_ID} ${FIXTURE_PROCESS_ID} 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 98765 1000 10\n`,
      );
      yield* fileSystem.writeFileString(
        `${fixtures}/cmdline`,
        "\uFEFF\uFEFFperl\0\uFEFF\uFEFFsecond\0",
      );
      const fixturePathOf = (filePath: string) =>
        filePath.replace(`/proc/${FIXTURE_PROCESS_ID}`, fixtures);
      const listed = yield* listProcessesFromProcDirectory(null).pipe(
        Effect.provideService(
          FileSystem.FileSystem,
          FileSystem.makeNoop({
            readDirectory: () => Effect.succeed([String(FIXTURE_PROCESS_ID)]),
            readFile: (filePath) => fileSystem.readFile(fixturePathOf(filePath)),
            readFileString: (filePath) => fileSystem.readFileString(fixturePathOf(filePath)),
          }),
        ),
      );
      const listedPerl = yield* Effect.fromNullishOr(listed.get(FIXTURE_PROCESS_ID));
      expect(yield* listedPerl.readCommandLineUnlessGone).toBe("\uFEFFperl");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
