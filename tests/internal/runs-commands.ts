import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import {
  Duration,
  Effect,
  Exit,
  FileSystem,
  Option,
  PlatformError,
  Predicate,
  Redacted,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { CapturedCommandFailed, CommandFailed } from "../../src/lib/errors/index.ts";
import {
  checkWorkingDirectoryBeforeSpawn,
  recheckWorkingDirectoryAfterSpawnFailure,
} from "../../src/lib/worker-thread/lib/provide-task-services/internal/spawn-commands-with-bun.ts";
import { WorkingDirectoryUnusable } from "../../src/shared/command-errors.ts";
import { inheritedEnvironment } from "../../src/shared/read-environment-config.ts";
import { exists, read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue, failureAsValue } from "../shared/failure-as-value.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath, withRunLinePathNamed } from "../shared/name-output-files.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const readTaskSpan = (
  project: Parameters<typeof readProjectRunsDirectory>[0],
  ran: Parameters<typeof readProjectRunsDirectory>[1],
  taskName: string,
) =>
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
    return trace?.findTaskSpan("task", taskName);
  });

export const input = Task.make({
  name: "Task.exec streams exact input, closes default stdin and retains failure output",
  run: Effect.gen(function* () {
    const contents = "first line\nsecond line\n";
    expect(
      yield* Task.exec("cat", [], {
        stdin: Stream.succeed(new TextEncoder().encode(contents)),
      }),
    ).toBe(contents);
    expect(yield* Task.exec("cat", [])).toBe("");
    const failedOutput = yield* Task.exec("sh", ["-c", "printf error-output >&2; exit 7"]).pipe(
      Effect.flatMap((output) => Effect.die(`the command succeeded and printed ${output}`)),
      Effect.catchTag("CommandFailed", (failure) =>
        Effect.succeed([failure.standardOutput, failure.standardError]),
      ),
    );
    expect(failedOutput).toEqual(["", "error-output"]);
  }),
});

export const separateStreams = Task.make({
  name: "a failed command keeps its standard output and standard error apart and exact on its CommandFailed, whose message is the command, how it ended and each stream under its name",
  run: Effect.gen(function* () {
    const report = '{"diagnostics":[]}\n';
    yield* Task.exec("sh", [
      "-c",
      `printf '%s' '${report}'; printf 'progress line\\n' >&2; exit 1`,
    ]).pipe(
      Effect.flatMap((output) => Effect.die(`the command succeeded and printed ${output}`)),
      Effect.catchTag("CommandFailed", (failure) =>
        Effect.sync(() => {
          expect(failure.code).toBe(1);
          expect(failure.standardOutput).toBe(report);
          expect(failure.standardError).toBe("progress line\n");
          expect(failure.message).toBe(
            `${failure.command} exited with code 1\nstandard output:\n${report.trimEnd()}\nstandard error:\nprogress line`,
          );
        }),
      ),
    );
  }),
});

export const emptyStandardOutput = Task.make({
  name: "a failed command that prints nothing on standard output keeps it empty and its standard error exact, and its message ends with that standard error",
  run: Task.exec("sh", ["-c", String.raw`printf 'only error\n' >&2; exit 2`]).pipe(
    Effect.flatMap((output) => Effect.die(`the command succeeded and printed ${output}`)),
    Effect.catchTag("CommandFailed", (failure) =>
      Effect.sync(() => {
        expect(failure.standardOutput).toBe("");
        expect(failure.standardError).toBe("only error\n");
        expect(failure.message).toBe(
          `${failure.command} exited with code 2\nstandard error:\nonly error`,
        );
      }),
    ),
  ),
});

const MESSAGE_LINES = 20;

const MESSAGE_CHARACTERS = 4096;

const WRITTEN_LINES = 30;

const LONG_LINE_CHARACTERS = 8192;

const SURROGATE_PAIRS = 4096;

const SURROGATE_PAIRS_KEPT_WHOLE = 2047;

const SURROGATE_CODE_UNITS_LEFT_OUT = 4098;

const writtenLines = (count: number) =>
  Array.from({ length: count }, (_unused, index) => `line ${String(index + 1)}`);

const ERROR_HEADING = "c exited with code 1\nstandard error:";

const ONE_ERROR_LINE_LEFT_OUT = "(1 earlier line is not shown; see standardError)";

const LINE_AND_CHARACTERS_LEFT_OUT = `(1 earlier line and the first ${String(LONG_LINE_CHARACTERS - MESSAGE_CHARACTERS)} characters of this line are not shown; see standardError)`;

const failedWith = (standardError: string) =>
  new CommandFailed({ command: "c", code: 1, standardOutput: "", standardError }).message;

export const commandFailedMessageBound = makeTest(
  "a CommandFailed message carries at most the last 20 lines and 4096 characters of each of standard output and standard error, from its first whole line, under the name of each stream, says in one note how many earlier lines and how many characters at the start of the shown line it left out and which field holds them, and a CapturedCommandFailed message carries none",
  Effect.sync(() => {
    expect(failedWith(`${writtenLines(WRITTEN_LINES).join("\n")}\n\n`)).toBe(
      [
        ERROR_HEADING,
        `(${String(WRITTEN_LINES - MESSAGE_LINES)} earlier lines are not shown; see standardError)`,
        ...writtenLines(WRITTEN_LINES).slice(WRITTEN_LINES - MESSAGE_LINES),
      ].join("\n"),
    );
    expect(failedWith(`first\n${"y".repeat(LONG_LINE_CHARACTERS)}`)).toBe(
      `${ERROR_HEADING}\n${LINE_AND_CHARACTERS_LEFT_OUT}\n${"y".repeat(MESSAGE_CHARACTERS)}`,
    );
    expect(failedWith(`${"\u{1F600}".repeat(SURROGATE_PAIRS)}b`)).toBe(
      `${ERROR_HEADING}\n(the first ${String(SURROGATE_CODE_UNITS_LEFT_OUT)} characters of this line are not shown; see standardError)\n${"\u{1F600}".repeat(SURROGATE_PAIRS_KEPT_WHOLE)}b`,
    );
    expect(failedWith(`q${"x".repeat(MESSAGE_CHARACTERS)}`)).toBe(
      `${ERROR_HEADING}\n(the first 1 character of this line is not shown; see standardError)\n${"x".repeat(MESSAGE_CHARACTERS)}`,
    );
    expect(failedWith("\nok")).toBe(`${ERROR_HEADING}\n\nok`);
    expect(failedWith("…data")).toBe(`${ERROR_HEADING}\n…data`);
    expect(failedWith(`a\n…${"x".repeat(MESSAGE_CHARACTERS - 1)}`)).toBe(
      `${ERROR_HEADING}\n${ONE_ERROR_LINE_LEFT_OUT}\n…${"x".repeat(MESSAGE_CHARACTERS - 1)}`,
    );
    expect(failedWith(`a\n${"x".repeat(MESSAGE_CHARACTERS - 1)}\uD800`)).toBe(
      `${ERROR_HEADING}\n${ONE_ERROR_LINE_LEFT_OUT}\n${"x".repeat(MESSAGE_CHARACTERS - 1)}\uD800`,
    );
    expect(failedWith(`cut ${"z".repeat(MESSAGE_CHARACTERS)}\nwhole\n`)).toBe(
      `${ERROR_HEADING}\n${ONE_ERROR_LINE_LEFT_OUT}\nwhole`,
    );
    expect(failedWith(`a\n${"x".repeat(MESSAGE_CHARACTERS)}`)).toBe(
      `${ERROR_HEADING}\n${ONE_ERROR_LINE_LEFT_OUT}\n${"x".repeat(MESSAGE_CHARACTERS)}`,
    );
    expect(
      new CommandFailed({
        command: "c",
        code: 1,
        standardOutput: `${writtenLines(WRITTEN_LINES).join("\n")}\n`,
        standardError: `first\n${"y".repeat(LONG_LINE_CHARACTERS)}`,
      }).message,
    ).toBe(
      [
        "c exited with code 1",
        "standard output:",
        `(${String(WRITTEN_LINES - MESSAGE_LINES)} earlier lines are not shown; see standardOutput)`,
        ...writtenLines(WRITTEN_LINES).slice(WRITTEN_LINES - MESSAGE_LINES),
        "standard error:",
        LINE_AND_CHARACTERS_LEFT_OUT,
        "y".repeat(MESSAGE_CHARACTERS),
      ].join("\n"),
    );
    expect(
      new CapturedCommandFailed({
        command: "c",
        code: 1,
        standardOutput: Redacted.make("out"),
        standardError: Redacted.make("secret error"),
      }).message,
    ).toBe("c exited with code 1, and its captured output is not shown");
  }),
);

export const leadingWhitespace = Task.make({
  name: "a failed command keeps leading and trailing whitespace on both outputs",
  run: Task.exec("sh", [
    "-c",
    String.raw`printf '  indented report\n\n'; printf '\t tabbed error \n' >&2; exit 1`,
  ]).pipe(
    Effect.flatMap((output) => Effect.die(`the command succeeded and printed ${output}`)),
    Effect.catchTag("CommandFailed", (failure) =>
      Effect.sync(() => {
        expect(failure.standardOutput).toBe("  indented report\n\n");
        expect(failure.standardError).toBe("\t tabbed error \n");
      }),
    ),
  ),
});

const CONSUMER_EXEC_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const output = yield* Task.exec("sh", ["-c", 'printf "%s" "$GREETING"'], {
    env: { GREETING: "hello from Task.exec" },
  });
  yield* Effect.log(\`output: \${output}\`);
  const failure = yield* Task.exec("sh", ["-c", "printf broken >&2; exit 3"]).pipe(
    Effect.as("none"),
    Effect.catchTag("CommandFailed", (failed) => Effect.succeed(\`\${failed.code} \${failed.standardError}\`)),
  );
  yield* Effect.log(\`failure: \${failure}\`);
}) });
`;

export const consumerExec = makeTest(
  "a consumer project imports Task from the package to run commands with Task.exec and catch their failures",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(CONSUMER_EXEC_SOURCE);
      const project = yield* makeProject({ "main.ts": CONSUMER_EXEC_SOURCE });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] output: hello from Task.exec");
      expect(ran.standardError).toContain("[main] failure: 3 broken");
    }),
  ),
);

const ROOT_TASK_FAILURE_SOURCES = {
  "command.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "exit 7"]) });
`,
  "task-error.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.fail(new TaskError({ message: "broken" })) });
`,
  "awaited-command.ts": `import { Task } from "@sshkeda/task";
export const command = Task.make({ name: "command", run: Task.exec("sh", ["-c", "exit 7"]) });
export default Task.make({ name: "main", run: command });
`,
  "killed-command.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "kill -KILL $$"]) });
`,
} as const;

export const rootTaskCommandExitCode = makeTest(
  "a run whose root task fails with a command's exit code exits with that code, through the supervisor, and every other failure exits 1",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(ROOT_TASK_FAILURE_SOURCES);
      const exitCodes = yield* Effect.forEach(
        Object.keys(ROOT_TASK_FAILURE_SOURCES),
        (file) =>
          runTaskProcess(project, file).pipe(
            Effect.map((ran) => ({ file, exitCode: ran.exitCode })),
          ),
        { concurrency: "unbounded" },
      );
      expect(exitCodes).toEqual([
        { file: "command.ts", exitCode: 7 },
        { file: "task-error.ts", exitCode: 1 },
        { file: "awaited-command.ts", exitCode: 1 },
        { file: "killed-command.ts", exitCode: 1 },
      ]);
    }),
  ),
);

const KILLED_COMMAND_ATTEMPTS = ["first", "second", "third", "fourth", "fifth"] as const;

export const killedCommandOutput = makeTest(
  "a command killed by a signal right after writing still has everything it wrote in its task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const attempts = yield* encodeJson(KILLED_COMMAND_ATTEMPTS);
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.forEach(
  ${attempts},
  (attempt) => Task.exec("sh", ["-c", \`echo out-\${attempt}; echo err-\${attempt} >&2; kill -KILL $$\`]).pipe(
    Effect.catchTag("CommandFailed", () => Effect.void),
  ),
  { discard: true },
) });
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const written = yield* read(`${findRunDirectoryPath(ran.standardError)}/1.output`);
      for (const attempt of KILLED_COMMAND_ATTEMPTS) {
        expect(written).toContain(`[main] out-${attempt}\n`);
        expect(written).toContain(`[main] err-${attempt}\n`);
      }
    }),
  ),
);

export const signalKilledCommandFailed = makeTest(
  "a command killed by a signal fails with a CommandFailed that Effect.catchTag catches, carrying the signal and the command's output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const caught = yield* Task.exec("sh", ["-c", "echo caught-out; echo caught-err >&2; kill -SEGV $$"]).pipe(
    Effect.catchTag("CommandFailed", (failure) =>
      Effect.succeed([failure.code, failure.signal, failure.standardOutput, failure.standardError]),
    ),
  );
  yield* Effect.log(\`caught \${JSON.stringify(caught)}\`);
  yield* Task.exec("sh", ["-c", "echo out; echo err >&2; kill -SEGV $$"]);
}) });
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        '[main] caught [null,"SIGSEGV","caught-out\\n","caught-err\\n"]\n',
      );
      expect(ran.standardError).toContain("[main] out\n");
      expect(ran.standardError).toContain("[main] err\n");
      expect(withRunLinePathNamed(ran.standardError)).toMatch(
        /\nerror: \[main\] main\.ts:10:15: CommandFailed: sh was killed by signal SIGSEGV\nerror: \[main\] its output is in <run folder>\/1\.output\nended: \[main\], [\d.]+s, failed\nrecap: \[main\] 1\.output \| main\.ts:10:15: CommandFailed: sh was killed by signal SIGSEGV\nrecap: \[main\] out\nrecap: \[main\] err\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: .+\/$/u,
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
    }),
  ),
);

export const commandEndsWithCodeOrSignal = makeTest(
  "a CommandFailed or CapturedCommandFailed is refused unless it has exactly one of an exit code and a signal",
  Effect.sync(() => {
    const output = { standardOutput: "", standardError: "" };
    const capturedOutput = {
      standardOutput: Redacted.make(""),
      standardError: Redacted.make(""),
    };
    expect(new CommandFailed({ command: "c", code: 1, ...output }).message).toBe(
      "c exited with code 1",
    );
    expect(
      new CommandFailed({ command: "c", code: 1, standardOutput: "out\n", standardError: "\n" })
        .message,
    ).toBe("c exited with code 1\nstandard output:\nout");
    expect(
      new CommandFailed({ command: "c", code: null, signal: "SIGKILL", ...output }).message,
    ).toBe("c was killed by signal SIGKILL");
    for (const end of [{ code: null }, { code: 1, signal: "SIGKILL" }]) {
      expect(
        () => new CommandFailed({ command: "c", ...end, ...output }).message,
        String(end.code),
      ).toThrow();
      expect(
        () => new CapturedCommandFailed({ command: "c", ...end, ...capturedOutput }).message,
        String(end.code),
      ).toThrow();
    }
  }),
);

export const environment = makeTest(
  "a command sees PATH, HOME, TMPDIR, the run's own task config, the run's marker and what it declares, not the rest of the run's environment, without losing empty, nested or sparse numeric names",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { expect } from "bun:test";
import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";

const readEnvironment = (options: ChildProcess.CommandOptions) => Effect.scoped(
  Effect.gen(function* () {
    const child = yield* ChildProcess.make("env", ["-0"], options);
    const text = yield* Stream.mkString(Stream.decodeText(child.stdout));
    expect(yield* child.exitCode).toBe(0);
    const { SSHKEDA_TASK_RUN_MARKERS: runMarkers, ...seen } = Object.fromEntries(text.split("\\0").filter(Boolean).map((entry) => {
      const separator = entry.indexOf("=");
      return [entry.slice(0, separator), entry.slice(separator + 1)];
    }));
    expect(runMarkers).toMatch(/^[0-9a-f-]+( [0-9a-f-]+)*$/u);
    return seen;
  }),
);
const always = Object.fromEntries(["HOME", "PATH", "TMPDIR", "SSHKEDA_TASK_STATE_DIRECTORY", "SSHKEDA_TASK_WORKERS", "SSHKEDA_TASK_SETUP_LIMIT"].flatMap((name) => {
  const value = process.env[name];
  return value === undefined ? [] : [[name, value]];
}));
export default Task.make({ name: "main", run: Effect.gen(function* () {
  expect(process.env.TASK_ENV).toBe("parent");
  expect(always.PATH).toBe(process.env.PATH);
  expect(yield* readEnvironment({})).toEqual(always);
  expect(yield* readEnvironment({ extendEnv: true })).toEqual(always);
  const declared = {
    TASK_ENV_CHILD: "child",
    TASK_ENV__EMPTY: "",
    TASK_ENV_NUMBER_0: "zero",
    TASK_ENV_NUMBER_2: "two",
    TASK_ENV_NUMBER_1000000000: "sparse",
    TASK_ENV_NUMBER_0002: "leading zeroes",
    TASK_ENV_TEXT: "a=b\\nsecond line",
  };
  expect(yield* readEnvironment({ extendEnv: true, env: declared })).toEqual({ ...always, ...declared });
  const extended = yield* readEnvironment({ extendEnv: true, env: { HOME: undefined, TASK_ENV_NEW: "added" }});
  expect(extended).toEqual({ ...always, HOME: undefined, TASK_ENV_NEW: "added" });
  expect(Object.keys(extended)).not.toContain("HOME");
  const replaced = yield* readEnvironment({env: { ONLY: "replacement" }});
  expect(replaced).toEqual({ONLY: "replacement"});
}) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts", {
        TASK_ENV: "parent",
        TASK_ENV_CHILD: "parent",
        TASK_ENV__EMPTY: "parent",
      });
      expect(result.exitCode, result.standardError).toBe(0);
    }),
  ),
);

export const nestedRunConfig = makeTest(
  "a task run that a command starts keeps the outer run's state directory, worker count and setup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "outer/package.json": `{ "name": "outer", "type": "module" }`,
        "outer/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("bun", ["inner.ts"], { cwd: "nested", print: true }) });`,
        "outer/nested/package.json": `{ "name": "nested", "type": "module" }`,
        "outer/nested/inner.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const inner = Task.make({ name: "inner", run: Effect.log(\`inner sees workers=\${process.env.SSHKEDA_TASK_WORKERS} setup=\${process.env.SSHKEDA_TASK_SETUP_LIMIT}\`) });
export default inner;`,
      });
      const taskConfig = {
        SSHKEDA_TASK_STATE_DIRECTORY: "state",
        SSHKEDA_TASK_WORKERS: "3",
        SSHKEDA_TASK_SETUP_LIMIT: "50 seconds",
      };
      const first = yield* runTaskProcess(project, "outer/main.ts", {
        ...taskConfig,
      });
      expect(first.exitCode, first.standardError).toBe(0);
      expect(first.standardError).toContain(
        "[main] [inner] inner sees workers=3 setup=50 seconds\n",
      );
      const fileSystem = yield* FileSystem.FileSystem;
      const runDirectoryNames = yield* fileSystem.readDirectory(
        `${project.directory}/state/runs/nested`,
      );
      expect(runDirectoryNames).toHaveLength(1);
      expect(
        yield* fileSystem.exists(
          `${project.directory}/state/runs/nested/${String(runDirectoryNames[0])}/trace.jsonl`,
        ),
      ).toBe(true);
      expect(yield* fileSystem.exists(`${project.directory}/outer/state`)).toBe(false);
      expect(yield* fileSystem.exists(`${project.directory}/outer/nested/state`)).toBe(false);
      expect(yield* fileSystem.exists(`${project.home}/.sshkeda-task`)).toBe(false);
    }),
  ),
);

export const declaredEnvironment = makeTest(
  "a task's commands see a variable from the run only when the task passes it, besides PATH",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
const declared = Config.String("TASK_TEST_DECLARED");
const printVariables = 'printf "undeclared=%s declared=%s %s path=%s" "$TASK_TEST_UNDECLARED" "$TASK_TEST_DECLARED" "$(git --version)" "$PATH"';
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log(yield* Task.exec("sh", ["-c", printVariables], { env: { TASK_TEST_DECLARED: yield* declared } }));
}) });`,
      });
      const { PATH: inheritedPath } = yield* inheritedEnvironment;
      const runPath = yield* Effect.fromNullishOr(inheritedPath);
      const gitVersion = (yield* Task.exec("git", ["--version"])).trim();
      const printed = (declared: string) =>
        `undeclared= declared=${declared} ${gitVersion} path=${runPath}`;
      const first = yield* runTaskProcess(project, "main.ts", {
        TASK_TEST_UNDECLARED: "one",
        TASK_TEST_DECLARED: "a",
      });
      expect(first.exitCode, first.standardError).toBe(0);
      expect(first.standardError.split("\n")).toContain(`[main] ${printed("a")}`);
      const pathChanged = yield* runTaskProcess(project, "main.ts", {
        TASK_TEST_DECLARED: "b",
        PATH: `${runPath}:${project.directory}`,
      });
      expect(pathChanged.standardError).toContain(`path=${runPath}:${project.directory}\n`);
    }),
  ),
);

export const unsupportedCommandRefusals = makeTest(
  "a refused piped command or spawn option prints its tag",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const piped = Task.make({ name: "piped", run: Effect.scoped(
  ChildProcess.make("echo", ["a"]).pipe(ChildProcess.pipeTo(ChildProcess.make("cat", []))),
) });
export const shell = Task.make({ name: "shell", run: Effect.scoped(ChildProcess.make("echo", ["a"], { shell: true })) });
export default Task.make({ name: "main", run: Task.allSettled([piped, shell]) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(result.standardError.split("\n")).toContain(
        "error: [piped] main.ts:4:27: CommandPipelineRefused: task's process service refused the pipeline echo | cat",
      );
      expect(result.standardError.split("\n")).toContain(
        "error: [shell] main.ts:7:27: CommandOptionsRefused: task's process service refused shell for echo",
      );
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
    }),
  ),
);

export const missingWorkingDirectoryCheckedBeforeSpawn = makeTest(
  "a command whose working directory doesn't exist is not spawned, so its CommandNotStarted cause is the check of that directory, while a spawn failure in an existing directory keeps Bun's own error",
  Effect.gen(function* () {
    const directory = yield* makeTemporaryDirectory();
    const missing = yield* Task.exec("sh", ["-c", "true"], { cwd: `${directory}/missing` }).pipe(
      failureAsValue,
    );
    expect(missing.message).toBe(
      `the working directory ${directory}/missing doesn't exist\ncould not start sh`,
    );
    const checked = Predicate.hasProperty(missing.cause, "reason") ? missing.cause.reason : null;
    const unusable = Predicate.hasProperty(checked, "cause") ? checked.cause : null;
    expect(unusable).toBeInstanceOf(WorkingDirectoryUnusable);
    expect(Predicate.hasProperty(unusable, "cause") ? unusable.cause : null).toMatchObject({
      module: "FileSystem",
      method: "stat",
      pathOrDescriptor: `${directory}/missing`,
    });
    yield* Task.exec("sh", ["-c", `printf '' > '${directory}/not-executable'`]);
    const notExecutable = yield* Task.exec(`${directory}/not-executable`, [], {
      cwd: directory,
    }).pipe(failureAsValue);
    const spawnFailure = Predicate.hasProperty(notExecutable.cause, "reason")
      ? notExecutable.cause.reason
      : null;
    expect(Predicate.hasProperty(spawnFailure, "cause") ? spawnFailure.cause : null).toMatchObject({
      code: "EACCES",
      syscall: "posix_spawn",
    });
  }),
);

export const workingDirectoryRemovedBetweenCheckAndSpawn = makeTest(
  "a working directory removed after the check and before the spawn is named as the cause, with Bun's spawn error inside, while a spawn failure in a directory that still exists keeps Bun's error unchanged",
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = yield* makeTemporaryDirectory();
    const removed = `${directory}/removed`;
    yield* fileSystem.makeDirectory(removed);
    yield* checkWorkingDirectoryBeforeSpawn("true", directory, removed);
    yield* fileSystem.remove(removed, { recursive: true });
    const spawnFailure = yield* Effect.try({
      try: () => Bun.spawn(["/usr/bin/true"], { cwd: removed }),
      catch: (cause) =>
        PlatformError.systemError({
          _tag: "NotFound",
          module: "ChildProcess",
          method: "spawn",
          pathOrDescriptor: "true",
          description: String(cause),
          cause,
        }),
    }).pipe(failureAsValue);
    expect(spawnFailure.message).toContain("posix_spawn");
    const blamed = yield* recheckWorkingDirectoryAfterSpawnFailure(
      "true",
      directory,
      removed,
      spawnFailure,
    ).pipe(failureAsValue);
    expect(blamed.message).toBe(
      "NotFound: ChildProcess.spawn (true): the working directory removed doesn't exist",
    );
    expect(blamed.reason.cause).toBeInstanceOf(WorkingDirectoryUnusable);
    expect(
      Predicate.hasProperty(blamed.reason.cause, "cause") ? blamed.reason.cause.cause : null,
    ).toBe(spawnFailure);
    const kept = yield* recheckWorkingDirectoryAfterSpawnFailure(
      "true",
      directory,
      directory,
      spawnFailure,
    ).pipe(failureAsValue);
    expect(kept).toBe(spawnFailure);
  }),
);

export const commandsThatCannotStart = makeTest(
  "a command that cannot start fails with CommandNotStarted, which names a missing or non-directory working directory instead of the program, and keeps Bun's text for a missing program",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "a-file.txt": "",
        "tools/build.sh": "echo built",
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const missingDirectory = Task.make({ name: "missing-directory", run: Task.exec("sh", ["build.sh"], { cwd: "tool" }) });
export const fileDirectory = Task.make({ name: "file-directory", run: Task.exec("sh", ["build.sh"], { cwd: "a-file.txt" }) });
export const missingProgram = Task.make({ name: "missing-program", run: Task.exec("no-such-program", ["--version"]) });
export const caught = Task.make({ name: "caught", run: Task.exec("sh", ["build.sh"], { cwd: "tool" }).pipe(
  Effect.catchTag("CommandNotStarted", (failure) => Effect.log(\`caught: \${failure.command}\`)),
) });
export const started = Task.make({ name: "started", run: Task.exec("sh", ["build.sh"], { cwd: "tools", print: true }) });
export default Task.make({ name: "main", run: Task.allSettled([missingDirectory, fileDirectory, missingProgram, caught, started]) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(result.standardError).toContain(
        "error: [missing-directory] main.ts:3:82: WorkingDirectoryUnusable: the working directory tool doesn't exist\nerror: [missing-directory] could not start sh\n",
      );
      expect(result.standardError).toContain(
        "error: [file-directory] main.ts:4:76: WorkingDirectoryUnusable: the working directory a-file.txt is not a directory\nerror: [file-directory] could not start sh\n",
      );
      expect(result.standardError).toContain(
        'error: [missing-program] main.ts:5:78: Error: Executable not found in $PATH: "no-such-program"\nerror: [missing-program] could not start no-such-program\n',
      );
      expect(result.standardError).toContain("[caught] caught: sh\n");
      expect(result.standardError).toContain("[started] built\n");
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
    }),
  ),
);

export const failingStandardInputStreams = makeTest(
  "a standard input stream that fails or dies fails the run with its own error and says which command's standard input stream failed, next to CommandFailed when the command it stopped still exits non-zero, while a command that stops reading its standard input still passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, PlatformError, Stream } from "effect";
import { Task } from "@sshkeda/task";
export const failing = Task.make({ name: "failing", run: Task.exec("cat", [], {
  stdin: Stream.fail(PlatformError.systemError({ _tag: "NotFound", module: "FileSystem", method: "stream", description: "input file missing" })),
}) });
export const dying = Task.make({ name: "dying", run: Task.exec("cat", [], { stdin: Stream.die(new Error("input stream died")) }) });
const dyingOnceTrapIsSet = Stream.fromEffect(Effect.promise(async () => {
  while (!(await Bun.file("trap-set").exists())) await Bun.sleep(10);
  return new Uint8Array();
})).pipe(Stream.concat(Stream.die(new Error("input stream died"))));
export const dyingAndExiting = Task.make({ name: "dying-and-exiting", run: Task.exec("sh", ["-c", "trap 'exit 3' TERM; touch trap-set; cat; exit 3"], { stdin: dyingOnceTrapIsSet }) });
export const stopsReading = Task.make({ name: "stops-reading", run: Task.exec("head", ["-c", "1"], {
  stdin: Stream.forever(Stream.make(new Uint8Array(65536))),
}) });
export default Task.make({ name: "main", run: Task.allSettled([failing, dying, dyingAndExiting, stopsReading]) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      const lines = result.standardError.split("\n");
      expect(lines).toContain(
        "error: [failing] main.ts:4:36: PlatformError: NotFound: FileSystem.stream: input file missing",
      );
      expect(lines).toContain("error: [failing] the standard input stream of cat failed");
      expect(lines).toContain("error: [dying] main.ts:6:99: Error: input stream died");
      expect(lines).toContain("error: [dying] the standard input stream of cat failed");
      expect(
        lines.filter((line) => line.startsWith("error: [dying-and-exiting] main.ts:")),
      ).toEqual([
        "error: [dying-and-exiting] main.ts:11:81: CommandFailed: sh exited with code 3",
        "error: [dying-and-exiting] main.ts:10:39: Error: input stream died",
      ]);
      expect(lines).toContain("error: [dying-and-exiting] the standard input stream of sh failed");
      expect(
        readAttribute(
          (yield* readTaskSpan(project, result, "stops-reading"))?.attributes ?? [],
          "sshkeda_task.state",
        ),
      ).toBe("passed");
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
    }),
  ),
);

const STANDARD_INPUT_CHUNK_BYTES = 65_536;

const PROMPT_FAILURE_LIMIT_MILLISECONDS = 3000;

const NANOSECONDS_PER_MILLISECOND = 1_000_000;

const HELD_OUTPUT_FAILURE_LIMIT_MILLISECONDS = 20_000;

const isBrokenPipe = (failure: Option.Option<PlatformError.PlatformError>) =>
  Option.exists(
    failure,
    (platformError) =>
      Predicate.hasProperty(platformError.cause, "code") && platformError.cause.code === "EPIPE",
  );

export const standardInputClosedByCommand = makeTest(
  "a command that stops reading 64 KiB chunks of standard input ends the input of Task.exec and of a stdin stream, while writing them through handle.stdin fails with EPIPE",
  Effect.scoped(
    Effect.gen(function* () {
      const endlessInput = Stream.forever(Stream.make(new Uint8Array(STANDARD_INPUT_CHUNK_BYTES)));
      expect(yield* Task.exec("head", ["-c", "1"], { stdin: endlessInput })).toBe("\0");
      const fedFromStream = yield* ChildProcess.make("head", ["-c", "1"], { stdin: endlessInput });
      expect(yield* fedFromStream.exitCode).toBe(ChildProcessSpawner.ExitCode(0));
      const writtenThroughSink = yield* ChildProcess.make("head", ["-c", "1"]);
      const writtenExit = yield* exitAsValue(Stream.run(endlessInput, writtenThroughSink.stdin));
      const writtenFailure = Exit.findErrorOption(writtenExit);
      expect(isBrokenPipe(writtenFailure), String(writtenExit)).toBe(true);
      expect((yield* Effect.fromOption(writtenFailure)).message).toMatch(
        /^Unknown: ChildProcess\.fromWritable\(stdin\) \(head\): writing standard input failed with Error: EPIPE/u,
      );
      expect(yield* writtenThroughSink.exitCode).toBe(ChildProcessSpawner.ExitCode(0));
    }),
  ),
);

export const smallStandardInputClosedByCommand = makeTest(
  "a few bytes of standard input for a command that already closed it end the input of Task.exec and of a stdin stream, while writing them through handle.stdin fails with EPIPE",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const closesItsInput = (marker: string) => [
        "-c",
        `exec 0<&-; echo closed > '${directory}/${marker}'; sleep 0.2`,
      ];
      const fileSystem = yield* FileSystem.FileSystem;
      yield* Effect.forEach(["run-command", "stream", "sink"], (marker) =>
        Task.exec("mkfifo", [`${directory}/${marker}`]),
      );
      const fiveBytesOnceClosed = (marker: string) =>
        Stream.fromEffect(
          fileSystem
            .readFile(`${directory}/${marker}`)
            .pipe(Effect.as(new TextEncoder().encode("hello"))),
        );
      expect(
        yield* Task.exec("sh", closesItsInput("run-command"), {
          stdin: fiveBytesOnceClosed("run-command"),
        }),
      ).toBe("");
      const fedFromStream = yield* ChildProcess.make("sh", closesItsInput("stream"), {
        stdin: fiveBytesOnceClosed("stream"),
      });
      expect(yield* exitAsValue(fedFromStream.exitCode)).toEqual(
        Exit.succeed(ChildProcessSpawner.ExitCode(0)),
      );
      const writtenThroughSink = yield* ChildProcess.make("sh", closesItsInput("sink"));
      const writtenExit = yield* exitAsValue(
        Stream.run(fiveBytesOnceClosed("sink"), writtenThroughSink.stdin),
      );
      const writtenFailure = Exit.findErrorOption(writtenExit);
      expect(isBrokenPipe(writtenFailure), String(writtenExit)).toBe(true);
      expect((yield* Effect.fromOption(writtenFailure)).message).toMatch(
        /^Unknown: ChildProcess\.(?:fromWritable\(stdin\) \(sh\): writing standard input failed with Error: EPIPE|end\(stdin\) \(sh\): ending standard input failed with Error: EPIPE)/u,
      );
      expect(yield* writtenThroughSink.exitCode).toBe(ChildProcessSpawner.ExitCode(0));
    }),
  ),
);

export const standardInputStreamsOwnBrokenPipe = makeTest(
  "a standard input stream that fails with its own EPIPE fails Task.exec with that failure's own description",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, PlatformError, Stream } from "effect";
import { Task } from "@sshkeda/task";
const ownBrokenPipe = PlatformError.systemError({ _tag: "Unknown", module: "Upstream", method: "read", description: "the upstream socket closed", cause: Object.assign(new Error("EPIPE: broken pipe, read"), { code: "EPIPE", syscall: "read" }) });
export default Task.make({ name: "main", run: Task.exec("cat", [], { stdin: Stream.concat(Stream.make(new TextEncoder().encode("a")), Stream.fail(ownBrokenPipe)) }) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(
        result.standardError
          .split("\n")
          .filter((line) => line.startsWith("error: [main] main.ts:")),
      ).toEqual([
        "error: [main] main.ts:3:37: PlatformError: Unknown: Upstream.read: the upstream socket closed",
      ]);
    }),
  ),
);

export const selfInterruptingStandardInput = makeTest(
  "a standard input stream that interrupts itself fails with CauseWithoutFailure, through Task.exec and through a stdin stream",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
const interruptsItself = Stream.concat(Stream.make(new TextEncoder().encode("a")), Stream.fromEffect(Effect.interrupt));
const interruptsItselfAndFinishesAfterTheCommand = (marker: string) => Stream.fromEffect(Effect.sync(() => writeFileSync(\`\${import.meta.dir}/\${marker}\`, "")).pipe(Effect.andThen(Effect.interrupt))).pipe(Stream.ensuring(Effect.sleep("300 millis")));
const exitsOnceMarked = (marker: string) => ["-c", \`until [ -e \${marker} ]; do sleep 0.01; done\`];
export const throughRunCommand = Task.make({ name: "through-run-command", run: Task.exec("cat", [], { stdin: interruptsItself }) });
export const throughStream = Task.make({ name: "through-stream", run: Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("cat", [], { stdin: interruptsItself });
  return yield* child.exitCode;
})) });
export const throughRunCommandAfterExit = Task.make({ name: "through-run-command-after-exit", run: Task.exec("sh", exitsOnceMarked("run-command-stdin-started"), { stdin: interruptsItselfAndFinishesAfterTheCommand("run-command-stdin-started") }) });
export const throughStreamAfterExit = Task.make({ name: "through-stream-after-exit", run: Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("sh", exitsOnceMarked("stream-stdin-started"), { stdin: interruptsItselfAndFinishesAfterTheCommand("stream-stdin-started") });
  return yield* child.exitCode;
})) });
export default Task.make({ name: "main", run: Task.allSettled([throughRunCommand, throughStream, throughRunCommandAfterExit, throughStreamAfterExit]) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(
        result.standardError
          .split("\n")
          .filter((line) => /^error: \[through-[a-z-]+\] main\.ts:/u.test(line))
          .toSorted(),
      ).toEqual([
        "error: [through-run-command-after-exit] main.ts:13:105: CauseWithoutFailure: was interrupted without a failure",
        "error: [through-run-command] main.ts:8:85: CauseWithoutFailure: was interrupted without a failure",
        "error: [through-stream-after-exit] main.ts:14:44: CauseWithoutFailure: was interrupted without a failure",
        "error: [through-stream] main.ts:9:35: CauseWithoutFailure: was interrupted without a failure",
      ]);
      expect(result.standardError).toContain(
        "error: [through-run-command] main.ts:8:85: CauseWithoutFailure: was interrupted without a failure\nerror: [through-run-command] the standard input stream of cat failed\n",
      );
    }),
  ),
);

export const standardInputFailureStopsCommand = makeTest(
  "a standard input stream that fails while its command neither reads it nor exits fails at once and stops the command",
  Effect.scoped(
    Effect.gen(function* () {
      const [execElapsed, execFailure] = yield* Task.exec("sleep", ["30"], {
        stdin: Stream.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "FileSystem",
            method: "stream",
            description: "input file missing",
          }),
        ),
      }).pipe(failureAsValue, Effect.timed);
      expect(Duration.toMillis(execElapsed)).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
      expect(execFailure.message).toBe(
        "NotFound: FileSystem.stream: input file missing\nthe standard input stream of sleep failed",
      );
      const child = yield* ChildProcess.make("sleep", ["30"], {
        stdin: Stream.fail(
          PlatformError.systemError({
            _tag: "NotFound",
            module: "FileSystem",
            method: "stream",
            description: "input file missing",
          }),
        ),
      });
      const [exitCodeElapsed, exitCodeFailure] = yield* child.exitCode.pipe(
        failureAsValue,
        Effect.timed,
      );
      expect(Duration.toMillis(exitCodeElapsed)).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
      expect(exitCodeFailure.message).toBe("NotFound: FileSystem.stream: input file missing");
      expect(yield* child.isRunning).toBe(false);
    }),
  ),
);

export const standardInputFailureWhileOutputStaysOpen = makeTest(
  "a standard input stream that fails fails Task.exec at once, even when its stopped command exits cleanly and leaves a background child holding its output open",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { Task } from "@sshkeda/task";
const dyingOnceTrapIsSet = Stream.fromEffect(Effect.promise(async () => {
  while (!(await Bun.file("trap-set").exists())) await Bun.sleep(10);
  return new Uint8Array();
})).pipe(Stream.concat(Stream.die(new Error("input stream died"))));
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "trap 'sleep 600 & exit 0' TERM; touch trap-set; while :; do sleep 0.05; done"], { stdin: dyingOnceTrapIsSet }) });`,
      });
      const [elapsed, result] = yield* Effect.timed(runTaskProcess(project, "main.ts"));
      expect(result.exitCode, result.standardError).toBe(1);
      expect(
        result.standardError
          .split("\n")
          .filter((line) => line.startsWith("error: [main] main.ts:")),
      ).toEqual(["error: [main] main.ts:6:39: Error: input stream died"]);
      expect(Duration.toMillis(elapsed)).toBeLessThan(HELD_OUTPUT_FAILURE_LIMIT_MILLISECONDS);
    }),
  ),
  "30 seconds",
);

export const outputFailureStopsCommand = makeTest(
  "an output stream that fails fails Task.exec at once and stops its command",
  Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const outputReadFailure = PlatformError.systemError({
        _tag: "Unknown",
        module: "ChildProcess",
        method: "fromReadable(stdout)",
        description: "stdout read failed",
      });
      const spawnerWithFailingOutput = ChildProcessSpawner.make((command) =>
        Effect.map(spawner.spawn(command), (handle) =>
          ChildProcessSpawner.makeHandle({ ...handle, stdout: Stream.fail(outputReadFailure) }),
        ),
      );
      const [elapsed, failure] = yield* Task.exec("sleep", ["30"]).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawnerWithFailingOutput),
        failureAsValue,
        Effect.timed,
      );
      expect(Duration.toMillis(elapsed)).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
      expect(failure).toBe(outputReadFailure);
    }),
  ),
);

export const everyStandardInputFailure = makeTest(
  "a standard input stream that fails in several ways at once prints each failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Cause, Effect, Stream } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("cat", [], {
  stdin: Stream.failCause(Cause.combine(Cause.die(new Error("first input failure")), Cause.die(new Error("second input failure")))),
}) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(
        result.standardError
          .split("\n")
          .filter((line) => line.startsWith("error: [main] main.ts:")),
      ).toEqual([
        "error: [main] main.ts:4:55: Error: first input failure",
        "error: [main] main.ts:4:100: Error: second input failure",
      ]);
    }),
  ),
);

export const signalKilledCommandWithBackgroundChild = makeTest(
  "a command killed by a signal fails at once, even while a background child still holds its output open",
  Effect.scoped(
    Effect.gen(function* () {
      const [elapsed, failure] = yield* Task.exec("sh", ["-c", "sleep 6 & kill -9 $$"]).pipe(
        failureAsValue,
        Effect.timed,
      );
      expect(Duration.toMillis(elapsed)).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
      expect(failure.message).toBe("sh was killed by signal SIGKILL");
    }),
  ),
);

export const successfulCommandWithBackgroundChild = makeTest(
  "a command that exits 0 returns its output at once with its default cleanup limit, after printing that its output was still open and that it sent SIGTERM to the background child it left, through Task.exec and through ChildProcess",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const exec = Task.make({ name: "exec", run: Task.exec("sh", ["-c", "sleep 600 & echo $! > exec-pid; printf finished"]).pipe(Effect.flatMap((output) => Effect.log(\`output: \${output}\`))) });
export const child = Task.make({ name: "child", run: Effect.scoped(ChildProcess.make("sh", ["-c", "sleep 600 & echo $! > child-pid; printf finished"]).pipe(
  Effect.flatMap((spawned) => Stream.mkString(Stream.decodeText(spawned.stdout))),
  Effect.flatMap((output) => Effect.log(\`output: \${output}\`)),
)) });
export default Task.make({ name: "main", run: Task.all([exec, child]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      yield* Effect.forEach(
        ["exec", "child"],
        (taskName) =>
          Effect.gen(function* () {
            const sleep = (yield* read(`${project.directory}/${taskName}-pid`)).trim();
            expect(ran.standardError).toContain(
              `warning: [${taskName}] sh exited, but its output was still open 100ms later, so @sshkeda/task sent SIGTERM to the processes it left in its process group: ${sleep} (sleep)\n`,
            );
            expect(ran.standardError).toContain(`[${taskName}] output: finished\n`);
            const taskSpan = yield* readTaskSpan(project, ran, taskName);
            expect(readAttribute(taskSpan?.attributes ?? [], "sshkeda_task.state")).toBe("passed");
            expect(
              (Number(taskSpan?.endTimeUnixNano) - Number(taskSpan?.startTimeUnixNano)) /
                NANOSECONDS_PER_MILLISECOND,
            ).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
            expect(yield* isProcessAlive(Number(sleep))).toBe(false);
          }),
        { discard: true },
      );
    }),
  ),
);

export const helperHoldingOutputAfterTheCommandExits = makeTest(
  "a helper process that passes on a command's output and then keeps it open is named in the notice and sent SIGTERM, and everything it passed on before that is returned",
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("bash", ["-c", "exec > >(exec perl -e 'open my $f, q(>), q(helper-pid) or die; print $f $$; close $f; $| = 1; while (<STDIN>) { print qq([helper] $_) } sleep 600'); until [ -s helper-pid ]; do sleep 0.01; done; echo finished"]).pipe(Effect.flatMap((output) => Effect.log(\`output: \${output.trim()}\`))) });`,
    });
    const ran = yield* runTaskProcess(project, "main.ts");
    expect(ran.exitCode, ran.standardError).toBe(0);
    const helper = (yield* read(`${project.directory}/helper-pid`)).trim();
    expect(ran.standardError).toMatch(
      new RegExp(
        `^warning: \\[main\\] bash exited, but its output was still open 100ms later, so @sshkeda/task sent SIGTERM to the processes it left in its process group: .*\\b${helper} \\(perl[^)]*\\)`,
        "mu",
      ),
    );
    expect(ran.standardError).toContain("[main] output: [helper] finished\n");
    expect(yield* isProcessAlive(Number(helper))).toBe(false);
  }),
);

export const successfulCommandWithEscapedBackgroundChild = makeTest(
  "a command that exits 0 returns its output once its cleanup limit passes, even while a background child that left its process group holds its output open",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "perl -MPOSIX -e 'defined POSIX::setsid() or die; open my $f, q(>), q(escaped) or die; close $f; exec qw(sleep 600)' & echo $! > pid; until [ -e escaped ]; do sleep 0.01; done; printf finished"]).pipe(Effect.flatMap((output) => Effect.log(\`output: \${output}\`))), timeout: "20 seconds", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const sleep = Number((yield* read(`${project.directory}/pid`)).trim());
      yield* Effect.addFinalizer(() =>
        isProcessAlive(sleep).pipe(
          Effect.flatMap((alive) =>
            alive ? Task.exec("kill", ["-KILL", String(sleep)]) : Effect.void,
          ),
          failTestCleanup(`kill test process ${sleep}`),
        ),
      );
      expect(yield* exists(`${project.directory}/escaped`)).toBe(true);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] output: finished");
    }),
  ),
);

export const successfulCommandWithEscapedAndGroupBackgroundChildren = makeTest(
  "a command that exits 0 returns its output once its cleanup limit passes, even while a background child that left its process group and one that stayed in it hold its output open",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "perl -MPOSIX -e 'defined POSIX::setsid() or die; open my $f, q(>), q(escaped) or die; close $f; exec qw(sleep 600)' & echo $! > pid; until [ -e escaped ]; do sleep 0.01; done; sleep 600 & printf finished"]).pipe(Effect.flatMap((output) => Effect.log(\`output: \${output}\`))), timeout: "20 seconds", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const sleep = Number((yield* read(`${project.directory}/pid`)).trim());
      yield* Effect.addFinalizer(() =>
        isProcessAlive(sleep).pipe(
          Effect.flatMap((alive) =>
            alive ? Task.exec("kill", ["-KILL", String(sleep)]) : Effect.void,
          ),
          failTestCleanup(`kill test process ${sleep}`),
        ),
      );
      expect(yield* exists(`${project.directory}/escaped`)).toBe(true);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] output: finished");
    }),
  ),
);

export const outputReadLongAfterExit = Task.make({
  name: "a command's output read after its cleanup limit has passed since it exited still holds everything it printed",
  run: Effect.scoped(
    Effect.gen(function* () {
      const command = yield* ChildProcess.make("printf", ["finished"], {
        forceKillAfter: "100 millis",
      });
      expect(Number(yield* command.exitCode)).toBe(0);
      yield* Task.exec("sleep", ["1"]);
      expect(yield* command.stdout.pipe(Stream.decodeText, Stream.mkString)).toBe("finished");
    }),
  ),
});

export const stoppedBackgroundChildCleansUp = makeTest(
  "a background child that stopped itself and holds a successful command's output open is continued after SIGTERM, so its TERM handler's output reaches Task.exec",
  Effect.gen(function* () {
    const output = yield* Task.exec("sh", [
      "-c",
      "sh -c 'trap \"echo cleaned; exit 0\" TERM; kill -STOP $$' & until ps -o stat= -p $! | grep -q T; do sleep 0.01; done; echo finished",
    ]);
    expect(output).toBe("finished\ncleaned\n");
  }),
  "30 seconds",
);

const HELPER_PROCESS_LINES = 20_000;

export const outputThroughHelperProcess = makeTest(
  "a command that sends its output through a helper process returns everything the helper passes on after the command exits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("bash", ["-c", "exec > >(sed -u 's/^/[build] /'); seq 1 ${HELPER_PROCESS_LINES}"]).pipe(
  Effect.flatMap((output) => {
    const lines = output.split("\\n").filter((line) => line !== "");
    return Effect.log(\`returned \${lines.length} lines, the last \${lines.at(-1)}\`);
  }),
) });
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(
        ran.standardError.split("\n").find((line) => line.startsWith("[main] returned ")),
      ).toBe(
        `[main] returned ${HELPER_PROCESS_LINES} lines, the last [build] ${HELPER_PROCESS_LINES}`,
      );
    }),
  ),
);

export const successfulCommandWithBackgroundChildIgnoringTerm = makeTest(
  "a background child that ignores SIGTERM and holds a successful command's output open is killed and reported once the SIGKILL grace period passes, and Task.exec returns its output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "trap '' TERM; sleep 600 & echo $! > pid; printf finished"]).pipe(Effect.flatMap((output) => Effect.log(\`output: \${output}\`))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const sleep = (yield* read(`${project.directory}/pid`)).trim();
      expect(ran.standardError).toContain(`killed: [main] ${sleep} (sleep)`);
      expect(ran.standardError).toContain("[main] output: finished");
      expect(yield* isProcessAlive(Number(sleep))).toBe(false);
    }),
  ),
  "30 seconds",
);

export const outputHeldUnderInfiniteCleanup = makeTest(
  "with an infinite cleanup limit, Task.exec still returns the output of a command whose background child ignores SIGTERM or leaves its session and holds the output open, and the child is killed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Duration, Effect } from "effect";
import { Task } from "@sshkeda/task";
const ignoresTerm = Task.exec("sh", ["-c", "trap '' TERM; sleep 600 & echo $! > ignores-term-pid; printf ignored"]);
const leavesSession = Task.exec("perl", ["-MPOSIX", "-e", "if (fork) { select(undef, undef, undef, 0.01) until -s 'left-session-pid'; print 'left'; exit } POSIX::setsid(); open my $file, '>', 'left-session-pid.tmp'; print $file $$; close $file; rename 'left-session-pid.tmp', 'left-session-pid'; sleep 600"]);
export default Task.make({ name: "main", run: Effect.all([ignoresTerm, leavesSession]).pipe(Effect.flatMap((outputs) => Effect.log(\`outputs: \${outputs.join(" ")}\`))), cleanup: Duration.infinity });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] outputs: ignored left");
      const ignoresTerm = (yield* read(`${project.directory}/ignores-term-pid`)).trim();
      const leftSession = (yield* read(`${project.directory}/left-session-pid`)).trim();
      expect(ran.standardError).toContain(`killed: [main] ${ignoresTerm} (sleep)`);
      expect(ran.standardError).toContain(
        "warning: [main] perl exited, but its output was still open 100ms later, and no process it left in its process group is still running, so another process holds it; @sshkeda/task waits up to 5s for it to close\n",
      );
      expect(yield* isProcessAlive(Number(ignoresTerm))).toBe(false);
      expect(yield* isProcessAlive(Number(leftSession))).toBe(false);
    }),
  ),
  "1 minute",
);

const MORE_CHARACTERS_THAN_ONE_STRING_HOLDS = 2_147_483_648;

export const outputTooLong = makeTest(
  "a command that prints more than one string can hold fails with an error naming the command, even when it exits 0",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.asVoid(Task.exec("sh", ["-c", "yes 0123456789 | head -c ${MORE_CHARACTERS_THAN_ONE_STRING_HOLDS}"], { secret: true })) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError.split("\n")).toContain(
        "error: [main] main.ts:3:21: CommandOutputTooLong: the standard output of sh is too long to hold in one string",
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
    }),
  ),
  "2 minutes",
);
