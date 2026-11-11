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
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

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
    const failure = yield* Task.exec("sh", ["-c", "printf error-output >&2; exit 7"]).pipe(
      failureAsValue,
    );
    expect(failure.message).toContain("error-output");
  }),
});

export const separateStreams = Task.make({
  name: "a failed command keeps its standard output and standard error apart and exact, and its message shows both",
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
            `${failure.command} exited with code 1\n{"diagnostics":[]}\nprogress line`,
          );
        }),
      ),
    );
  }),
});

export const emptyStandardOutput = Task.make({
  name: "a failed command that prints nothing on standard output keeps it empty and leaves it out of the message",
  run: Task.exec("sh", ["-c", String.raw`printf 'only error\n' >&2; exit 2`]).pipe(
    Effect.flatMap((output) => Effect.die(`the command succeeded and printed ${output}`)),
    Effect.catchTag("CommandFailed", (failure) =>
      Effect.sync(() => {
        expect(failure.standardOutput).toBe("");
        expect(failure.standardError).toBe("only error\n");
        expect(failure.message).toBe(`${failure.command} exited with code 2\nonly error`);
      }),
    ),
  ),
});

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
  "a command killed by a signal right after printing still logs everything it printed",
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
      for (const attempt of KILLED_COMMAND_ATTEMPTS) {
        expect(ran.standardError).toContain(`[main] out-${attempt}\n`);
        expect(ran.standardError).toContain(`[main] err-${attempt}\n`);
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
      expect(ran.standardError).toContain(
        "[main] error CommandFailed: sh was killed by signal SIGSEGV\nfailed: main (1 of 1 task), ",
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
export default Task.make({ name: "main", run: Task.exec("bun", ["inner.ts"], { cwd: "nested" }) });`,
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
      const first = yield* runTaskProcess(project, "outer/main.ts", taskConfig);
      expect(first.exitCode, first.standardError).toBe(0);
      expect(first.standardError).toContain(
        "[main] [inner] inner sees workers=3 setup=50 seconds\n",
      );
      const fileSystem = yield* FileSystem.FileSystem;
      expect(
        yield* fileSystem.readDirectory(`${project.directory}/state/traces/nested`),
      ).toHaveLength(1);
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
      expect(first.standardError).toContain(`\n[main] ${printed("a")}\n`);
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
      expect(result.standardError).toContain(
        "[piped] error CommandPipelineRefused: task's process service refused the pipeline echo | cat\n",
      );
      expect(result.standardError).toContain(
        "[shell] error CommandOptionsRefused: task's process service refused shell for echo\n",
      );
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
    }),
  ),
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
export const started = Task.make({ name: "started", run: Task.exec("sh", ["build.sh"], { cwd: "tools" }) });
export default Task.make({ name: "main", run: Task.allSettled([missingDirectory, fileDirectory, missingProgram, caught, started]) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(result.standardError).toContain(
        "[missing-directory] error CommandNotStarted: could not start sh: the working directory tool doesn't exist\n",
      );
      expect(result.standardError).toContain(
        "[file-directory] error CommandNotStarted: could not start sh: the working directory a-file.txt is not a directory\n",
      );
      expect(result.standardError).toContain(
        '[missing-program] error CommandNotStarted: could not start no-such-program: Executable not found in $PATH: "no-such-program"\n',
      );
      expect(result.standardError).toContain("[caught] caught: sh\n");
      expect(result.standardError).toContain("[started] built\n");
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
    }),
  ),
);

export const failingStandardInputStreams = makeTest(
  "a standard input stream that fails or dies fails the run with CommandStandardInputFailed, next to CommandFailed when the command it stopped still exits non-zero, while a command that stops reading its standard input still passes",
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
      expect(result.standardError).toContain(
        "[failing] error CommandStandardInputFailed: the standard input stream of cat failed with input file missing\n",
      );
      expect(result.standardError).toContain(
        "[dying] error CommandStandardInputFailed: the standard input stream of cat failed with input stream died\n",
      );
      expect(
        result.standardError
          .split("\n")
          .filter((line) => line.startsWith("[dying-and-exiting] ") && line.includes(" error ")),
      ).toEqual([
        "[dying-and-exiting] error CommandFailed: sh exited with code 3",
        "[dying-and-exiting] error CommandStandardInputFailed: the standard input stream of sh failed with input stream died",
      ]);
      expect(result.standardError).toMatch(/^\[stops-reading\] passed in /mu);
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
    }),
  ),
);

const STANDARD_INPUT_CHUNK_BYTES = 65_536;

const PROMPT_FAILURE_LIMIT_MILLISECONDS = 3000;

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
          .filter((line) => line.startsWith("[main] ") && line.includes(" error ")),
      ).toEqual([
        "[main] error CommandStandardInputFailed: the standard input stream of cat failed with the upstream socket closed",
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
          .filter((line) => line.startsWith("[through-") && line.includes(" error "))
          .toSorted(),
      ).toEqual([
        "[through-run-command-after-exit] error CommandStandardInputFailed: the standard input stream of sh was interrupted without a failure",
        "[through-run-command] error CommandStandardInputFailed: the standard input stream of cat was interrupted without a failure",
        "[through-stream-after-exit] error CauseWithoutFailure: was interrupted without a failure",
        "[through-stream] error CauseWithoutFailure: was interrupted without a failure",
      ]);
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
        "the standard input stream of sleep failed with input file missing",
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
          .filter((line) => line.startsWith("[main] ") && line.includes(" error ")),
      ).toEqual([
        "[main] error CommandStandardInputFailed: the standard input stream of sh failed with input stream died",
      ]);
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
          .filter((line) => line.startsWith("[main] ") && line.includes(" error ")),
      ).toEqual([
        "[main] error CommandStandardInputFailed: the standard input stream of cat failed with first input failure",
        "[main] error CommandStandardInputFailed: the standard input stream of cat failed with second input failure",
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
  "a command that exits 0 returns at once with its output and ends the background child it left holding its output open, through Task.exec and through ChildProcess",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const directory = yield* makeTemporaryDirectory();
      const [elapsed, output] = yield* Task.exec("sh", [
        "-c",
        `sleep 600 & echo $! > '${directory}/pid'; printf finished`,
      ]).pipe(Effect.timed);
      expect(Duration.toMillis(elapsed)).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
      expect(output).toBe("finished");
      expect(
        yield* isProcessAlive(
          Number((yield* fileSystem.readFileString(`${directory}/pid`)).trim()),
        ),
      ).toBe(false);
      const [childElapsed, childOutput] = yield* ChildProcess.make("sh", [
        "-c",
        "sleep 600 & printf '%s finished' $!",
      ]).pipe(
        Effect.flatMap((child) => Stream.mkString(Stream.decodeText(child.stdout))),
        Effect.timed,
      );
      expect(Duration.toMillis(childElapsed)).toBeLessThan(PROMPT_FAILURE_LIMIT_MILLISECONDS);
      const [backgroundChild, finished] = childOutput.split(" ");
      expect(finished).toBe("finished");
      expect(yield* isProcessAlive(Number(backgroundChild))).toBe(false);
    }),
  ),
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
      expect(ran.standardError).toContain(`killed ${sleep} (sleep), started by "main"`);
      expect(ran.standardError).toContain("[main] output: finished");
      expect(yield* isProcessAlive(Number(sleep))).toBe(false);
    }),
  ),
  "30 seconds",
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
      expect(ran.standardError).toContain(
        "[main] error CommandOutputTooLong: the standard output of sh is too long to hold in one string\n",
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
    }),
  ),
  "2 minutes",
);
