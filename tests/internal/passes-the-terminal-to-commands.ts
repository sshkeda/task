import { expect } from "bun:test";

import { Duration, Effect, FileSystem, Pull, Queue, Ref } from "effect";
import type { Cause } from "effect";

import { runInTerminal, TerminalCommandEnded } from "../../src/shared/run-in-terminal.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { expectOneOfTheOrders } from "../shared/expect-one-of-the-orders.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import type { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { withRunLinePathNamed, withSecondsNamed } from "../shared/name-output-files.ts";
import { makeTaskProcessEnvironment, runTaskProcess } from "../shared/run-task-processes.ts";
import { startTerminalCommand } from "../shared/start-terminal-commands.ts";
import { NeverPrinted } from "../shared/test-errors.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const CTRL_C = "\u0003";

const RESIZED_COLUMNS = 100;

const RESIZED_ROWS = 30;

const PROMPT = `console.log(\`terminal \${process.stdin.isTTY} \${process.stdout.isTTY} \${process.stderr.isTTY}\`);
process.stdout.write("name? ");
for await (const line of console) {
  console.log(\`hello \${line}\`);
  break;
}`;

const ROOT_TASK_PROMPTING = `import { Task } from "@sshkeda/task";
export const greet = Task.make({
  name: "greet",
  run: Task.exec("${process.execPath}", [\`\${import.meta.dir}/prompt.ts\`], { stdio: "inherit" }),
});
export default greet;`;

const startInTerminal = (
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
  file: string,
) =>
  Effect.gen(function* () {
    const environment = yield* makeTaskProcessEnvironment(project);
    const chunks = yield* Queue.unbounded<string, Cause.Done>();
    const decoder = new TextDecoder();
    const child = yield* startTerminalCommand([process.execPath, `${project.directory}/${file}`], {
      workingDirectory: project.directory,
      environment,
      terminal: {
        data: (_terminal, data) => {
          Queue.offerUnsafe(chunks, decoder.decode(data, { stream: true }));
        },
        exit: () => {
          Queue.endUnsafe(chunks);
        },
      },
    });
    const shown = yield* Ref.make("");
    const showUntil = (isDone: (text: string) => boolean): Effect.Effect<string> =>
      Effect.gen(function* () {
        const text = yield* Ref.get(shown);
        if (isDone(text)) {
          return text;
        }
        const ended = yield* Queue.take(chunks).pipe(
          Effect.flatMap((chunk) => Ref.update(shown, (earlier) => `${earlier}${chunk}`)),
          Effect.as(false),
          Pull.catchDone(() => Effect.succeed(true)),
        );
        return ended ? text : yield* showUntil(isDone);
      });
    return {
      waitForText: (wanted: string) =>
        showUntil((text) => text.includes(wanted)).pipe(
          Effect.flatMap((text) =>
            text.includes(wanted) ? Effect.void : new NeverPrinted({ wanted, seen: text }),
          ),
        ),
      type: (text: string) =>
        Effect.sync(() => {
          child.terminal?.write(text);
        }),
      interrupt: Effect.sync(() => process.kill(child.pid, "SIGINT")),
      resize: (columns: number, rows: number) =>
        Effect.sync(() => {
          child.terminal?.resize(columns, rows);
        }),
      waitForExit: Effect.gen(function* () {
        const exitCode = yield* Effect.promise(() => child.exited);
        return { exitCode, shown: yield* showUntil(() => false) };
      }),
    };
  });

export const interactiveCommand = makeTest(
  'a command run with stdio: "inherit" in the root task gets the run\'s terminal, so it reads a typed line and sees a TTY',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": ROOT_TASK_PROMPTING, "prompt.ts": PROMPT });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("name? ");
      yield* terminal.type("Ada\r");
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("terminal true true true");
      expect(exited.shown).toContain("hello Ada");
      expect(exited.shown).toContain("passed: 1 task, ");
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

export const controllingTerminal = makeTest(
  "a terminal command can open /dev/tty, as password prompts do, and hears the terminal resize",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const login = Task.make({
  name: "login",
  run: Task.exec(
    "sh",
    [
      "-c",
      'exec 3<>/dev/tty; printf "password: " >&3; read -r password <&3; echo "read $password from /dev/tty"; trap \\'echo "resized to $(stty size <&3)"; exit 0\\' WINCH; echo "waiting for a resize"; while :; do sleep 0.1; done',
    ],
    { stdio: "inherit" },
  ),
});
export default login;`,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("password: ");
      yield* terminal.type("hunter2\r");
      yield* terminal.waitForText("waiting for a resize");
      yield* terminal.resize(RESIZED_COLUMNS, RESIZED_ROWS);
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("read hunter2 from /dev/tty");
      expect(exited.shown).toContain(`resized to ${RESIZED_ROWS} ${RESIZED_COLUMNS}`);
      expect(exited.shown).toContain("passed: 1 task, ");
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

export const ctrlCWhileInteractive = makeTest(
  "Ctrl-C while a terminal command waits for input stops the run and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": ROOT_TASK_PROMPTING, "prompt.ts": PROMPT });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("name? ");
      yield* terminal.type(CTRL_C);
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("terminal true true true");
      expect(exited.shown).toMatch(
        /\r\nstopped: \[greet\] \(1 of 1 task\), [\d.]+s\r\nrun: \S+\/\r\n$/u,
      );
      expect(exited.shown).not.toContain("interrupted by signal SIGINT");
      expect(exited.shown).not.toContain("hello");
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    }),
  ),
);

const MISSING_THEN_PROMPTING = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "missing",
  run: Effect.gen(function* () {
    const failed = yield* Task.exec("/nonexistent/command", [], { stdio: "inherit" }).pipe(Effect.flip);
    yield* Effect.log(\`failed \${failed._tag}\`);
    const supervisor = yield* Task.exec("ps", ["-o", "pgid=,tpgid=", "-p", String(process.ppid)]);
    yield* Effect.log(\`supervisor \${supervisor.trim()}\`);
    yield* Task.exec("sh", ["-c", 'printf "name? "; read -r name; echo "hello $name"'], { stdio: "inherit" });
  }),
});`;

export const missingTerminalCommand = makeTest(
  "a terminal command whose absolute path does not exist fails as not started, the supervisor's group holds the terminal's foreground again, and a following terminal command still reads from the terminal",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": MISSING_THEN_PROMPTING });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("name? ");
      yield* terminal.type("Ada\r");
      const exited = yield* terminal.waitForExit;
      const supervisor = /supervisor (?<group>\d+) +(?<foreground>\d+)/u.exec(exited.shown)?.groups;
      expect(exited.shown).toContain("[missing] failed CommandNotStarted");
      expect(supervisor?.["foreground"]).toBe(supervisor?.["group"]);
      expect(exited.shown).toContain("hello Ada");
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

const LONG_ARGUMENT_COUNT = 2000;
const LONG_VARIABLE_COUNT = 2000;
const LONG_COMMAND_COUNT = 40;

const RECEIVING_LONG_ARGUMENTS = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const words = Array.from({ length: ${LONG_ARGUMENT_COUNT} }, (_, index) => \`argument-\${index}-\${"x".repeat(50)}\`);
const env = Object.fromEntries(Array.from({ length: ${LONG_VARIABLE_COUNT} }, (_, index) => [\`NAME_\${index}\`, "y".repeat(200)]));
const check = 'x=$(printf "%050d" 0 | tr 0 x); i=0; for word in "$@"; do [ "$word" = "argument-$i-$x" ] || exit 1; i=$((i+1)); done; echo "intact $# $(env | grep -c "^NAME_")"';
export default Task.make({
  name: "long",
  run: Effect.forEach(
    Array.from({ length: ${LONG_COMMAND_COUNT} }),
    () => Task.exec("sh", ["-c", check, "sh", ...words], { stdio: "inherit", env }),
    { discard: true },
  ),
});`;

export const longTerminalCommand = makeTest(
  "terminal commands with thousands of long arguments and environment variables receive every one intact",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": RECEIVING_LONG_ARGUMENTS });
      const terminal = yield* startInTerminal(project, "main.ts");
      const exited = yield* terminal.waitForExit;
      expect(
        countOccurrences(exited.shown, `intact ${LONG_ARGUMENT_COUNT} ${LONG_VARIABLE_COUNT}\r\n`),
      ).toBe(LONG_COMMAND_COUNT);
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

export const ctrlCAgainAfterTerminalCommand = makeTest(
  "a second Ctrl-C after a first one ended a terminal command reaches the run again and forces its stop",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "greet",
  cleanup: "1 minute",
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.log("cleaning up").pipe(Effect.andThen(Effect.never)));
    yield* Task.exec("${process.execPath}", [\`\${import.meta.dir}/prompt.ts\`], { stdio: "inherit" });
  }),
});`,
        "prompt.ts": PROMPT,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("name? ");
      yield* terminal.type(CTRL_C);
      yield* terminal.waitForText("cleaning up");
      yield* terminal.type(CTRL_C);
      const exited = yield* terminal.waitForExit;
      expect(countOccurrences(exited.shown, "stopping: press ctrl-c again to force")).toBe(1);
      expect(exited.shown).toContain("its cleanup was cut off because the stop was forced");
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    }),
  ),
);

const INTERRUPT_THE_FOREGROUND_WHILE_THE_SUPERVISOR_IS_STOPPED = `supervisor=$PPID
(trap "" INT; until [ "$(ps -o stat= -p $$ | cut -c1)" = Z ]; do :; done; kill -CONT "$supervisor") &
kill -STOP "$supervisor"
until [ "$(ps -o stat= -p "$supervisor" | cut -c1)" = T ]; do :; done
kill -INT 0
`;

export const foregroundInterruptedWhileSupervisorStopped = makeTest(
  "SIGINT sent to the terminal's foreground process group while the supervisor is stopped stops the run once when the supervisor continues",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({
  name: "interrupt",
  run: Task.exec("sh", [\`\${import.meta.dir}/interrupt.sh\`], { stdio: "inherit" }),
});`,
        "interrupt.sh": INTERRUPT_THE_FOREGROUND_WHILE_THE_SUPERVISOR_IS_STOPPED,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("stopped: [interrupt] (1 of 1 task), ");
      expect(countOccurrences(exited.shown, "stopping: press ctrl-c again to force")).toBe(1);
      expect(exited.shown).not.toContain("forced");
      expect(exited.shown).not.toContain("interrupted by signal SIGINT");
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    }),
  ),
);

export const stopWaitsForTerminalCommand = makeTest(
  "SIGINT sent to the run from outside sends SIGTERM to a terminal command that ignores SIGINT and waits for its slow cleanup before the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const wait = Task.make({
  name: "wait",
  run: Task.exec(
    "sh",
    [
      "-c",
      'trap "" INT; trap \\'sleep 1; echo "cleaned up"; exit 0\\' TERM; echo "waiting"; while :; do sleep 0.1; done',
    ],
    { stdio: "inherit" },
  ),
});
export default wait;`,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("waiting");
      yield* terminal.interrupt;
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("cleaned up");
      expect(exited.shown).toContain("stopped: [wait] (1 of 1 task), ");
      expect(exited.shown).not.toContain("killed");
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    }),
  ),
);

export const ctrlCCaughtByTerminalCommand = makeTest(
  "Ctrl-C reaches only the terminal command, so a command that ignores it keeps the run going, as in a shell",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({
  name: "ignore",
  run: Task.exec("sh", ["-c", 'trap "" INT; printf "name? "; read -r name; echo "hello $name"'], { stdio: "inherit" }),
});`,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      yield* terminal.waitForText("name? ");
      yield* terminal.type(CTRL_C);
      yield* terminal.type("Ada\r");
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("hello Ada");
      expect(exited.shown).toContain("passed: 1 task, ");
      expect(exited.shown).not.toContain("stopping");
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

const SEES_ITS_GROUP_IN_THE_FOREGROUND = `use POSIX;
print((tcgetpgrp(0) == getpgrp()) ? "foreground\\n" : "background\\n");
`;

const PROBE_COUNT = 20;

export const foregroundBeforeFirstInstruction = makeTest(
  "a terminal command's process group already holds the terminal's foreground when the command starts, every time",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "probe",
  run: Effect.forEach(
    Array.from({ length: ${PROBE_COUNT} }),
    () => Task.exec("perl", [\`\${import.meta.dir}/probe.pl\`], { stdio: "inherit" }),
    { discard: true },
  ),
});`,
        "probe.pl": SEES_ITS_GROUP_IN_THE_FOREGROUND,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      const exited = yield* terminal.waitForExit;
      expect(countOccurrences(exited.shown, "foreground\r\n")).toBe(PROBE_COUNT);
      expect(exited.shown).not.toContain("background");
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

export const terminalForeground = makeTest(
  "a terminal command runs in its own process group, which holds the terminal's foreground until the command ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "groups",
  run: Effect.gen(function* () {
    yield* Task.exec("sh", ["-c", 'echo "command $$ $(ps -o pgid=,tpgid= -p $$)"'], { stdio: "inherit" });
    const supervisor = yield* Task.exec("ps", ["-o", "pgid=,tpgid=", "-p", String(process.ppid)]);
    yield* Effect.log(\`supervisor \${supervisor.trim()}\`);
  }),
});`,
      });
      const terminal = yield* startInTerminal(project, "main.ts");
      const exited = yield* terminal.waitForExit;
      const command = /command (?<process>\d+) +(?<group>\d+) +(?<foreground>\d+)/u.exec(
        exited.shown,
      )?.groups;
      const supervisor = /supervisor (?<group>\d+) +(?<foreground>\d+)/u.exec(exited.shown)?.groups;
      expect(command?.["group"]).toBe(command?.["process"]);
      expect(command?.["foreground"]).toBe(command?.["process"]);
      expect(supervisor?.["foreground"]).toBe(supervisor?.["group"]);
      expect(supervisor?.["group"]).not.toBe(command?.["group"]);
      expect(exited.exitCode).toBe(0);
    }),
  ),
);

export const terminalCommandsTakeTurns = makeTest(
  "terminal commands started together in the root task take turns, and without a TTY their output goes straight to the run's output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const takeTurn = (name: string) =>
  Task.exec(
    "sh",
    ["-c", \`echo "\${name} started, terminal $(test -t 1 && echo yes || echo no)"; sleep 0.5; echo "\${name} ended"\`],
    { stdio: "inherit" },
  );
export default Task.make({
  name: "main",
  run: Effect.all([takeTurn("first"), takeTurn("second")], { concurrency: "unbounded", discard: true }),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardOutput).toBe(
        "first started, terminal no\nfirst ended\nsecond started, terminal no\nsecond ended\n",
      );
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const terminalCommandsLeftToBun = makeTest(
  "the supervisor's orphan reaper leaves terminal commands to Bun, so each of many short terminal commands reports its own exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.forEach(Array.from({ length: 300 }), () => Task.exec("true", [], { stdio: "inherit" }), {
    discard: true,
  }),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("ECHILD");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const terminalWorkingDirectoryMissing = makeTest(
  "a terminal command whose working directory doesn't exist fails with the same error as other commands",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "true"], { cwd: "missing", stdio: "inherit" }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(
        /^run: \S+\/\nerror: \[main\] main\.ts:2:52: WorkingDirectoryUnusable: the working directory missing doesn't exist\nerror: \[main\] could not start sh\nerror: \[main\] its output is in \S+\/1\.output\nended: \[main\], [\d.]+s, failed\nrecap: \[main\] 1\.output \| main\.ts:2:52: WorkingDirectoryUnusable: the working directory missing doesn't exist\nfailed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

const NULL_BYTE_COMMANDS = {
  "argument.ts": { word: `"one\\0two"`, options: `{ stdio: "inherit" }` },
  "env.ts": { word: `"plain"`, options: `{ stdio: "inherit", env: { VALUE: "one\\0two" } }` },
  "cwd.ts": { word: `"plain"`, options: `{ stdio: "inherit", cwd: "/tm\\0p" }` },
} as const;

const errorLinesOf = (shown: string) =>
  shown
    .replaceAll("\r\n", "\n")
    .split("\n")
    .filter((line) => line.startsWith("error: ") && !line.includes(" its output is in "));

export const terminalNullBytes = makeTest(
  "a terminal command with a null byte in an argument, an env value or its working directory fails before it runs, with the same error as without a terminal",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(
        Object.fromEntries(
          Object.entries(NULL_BYTE_COMMANDS).map(([file, { word, options }]) => [
            file,
            `import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Task.exec("/bin/sh", ["-c", 'touch "$0"; echo command-ran', \`\${import.meta.dir}/ran\`, ${word}], ${options}),
});`,
          ]),
        ),
      );
      const fileSystem = yield* FileSystem.FileSystem;
      for (const file of Object.keys(NULL_BYTE_COMMANDS)) {
        const piped = yield* runTaskProcess(project, file);
        const terminal = yield* startInTerminal(project, file);
        const exited = yield* terminal.waitForExit;
        expect(errorLinesOf(piped.standardError).join("\n")).toContain("without null bytes");
        expect(errorLinesOf(exited.shown)).toEqual(errorLinesOf(piped.standardError));
        expect(exited.shown).not.toContain("command-ran");
        expect(yield* fileSystem.exists(`${project.directory}/ran`)).toBe(false);
        expect(exited.exitCode).toBe(1);
      }
    }),
  ),
);

export const terminalInfiniteCleanup = makeTest(
  "a terminal command runs in a task with an infinite cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Duration } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("echo", ["ran"], { stdio: "inherit" }), cleanup: Duration.infinity });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(/^run: \S+\/\npassed: 1 task, [\d.]+s\nrun: \S+\/$/u);
      expect(ran.standardOutput).toContain("ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const terminalOutsideRootTask = makeTest(
  "a task other than the root task fails with StandardInputReadOutsideRootTask when it runs a terminal command",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const prompt = Task.make({ name: "prompt", run: Task.exec("echo", ["prompted"], { stdio: "inherit" }) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* prompt;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const beforeTheEnds = [
        "run: <run folder>/",
        "error: [prompt] main.ts:3:28: StandardInputReadOutsideRootTask: read standard input, which only the root task [main] can read",
      ];
      const promptEnd = [
        "error: [prompt] its output is in <run folder>/2.output",
        "ended: [prompt], <seconds>, failed",
      ];
      const mainEnd = ["ended: [main], <seconds>, failed"];
      const afterTheEnds = [
        "recap: [prompt] 2.output | main.ts:3:28: StandardInputReadOutsideRootTask: read standard input, which only the root task [main] can read",
        "failed: [prompt] (1 of 2 tasks), <seconds>",
        "run: <run folder>/",
      ];
      expectOneOfTheOrders(withRunLinePathNamed(withSecondsNamed(ran.standardError)).split("\n"), [
        [...beforeTheEnds, ...promptEnd, ...mainEnd, ...afterTheEnds],
        [...beforeTheEnds, ...mainEnd, ...promptEnd, ...afterTheEnds],
      ]);
      expect(ran.standardOutput).not.toContain("prompted");
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const terminalOptionTypes = makeTest(
  'Task.exec with a boolean secret option is typed with every output it may return, and stdio: "inherit" takes neither stdin nor secret',
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect, Stream } from "effect";
import { Task } from "@sshkeda/task";
const flag = Math.random() < 1;
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const secretOrNot: string = yield* Task.exec("echo", ["a"], { secret: flag });
  yield* Task.exec("cat", [], { stdio: "inherit", stdin: Stream.empty });
  yield* Task.exec("echo", ["c"], { stdio: "inherit", secret: true });
  yield* Effect.log(secretOrNot);
}) });`),
      );
      expect(failed.message).toContain(
        "main.ts(5,9): error TS2322: Type 'string | Redacted<string>' is not assignable to type 'string'.",
      );
      expect(failed.message).toContain(
        "main.ts(6,51): error TS2561: Object literal may only specify known properties, but 'stdin' does not exist",
      );
      expect(failed.message).toContain(
        "main.ts(7,55): error TS2322: Type 'true' is not assignable to type 'never'.",
      );
    }),
  ),
);

export const terminalCommandSignalNotReceivedByTheRun = makeTest(
  "a terminal command killed by a stop signal this process did not receive ends as signaled at once, even while a run stop is under way",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const ended = yield* runInTerminal(
        Effect.void,
        null,
      )({
        id: "killed-by-itself",
        command: "/bin/sh",
        args: ["-c", "kill -INT $$"],
        env: {},
        cwd: directory,
        cleanup: Duration.seconds(1),
      });
      expect(ended).toEqual(TerminalCommandEnded.cases.Signaled.make({ signal: "SIGINT" }));
    }),
  ),
  "10 seconds",
);

export const terminalCommandKilledBySignalIsCommandFailed = makeTest(
  "a terminal command killed by a signal fails with CommandFailed naming the signal, like a piped command, so catchTag catches it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Task.exec("sh", ["-c", "kill -TERM $$"], { stdio: "inherit" }).pipe(
    Effect.catchTag("CommandFailed", (failed) =>
      Effect.log(\`caught \${failed._tag} \${failed.signal} \${failed.stdio}\`),
    ),
  ),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] caught CommandFailed SIGTERM inherit");
    }),
  ),
);
