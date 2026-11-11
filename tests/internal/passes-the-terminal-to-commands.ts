import { expect } from "bun:test";

import { Effect, Pull, Queue, Ref } from "effect";
import type { Cause } from "effect";

import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import type { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { makeTaskProcessEnvironment, runTaskProcess } from "../shared/run-task-processes.ts";
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
    const child = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.spawn([process.execPath, `${project.directory}/${file}`], {
          cwd: project.directory,
          env: environment,
          terminal: {
            data: (_terminal, data) => {
              Queue.offerUnsafe(chunks, decoder.decode(data, { stream: true }));
            },
            exit: () => {
              Queue.endUnsafe(chunks);
            },
          },
        }),
      ),
      (spawned) =>
        Effect.sync(() => {
          spawned.kill("SIGKILL");
        }).pipe(Effect.andThen(Effect.promise(() => spawned.exited))),
    );
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
      expect(exited.shown).toContain("[greet] passed in");
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
      expect(exited.shown).toContain("[login] passed in");
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
      expect(exited.shown).toContain("error StopRequested: received SIGINT");
      expect(exited.shown).toContain("failed: 0 of 1 task, 1 stopped");
      expect(exited.shown).not.toContain("interrupted by signal SIGINT");
      expect(exited.shown).not.toContain("hello");
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    }),
  ),
);

export const stopWaitsForTerminalCommand = makeTest(
  "Ctrl-C sends SIGTERM to a terminal command that ignores SIGINT and waits for its slow cleanup before the run ends",
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
      yield* terminal.type(CTRL_C);
      const exited = yield* terminal.waitForExit;
      expect(exited.shown).toContain("cleaned up");
      expect(exited.shown).toContain("failed: 0 of 1 task, 1 stopped");
      expect(exited.shown).not.toContain("killed");
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
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
      expect(ran.standardError).toContain(
        "[main] error CommandNotStarted: could not start sh: the working directory missing doesn't exist",
      );
      expect(ran.exitCode).toBe(1);
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
      expect(ran.standardError).not.toContain("error");
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
      expect(ran.standardError).toContain(
        `[prompt] error StandardInputReadOutsideRootTask: read standard input, which only the root task "main" can read`,
      );
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
        "main.ts(7,55): error TS2353: Object literal may only specify known properties, and 'secret' does not exist",
      );
    }),
  ),
);
