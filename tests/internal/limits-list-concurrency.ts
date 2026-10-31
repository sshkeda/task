import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const HELD_TASK = `import { Effect, FileSystem } from "effect";
import { ChildProcess } from "effect/process";
const shell = (script: string) => ChildProcess.make("sh", ["-c", script]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
const held = (name: string) => Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* fileSystem.makeDirectory(\`running/\${name}\`, { recursive: true });
  const running = yield* fileSystem.readDirectory("running");
  yield* Effect.log(\`\${name} started with \${running.length} running\`);
  yield* shell(\`cat release-\${name}\`);
  yield* fileSystem.remove(\`running/\${name}\`, { recursive: true });
});`;

const STARTED_LINE = /\[(?<taskName>[\w-]+)\] [\w-]+ started with (?<runningCount>\d+) running$/u;

const LIMITED_TASK_COUNT = 5;

const ALL_CONCURRENCY_LIMIT = 2;

const ALL_SETTLED_CONCURRENCY_LIMIT = 3;

const UNBOUNDED_TASK_COUNT = 4;

const heldTaskNames = (count: number) =>
  Array.from({ length: count }, (_unused, index) => `held-${index + 1}`);

const heldTaskDefinitions = (taskNames: readonly string[]) =>
  taskNames
    .map(
      (taskName) =>
        `export const ${taskName.replace("-", "")} = Task.make("${taskName}", held("${taskName}"));`,
    )
    .join("\n");

const heldTaskList = (taskNames: readonly string[]) =>
  `[${taskNames.map((taskName) => taskName.replace("-", "")).join(", ")}]`;

const makeReleaseFifos = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  taskNames: readonly string[],
) =>
  Task.exec(
    "mkfifo",
    taskNames.map((taskName) => `release-${taskName}`),
    { cwd: project.directory },
  );

const release = (project: Effect.Success<ReturnType<typeof makeProject>>, taskName: string) =>
  Task.exec("sh", ["-c", `echo > release-${taskName}`], {
    cwd: project.directory,
  });

const driveHeldTasks = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  command: Effect.Success<ReturnType<typeof startTaskProcess>>,
  taskCount: number,
  heldAtOnce: number,
) =>
  Effect.gen(function* () {
    const takeStart = Effect.flatMap(command.waitForLine(" started with "), (line) =>
      Effect.fromNullishOr(STARTED_LINE.exec(line)?.[1]),
    );
    const startedTaskNames = [...(yield* Effect.replicateEffect(takeStart, heldAtOnce))];
    yield* Effect.forEach(
      Array.from({ length: taskCount - heldAtOnce }, (_unused, releasedIndex) => releasedIndex),
      (releasedIndex) =>
        Effect.gen(function* () {
          yield* release(project, yield* Effect.fromNullishOr(startedTaskNames[releasedIndex]));
          startedTaskNames.push(yield* takeStart);
        }),
      { discard: true },
    );
    yield* Effect.forEach(
      startedTaskNames.slice(taskCount - heldAtOnce),
      (taskName) => release(project, taskName),
      { discard: true },
    );
    return startedTaskNames;
  });

const limitedListProject = (
  listFunctionName: "all" | "allSettled",
  taskNames: readonly string[],
  listOptions: string,
) =>
  makeProject({
    "main.ts": `import { Task } from "@sshkeda/task";
${HELD_TASK}
${heldTaskDefinitions(taskNames)}
export const main = Task.make("main", Task.${listFunctionName}(${heldTaskList(taskNames)}${listOptions}));
if (import.meta.main) Task.main(main);`,
  });

const expectHeldAtMost = (
  listFunctionName: "all" | "allSettled",
  listOptions: string,
  taskCount: number,
  heldAtOnce: number,
) =>
  Effect.gen(function* () {
    const taskNames = heldTaskNames(taskCount);
    const project = yield* limitedListProject(listFunctionName, taskNames, listOptions);
    yield* makeReleaseFifos(project, taskNames);
    const command = yield* startTaskProcess(project, "main.ts");
    const startedTaskNames = yield* driveHeldTasks(project, command, taskCount, heldAtOnce);
    const exited = yield* command.waitForExit;
    expect(exited.exitCode).toBe(0);
    expect(startedTaskNames.toSorted()).toEqual(taskNames.toSorted());
    const runningCounts = exited.standardError
      .split("\n")
      .flatMap((line) => {
        const { runningCount } = STARTED_LINE.exec(line)?.groups ?? {};
        return runningCount ?? [];
      })
      .map((runningCount) => Number(runningCount));
    expect(runningCounts).toHaveLength(taskCount);
    expect(Math.max(...runningCounts)).toBe(heldAtOnce);
    expect(exited.standardError).toMatch(
      new RegExp(`^passed: ${taskCount + 1} tasks, [\\d.]+s$`, "mu"),
    );
  });

export const allRunsAtMostTheLimit = Task.make(
  "all with a concurrency limit runs at most that many of its tasks at once",
  Effect.scoped(
    expectHeldAtMost(
      "all",
      `, { concurrency: ${ALL_CONCURRENCY_LIMIT} }`,
      LIMITED_TASK_COUNT,
      ALL_CONCURRENCY_LIMIT,
    ),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const allSettledRunsAtMostTheLimit = Task.make(
  "allSettled with a concurrency limit runs at most that many of its tasks at once",
  Effect.scoped(
    expectHeldAtMost(
      "allSettled",
      `, { concurrency: ${ALL_SETTLED_CONCURRENCY_LIMIT} }`,
      LIMITED_TASK_COUNT,
      ALL_SETTLED_CONCURRENCY_LIMIT,
    ),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const allDefaultIsUnbounded = Task.make(
  "all without options still runs every listed task at once",
  Effect.scoped(expectHeldAtMost("all", "", UNBOUNDED_TASK_COUNT, UNBOUNDED_TASK_COUNT)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const allSettledDefaultIsUnbounded = Task.make(
  "allSettled without options, or with concurrency unbounded, still runs every listed task at once",
  Effect.scoped(
    Effect.all(
      [
        expectHeldAtMost("allSettled", "", UNBOUNDED_TASK_COUNT, UNBOUNDED_TASK_COUNT),
        expectHeldAtMost(
          "allSettled",
          ', { concurrency: "unbounded" }',
          UNBOUNDED_TASK_COUNT,
          UNBOUNDED_TASK_COUNT,
        ),
      ],
      { discard: true },
    ),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const limitedAllSettledReportsEveryFailure = Task.make(
  "allSettled with a concurrency limit still runs every task and reports every failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const one = Task.make("one", Effect.fail(new TaskError({ message: "one broke" })));
export const two = Task.make("two", Effect.log("two ran"));
export const three = Task.make("three", Effect.fail(new TaskError({ message: "three broke" })));
export const four = Task.make("four", Effect.log("four ran"));
export const five = Task.make("five", Effect.fail(new TaskError({ message: "five broke" })));
export const main = Task.make("main", Task.allSettled([one, { two, three }, four, five], { concurrency: 1 }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      for (const failedTaskName of ["one", "three", "five"]) {
        expect(ran.standardError).toMatch(
          new RegExp(
            `^\\[${failedTaskName}\\] \\S+: error TaskError: ${failedTaskName} broke$`,
            "mu",
          ),
        );
      }
      expect(ran.standardError).toContain("[two] two ran");
      expect(ran.standardError).toContain("[four] four ran");
      expect(ran.standardError).toContain("[two] passed in");
      expect(ran.standardError).toContain("[four] passed in");
      expect(ran.standardError).toContain(
        "[main] error SettledTasksFailed: 3 of 5 tasks failed: one, three, five",
      );
      expect(ran.standardError).toMatch(/^failed: one, three, five \(3 of 6 tasks\), [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const nestedLimitedLists = Task.make(
  "limited lists whose tasks yield limited lists finish on one worker, since a waiting parent holds no slot",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const leafA = Task.make("leaf-a", Effect.log("leaf-a ran"));
export const leafB = Task.make("leaf-b", Effect.log("leaf-b ran"));
export const leafC = Task.make("leaf-c", Effect.log("leaf-c ran"));
export const leafD = Task.make("leaf-d", Effect.log("leaf-d ran"));
export const inner = Task.make("inner", Task.allSettled([leafC, leafD], { concurrency: 1 }));
export const left = Task.make("left", Task.all([leafA, leafB], { concurrency: 1 }));
export const middle = Task.make("middle", Task.allSettled([leafB, inner, leafC], { concurrency: 1 }));
export const right = Task.make("right", Effect.gen(function* () {
  yield* Task.all([inner, leafA], { concurrency: 1 });
  yield* Effect.log("right ran");
}));
export const main = Task.make("main", Task.all([left, middle, right], { concurrency: 1 }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(0);
      for (const leafName of ["leaf-a", "leaf-b", "leaf-c", "leaf-d"]) {
        expect(ran.standardError).toContain(`[${leafName}] ${leafName} ran`);
      }
      expect(ran.standardError).toContain("[right] right ran");
      expect(ran.standardError).toMatch(/^passed: 9 tasks, [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const MAIN_REFUSAL_ERROR = "TaskWaitsOnItself | TaskNotMadeAtTopLevel";

const MAIN_ALL_ERROR = `AwaitedTaskFailed | ${MAIN_REFUSAL_ERROR}`;

const MAIN_ALL_SETTLED_ERROR = `SettledTasksFailed | ${MAIN_REFUSAL_ERROR}`;

const exactErrorChecks = (listName: string, listCall: string, mainError: string) =>
  `export const ${listName} = ${listCall};
export const ${listName}ErrorIsMainError: ${mainError} = undefined as unknown as Effect.Error<typeof ${listName}>;
export const mainErrorIs${listName}Error: Effect.Error<typeof ${listName}> = undefined as unknown as ${mainError};`;

export const unchangedErrorTypes = Task.make(
  "all and allSettled keep exactly the error types they had before the concurrency option, with or without it",
  Effect.scoped(
    typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { type AwaitedTaskFailed, type SettledTasksFailed, TaskError, type TaskNotMadeAtTopLevel, type TaskWaitsOnItself } from "@sshkeda/task/errors";
export const one = Task.make("one", Effect.fail(new TaskError({ message: "one broke" })));
${exactErrorChecks("plainAll", "Task.all([one, { one }])", MAIN_ALL_ERROR)}
${exactErrorChecks("limitedAll", "Task.all([one], { concurrency: 2 })", MAIN_ALL_ERROR)}
${exactErrorChecks("plainAllSettled", "Task.allSettled([one, { one }])", MAIN_ALL_SETTLED_ERROR)}
${exactErrorChecks("limitedAllSettled", 'Task.allSettled([one], { concurrency: "unbounded" })', MAIN_ALL_SETTLED_ERROR)}`),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const concurrencyTypes = Task.make(
  "concurrency accepts a number or unbounded, and nothing else, in all and allSettled",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const one = Task.make("one", Effect.succeed(1));
export const a = Task.make("a", Task.all([one], { concurrency: 2 }));
export const b = Task.make("b", Task.allSettled([{ one }], { concurrency: "unbounded" }));
export const main = Task.make("main", Task.all([a, b], {}));
if (import.meta.main) Task.main(main);`);
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const one = Task.make("one", Effect.succeed(1));
export const main = Task.make("main", Task.all([one], { concurrency: "inherit" }));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(4,57): error TS2322");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
