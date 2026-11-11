import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
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
        `export const ${taskName.replace("-", "")} = Task.make({ name: "${taskName}", run: held("${taskName}") });`,
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
export default Task.make({ name: "main", run: Task.${listFunctionName}(${heldTaskList(taskNames)}${listOptions}) });`,
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

export const allRunsAtMostTheLimit = makeTest(
  "all with a concurrency limit runs at most that many of its tasks at once",
  Effect.scoped(
    expectHeldAtMost(
      "all",
      `, { concurrency: ${ALL_CONCURRENCY_LIMIT} }`,
      LIMITED_TASK_COUNT,
      ALL_CONCURRENCY_LIMIT,
    ),
  ),
);

export const allSettledRunsAtMostTheLimit = makeTest(
  "allSettled with a concurrency limit runs at most that many of its tasks at once",
  Effect.scoped(
    expectHeldAtMost(
      "allSettled",
      `, { concurrency: ${ALL_SETTLED_CONCURRENCY_LIMIT} }`,
      LIMITED_TASK_COUNT,
      ALL_SETTLED_CONCURRENCY_LIMIT,
    ),
  ),
);

export const allDefaultIsUnbounded = makeTest(
  "all without options still runs every listed task at once",
  Effect.scoped(expectHeldAtMost("all", "", UNBOUNDED_TASK_COUNT, UNBOUNDED_TASK_COUNT)),
);

export const allSettledDefaultIsUnbounded = makeTest(
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
);

export const limitedAllSettledReportsEveryFailure = makeTest(
  "allSettled with a concurrency limit still runs every task and reports every failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const one = Task.make({ name: "one", run: Effect.fail(new TaskError({ message: "one broke" })) });
export const two = Task.make({ name: "two", run: Effect.log("two ran") });
export const three = Task.make({ name: "three", run: Effect.fail(new TaskError({ message: "three broke" })) });
export const four = Task.make({ name: "four", run: Effect.log("four ran") });
export const five = Task.make({ name: "five", run: Effect.fail(new TaskError({ message: "five broke" })) });
export default Task.make({ name: "main", run: Task.allSettled([one, { two, three }, four, five], { concurrency: 1 }) });`,
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
);

export const nestedLimitedLists = makeTest(
  "limited lists whose tasks yield limited lists finish on one worker, since a waiting parent holds no slot",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const leafA = Task.make({ name: "leaf-a", run: Effect.log("leaf-a ran") });
export const leafB = Task.make({ name: "leaf-b", run: Effect.log("leaf-b ran") });
export const leafC = Task.make({ name: "leaf-c", run: Effect.log("leaf-c ran") });
export const leafD = Task.make({ name: "leaf-d", run: Effect.log("leaf-d ran") });
export const inner = Task.make({ name: "inner", run: Task.allSettled([leafC, leafD], { concurrency: 1 }) });
export const left = Task.make({ name: "left", run: Task.all([leafA, leafB], { concurrency: 1 }) });
export const middle = Task.make({ name: "middle", run: Task.allSettled([leafB, inner, leafC], { concurrency: 1 }) });
export const right = Task.make({ name: "right", run: Effect.gen(function* () {
  yield* Task.all([inner, leafA], { concurrency: 1 });
  yield* Effect.log("right ran");
}) });
export default Task.make({ name: "main", run: Task.all([left, middle, right], { concurrency: 1 }) });`,
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
);

const MAIN_REFUSAL_ERROR = "TaskWaitsOnItself | TaskNotMadeWhileLoading | TaskMadeByAnotherCopy";

const MAIN_ALL_ERROR = `AwaitedTaskFailed | ${MAIN_REFUSAL_ERROR}`;

const MAIN_ALL_SETTLED_ERROR = `SettledTasksFailed | ${MAIN_REFUSAL_ERROR}`;

const exactErrorChecks = (listName: string, listCall: string, mainError: string) =>
  `export const ${listName} = ${listCall};
export const ${listName}ErrorIsMainError: ${mainError} = undefined as unknown as Effect.Error<typeof ${listName}>;
export const mainErrorIs${listName}Error: Effect.Error<typeof ${listName}> = undefined as unknown as ${mainError};`;

export const unchangedErrorTypes = makeTest(
  "all and allSettled keep exactly the error types they had before the concurrency option, with or without it",
  Effect.scoped(
    typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { type AwaitedTaskFailed, type SettledTasksFailed, TaskError, type TaskMadeByAnotherCopy, type TaskNotMadeWhileLoading, type TaskWaitsOnItself } from "@sshkeda/task/errors";
export const one = Task.make({ name: "one", run: Effect.fail(new TaskError({ message: "one broke" })) });
${exactErrorChecks("plainAll", "Task.all([one, { one }])", MAIN_ALL_ERROR)}
${exactErrorChecks("limitedAll", "Task.all([one], { concurrency: 2 })", MAIN_ALL_ERROR)}
${exactErrorChecks("plainAllSettled", "Task.allSettled([one, { one }])", MAIN_ALL_SETTLED_ERROR)}
${exactErrorChecks("limitedAllSettled", 'Task.allSettled([one], { concurrency: "unbounded" })', MAIN_ALL_SETTLED_ERROR)}`),
  ),
);

export const concurrencyTypes = makeTest(
  "concurrency accepts a number or unbounded, and nothing else, in all and allSettled",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const one = Task.make({ name: "one", run: Effect.void });
export const a = Task.make({ name: "a", run: Task.all([one], { concurrency: 2 }) });
export const b = Task.make({ name: "b", run: Task.allSettled([{ one }], { concurrency: "unbounded" }) });
export default Task.make({ name: "main", run: Task.all([a, b], {}) });`);
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const one = Task.make({ name: "one", run: Effect.void });
export default Task.make({ name: "main", run: Task.all([one], { concurrency: "inherit" }) });`),
      );
      expect(failed.message).toContain("main.ts(4,65): error TS2322");
    }),
  ),
);
