import { expect } from "bun:test";

import { Effect, Schema } from "effect";

import { encodeJson } from "../shared/encode-json.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const DrivenRun = Schema.Struct({
  lines: Schema.Array(Schema.String),
  files: Schema.Record(Schema.String, Schema.String),
  runFailedByTaskName: Schema.Record(
    Schema.String,
    Schema.Struct({ runFailed: Schema.Boolean, outcome: Schema.String }),
  ),
});

const RACED_TASKS_PER_BUDGET = 5;

const SMALLEST_OPERATION_BUDGET = 3;

const LARGEST_OPERATION_BUDGET = 15;

const RACED_TASK_COUNT =
  (LARGEST_OPERATION_BUDGET - SMALLEST_OPERATION_BUDGET + 1) * RACED_TASKS_PER_BUDGET;

const runDriver = (scenario: string) =>
  Effect.gen(function* () {
    const repository = yield* REPOSITORY_DIRECTORY;
    const runState = "lib/main-thread/lib/run-tasks-on-workers/shared";
    const files = [
      `${runState}/define-task-run-state.ts`,
      `${runState}/define-task-run-status.ts`,
      `${runState}/stop-task.ts`,
      "lib/main-thread/shared/fail-run.ts",
      "lib/main-thread/shared/provide-process-groups.ts",
      "lib/main-thread/shared/provide-run-root-task-config.ts",
      "lib/main-thread/shared/provide-stop-state.ts",
      "lib/main-thread/shared/provide-worker-pool.ts",
      "lib/main-thread/shared/write-task-output-files.ts",
      "shared/print-to-standard-error.ts",
      "shared/process-errors.ts",
      "shared/task-spans.ts",
    ] as const;
    const encodedPaths = new Map<string, string>();
    for (const file of files) {
      encodedPaths.set(file, yield* encodeJson(`${repository}/src/${file}`));
    }
    const source = (file: (typeof files)[number]) => encodedPaths.get(file) ?? "";
    const encodedScenario = yield* encodeJson(scenario);
    const project = yield* makeProject({
      "drive.ts": `import { Deferred, Duration, Effect, Exit, Fiber, Queue, Tracer } from "effect";
import { MaxOpsBeforeYield } from "effect/Scheduler";
import * as BunFileSystem from "@effect/platform-bun/BunFileSystem";
import * as BunPath from "@effect/platform-bun/BunPath";
import * as BunStdio from "@effect/platform-bun/BunStdio";
import { makeTaskRunsState } from ${source(`${runState}/define-task-run-state.ts`)};
import { TaskRunEvent } from ${source(`${runState}/define-task-run-status.ts`)};
import { stopTask } from ${source(`${runState}/stop-task.ts`)};
import { ProcessNotSignaled } from ${source("shared/process-errors.ts")};
import { failRunOnFailure } from ${source("lib/main-thread/shared/fail-run.ts")};
import { ProcessGroups } from ${source("lib/main-thread/shared/provide-process-groups.ts")};
import { RunRootTaskConfig } from ${source("lib/main-thread/shared/provide-run-root-task-config.ts")};
import { StopState, StopStateLive } from ${source("lib/main-thread/shared/provide-stop-state.ts")};
import { WorkerPool } from ${source("lib/main-thread/shared/provide-worker-pool.ts")};
import { TaskOutputFiles, makeTaskOutputFiles } from ${source("lib/main-thread/shared/write-task-output-files.ts")};
import { standardErrorLinesDeferred } from ${source("shared/print-to-standard-error.ts")};
import { startTaskSpan } from ${source("shared/task-spans.ts")};

const runDirectoryPath = process.cwd() + "/run";
const cleanupLimitsPassed = new Map();
const program = Effect.gen(function* () {
  const lines = yield* Queue.unbounded();
  yield* Deferred.succeed(standardErrorLinesDeferred, lines);
  const taskOutputFiles = yield* makeTaskOutputFiles({ runDirectoryPath, isRunDirectoryWritable: true, hasStopSignal: Effect.succeed(false) });
  yield* Effect.gen(function* () {
  const taskRunsState = yield* makeTaskRunsState;
  const startTask = (taskName) => Effect.gen(function* () {
    const taskSpan = yield* startTaskSpan("task", taskName, { parent: null });
    taskRunsState.createRecord(taskName, taskSpan);
    yield* taskRunsState.recordWorkerAssignment(taskName, 1);
    yield* taskRunsState.moveTaskRun(taskName, TaskRunEvent.cases.StartBody.make({}));
    yield* taskRunsState.printStart(taskName);
  });
  const taskNames = [];
  const runFailedByTaskName = {};
  const scenario = ${encodedScenario};
  const budgets = [];
  for (let budget = ${SMALLEST_OPERATION_BUDGET}; budget <= ${LARGEST_OPERATION_BUDGET}; budget++) {
    budgets.push(budget);
  }
  const withBudget = (budget) => (effect) => effect.pipe(Effect.provideService(MaxOpsBeforeYield, budget));
  const yieldUntil = (isReady) => Effect.gen(function* () {
    for (let attempt = 0; attempt < 100000 && !isReady(); attempt++) {
      yield* Effect.yieldNow;
    }
  });
  const fakeWorkerPool = {
    stopTaskFiber: () => Effect.void,
    awaitAnswer: () => Effect.never,
    requestWorkerTermination: () => Effect.void,
  };
  const runStop = (taskName, budget, killForTask) =>
    stopTask(taskRunsState, taskName, () => Effect.void).pipe(
      failRunOnFailure(taskRunsState.markFailed),
      Effect.provideService(WorkerPool, fakeWorkerPool),
      Effect.provideService(ProcessGroups, { killForTask }),
      withBudget(budget),
    );
  const prepareStoppedTask = (taskName) => Effect.gen(function* () {
    yield* startTask(taskName);
    taskRunsState.discoveredTasksByName.set(taskName, { cleanupDuration: Duration.millis(20) });
    taskRunsState.markEndedFromExit(taskName, Exit.interrupt());
    yield* taskRunsState.endTaskSpanAfterExecution(taskName);
  });
  const endOnWorker = (taskName) => Effect.gen(function* () {
    yield* taskRunsState.moveTaskRun(taskName, TaskRunEvent.cases.End.make({}));
    yield* taskRunsState.endTaskSpanAfterWorker(taskName);
  });
  for (const budget of budgets) {
    for (let index = 0; index < ${RACED_TASKS_PER_BUDGET}; index++) {
      const taskName = scenario + "-" + budget + "-" + index;
      taskNames.push(taskName);
      yield* Effect.gen(function* () {
        if (scenario === "raced-ends") {
          yield* startTask(taskName);
          yield* taskRunsState.openCleanupSpan(taskName, 0n);
          yield* taskRunsState.markPassed(taskName);
          yield* taskRunsState.moveTaskRun(taskName, TaskRunEvent.cases.End.make({}));
          yield* Effect.all(
            [taskRunsState.endTaskSpanAfterWorker(taskName), taskRunsState.endTaskSpanAfterExecution(taskName)],
            { concurrency: "unbounded", discard: true },
          ).pipe(withBudget(budget));
        } else if (scenario === "interrupted-end") {
          yield* startTask(taskName);
          const record = taskRunsState.runsByTaskName.get(taskName);
          yield* taskRunsState.markPassed(taskName);
          yield* taskRunsState.endTaskSpanAfterExecution(taskName);
          yield* taskRunsState.moveTaskRun(taskName, TaskRunEvent.cases.End.make({}));
          const ender = yield* Effect.forkChild(
            taskRunsState.endTaskSpanAfterWorker(taskName).pipe(withBudget(budget)),
          );
          yield* yieldUntil(() => record.hasEnded);
          yield* Fiber.interrupt(ender);
          yield* taskRunsState.endTaskSpanAfterWorker(taskName);
        } else if (scenario === "exit-during-kill") {
          yield* prepareStoppedTask(taskName);
          yield* runStop(taskName, budget, () => endOnWorker(taskName));
        } else if (scenario === "cleanup-finishes-after-its-limit") {
          yield* prepareStoppedTask(taskName);
          const record = taskRunsState.runsByTaskName.get(taskName);
          const limitPassed = Deferred.makeUnsafe();
          cleanupLimitsPassed.set("cleanup " + taskName, limitPassed);
          const cleanupFinishes = yield* Effect.forkChild(
            yieldUntil(() => record.stop.readStage()._tag === "StopSent").pipe(
              Effect.andThen(record.stop.move({ _tag: "StartCleanup" })),
              Effect.andThen(Deferred.await(limitPassed)),
              Effect.andThen(endOnWorker(taskName)),
            ),
          );
          yield* runStop(taskName, budget, () => Effect.void);
          yield* Fiber.join(cleanupFinishes);
        } else {
          yield* prepareStoppedTask(taskName);
          yield* runStop(taskName, budget, () =>
            endOnWorker(taskName).pipe(
              Effect.andThen(
                Effect.fail(new ProcessNotSignaled({ taskName, processId: 1, signal: "SIGKILL", cause: new Error("operation not permitted") })),
              ),
            ),
          );
        }
        runFailedByTaskName[taskName] = {
          runFailed: yield* (yield* StopState).hasFailed,
          outcome: taskRunsState.runsByTaskName.get(taskName).outcome._tag,
        };
      }).pipe(Effect.provide(StopStateLive));
    }
  }
  const printed = yield* Queue.clear(lines);
  const printedLines = printed.flatMap((line) => line.output.text.split("\\n"));
  const files = {};
  for (const taskName of taskNames) {
    const fileName = printedLines.find((line) => line.startsWith(\`started: [\${taskName}], \`))?.split(", ").at(-1);
    const file = Bun.file(\`\${runDirectoryPath}/\${fileName}\`);
    files[taskName] = fileName !== undefined && (yield* Effect.promise(() => file.exists())) ? yield* Effect.promise(() => file.text()) : "";
  }
  console.log(JSON.stringify({ lines: printedLines, files, runFailedByTaskName }));
  }).pipe(Effect.provideService(TaskOutputFiles, taskOutputFiles));
}).pipe(
  Effect.withSpan("run"),
  (effect) => Effect.flatMap(Effect.tracer, (tracer) =>
    effect.pipe(Effect.withTracer(Tracer.make({
      span: (options) => {
        const span = tracer.span(options);
        const limitPassed = cleanupLimitsPassed.get(options.name);
        if (limitPassed !== undefined) {
          const recordEvent = span.event.bind(span);
          span.event = (name, startTime, attributes) => {
            recordEvent(name, startTime, attributes);
            if (name === "cleanup limit passed") {
              Deferred.doneUnsafe(limitPassed, Exit.void);
            }
          };
        }
        return span;
      },
      context: tracer.context,
    }))),
  ),
  Effect.scoped,
  Effect.provideService(RunRootTaskConfig, { runStartedAtMilliseconds: Date.now(), rootTaskName: "main", log: "info" }),
  Effect.provide([BunFileSystem.layer, BunPath.layer, BunStdio.layer]),
);
await Effect.runPromise(program);
`,
      "run/.keep": "",
    });
    const ran = yield* runTaskProcess(project, "drive.ts");
    expect(ran.exitCode, ran.standardError).toBe(0);
    return yield* Schema.decodeEffect(Schema.fromJsonString(DrivenRun))(
      ran.standardOutput.trimEnd().split("\n").at(-1) ?? "",
    );
  });

const endedLinesOf = (lines: readonly string[], taskName: string) =>
  lines.filter((line) => line.startsWith(`ended: [${taskName}], `));

const expectOneEndedLineEach = (driven: typeof DrivenRun.Type) => {
  const taskNames = Object.keys(driven.files);
  expect(taskNames).toHaveLength(RACED_TASK_COUNT);
  for (const taskName of taskNames) {
    expect(endedLinesOf(driven.lines, taskName), taskName).toHaveLength(1);
    expect(
      endedLinesOf((driven.files[taskName] ?? "").split("\n"), taskName),
      taskName,
    ).toHaveLength(1);
  }
};

const expectTheLateErrorToCount = (errorText: string) => (driven: typeof DrivenRun.Type) => {
  expectOneEndedLineEach(driven);
  for (const taskName of Object.keys(driven.files)) {
    const printed = driven.lines.join("\n");
    expect(
      driven.lines.filter(
        (line) => line.startsWith(`error: [${taskName}] `) && line.includes(errorText),
      ),
      printed,
    ).toHaveLength(1);
    expect(driven.runFailedByTaskName[taskName], printed).toEqual({
      runFailed: true,
      outcome: "Failed",
    });
  }
};

export const racedEndsPrintOneEndedLine = makeTest(
  "when a task's worker end and execution end run at the same time, with 3 to 15 operations between yields, the task prints one ended line on the terminal and in its file",
  Effect.scoped(Effect.map(runDriver("raced-ends"), expectOneEndedLineEach)),
);

export const interruptedEndStillPrintsItsEndedLine = makeTest(
  "a task end interrupted right after it is claimed, with 3 to 15 operations between yields, still prints the task's one ended line",
  Effect.scoped(Effect.map(runDriver("interrupted-end"), expectOneEndedLineEach)),
);

export const cleanupFinishingAfterItsLimitStillFails = makeTest(
  "a stopped task whose cleanup passes its limit and then finishes, with 3 to 15 operations between yields, prints one ended line and its TaskCleanupTimedOut error, which fails the task and the run",
  Effect.scoped(
    Effect.map(
      runDriver("cleanup-finishes-after-its-limit"),
      expectTheLateErrorToCount(" TaskCleanupTimedOut: "),
    ),
  ),
);

export const workerExitDuringTheKillStillFails = makeTest(
  "a stopped task whose worker exit is handled while its cleanup limit kills its commands, with 3 to 15 operations between yields, prints one ended line and its TaskCleanupNotStarted error, which fails the task and the run",
  Effect.scoped(
    Effect.map(
      runDriver("exit-during-kill"),
      expectTheLateErrorToCount(" TaskCleanupNotStarted: "),
    ),
  ),
);

export const processCleanupFailureAtTheLimitStillFails = makeTest(
  "a stopped task whose commands cannot be killed at its cleanup limit, with 3 to 15 operations between yields, prints one ended line and the error that it could not send SIGKILL, which fails the task and the run",
  Effect.scoped(
    Effect.map(
      runDriver("process-cleanup-failure"),
      expectTheLateErrorToCount("could not send SIGKILL to process 1"),
    ),
  ),
);
