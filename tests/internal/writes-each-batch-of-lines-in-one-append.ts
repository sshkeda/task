import { expect } from "bun:test";

import { Effect, Schema } from "effect";

import { read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunLinePath } from "../shared/name-output-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const FILE_LINE_COUNT = 5000;

const TIMED_OUT_LINE_COUNT = 300;

const SHOWN_FAILURE_CHARACTERS = 2000;

const expectedLines = (count: number) =>
  Array.from({ length: count }, (_unused, line) => `[main] line ${String(line)}`);

const readOutputFileLines = (standardError: string) =>
  Effect.map(read(`${findRunLinePath(standardError) ?? ""}1.output`), (file) =>
    file.trimEnd().split("\n"),
  );

const expectLinesThenPointerThenEnded = (fileLines: readonly string[], loggedCount: number) => {
  const pointerIndex = fileLines.findIndex((line) =>
    line.startsWith("error: [main] its output is in "),
  );
  expect(fileLines[0]).toStartWith("started: [main], ");
  expect(fileLines.filter((line) => line.startsWith("[main] "))).toEqual(
    expectedLines(loggedCount),
  );
  expect(pointerIndex).toBeGreaterThan(
    fileLines.lastIndexOf(`[main] line ${String(loggedCount - 1)}`),
  );
  expect(fileLines.at(-1)).toStartWith("ended: [main], ");
};

export const passedTaskFileComplete = makeTest(
  "a passed task's file holds its started line, every logged line in order and its ended line last",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  for (let line = 0; line < ${FILE_LINE_COUNT}; line++) yield* Effect.log(\`line \${line}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const fileLines = yield* readOutputFileLines(ran.standardError);
      expect(fileLines[0]).toStartWith("started: [main], ");
      expect(fileLines.slice(1, -1)).toEqual(expectedLines(FILE_LINE_COUNT));
      expect(fileLines.at(-1)).toStartWith("ended: [main], ");
    }),
  ),
);

export const failedTaskFileComplete = makeTest(
  "a failed task's file holds every logged line in order before the line that points at it, and holds them all once that line prints",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  for (let line = 0; line < ${FILE_LINE_COUNT}; line++) yield* Effect.log(\`line \${line}\`);
  return yield* new TaskError({ message: "broke after logging" });
}) });`,
      });
      const started = yield* startTaskProcess(project, "main.ts");
      const pointerLine = yield* started.waitForLine("error: [main] its output is in ");
      const filePath = pointerLine.slice(pointerLine.indexOf("/"));
      const fileWhenPointed = (yield* read(filePath)).split("\n");
      expect(fileWhenPointed.filter((line) => line.startsWith("[main] "))).toEqual(
        expectedLines(FILE_LINE_COUNT),
      );
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(1);
      expectLinesThenPointerThenEnded(
        yield* readOutputFileLines(ran.standardError),
        FILE_LINE_COUNT,
      );
    }),
  ),
);

export const timedOutTaskFileComplete = makeTest(
  "a timed-out task's terminal and file hold every line it logged before its timeout, in order, and the file holds them before the line that points at it and its ended line last",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", timeout: "1 second", run: Effect.gen(function* () {
  for (let line = 0; line < ${TIMED_OUT_LINE_COUNT}; line++) yield* Effect.log(\`line \${line}\`);
  yield* Effect.never;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(1);
      expect(ran.standardError).toContain("TaskTimedOut: timed out after 1s");
      expect(ran.standardError.split("\n").filter((line) => line.startsWith("[main] "))).toEqual(
        expectedLines(TIMED_OUT_LINE_COUNT),
      );
      expectLinesThenPointerThenEnded(
        yield* readOutputFileLines(ran.standardError),
        TIMED_OUT_LINE_COUNT,
      );
    }),
  ),
);

const BURST_LINE_COUNT = 1000;

const LONG_LINE_CHARACTERS = 3_145_728;

const DrivenDispatch = Schema.Struct({
  appends: Schema.Array(Schema.Struct({ file: Schema.String, lineCount: Schema.Int })),
  printed: Schema.Array(Schema.String),
});

const driveDispatcher = (events: string) =>
  Effect.gen(function* () {
    const repository = yield* REPOSITORY_DIRECTORY;
    const source = (file: string) => encodeJson(`${repository}/src/${file}`);
    const project = yield* makeProject({
      "drive.ts": `import { Deferred, Effect, FileSystem, Path, Queue } from "effect";
import { splitWorkerEvents } from ${yield* source("lib/main-thread/lib/run-tasks-on-workers/internal/handle-worker-events.ts")};
import { makeTaskOutputFiles, TaskOutputFiles } from ${yield* source("lib/main-thread/shared/write-task-output-files.ts")};
import { WorkerPool } from ${yield* source("lib/main-thread/shared/provide-worker-pool.ts")};
import { TraceFile } from ${yield* source("lib/main-thread/shared/write-trace-file.ts")};
import { standardErrorLinesDeferred } from ${yield* source("shared/print-to-standard-error.ts")};
const logged = (taskName, text) => ({
  _tag: "MessageReceived",
  workerId: 1,
  message: { _tag: "StandardErrorLineRequested", line: { _tag: "Logged", taskName, text } },
});
const appends = [];
await Effect.runPromise(Effect.gen(function* () {
  const terminal = yield* Queue.unbounded();
  yield* Deferred.succeed(standardErrorLinesDeferred, terminal);
  const taskOutputFiles = yield* makeTaskOutputFiles({
    runDirectoryPath: "/run",
    isRunDirectoryWritable: true,
    hasStopSignal: Effect.succeed(false),
  }).pipe(
    Effect.provideService(FileSystem.FileSystem, FileSystem.makeNoop({
      writeFile: (file, data) => Effect.sync(() => {
        appends.push({ file, lineCount: new TextDecoder().decode(data).split("\\n").length - 1 });
      }),
    })),
    Effect.provide(Path.layer),
  );
  const steps = splitWorkerEvents({}, () => Effect.void, ${events});
  yield* Effect.forEach(steps, (step) => step, { discard: true }).pipe(
    Effect.provideService(TaskOutputFiles, taskOutputFiles),
    Effect.provideService(WorkerPool, { releaseTaskOutputBytes: () => Effect.void }),
    Effect.provideService(TraceFile, { writeWorkerLogRecord: () => {} }),
  );
  const printed = (yield* Queue.clear(terminal)).map(({ output }) => output.text.slice(0, 20));
  console.log(JSON.stringify({ appends, printed }));
}));
`,
    });
    const ran = yield* runTaskProcess(project, "drive.ts");
    expect(ran.exitCode, ran.standardError).toBe(0);
    return yield* Schema.decodeEffect(Schema.fromJsonString(DrivenDispatch))(
      ran.standardOutput.trimEnd().split("\n").at(-1) ?? "",
    );
  });

const DrivenWorkerEvents = Schema.Struct({
  appendLineCounts: Schema.Array(Schema.Int),
  printedLineCount: Schema.Int,
  printedInOrder: Schema.Boolean,
});

const driveWorkerEventHandling = Effect.gen(function* () {
  const repository = yield* REPOSITORY_DIRECTORY;
  const source = (file: string) => encodeJson(`${repository}/src/${file}`);
  const project = yield* makeProject({
    "drive.ts": `import { Context, Deferred, Effect, FileSystem, Layer, Path, Queue, Stream } from "effect";
import * as BunServices from "@effect/platform-bun/BunServices";
import { handleWorkerEventsAsTheyArrive } from ${yield* source("lib/main-thread/internal/run-root-task-fiber.ts")};
import { TaskRunsLive } from ${yield* source("lib/main-thread/lib/run-tasks-on-workers/index.ts")};
import { makeTaskOutputFiles, TaskOutputFiles } from ${yield* source("lib/main-thread/shared/write-task-output-files.ts")};
import { RunRootTaskConfig } from ${yield* source("lib/main-thread/shared/provide-run-root-task-config.ts")};
import { StopState } from ${yield* source("lib/main-thread/shared/provide-stop-state.ts")};
import { WorkerPool } from ${yield* source("lib/main-thread/shared/provide-worker-pool.ts")};
import { TraceFile } from ${yield* source("lib/main-thread/shared/write-trace-file.ts")};
import { standardErrorLinesDeferred } from ${yield* source("shared/print-to-standard-error.ts")};
const appendLineCounts = [];
await Effect.runPromise(Effect.gen(function* () {
  const terminal = yield* Queue.unbounded();
  yield* Deferred.succeed(standardErrorLinesDeferred, terminal);
  const workerEvents = yield* Queue.unbounded();
  yield* Queue.offerAll(workerEvents, Array.from({ length: ${BURST_LINE_COUNT} }, (_, line) => ({
    _tag: "MessageReceived",
    workerId: 1,
    message: { _tag: "StandardErrorLineRequested", line: { _tag: "Logged", taskName: "a", text: "line " + line } },
  })));
  yield* Queue.end(workerEvents);
  const fileSystem = FileSystem.makeNoop({
    writeFile: (_file, data) => Effect.sync(() => {
      appendLineCounts.push(new TextDecoder().decode(data).split("\\n").length - 1);
    }),
  });
  const taskOutputFiles = yield* makeTaskOutputFiles({
    runDirectoryPath: "/run",
    isRunDirectoryWritable: true,
    hasStopSignal: Effect.succeed(false),
  }).pipe(Effect.provideService(FileSystem.FileSystem, fileSystem), Effect.provide(Path.layer));
  const services = Layer.succeedContext(Context.empty().pipe(
    Context.add(FileSystem.FileSystem, fileSystem),
    Context.add(TaskOutputFiles, taskOutputFiles),
    Context.add(RunRootTaskConfig, { rootTaskName: "root" }),
    Context.add(StopState, { readTaskLocationsFrom: () => Effect.void }),
    Context.add(WorkerPool, { events: Stream.fromQueue(workerEvents), releaseTaskOutputBytes: () => Effect.void }),
    Context.add(TraceFile, { writeWorkerLogRecord: () => {} }),
  ));
  yield* handleWorkerEventsAsTheyArrive.pipe(
    Effect.provide(TaskRunsLive.pipe(Layer.provideMerge(services))),
    Effect.provide(BunServices.layer),
    Effect.withSpan("run"),
  );
  const printed = (yield* Queue.clear(terminal)).map(({ output }) => output.text);
  console.log(JSON.stringify({
    appendLineCounts,
    printedLineCount: printed.length,
    printedInOrder: printed.every((text, line) => text === "[a] line " + line),
  }));
}));
`,
  });
  const ran = yield* runTaskProcess(project, "drive.ts");
  expect(ran.exitCode, ran.standardError).toBe(0);
  return yield* Schema.decodeEffect(Schema.fromJsonString(DrivenWorkerEvents))(
    ran.standardOutput.trimEnd().split("\n").at(-1) ?? "",
  );
});

export const burstOfLogLinesIsOneAppend = makeTest(
  "a burst of one task's logged lines waiting on the worker-event queue is written to its file in one append and printed in order",
  Effect.gen(function* () {
    const driven = yield* driveWorkerEventHandling;
    expect(driven.appendLineCounts).toEqual([BURST_LINE_COUNT]);
    expect(driven.printedLineCount).toBe(BURST_LINE_COUNT);
    expect(driven.printedInOrder).toBe(true);
  }).pipe(Effect.scoped),
);

export const interleavedTasksKeepTheirOrder = makeTest(
  "logged lines of two tasks interleaved in one batch print in the order they arrived, each in its own append",
  Effect.gen(function* () {
    const driven = yield* driveDispatcher(`[logged("a", "1"), logged("b", "1"), logged("a", "2")]`);
    expect(driven.printed).toEqual(["[a] 1", "[b] 1", "[a] 2"]);
    expect(driven.appends).toEqual([
      { file: "/run/1.output", lineCount: 1 },
      { file: "/run/2.output", lineCount: 1 },
      { file: "/run/1.output", lineCount: 1 },
    ]);
  }).pipe(Effect.scoped),
);

export const longLoggedLinesAreSeveralAppends = makeTest(
  "logged lines that together pass 4 MiB are written in several appends, each under the limit unless one line is longer",
  Effect.gen(function* () {
    const driven = yield* driveDispatcher(
      `Array.from({ length: 3 }, () => logged("a", "x".repeat(${LONG_LINE_CHARACTERS})))`,
    );
    expect(driven.appends).toEqual([
      { file: "/run/1.output", lineCount: 1 },
      { file: "/run/1.output", lineCount: 1 },
      { file: "/run/1.output", lineCount: 1 },
    ]);
  }).pipe(Effect.scoped),
);
