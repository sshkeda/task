import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Clock, Duration, Effect, Exit, FileSystem, Path, Schema } from "effect";
import { OtlpExporter, OtlpSerialization } from "effect/observability";

import taskPackageJson from "../../package.json" with { type: "json" };
import { makeTraceFile } from "../../src/lib/main-thread/shared/write-trace-file.ts";
import { availableParallelism } from "../../src/shared/read-environment-config.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath } from "../shared/name-output-files.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const WAIT_UNTIL_TRACED = new URL("../shared/wait-until-traced.ts", import.meta.url);

const ERROR_AND_CONTEXT_LINE_COUNT = 2;

const OWNER_READ_WRITE_MODE = 0o600;

const OWNER_READ_EXECUTE_MODE = 0o500;

const OWNER_FULL_MODE = 0o700;

const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

const SPAN_STATUS_UNSET = 0;

const SPAN_STATUS_OK = 1;

const TRACE_ID_LENGTH = 32;

const WORKER_ENDED_AT = 3n;

const SPAN_STATUS_ERROR = 2;

const SPAN_KIND_INTERNAL = 1;

const SPAN_KIND_CLIENT = 3;

const ANNOTATED_COUNT = 2;

const PROCESS_EXIT_CODE = 3;

const LOGGED_LINE_COUNT = 100;

const LOGGED_BEFORE_EXIT_COUNT = 500;

const WORKER_EXIT_CODE = 7;

const SPANS_BEFORE_EXIT_COUNT = 500;

const DAY_MILLISECONDS = Duration.toMillis(Duration.days(1));

const OLD_RUN_AGE_DAYS = 15;

const RECENT_RUN_AGE_DAYS = 13;

const SEEDED_RUN_DIRECTORY_COUNT = 10;

const seedFile = (filePath: string, ageDays: number) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    yield* fileSystem.makeDirectory(filePath.slice(0, filePath.lastIndexOf("/")), {
      recursive: true,
    });
    yield* fileSystem.writeFileString(filePath, "{}\n");
    const modified = new Date((yield* Clock.currentTimeMillis) - ageDays * DAY_MILLISECONDS);
    yield* fileSystem.utimes(filePath, modified, modified);
  });

const ageDirectory = (directoryPath: string, ageDays: number) =>
  Effect.gen(function* () {
    const modified = new Date((yield* Clock.currentTimeMillis) - ageDays * DAY_MILLISECONDS);
    yield* (yield* FileSystem.FileSystem).utimes(directoryPath, modified, modified);
  });

const seedRunDirectory = (runDirectoryPath: string, ageDays: number) =>
  Effect.gen(function* () {
    yield* seedFile(`${runDirectoryPath}/trace.jsonl`, ageDays);
    yield* ageDirectory(runDirectoryPath, ageDays);
  });

const ProjectRunsDirectory = Schema.Struct({
  home: Schema.String,
  directory: Schema.String,
  standardError: Schema.String,
  exitCode: Schema.Finite,
  directoryModes: Schema.mutable(Schema.Array(Schema.Finite)),
  files: Schema.mutable(
    Schema.Array(Schema.Struct({ path: Schema.String, mode: Schema.Finite, text: Schema.String })),
  ),
});

const ENVIRONMENT = { MODE: "quickly", NAME: "someone", TOKEN: "hunter2" } as const;

export const traced = Task.make({
  name: "a traced run",
  output: ProjectRunsDirectory,
  run: Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
const work = Effect.fn("work")(function* () { yield* Effect.log("working"); });
export const dependency = Task.make({ name: "dep", run: Effect.gen(function* () {
  yield* Config.String("NAME");
  const token = yield* Config.Redacted("TOKEN");
  yield* Effect.log(token);
  yield* Effect.log(\`token \${token}\`, { token });
  yield* Effect.annotateCurrentSpan("token", token);
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* dependency;
  yield* work();
  yield* Effect.log("annotated").pipe(Effect.annotateLogs({ step: "one", count: 2 }), Effect.withLogSpan("phase"));
  yield* ChildProcess.make("sh", ["-c", "exit 3"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
  yield* Effect.log(\`spawned \${Bun.spawn(["sleep", "1000"]).pid}\`);
}) });`,
      });
      return yield* readProjectRunsDirectory(
        project,
        yield* runTaskProcess(project, "main.ts", ENVIRONMENT),
      );
    }),
  ),
  timeout: "1 minute",
  cleanup: "5 seconds",
});

export const failed = Task.make({
  name: "a traced run that fails",
  output: ProjectRunsDirectory,
  run: Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () { return yield* Effect.fail(yield* Config.Redacted("TOKEN")); }) });`,
      });
      return yield* readProjectRunsDirectory(
        project,
        yield* runTaskProcess(project, "main.ts", ENVIRONMENT),
      );
    }),
  ),
  timeout: "1 minute",
  cleanup: "5 seconds",
});

export const fresh = Task.make({
  name: "a run writes its trace file as OTLP JSON lines in a run folder that only its owner can read and names that folder at the end",
  run: Effect.gen(function* () {
    const projectRuns = yield* traced;
    const [trace, ...others] = yield* readTraces(projectRuns);
    expect(others).toHaveLength(0);
    expect(trace?.file).toEndWith("/trace.jsonl");
    expect(projectRuns.standardError.trimEnd()).toEndWith(
      `\nrun: ${trace?.file.slice(0, -"trace.jsonl".length)}`,
    );
    expect(trace?.mode).toBe(OWNER_READ_WRITE_MODE);
    expect(projectRuns.directoryModes).toEqual([OWNER_FULL_MODE, OWNER_FULL_MODE, OWNER_FULL_MODE]);
    const run = trace?.findRunSpan();
    expect(run?.name).toBe("run main");
    expect(run?.parentSpanId).toBeUndefined();
    expect(run?.status.code).toBe(SPAN_STATUS_UNSET);
    expect(run?.kind).toBe(SPAN_KIND_INTERNAL);
    expect(run?.scope).toEqual({ name: taskPackageJson.name, version: taskPackageJson.version });
    expect(run?.serviceName).toBe(taskPackageJson.name);
    expect(readAttribute(run?.resource ?? [], "service.version")).toBe(taskPackageJson.version);
    expect(readAttribute(run?.resource ?? [], "process.runtime.name")).toBe("bun");
    expect(readAttribute(run?.resource ?? [], "process.runtime.version")).toBe(Bun.version);
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.root.task")).toBe("main");
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.entry")).toBe(
      `${projectRuns.directory}/main.ts`,
    );
    expect(readAttribute(run?.attributes ?? [], "process.exit.code")).toBe(0);
    const parallelism = yield* availableParallelism;
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.workers.available")).toBe(
      parallelism,
    );
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.workers.limit")).toBe(parallelism);
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.workers.started")).toBe(1);
    expect(
      readAttribute(run?.attributes ?? [], "sshkeda_task.elapsed.milliseconds"),
    ).toBeGreaterThan(0);
    expect([...new Set(trace?.spans.map((span) => span.traceId))]).toEqual([run?.traceId ?? ""]);
    const main = trace?.findTaskSpan("task", "main");
    expect(main?.parentSpanId).toBe(run?.spanId);
    expect(main?.status.code).toBe(SPAN_STATUS_UNSET);
    expect(readAttribute(main?.attributes ?? [], "sshkeda_task.state")).toBe("passed");
    expect(
      readAttribute(trace?.findTaskSpan("task", "dep")?.attributes ?? [], "sshkeda_task.state"),
    ).toBe("passed");
  }),
});

export const failure = Task.make({
  name: "a failed run's trace holds the failed task and its exit code",
  run: Effect.gen(function* () {
    const projectRuns = yield* failed;
    expect(projectRuns.exitCode).toBe(1);
    const [trace] = yield* readTraces(projectRuns);
    const main = trace?.findTaskSpan("task", "main");
    expect(main?.status.code).toBe(SPAN_STATUS_ERROR);
    expect(readAttribute(main?.attributes ?? [], "error.type")).toBe("UntaggedFailure");
    expect(readAttribute(main?.attributes ?? [], "sshkeda_task.state")).toBe("failed");
    expect(trace?.findTaskSpan("body", "main")?.status.code).toBe(SPAN_STATUS_ERROR);
    const run = trace?.findRunSpan();
    expect(readAttribute(run?.attributes ?? [], "process.exit.code")).toBe(1);
    expect(run?.status.code).toBe(SPAN_STATUS_ERROR);
    expect(readAttribute(run?.attributes ?? [], "error.type")).toBe("UntaggedFailure");
    expect(run?.status.message).toBe(main?.status.message);
  }),
});

export const stopped = makeTest(
  "a run stopped with ctrl-c is traced as failed with its exit code, with the cleanup's process listing as a log record on the run span",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("waiting").pipe(Effect.andThen(Effect.never)) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      const run = trace?.findRunSpan();
      expect(readAttribute(run?.attributes ?? [], "process.exit.code")).toBe(
        EXIT_CODE_AFTER_SIGINT,
      );
      expect(run?.status.code).toBe(SPAN_STATUS_ERROR);
      expect(readAttribute(run?.attributes ?? [], "error.type")).toBe("StopRequested");
      expect(run?.status.message).toBe("received SIGINT");
      const main = trace?.findTaskSpan("task", "main");
      expect(main?.status.code).toBe(SPAN_STATUS_UNSET);
      expect(readAttribute(main?.attributes ?? [], "sshkeda_task.state")).toBe("cancelled");
      expect(trace?.findTaskSpan("body", "main")?.status.code).toBe(SPAN_STATUS_UNSET);
      const listingLog = trace?.logs.find(
        (log) => log.body.stringValue === "listed processes to clean up",
      );
      expect(readAttribute(listingLog?.attributes ?? [], "sshkeda_task.processes.killed")).toBe(0);
      expect(listingLog?.spanId).toBe(run?.spanId);
    }),
  ),
);

export const requestedTaskEvent = makeTest(
  "a body that awaits another task puts a requested task event naming it on its task span",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const dep = Task.make({ name: "dep", run: Effect.void });
export default Task.make({ name: "main", run: dep });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const requested = (trace?.findTaskSpan("task", "main")?.events ?? []).filter(
        (event) => event.name === "requested task",
      );
      expect(
        requested.map((event) => readAttribute(event.attributes, "sshkeda_task.wanted_task")),
      ).toEqual(["dep"]);
    }),
  ),
);

export const hung = makeTest(
  "a run killed while a task hangs still traces that the task started",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const waitUntilTraced = yield* (yield* Path.Path).fromFileUrl(WAIT_UNTIL_TRACED);
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "hang", run: Effect.never });`,
      });
      const directory = `${project.home}/.sshkeda-task/runs/project`;
      yield* fileSystem.makeDirectory(directory, { recursive: true });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* Task.exec(process.execPath, [waitUntilTraced, directory, '"task started"']);
      yield* command.sendSignal("KILL");
      expect(Exit.isFailure(yield* exitAsValue(command.waitForExit))).toBe(true);
      const [trace] = yield* readTraces(
        yield* readProjectRunsDirectory(project, { standardError: "", exitCode: 137 }),
      );
      const started = trace?.logs.find(
        (log) =>
          log.body.stringValue === "task started" &&
          readAttribute(log.attributes, "sshkeda_task.task") === "hang",
      );
      expect(started?.spanId).toMatch(/^[0-9a-f]{16}$/u);
      expect(started?.traceId).toMatch(/^[0-9a-f]{32}$/u);
      expect(trace?.spans.map((span) => span.spanId)).not.toContain(started?.spanId);
      expect(trace?.withRole("run")).toHaveLength(0);
    }),
  ),
);

export const nested = Task.make({
  name: "a body's own span is on the run's trace, comes from a worker, nests under its task and the run, and shares its span with its logs",
  run: Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const work = trace?.findOnlySpan("work");
    expect(work?.thread).toBe("worker");
    const run = trace?.findRunSpan();
    expect(work?.traceId).toBe(run?.traceId);
    expect(work?.parentSpanId).toBe(trace?.findTaskSpan("body", "main")?.spanId);
    const ancestors = trace?.findParentSpans(work) ?? [];
    expect(ancestors.map((span) => span.spanId)).toContain(
      trace?.findTaskSpan("task", "main")?.spanId ?? "",
    );
    expect(ancestors.at(-1)?.spanId).toBe(run?.spanId);
    const working = trace?.logs.find((log) => log.body.stringValue === "working");
    expect(working?.spanId).toBe(work?.spanId);
  }),
});

export const logAnnotations = Task.make({
  name: "annotations and log spans a body puts on its own logs reach the trace file with that log",
  run: Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const annotated = trace?.logs.find((log) => log.body.stringValue === "annotated");
    const attributes = annotated?.attributes ?? [];
    expect(readAttribute(attributes, "step")).toBe("one");
    expect(readAttribute(attributes, "count")).toBe(ANNOTATED_COUNT);
    expect(readAttribute(attributes, "sshkeda_task.task")).toBe("main");
    const logSpanDuration = readAttribute(attributes, "effect.log_span.phase");
    expect(typeof logSpanDuration).toBe("number");
    expect(Number.isFinite(logSpanDuration)).toBe(true);
    expect(logSpanDuration).toBeGreaterThanOrEqual(0);
  }),
});

export const processes = Task.make({
  name: "each process a task starts through ChildProcess is a client span named by its program with its pid and exit code, and a nonzero exit code makes it an error though the body reads that code and passes",
  run: Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const [spawned, ...others] = trace?.withRole("process") ?? [];
    expect(others).toHaveLength(0);
    expect(spawned?.name).toBe("sh");
    expect(spawned?.kind).toBe(SPAN_KIND_CLIENT);
    expect(spawned?.thread).toBe("worker");
    expect(readAttribute(spawned?.attributes ?? [], "process.executable.name")).toBe("sh");
    expect(readAttribute(spawned?.attributes ?? [], "process.pid")).toBeGreaterThan(0);
    expect(readAttribute(spawned?.attributes ?? [], "process.exit.code")).toBe(PROCESS_EXIT_CODE);
    expect(spawned?.status.code).toBe(SPAN_STATUS_ERROR);
    expect(readAttribute(spawned?.attributes ?? [], "error.type")).toBe(String(PROCESS_EXIT_CODE));
    expect(spawned?.parentSpanId).toBe(trace?.findTaskSpan("body", "main")?.spanId);
    expect(trace?.findTaskSpan("body", "main")?.status.code).toBe(SPAN_STATUS_UNSET);
  }),
});

export const killedProcess = Task.make({
  name: "a process killed at the end is traced as a log record with its pid and program name",
  run: Effect.gen(function* () {
    const projectRuns = yield* traced;
    const processId = Number(
      /\[main\] spawned (?<processId>\d+)/u.exec(projectRuns.standardError)?.[1],
    );
    const [trace] = yield* readTraces(projectRuns);
    const killed = trace?.logs.find((log) => log.body.stringValue === "process killed");
    expect(readAttribute(killed?.attributes ?? [], "process.pid")).toBe(processId);
    expect(readAttribute(killed?.attributes ?? [], "process.executable.name")).toBe("sleep");
  }),
});

export const redacted = Task.make({
  name: "a redacted value appears nowhere in the trace file, whether logged, annotated or failed with",
  run: Effect.gen(function* () {
    for (const projectRuns of [yield* traced, yield* failed]) {
      expect(projectRuns.files.map((file) => file.text).join("")).toContain("<redacted>");
      for (const file of projectRuns.files) {
        expect(file.text).not.toContain(ENVIRONMENT.TOKEN);
      }
    }
  }),
});

export const processUsage = Task.make({
  name: "each process span holds its CPU time and peak memory",
  run: Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const attributes = trace?.withRole("process")[0]?.attributes ?? [];
    expect(readAttribute(attributes, "sshkeda_task.process.cpu.milliseconds")).toBeGreaterThan(0);
    expect(readAttribute(attributes, "sshkeda_task.process.memory.peak.bytes")).toBeGreaterThan(0);
  }),
});

const BODY_START_DELAY_MILLISECONDS = 3000;

const SLEEP_TOLERANCE_MILLISECONDS = 100;

export const lateBodySetupSteps = makeTest(
  "a task whose module takes 3s to load prints no setup step, and its trace shows the wait in its start task span and in its worker's import span",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "slow.ts": `import { isMainThread } from "node:worker_threads";
if (!isMainThread) {
  await Bun.sleep(${BODY_START_DELAY_MILLISECONDS});
}
export const greeting = "hello";`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { greeting } from "./slow.ts";
export default Task.make({ name: "main", run: Effect.log(greeting) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] hello");
      expect(ran.standardError).not.toContain("[main] loading");
      expect(ran.standardError).not.toContain("[main] waiting for worker");
      expect(ran.standardError).not.toContain("[main] starting a worker");
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const main = trace?.findTaskSpan("task", "main");
      const setup = (trace?.findChildSpans(main) ?? []).filter((span) => span.role === "setup");
      expect(setup.map((span) => span.name)).toEqual(["wait for worker", "start task"]);
      const [, start] = setup;
      expect(readAttribute(start?.attributes ?? [], "sshkeda_task.worker")).toBe(0);
      const workerImport = (trace?.findChildSpans(main) ?? []).find(
        (span) => span.role === "import",
      );
      expect(workerImport?.thread).toBe("worker");
      for (const waited of [start, workerImport]) {
        expect(
          Number(
            (BigInt(waited?.endTimeUnixNano ?? "0") - BigInt(waited?.startTimeUnixNano ?? "0")) /
              NANOSECONDS_PER_MILLISECOND,
          ),
        ).toBeGreaterThanOrEqual(BODY_START_DELAY_MILLISECONDS - SLEEP_TOLERANCE_MILLISECONDS);
      }
      expect(
        BigInt(trace?.findTaskSpan("body", "main")?.startTimeUnixNano ?? "0"),
      ).toBeGreaterThanOrEqual(BigInt(workerImport?.endTimeUnixNano ?? "0"));
    }),
  ),
);

export const live = makeTest(
  "a body's own span reaches the trace file while the run is still going",
  Effect.scoped(
    Effect.gen(function* () {
      const waitUntilTraced = yield* (yield* Path.Path).fromFileUrl(WAIT_UNTIL_TRACED);
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.void.pipe(Effect.withSpan("marker"));
  const directory = \`\${yield* Config.String("HOME")}/.sshkeda-task/runs/project\`;
  yield* Task.exec(process.execPath, ["${waitUntilTraced}", directory, '"name":"marker"']);
  yield* Effect.log("marker traced");
}), timeout: "30 seconds", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] marker traced");
    }),
  ),
);

export const terminatedSpans = makeTest(
  "a span a body ended just before its worker froze and was terminated is in the trace file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make({ name: "spin", run: Effect.void.pipe(
  Effect.withSpan("before freezing"),
  Effect.andThen(Effect.sync(() => { while (true) {} })),
), timeout: "1 second", cleanup: "1 second" });
export default Task.make({ name: "main", run: spin });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError.split("\n")).toContain(
        "error: [spin] main.ts:3:26: TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(trace?.findOnlySpan("before freezing")?.thread).toBe("worker");
    }),
  ),
);

export const lastBatch = makeTest(
  "the trace file written at the end holds every span, including one a body ended as the run finished",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void.pipe(Effect.withSpan("last")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(trace?.findOnlySpan("last")?.thread).toBe("worker");
      const run = trace?.findRunSpan();
      expect(run).toBeDefined();
      expect(trace?.findTaskSpan("task", "main")?.parentSpanId).toBe(run?.spanId);
    }),
  ),
);

export const loggedLineBatches = makeTest(
  "lines a body logs reach the trace file in batches of log records, not one file line each",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.forEach(Array.from({ length: ${LOGGED_LINE_COUNT} }, (_, line) => line), (line) => Effect.log(\`line \${line}\`), { discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        trace?.logs.filter((log) => log.body.stringValue?.startsWith("line ") === true),
      ).toHaveLength(LOGGED_LINE_COUNT);
      expect(
        trace?.text.split("\n").filter((line) => line.includes('"resourceLogs"')).length,
      ).toBeLessThan(LOGGED_LINE_COUNT);
    }),
  ),
);

export const loggedLines = makeTest(
  "lines a body logs reach the trace as its log records, without a main-thread span for each line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.forEach(Array.from({ length: ${LOGGED_LINE_COUNT} }, (_, line) => line), (line) => Effect.log(\`line \${line}\`), { discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain(`[main] line ${LOGGED_LINE_COUNT - 1}\n`);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        trace?.logs.filter((log) => log.body.stringValue?.startsWith("line ") === true),
      ).toHaveLength(LOGGED_LINE_COUNT);
      expect(trace?.spans.filter((span) => span.thread === "main-thread").length).toBeLessThan(
        LOGGED_LINE_COUNT,
      );
    }),
  ),
);

export const loggedBeforeProcessExit = makeTest(
  "every line a body logs right before it calls process.exit reaches the trace file as a log record",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.forEach(Array.from({ length: ${LOGGED_BEFORE_EXIT_COUNT} }, (_, line) => line), (line) => Effect.log(\`line \${line}\`), { discard: true }).pipe(Effect.andThen(Effect.sync(() => process.exit(${WORKER_EXIT_CODE})))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError.split("\n")).toContain(
        `error: [main] main.ts:3:21: WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`,
      );
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const logged = (trace?.logs ?? []).filter(
        (log) =>
          log.body.stringValue?.startsWith("line ") === true &&
          readAttribute(log.attributes, "sshkeda_task.task") === "main",
      );
      expect(new Set(logged.map((log) => log.body.stringValue)).size).toBe(
        LOGGED_BEFORE_EXIT_COUNT,
      );
    }),
  ),
);

export const spansBeforeProcessExit = makeTest(
  "every span a body ends right before it calls process.exit reaches the trace file, in batches of spans, the task and the body span its worker left open are errors naming the exit, and a user span left open stays out",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.forEach(Array.from({ length: ${SPANS_BEFORE_EXIT_COUNT} }, (_, span) => span), (span) => Effect.withSpan(Effect.void, \`span \${span}\`), { discard: true }).pipe(Effect.andThen(Effect.log("about to exit")), Effect.andThen(Effect.sync(() => process.exit(${WORKER_EXIT_CODE})).pipe(Effect.withSpan("unfinished user span")))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError.split("\n")).toContain(
        `error: [main] main.ts:3:21: WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`,
      );
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const main = trace?.findTaskSpan("task", "main");
      expect(main?.status.code).toBe(SPAN_STATUS_ERROR);
      expect(readAttribute(main?.attributes ?? [], "error.type")).toBe("WorkerExited");
      expect(trace?.findRunSpan()?.status.code).toBe(SPAN_STATUS_ERROR);
      const body = trace?.findTaskSpan("body", "main");
      expect(body?.status).toEqual({
        code: SPAN_STATUS_ERROR,
        message: `worker 0 exited with code ${WORKER_EXIT_CODE} before this span ended`,
      });
      expect(readAttribute(body?.attributes ?? [], "error.type")).toBe("WorkerExited");
      expect(body?.parentSpanId).toBe(main?.spanId);
      expect(trace?.logs.find((log) => log.body.stringValue === "about to exit")?.spanId).toBe(
        body?.spanId,
      );
      expect(trace?.named("unfinished user span")).toEqual([]);
      const ended = (trace?.spans ?? []).filter(
        (span) => span.name.startsWith("span ") && span.thread === "worker",
      );
      expect(new Set(ended.map((span) => span.name)).size).toBe(SPANS_BEFORE_EXIT_COUNT);
      expect(
        trace?.text.split("\n").filter((line) => line.includes('"resourceSpans"')).length,
      ).toBeLessThan(SPANS_BEFORE_EXIT_COUNT);
    }),
  ),
);

export const commandOpenAtForcedStop = makeTest(
  "a command a finalizer still runs when a second Ctrl-C terminates its worker stays in the trace as an error naming the termination, with the pid it got after it started",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Task.exec("sh", ["-c", "echo pid $$; echo spawned; exec sleep 30"], { print: true }).pipe(Effect.ignore));
  yield* Effect.log("started");
  return yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] started");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[main] spawned");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      const printedPid = exited.standardError
        .split("\n")
        .find((line) => line.startsWith("[main] pid "))
        ?.slice("[main] pid ".length);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      const sleep = trace?.withRole("process").find((span) => span.name === "sh");
      expect(sleep?.kind).toBe(SPAN_KIND_CLIENT);
      expect(sleep?.status).toEqual({
        code: SPAN_STATUS_ERROR,
        message: "worker 0 was terminated before this span ended",
      });
      expect(readAttribute(sleep?.attributes ?? [], "error.type")).toBe("WorkerTerminated");
      expect(readAttribute(sleep?.attributes ?? [], "process.executable.name")).toBe("sh");
      expect(printedPid).toMatch(/^\d+$/u);
      expect(readAttribute(sleep?.attributes ?? [], "process.pid")).toBe(Number(printedPid));
      expect(sleep?.traceId).toBe(trace?.findRunSpan()?.traceId);
    }),
  ),
);

export const taskSpanEndedUnderUserTracer = makeTest(
  "a Task span that ends under a tracer the body supplies, even one whose worker exits right after exporting it, is not traced again as left open when its worker exits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "native.ts": `import { Effect, Tracer } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("true").pipe(Effect.withTracer(Tracer.nativeTracer)) });`,
        "exiting.ts": `import { Effect, Tracer } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const underlying = yield* Effect.tracer;
  yield* Task.exec("true").pipe(Effect.withTracer(Tracer.make({ span: (options) => {
    const span = underlying.span(options);
    const end = span.end.bind(span);
    span.end = (endTime, exit) => {
      end(endTime, exit);
      process.exit(${WORKER_EXIT_CODE});
    };
    return span;
  } })));
}) });`,
        "proxied.ts": `import { Effect, Tracer } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const underlying = yield* Effect.tracer;
  yield* Task.exec("true").pipe(Effect.withTracer(Tracer.make({ span: (options) => new Proxy(underlying.span(options), {}) })));
}) });`,
      });
      const native = yield* runTaskProcess(project, "native.ts");
      expect(native.exitCode, native.standardError).toBe(0);
      const [nativeTrace] = yield* readTraces(yield* readProjectRunsDirectory(project, native));
      expect(nativeTrace?.spans.filter((span) => span.name === "true")).toEqual([]);
      const exiting = yield* runTaskProcess(project, "exiting.ts");
      expect(exiting.exitCode, exiting.standardError).toBe(1);
      const exitingTrace = (yield* readTraces(
        yield* readProjectRunsDirectory(project, exiting),
      )).find((trace) =>
        String(readAttribute(trace.findRunSpan()?.attributes ?? [], "sshkeda_task.entry")).endsWith(
          "/exiting.ts",
        ),
      );
      expect(
        exitingTrace?.spans
          .filter((span) => span.name === "true")
          .map((span) => [span.scope.name, span.status.code]),
      ).toEqual([["@sshkeda/task", SPAN_STATUS_UNSET]]);
      const proxied = yield* runTaskProcess(project, "proxied.ts");
      expect(proxied.exitCode, proxied.standardError).toBe(0);
      const proxiedTraces = yield* readTraces(yield* readProjectRunsDirectory(project, proxied));
      const proxiedTrace = proxiedTraces.find((trace) =>
        String(readAttribute(trace.findRunSpan()?.attributes ?? [], "sshkeda_task.entry")).endsWith(
          "/proxied.ts",
        ),
      );
      expect(
        proxiedTrace?.spans
          .filter((span) => span.name === "true")
          .map((span) => [span.scope.name, span.status.code]),
      ).toEqual([["effect", SPAN_STATUS_OK]]);
    }),
  ),
);

const startedWorkerSpan = (spanId: string) => ({
  traceId: "0".repeat(TRACE_ID_LENGTH),
  spanId,
  parentSpanId: null,
  name: `span ${spanId}`,
  kind: "internal" as const,
  startTimeUnixNano: "1",
  attributes: [["sshkeda_task.span.role", "body"] as const],
});

const exportedWorkerSpanJson = (spanId: string) =>
  `{"traceId":"${"0".repeat(TRACE_ID_LENGTH)}","spanId":"${spanId}","name":"span ${spanId}","kind":1,"startTimeUnixNano":"1","endTimeUnixNano":"2","attributes":[],"droppedAttributesCount":0,"events":[],"droppedEventsCount":0,"status":{"code":0},"links":[],"droppedLinksCount":0}`;

export const workerSpansLeftOpen = makeTest(
  "the trace file makes up a worker's Task span only when nothing ended or exported it by the time the file closes, in whatever order its worker's reports and exit arrive",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const traceFilePath = `${directory}/run/trace.jsonl`;
      const [exported, endedAfterExit, startedAfterExit, leftOpen] = [
        "000000000000000a",
        "000000000000000b",
        "000000000000000c",
        "000000000000000d",
      ];
      yield* Effect.scoped(
        Effect.gen(function* () {
          const traceFile = yield* makeTraceFile({ traceFilePath, createdBelow: directory });
          traceFile.recordWorkerTaskSpanStart(0, startedWorkerSpan(exported));
          traceFile.writeWorkerSpan({
            spanJson: exportedWorkerSpanJson(exported),
            spanId: exported,
            isTaskSpan: true,
          });
          traceFile.recordWorkerTaskSpanStart(0, startedWorkerSpan(endedAfterExit));
          traceFile.recordWorkerTaskSpanStart(0, startedWorkerSpan(leftOpen));
          traceFile.recordWorkerEnded(0, WORKER_ENDED_AT, {
            errorType: "WorkerExited",
            message: "worker 0 exited with code 7 before this span ended",
          });
          traceFile.recordWorkerTaskSpanEnd(endedAfterExit);
          traceFile.recordWorkerTaskSpanEnd(exported);
          traceFile.recordWorkerTaskSpanStart(0, startedWorkerSpan(startedAfterExit));
          traceFile.recordWorkerTaskSpanEnd(startedAfterExit);
        }).pipe(Effect.provide([OtlpExporter.layerFlusher, OtlpSerialization.layerJson])),
      );
      const text = yield* (yield* FileSystem.FileSystem).readFileString(traceFilePath);
      expect(
        [exported, endedAfterExit, startedAfterExit, leftOpen].map((spanId) =>
          countOccurrences(text, `"spanId":"${spanId}"`),
        ),
      ).toEqual([1, 0, 0, 1]);
      expect(countOccurrences(text, '"key":"error.type"')).toBe(1);
    }),
  ),
);

export const bodyInterruptedItself = makeTest(
  "a body that interrupts itself is traced as an error with error.type CauseWithoutFailure, on its body and on its task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.interrupt });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const body = trace?.findTaskSpan("body", "main");
      expect(body?.status).toEqual({
        code: SPAN_STATUS_ERROR,
        message: "was interrupted without a failure",
      });
      expect(readAttribute(body?.attributes ?? [], "error.type")).toBe("CauseWithoutFailure");
      const main = trace?.findTaskSpan("task", "main");
      expect(main?.status.code).toBe(SPAN_STATUS_ERROR);
      expect(readAttribute(main?.attributes ?? [], "error.type")).toBe("CauseWithoutFailure");
    }),
  ),
);

export const loggedAttributes = makeTest(
  "a logged line's record writes a non-finite annotation as OTLP/JSON does and one attribute per log span label",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("measured").pipe(Effect.annotateLogs({ ratio: Infinity }), Effect.withLogSpan("phase"), Effect.withLogSpan("phase")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const text = (yield* readProjectRunsDirectory(project, ran)).files
        .map((file) => file.text)
        .join("\n");
      expect(text).toContain('{"key":"ratio","value":{"doubleValue":"Infinity"}}');
      expect(countOccurrences(text, '"key":"effect.log_span.phase"')).toBe(1);
    }),
  ),
);

export const unwritable = makeTest(
  "an unwritable project runs directory does not change the exit code, prints the mkdir error and that the trace file is incomplete once, without a second error about the task output files, and still ends with the run line naming the folder it tried to make",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
      });
      const directory = `${project.home}/.sshkeda-task/runs/project`;
      yield* seedRunDirectory(`${directory}/old`, OLD_RUN_AGE_DAYS);
      yield* fileSystem.chmod(directory, OWNER_READ_EXECUTE_MODE);
      yield* Effect.addFinalizer(() =>
        fileSystem
          .chmod(directory, OWNER_FULL_MODE)
          .pipe(failTestCleanup("restore run folder fixture permissions")),
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] ran");
      expect(ran.standardError).toMatch(
        /^error: \S+: Error: EACCES: permission denied, mkdir '.+' \(EACCES\)\nerror: could not make \S+, so the trace file is incomplete$/mu,
      );
      expect(ran.standardError).not.toContain("TaskOutputFilesIncomplete");
      expect(countOccurrences(ran.standardError, "error: ")).toBe(ERROR_AND_CONTEXT_LINE_COUNT);
      const lines = ran.standardError.split("\n");
      expect(lines.at(-1)).toStartWith(`run: ${directory}/`);
      expect(lines.at(-1)).toEndWith("/");
      expect(yield* fileSystem.readDirectory(directory)).toEqual(["old"]);
    }),
  ),
);

export const commandOutputWithoutRunDirectory = makeTest(
  "when the run folder cannot be made, a passing command's standard output and standard error print under the task's name once each, since there is no file to keep them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.asVoid(Task.exec("sh", ["-c", "echo out-from-command; echo err-from-command >&2"])) });`,
      });
      yield* (yield* FileSystem.FileSystem).writeFileString(`${project.home}/.sshkeda-task`, "");
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toMatch(
        /^error: could not make \S+, so the trace file is incomplete$/mu,
      );
      expect(countOccurrences(ran.standardError, "[main] out-from-command\n")).toBe(1);
      expect(countOccurrences(ran.standardError, "[main] err-from-command\n")).toBe(1);
    }),
  ),
);

export const linkedProjectRunsDirectory = makeTest(
  "a run whose project runs directory in a shared state directory is a symbolic link to a directory owned by another user writes no trace, deletes no old run folders where it points, and its run line names the folder it tried to make",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
      });
      const sharedStateDirectory = `${project.home}/shared`;
      const linkedDirectory = "/tmp";
      yield* fileSystem.makeDirectory(`${sharedStateDirectory}/runs`, { recursive: true });
      const dataDirectory = `${linkedDirectory}/${(yield* Path.Path).basename(project.home)}-data`;
      yield* Effect.addFinalizer(() =>
        fileSystem
          .remove(dataDirectory, { recursive: true, force: true })
          .pipe(failTestCleanup("remove test data directory")),
      );
      yield* seedRunDirectory(dataDirectory, OLD_RUN_AGE_DAYS);
      yield* fileSystem.symlink(linkedDirectory, `${sharedStateDirectory}/runs/project`);
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_STATE_DIRECTORY: sharedStateDirectory,
      });
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] ran");
      expect(ran.standardError).toMatch(/^error: \S+: TraceFileIncomplete: /mu);
      expect(ran.standardError).toContain(
        `: TraceFileIncomplete: ${sharedStateDirectory}/runs/project is not a directory owned by the current user\nerror: the trace file is incomplete\n`,
      );
      const runLine = ran.standardError.split("\n").at(-1);
      expect(runLine).toStartWith(`run: ${sharedStateDirectory}/runs/project/`);
      expect(runLine).toEndWith("/");
      expect(yield* fileSystem.exists(`${dataDirectory}/trace.jsonl`)).toBe(true);
    }),
  ),
);

export const linkedRunsDirectoryOfTheUser = makeTest(
  "a run whose runs directory is a symbolic link the user made to a directory they own writes its run folder through the link",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
      });
      const runsDirectory = `${project.home}/.sshkeda-task/runs`;
      const linkedDirectory = `${project.home}/my-runs`;
      yield* fileSystem.makeDirectory(`${project.home}/.sshkeda-task`, { recursive: true });
      yield* fileSystem.makeDirectory(linkedDirectory);
      yield* fileSystem.symlink(linkedDirectory, runsDirectory);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).not.toContain("TraceFileIncomplete");
      const runDirectoryPath = findRunDirectoryPath(ran.standardError) ?? "";
      expect(runDirectoryPath.startsWith(`${runsDirectory}/project/`)).toBe(true);
      const runDirectoryName = runDirectoryPath.slice(`${runsDirectory}/project/`.length);
      expect(yield* fileSystem.readDirectory(`${linkedDirectory}/project`)).toEqual([
        runDirectoryName,
      ]);
      expect(
        (yield* fileSystem.readDirectory(
          `${linkedDirectory}/project/${runDirectoryName}`,
        )).toSorted(),
      ).toEqual(["1.output", "trace.jsonl"]);
    }),
  ),
);

export const retention = makeTest(
  "a run deletes its project's run folders older than 14 days and keeps recent ones, its own and other projects'",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
      });
      const runsDirectory = `${project.home}/.sshkeda-task/runs`;
      for (const { runDirectory, ageDays } of [
        { runDirectory: "project/old", ageDays: OLD_RUN_AGE_DAYS },
        { runDirectory: "project/recent", ageDays: RECENT_RUN_AGE_DAYS },
        { runDirectory: "other/old", ageDays: OLD_RUN_AGE_DAYS },
      ]) {
        yield* seedRunDirectory(`${runsDirectory}/${runDirectory}`, ageDays);
      }
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const projectRunsDirectory = `${runsDirectory}/project/`;
      const currentRunDirectoryPath = findRunDirectoryPath(ran.standardError) ?? "";
      expect(currentRunDirectoryPath.startsWith(projectRunsDirectory)).toBe(true);
      expect((yield* fileSystem.readDirectory(projectRunsDirectory)).toSorted()).toEqual(
        [currentRunDirectoryPath.slice(projectRunsDirectory.length), "recent"].toSorted(),
      );
      expect(yield* fileSystem.readDirectory(`${runsDirectory}/other`)).toEqual(["old"]);
    }),
  ),
);

export const retentionKeepsLiveRunsAndOtherDirectories = makeTest(
  "retention ages a run folder by its trace.jsonl, so it keeps an old folder whose trace is still being written, and never deletes a folder without a trace.jsonl in it",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
      });
      const projectRunsDirectory = `${project.home}/.sshkeda-task/runs/project`;
      yield* seedFile(`${projectRunsDirectory}/long-running/trace.jsonl`, RECENT_RUN_AGE_DAYS);
      yield* ageDirectory(`${projectRunsDirectory}/long-running`, OLD_RUN_AGE_DAYS);
      yield* seedFile(`${projectRunsDirectory}/notes/precious.txt`, OLD_RUN_AGE_DAYS);
      yield* seedFile(`${projectRunsDirectory}/notes/nested/trace.jsonl`, OLD_RUN_AGE_DAYS);
      yield* ageDirectory(`${projectRunsDirectory}/notes`, OLD_RUN_AGE_DAYS);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const currentRunDirectoryPath = findRunDirectoryPath(ran.standardError) ?? "";
      expect((yield* fileSystem.readDirectory(projectRunsDirectory)).toSorted()).toEqual(
        [
          currentRunDirectoryPath.slice(projectRunsDirectory.length + 1),
          "long-running",
          "notes",
        ].toSorted(),
      );
      expect(yield* fileSystem.exists(`${projectRunsDirectory}/notes/precious.txt`)).toBe(true);
      expect(yield* fileSystem.exists(`${projectRunsDirectory}/notes/nested/trace.jsonl`)).toBe(
        true,
      );
    }),
  ),
);

export const retentionKeepsRunsStillAlive = makeTest(
  "retention never deletes the folder of a run whose process is still alive, however old its trace.jsonl, and still deletes an old folder whose run has ended",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
        "serve.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "serve", run: Effect.log("serving").pipe(Effect.andThen(Effect.never)) });`,
      });
      const ended = yield* runTaskProcess(project, "main.ts");
      const endedRunDirectoryPath = findRunDirectoryPath(ended.standardError) ?? "";
      const serving = yield* startTaskProcess(project, "serve.ts");
      const servingRunDirectoryPath =
        findRunDirectoryPath(yield* serving.waitForLine("run: ")) ?? "";
      yield* serving.waitForLine("[serve] serving");
      const modified = new Date(
        (yield* Clock.currentTimeMillis) - OLD_RUN_AGE_DAYS * DAY_MILLISECONDS,
      );
      for (const runDirectoryPath of [endedRunDirectoryPath, servingRunDirectoryPath]) {
        yield* fileSystem.utimes(`${runDirectoryPath}/trace.jsonl`, modified, modified);
      }
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(yield* fileSystem.exists(endedRunDirectoryPath)).toBe(false);
      expect(yield* fileSystem.exists(`${servingRunDirectoryPath}/trace.jsonl`)).toBe(true);
      yield* serving.sendSignal("INT");
      expect((yield* serving.waitForExit).exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    }),
  ),
);

export const refusedWithOldRunDirectories = makeTest(
  "a run refused for broken task definitions exits 1 and still deletes old run folders when its project runs directory already holds them",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "same", run: Effect.void });
export const b = Task.make({ name: "same", run: Effect.void });`,
      });
      const projectRunsDirectory = `${project.home}/.sshkeda-task/runs/project`;
      const seededRunDirectories = [
        { name: "old", ageDays: OLD_RUN_AGE_DAYS },
        { name: "recent", ageDays: RECENT_RUN_AGE_DAYS },
      ].flatMap(({ name, ageDays }) =>
        Array.from({ length: SEEDED_RUN_DIRECTORY_COUNT }, (_unused, index) => ({
          runDirectoryName: `${name}-${index}`,
          ageDays,
        })),
      );
      yield* Effect.forEach(
        seededRunDirectories,
        ({ runDirectoryName, ageDays }) =>
          seedRunDirectory(`${projectRunsDirectory}/${runDirectoryName}`, ageDays),
        { discard: true },
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError.split("\n")).toContain(
        "error: main.ts:3:21: TaskMadeMoreThanOnce: makes the task [same], which 2 Task.make() calls make",
      );
      expect(ran.standardError).toContain("failed: 0 of 0 tasks");
      const currentRunDirectoryPath = findRunDirectoryPath(ran.standardError) ?? "";
      expect(currentRunDirectoryPath.startsWith(`${projectRunsDirectory}/`)).toBe(true);
      expect((yield* fileSystem.readDirectory(projectRunsDirectory)).toSorted()).toEqual(
        [
          currentRunDirectoryPath.slice(projectRunsDirectory.length + 1),
          ...seededRunDirectories
            .filter(({ ageDays }) => ageDays === RECENT_RUN_AGE_DAYS)
            .map(({ runDirectoryName }) => runDirectoryName),
        ].toSorted(),
      );
    }),
  ),
);

export const taskFiles = makeTest(
  "the main thread traces one import, of the root task file, however many files make tasks, and each task traces its worker's import of that file under its own span",
  Effect.scoped(
    Effect.gen(function* () {
      const files = {
        "lib.ts": "export const base = 1;\n",
        "a.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./lib.ts";
export const a = Task.make({ name: "a", run: Effect.succeed(base) });`,
        "b.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const b = Task.make({ name: "b", run: Effect.succeed(2) });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { a } from "./a.ts";
import { b } from "./b.ts";
export default Task.make({ name: "main", run: Task.all([a, b]) });`,
      };
      const project = yield* makeProject(files);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const imports = trace?.withRole("import") ?? [];
      const importedFile = (span: (typeof imports)[number] | undefined) =>
        readAttribute(span?.attributes ?? [], "sshkeda_task.file");
      const mainThreadImports = imports.filter((span) => span.thread === "main-thread");
      expect(mainThreadImports.map((span) => importedFile(span))).toEqual(["main.ts"]);
      expect(mainThreadImports[0]?.parentSpanId).toBe(trace?.findRunSpan()?.spanId);
      for (const taskName of ["main", "a", "b"]) {
        const taskImports = (
          trace?.findChildSpans(trace.findTaskSpan("task", taskName)) ?? []
        ).filter((span) => span.role === "import");
        expect(taskImports.map((span) => [span.thread, importedFile(span)])).toEqual([
          ["worker", "main.ts"],
        ]);
      }
    }),
  ),
);

export const runsDirectoryOfAnInstalledStart = makeTest(
  "a run started from a file inside node_modules files its run folder under the package of the folder it was started in, or under that folder's name when its package.json gives no name, and fails naming that package.json when it is not valid",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/tool/package.json": '{ "name": "tool", "type": "module" }',
        "node_modules/tool/run.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });`,
        "nameless/package.json": "{}",
        "unreadable/package.json": "not json",
      });
      const state = `${project.home}/.sshkeda-task`;
      for (const [startedIn, directoryName] of [
        [project.directory, "project"],
        [`${project.directory}/nameless`, "nameless"],
        [`${project.directory}/node_modules/tool`, "tool"],
      ] as const) {
        const ran = yield* runTaskProcess(project, "node_modules/tool/run.ts", {}, startedIn);
        expect(ran.exitCode, ran.standardError).toBe(0);
        expect(ran.standardError).toContain(`run: ${state}/runs/${directoryName}/`);
      }
      const unreadableDirectory = `${project.directory}/unreadable`;
      const refused = yield* runTaskProcess(
        project,
        "node_modules/tool/run.ts",
        {},
        unreadableDirectory,
      );
      expect(refused.exitCode).toBe(1);
      expect(refused.standardError).toMatch(
        /^error: package\.json: SyntaxError: JSONC Parse error: Unexpected not\nerror: could not parse package\.json$/mu,
      );
      expect(refused.standardError).not.toContain("[main] ran");
      expect(
        yield* FileSystem.FileSystem.use((fileSystem) =>
          fileSystem.exists(`${state}/runs/unreadable`),
        ),
      ).toBe(false);
    }),
  ),
);

export const helloTree = makeTest(
  "a passing run of one task that runs one command is one trace with one root and nine Task spans: the run, its import, the task with its worker wait, start, worker import, body with the command, and cleanup, all unset",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "hello", run: Task.exec("echo", ["hello world"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const spans = trace?.spans ?? [];
      const nameOf = (spanId: string | undefined) =>
        spans.find((span) => span.spanId === spanId)?.name ?? null;
      expect(
        spans
          .map((span) => [span.name, nameOf(span.parentSpanId), span.role, span.thread])
          .toSorted((left, right) => String(left).localeCompare(String(right))),
      ).toEqual(
        [
          ["run hello", null, "run", "main-thread"],
          ["import task file", "run hello", "import", "main-thread"],
          ["hello", "run hello", "task", "main-thread"],
          ["wait for worker", "hello", "setup", "main-thread"],
          ["start task", "hello", "setup", "main-thread"],
          ["import task file", "hello", "import", "worker"],
          ["body hello", "hello", "body", "worker"],
          ["echo", "body hello", "process", "worker"],
          ["cleanup hello", "hello", "cleanup", "main-thread"],
        ].toSorted((left, right) => String(left).localeCompare(String(right))),
      );
      expect(new Set(spans.map((span) => span.traceId)).size).toBe(1);
      expect(spans.map((span) => span.status.code)).toEqual(spans.map(() => SPAN_STATUS_UNSET));
      const hello = trace?.findTaskSpan("task", "hello");
      expect(readAttribute(hello?.attributes ?? [], "sshkeda_task.output.file.name")).toBe(
        "1.output",
      );
      expect(readAttribute(hello?.attributes ?? [], "sshkeda_task.state")).toBe("passed");
      const printed = trace?.logs.find((log) => log.body.stringValue === "hello world");
      expect(printed?.spanId).toBe(trace?.findTaskSpan("body", "hello")?.spanId);
    }),
  ),
);

const USER_SPAN_NAMES = [
  "executeTaskFiber",
  "runTaskBody",
  "check",
  "body check",
  "cleanup check",
  "import task file",
  "run check",
  "start task",
  "wait for worker",
  "echo",
  "process",
] as const;

const readAttributes = (attributes: Parameters<typeof readAttribute>[0]) =>
  Object.fromEntries(attributes.map(({ key }) => [key, readAttribute(attributes, key)]));

const CapturedAttributes = Schema.Record(
  Schema.String,
  Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]),
);

const CapturedSpans = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    traceId: Schema.String,
    spanId: Schema.String,
    parentSpanId: Schema.optionalKey(Schema.String),
    start: Schema.String,
    end: Schema.NullOr(Schema.String),
    attributes: CapturedAttributes,
    events: Schema.Array(Schema.Tuple([Schema.String, Schema.String, CapturedAttributes])),
    links: Schema.Array(Schema.Tuple([Schema.String, CapturedAttributes])),
  }),
);

export const userSpans = makeTest(
  "spans a body makes, even ones named like Task's own, keep their names, times, attributes, events, links, ids, parents, status and log records, apart from Task's spans",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Clock, Effect, Option } from "effect";
import type { Tracer } from "effect";
import { Task } from "@sshkeda/task";
const captured: Tracer.Span[] = [];
const capture = Effect.currentSpan.pipe(Effect.tap((span) => Effect.sync(() => { captured.push(span); })), Effect.orDie);
const lint = Effect.fn("lint-rules")(function* () {
  yield* capture;
  yield* Effect.annotateCurrentSpan("check.command", "oxlint --type-aware");
  yield* Effect.annotateCurrentSpan("check.failure", "rule x failed");
  const span = yield* Effect.currentSpan;
  span.event("checked", yield* Clock.currentTimeNanos, { "check.rule": "x" });
  yield* Effect.log("linting");
  for (const name of [${USER_SPAN_NAMES.map((name) => `"${name}"`).join(", ")}]) {
    yield* capture.pipe(Effect.andThen(Effect.log(\`inside \${name}\`)), Effect.withSpan(name, { attributes: { "check.command": name } }));
  }
  yield* Task.exec("echo", ["from lint"]);
  yield* capture.pipe(Effect.withSpan("linked", { links: [{ span: captured[0]!, attributes: { "check.link": true } }] }));
  yield* capture.pipe(Effect.withSpan("own root", { root: true }));
  yield* capture.pipe(Effect.andThen(Effect.fail("bad")), Effect.withSpan("failing user span"), Effect.ignore);
});
export default Task.make({ name: "check", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    const json = captured.map((span) => ({
      name: span.name,
      traceId: span.traceId,
      spanId: span.spanId,
      ...Option.match(span.parent, { onNone: () => ({}), onSome: (parent) => ({ parentSpanId: parent.spanId }) }),
      start: String(span.status.startTime),
      end: span.status._tag === "Ended" ? String(span.status.endTime) : null,
      attributes: Object.fromEntries(span.attributes),
      events: (span as unknown as { _events?: [string, bigint, unknown][] })._events?.map(([name, time, attributes]) => [name, String(time), attributes]) ?? [],
      links: span.links.map((link) => [link.span.spanId, link.attributes]),
    }));
    return Bun.write(\`\${process.env.HOME}/captured.json\`, JSON.stringify(json));
  }).pipe(Effect.andThen((write) => Effect.promise(() => write))));
  yield* lint();
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const captured = yield* Schema.decodeEffect(Schema.fromJsonString(CapturedSpans))(
        yield* (yield* FileSystem.FileSystem).readFileString(`${project.home}/captured.json`),
      );
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(captured.map((span) => span.name)).toEqual([
        "lint-rules",
        ...USER_SPAN_NAMES,
        "linked",
        "own root",
        "failing user span",
      ]);
      for (const capturedSpan of captured) {
        const span = trace?.spans.find((candidate) => candidate.spanId === capturedSpan.spanId);
        expect(span?.role).toBeUndefined();
        expect(span?.scope.name).toBe("effect");
        expect(span?.kind).toBe(SPAN_KIND_INTERNAL);
        expect({
          name: span?.name,
          traceId: span?.traceId,
          spanId: span?.spanId,
          ...(span?.parentSpanId === undefined ? {} : { parentSpanId: span.parentSpanId }),
          start: span?.startTimeUnixNano,
          end: capturedSpan.end === null ? null : span?.endTimeUnixNano,
          attributes: readAttributes(span?.attributes ?? []),
          events: (span?.events ?? [])
            .filter((event) => event.name !== "exception")
            .map((event) => [event.name, event.timeUnixNano, readAttributes(event.attributes)]),
          links: (span?.links ?? []).map((link) => [link.spanId, readAttributes(link.attributes)]),
        }).toEqual({
          ...capturedSpan,
          events: capturedSpan.events.map((event) => [...event]),
          links: capturedSpan.links.map((link) => [...link]),
        });
        expect(span?.status).toEqual(
          capturedSpan.name === "failing user span"
            ? { code: SPAN_STATUS_ERROR, message: "bad" }
            : { code: SPAN_STATUS_OK },
        );
      }
      const lintRules = trace?.findOnlySpan("lint-rules");
      expect(lintRules?.parentSpanId).toBe(trace?.findTaskSpan("body", "check")?.spanId);
      expect(trace?.findOnlySpan("own root")?.parentSpanId).toBeUndefined();
      const echo = trace?.withRole("process").find((span) => span.name === "echo");
      expect(echo?.parentSpanId).toBe(lintRules?.spanId);
      for (const name of USER_SPAN_NAMES) {
        const logged = trace?.logs.find((log) => log.body.stringValue === `inside ${name}`);
        expect(logged?.spanId).toBe(
          captured.find((capturedSpan) => capturedSpan.name === name)?.spanId,
        );
      }
      expect(trace?.logs.find((log) => log.body.stringValue === "linting")?.spanId).toBe(
        lintRules?.spanId,
      );
    }),
  ),
);
