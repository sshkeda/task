import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Clock, Duration, Effect, Exit, FileSystem, Path } from "effect";

import taskPackageJson from "../../package.json" with { type: "json" };
import { availableParallelism } from "../../src/shared/read-environment-config.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const WAIT_UNTIL_TRACED = new URL("../shared/wait-until-traced.ts", import.meta.url);

const OWNER_READ_WRITE_MODE = 0o600;

const OWNER_READ_EXECUTE_MODE = 0o500;

const OWNER_FULL_MODE = 0o700;

const SPAN_STATUS_ERROR = 2;

const ANNOTATED_COUNT = 2;

const PROCESS_EXIT_CODE = 3;

const LOGGED_LINE_COUNT = 100;

const LOGGED_BEFORE_EXIT_COUNT = 500;

const WORKER_EXIT_CODE = 7;

const SPANS_BEFORE_EXIT_COUNT = 500;

const DAY_MILLISECONDS = Duration.toMillis(Duration.days(1));

const OLD_TRACE_AGE_DAYS = 15;

const RECENT_TRACE_AGE_DAYS = 13;

const ENVIRONMENT = { MODE: "quickly", NAME: "someone", TOKEN: "hunter2" } as const;

export const traced = Task.make(
  "a traced run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
const work = Effect.fn("work")(function* () { yield* Effect.log("working"); });
export const dependency = Task.make("dep", Effect.gen(function* () {
  yield* Config.String("NAME");
  const token = yield* Config.Redacted("TOKEN");
  yield* Effect.log(token);
  yield* Effect.log(\`token \${token}\`, { token });
  yield* Effect.annotateCurrentSpan("token", token);
}));
export const main = Task.make("main", Effect.gen(function* () {
  yield* dependency;
  yield* work();
  yield* Effect.log("annotated").pipe(Effect.annotateLogs({ step: "one", count: 2 }), Effect.withLogSpan("phase"));
  yield* ChildProcess.make("sh", ["-c", "exit 3"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
  yield* Effect.log(\`spawned \${Bun.spawn(["sleep", "1000"]).pid}\`);
}));
if (import.meta.main) Task.main(main);`,
      });
      return yield* readTraceDirectory(
        project,
        yield* runTaskProcess(project, "main.ts", ENVIRONMENT),
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failed = Task.make(
  "a traced run that fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () { return yield* Effect.fail(yield* Config.Redacted("TOKEN")); }));
if (import.meta.main) Task.main(main);`,
      });
      return yield* readTraceDirectory(
        project,
        yield* runTaskProcess(project, "main.ts", ENVIRONMENT),
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const fresh = Task.make(
  "a run writes its trace file as OTLP JSON lines that only its owner can read and names it at the end",
  Effect.gen(function* () {
    const traceDirectory = yield* traced;
    const [trace, ...others] = yield* readTraces(traceDirectory);
    expect(others).toHaveLength(0);
    expect(
      traceDirectory.standardError.trimEnd().endsWith(`
wrote the trace to ${trace?.file}`),
    ).toBe(true);
    expect(trace?.mode).toBe(OWNER_READ_WRITE_MODE);
    const run = trace?.findOnlySpan("runRootTaskFiber");
    expect(run?.parentSpanId).toBeUndefined();
    expect(run?.serviceName).toBe(taskPackageJson.name);
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.root.task")).toBe("main");
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.entry")).toBe(
      `${traceDirectory.directory}/main.ts`,
    );
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.exit.code")).toBe(0);
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.version")).toMatch(/^\d+\.\d+\.\d+/u);
    expect(readAttribute(run?.attributes ?? [], "host.cpus")).toBeGreaterThan(0);
    const parallelism = yield* availableParallelism;
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.workers.limit")).toBe(parallelism);
    expect(readAttribute(run?.attributes ?? [], "sshkeda_task.workers.started")).toBe(1);
    expect(
      readAttribute(run?.attributes ?? [], "sshkeda_task.elapsed.milliseconds"),
    ).toBeGreaterThan(0);
    const main = trace?.findOnlySpan("executeTaskFiber main");
    expect(main?.parentSpanId).toBe(run?.spanId);
    expect(readAttribute(main?.attributes ?? [], "sshkeda_task.state")).toBe("passed");
    expect(
      readAttribute(
        trace?.findOnlySpan("executeTaskFiber dep")?.attributes ?? [],
        "sshkeda_task.state",
      ),
    ).toBe("passed");
  }),
);

export const failure = Task.make(
  "a failed run's trace holds the failed task and its exit code",
  Effect.gen(function* () {
    const traceDirectory = yield* failed;
    expect(traceDirectory.exitCode).toBe(1);
    const [trace] = yield* readTraces(traceDirectory);
    const main = trace?.findOnlySpan("executeTaskFiber main");
    expect(main?.status.code).toBe(SPAN_STATUS_ERROR);
    expect(readAttribute(main?.attributes ?? [], "sshkeda_task.state")).toBe("failed");
    expect(
      readAttribute(
        trace?.findOnlySpan("runRootTaskFiber")?.attributes ?? [],
        "sshkeda_task.exit.code",
      ),
    ).toBe(1);
  }),
);

export const hung = Task.make(
  "a run killed while a task hangs still traces that the task started",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const waitUntilTraced = yield* (yield* Path.Path).fromFileUrl(WAIT_UNTIL_TRACED);
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const hang = Task.make("hang", Effect.never);
if (import.meta.main) Task.main(hang);`,
      });
      const directory = `${project.home}/.sshkeda-task/traces/project`;
      yield* fileSystem.makeDirectory(directory, { recursive: true });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* Task.exec(process.execPath, [waitUntilTraced, directory, '"task started"']);
      yield* command.sendSignal("KILL");
      expect(Exit.isFailure(yield* exitAsValue(command.waitForExit))).toBe(true);
      const [trace] = yield* readTraces(
        yield* readTraceDirectory(project, { standardError: "", exitCode: 137 }),
      );
      const started = trace?.logs.find(
        (log) =>
          log.body.stringValue === "task started" &&
          readAttribute(log.attributes, "sshkeda_task.task") === "hang",
      );
      expect(started?.spanId).toMatch(/^[0-9a-f]{16}$/u);
      expect(started?.traceId).toMatch(/^[0-9a-f]{32}$/u);
      expect(trace?.spans.map((span) => span.spanId)).not.toContain(started?.spanId);
      expect(trace?.named("runRootTaskFiber")).toHaveLength(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const nested = Task.make(
  "a body's own span is on the run's trace, comes from a worker, nests under its task and the run, and shares its span with its logs",
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const work = trace?.findOnlySpan("work");
    expect(work?.thread).toBe("worker");
    expect(work?.traceId).toBe(trace?.findOnlySpan("runRootTaskFiber")?.traceId);
    const ancestors = trace?.findParentSpanNames(work) ?? [];
    expect(ancestors).toContain("executeTaskFiber main");
    expect(ancestors.at(-1)).toBe("runRootTaskFiber");
    const working = trace?.logs.find((log) => log.body.stringValue === "working");
    expect(working?.spanId).toBe(work?.spanId);
  }),
);

export const logAnnotations = Task.make(
  "annotations and log spans a body puts on its own logs reach the trace file with that log",
  Effect.gen(function* () {
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
);

export const processes = Task.make(
  "each process a task starts through ChildProcess is a span with its program name, pid and exit code",
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const spawned = trace?.findOnlySpan("process");
    expect(spawned?.thread).toBe("worker");
    expect(readAttribute(spawned?.attributes ?? [], "process.executable.name")).toBe("sh");
    expect(readAttribute(spawned?.attributes ?? [], "process.pid")).toBeGreaterThan(0);
    expect(readAttribute(spawned?.attributes ?? [], "process.exit.code")).toBe(PROCESS_EXIT_CODE);
    expect(trace?.findParentSpanNames(spawned)).toContain("executeTaskFiber main");
  }),
);

export const killedProcess = Task.make(
  "a process killed at the end is traced with its pid and program name, and the trace lists the program names it killed",
  Effect.gen(function* () {
    const traceDirectory = yield* traced;
    const processId = Number(
      /\[main\] spawned (?<processId>\d+)/u.exec(traceDirectory.standardError)?.[1],
    );
    const [trace] = yield* readTraces(traceDirectory);
    const killed = trace?.logs.find((log) => log.body.stringValue === "process killed");
    expect(readAttribute(killed?.attributes ?? [], "process.pid")).toBe(processId);
    expect(readAttribute(killed?.attributes ?? [], "process.executable.name")).toBe("sleep");
    expect(
      (trace?.spans ?? []).map((span) =>
        readAttribute(span.attributes, "sshkeda_task.processes.killed.executable_names"),
      ),
    ).toContainEqual(["sleep"]);
  }),
);

export const redacted = Task.make(
  "a redacted value appears nowhere in the trace file, whether logged, annotated or failed with",
  Effect.gen(function* () {
    for (const traceDirectory of [yield* traced, yield* failed]) {
      expect(traceDirectory.files.map((file) => file.text).join("")).toContain("<redacted>");
      for (const file of traceDirectory.files) {
        expect(file.text).not.toContain(ENVIRONMENT.TOKEN);
      }
    }
  }),
);

export const processUsage = Task.make(
  "each process span holds its CPU time and peak memory",
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* traced);
    const attributes = trace?.findOnlySpan("process")?.attributes ?? [];
    expect(readAttribute(attributes, "sshkeda_task.process.cpu.milliseconds")).toBeGreaterThan(0);
    expect(readAttribute(attributes, "sshkeda_task.process.memory.peak.bytes")).toBeGreaterThan(0);
  }),
);

export const live = Task.make(
  "a body's own span reaches the trace file while the run is still going",
  Effect.scoped(
    Effect.gen(function* () {
      const waitUntilTraced = yield* (yield* Path.Path).fromFileUrl(WAIT_UNTIL_TRACED);
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.void.pipe(Effect.withSpan("marker"));
  const directory = \`\${yield* Config.String("HOME")}/.sshkeda-task/traces/project\`;
  yield* Task.exec(process.execPath, ["${waitUntilTraced}", directory, '"name":"marker"']);
  yield* Effect.log("marker traced");
}), { timeout: "30 seconds", cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] marker traced");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const terminatedSpans = Task.make(
  "a span a body ended just before its worker froze and was terminated is in the trace file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make("spin", Effect.void.pipe(
  Effect.withSpan("before freezing"),
  Effect.andThen(Effect.sync(() => { while (true) {} })),
), { timeout: "1 second", cleanup: "1 second" });
export const main = Task.make("main", spin);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[spin] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(trace?.findOnlySpan("before freezing")?.thread).toBe("worker");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const lastBatch = Task.make(
  "the trace file written at the end holds every span, including one a body ended as the run finished",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.void.pipe(Effect.withSpan("last")));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(trace?.findOnlySpan("last")?.thread).toBe("worker");
      const run = trace?.findOnlySpan("runRootTaskFiber");
      expect(run).toBeDefined();
      expect(trace?.findOnlySpan("executeTaskFiber main")?.parentSpanId).toBe(run?.spanId);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const loggedLineBatches = Task.make(
  "lines a body logs reach the trace file in batches of log records, not one file line each",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.forEach(Array.from({ length: ${LOGGED_LINE_COUNT} }, (_, line) => line), (line) => Effect.log(\`line \${line}\`), { discard: true }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(
        trace?.logs.filter((log) => log.body.stringValue?.startsWith("line ") === true),
      ).toHaveLength(LOGGED_LINE_COUNT);
      expect(
        trace?.text.split("\n").filter((line) => line.includes('"resourceLogs"')).length,
      ).toBeLessThan(LOGGED_LINE_COUNT);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const loggedBeforeProcessExit = Task.make(
  "every line a body logs right before it calls process.exit reaches the trace file as a log record",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.forEach(Array.from({ length: ${LOGGED_BEFORE_EXIT_COUNT} }, (_, line) => line), (line) => Effect.log(\`line \${line}\`), { discard: true }).pipe(Effect.andThen(Effect.sync(() => process.exit(${WORKER_EXIT_CODE})))));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(
        `[main] error WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`,
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const spansBeforeProcessExit = Task.make(
  "every span a body ends right before it calls process.exit reaches the trace file, in batches of spans",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.forEach(Array.from({ length: ${SPANS_BEFORE_EXIT_COUNT} }, (_, span) => span), (span) => Effect.withSpan(Effect.void, \`span \${span}\`), { discard: true }).pipe(Effect.andThen(Effect.sync(() => process.exit(${WORKER_EXIT_CODE})))));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(
        `[main] error WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`,
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      const ended = (trace?.spans ?? []).filter(
        (span) => span.name.startsWith("span ") && span.thread === "worker",
      );
      expect(new Set(ended.map((span) => span.name)).size).toBe(SPANS_BEFORE_EXIT_COUNT);
      expect(
        trace?.text.split("\n").filter((line) => line.includes('"resourceSpans"')).length,
      ).toBeLessThan(SPANS_BEFORE_EXIT_COUNT);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const loggedAttributes = Task.make(
  "a logged line's record writes a non-finite annotation as OTLP/JSON does and one attribute per log span label",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("measured").pipe(Effect.annotateLogs({ ratio: Infinity }), Effect.withLogSpan("phase"), Effect.withLogSpan("phase")));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const text = (yield* readTraceDirectory(project, ran)).files
        .map((file) => file.text)
        .join("\n");
      expect(text).toContain('{"key":"ratio","value":{"doubleValue":"Infinity"}}');
      expect(countOccurrences(text, '"key":"effect.log_span.phase"')).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unwritable = Task.make(
  "an unwritable trace directory does not change the exit code and prints 'the trace file is incomplete because ...'",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("ran"));
if (import.meta.main) Task.main(main);`,
      });
      const directory = `${project.home}/.sshkeda-task/traces/project`;
      yield* fileSystem.makeDirectory(directory, { recursive: true });
      const oldTrace = new Date(
        (yield* Clock.currentTimeMillis) - OLD_TRACE_AGE_DAYS * DAY_MILLISECONDS,
      );
      yield* fileSystem.writeFileString(`${directory}/old.jsonl`, "{}\n");
      yield* fileSystem.utimes(`${directory}/old.jsonl`, oldTrace, oldTrace);
      yield* fileSystem.chmod(directory, OWNER_READ_EXECUTE_MODE);
      yield* Effect.addFinalizer(() =>
        fileSystem
          .chmod(directory, OWNER_FULL_MODE)
          .pipe(failTestCleanup("restore trace fixture permissions")),
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] ran");
      expect(ran.standardError).toMatch(
        /: error TraceFileIncomplete: the trace file is incomplete because .+ could not be written \(/u,
      );
      expect(ran.standardError).not.toContain("wrote the trace to");
      expect(ran.standardError).not.toContain("old.jsonl");
      expect(yield* fileSystem.readDirectory(directory)).toEqual(["old.jsonl"]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const retention = Task.make(
  "a run deletes its project's trace files older than 14 days and keeps recent ones, its own and other projects'",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("ran"));
if (import.meta.main) Task.main(main);`,
      });
      const tracesDirectory = `${project.home}/.sshkeda-task/traces`;
      const seeded = [
        { file: "project/old.jsonl", ageDays: OLD_TRACE_AGE_DAYS },
        { file: "project/recent.jsonl", ageDays: RECENT_TRACE_AGE_DAYS },
        { file: "other/old.jsonl", ageDays: OLD_TRACE_AGE_DAYS },
      ];
      for (const { file, ageDays } of seeded) {
        const filePath = `${tracesDirectory}/${file}`;
        const modified = new Date((yield* Clock.currentTimeMillis) - ageDays * DAY_MILLISECONDS);
        yield* fileSystem.makeDirectory(filePath.slice(0, filePath.lastIndexOf("/")), {
          recursive: true,
        });
        yield* fileSystem.writeFileString(filePath, "{}\n");
        yield* fileSystem.utimes(filePath, modified, modified);
      }
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const { file: currentTraceFilePath = "" } =
        /^wrote the trace to (?<file>.+)$/mu.exec(ran.standardError)?.groups ?? {};
      const projectTracesDirectory = `${tracesDirectory}/project/`;
      expect(currentTraceFilePath.startsWith(projectTracesDirectory)).toBe(true);
      expect((yield* fileSystem.readDirectory(projectTracesDirectory)).toSorted()).toEqual(
        [currentTraceFilePath.slice(projectTracesDirectory.length), "recent.jsonl"].toSorted(),
      );
      expect(yield* fileSystem.readDirectory(`${tracesDirectory}/other`)).toEqual(["old.jsonl"]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const taskFiles = Task.make(
  "the trace has one entry per imported task file",
  Effect.scoped(
    Effect.gen(function* () {
      const files = {
        "lib.ts": "export const base = 1;\n",
        "a.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./lib.ts";
export const a = Task.make("a", Effect.succeed(base));`,
        "b.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const b = Task.make("b", Effect.succeed(2));`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { a } from "./a.ts";
import { b } from "./b.ts";
export const main = Task.make("main", Task.all([a, b]));
if (import.meta.main) Task.main(main);`,
      };
      const project = yield* makeProject(files);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      const entries = (trace?.spans ?? []).flatMap((span) => {
        const file = readAttribute(span.attributes, "sshkeda_task.file");
        return typeof file === "string" ? [file] : [];
      });
      expect(entries.toSorted()).toEqual(["a.ts", "b.ts", "main.ts"]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
