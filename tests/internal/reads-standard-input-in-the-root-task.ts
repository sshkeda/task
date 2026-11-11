import { expect } from "bun:test";

import { Effect, Queue, Stream } from "effect";

import { TASK_TRACE_LABEL_PREFIX } from "../../src/shared/names-from-package-json.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const encodeText = (text: string) => new TextEncoder().encode(text);

const HOOK_READING_JSON = `import { Effect, Schema, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
const HookInput = Schema.fromJsonString(Schema.Struct({ tool_name: Schema.String }));
export const hook = Task.make({ name: "hook", run: Effect.gen(function* () {
  const text = yield* Stdio.Stdio.use((stdio) => Stream.mkString(Stream.decodeText(stdio.stdin)));
  const input = yield* Schema.decodeUnknownEffect(HookInput)(text);
  yield* Effect.log(\`tool is \${input.tool_name}\`);
}) });
export default hook;`;

const HOOK_COUNTING_BYTES = `import { Effect, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
export const hook = Task.make({ name: "hook", run: Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  const counted = yield* Stream.runFold(stdio.stdin, () => ({ bytes: 0, checksum: 0 }), (sum, chunk) => {
    let checksum = sum.checksum;
    for (const byte of chunk) checksum = (checksum + byte) % 4294967296;
    return { bytes: sum.bytes + chunk.length, checksum };
  });
  yield* Effect.log(\`read \${counted.bytes} bytes with checksum \${counted.checksum}\`);
}) });
export default hook;`;

export const pipedJson = makeTest(
  "the root task reads piped JSON from standard input through Stdio",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOOK_READING_JSON });
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.make(encodeText('{"tool_name":'), encodeText('"Edit"}')),
      );
      expect(ran.standardError).toContain("[hook] tool is Edit");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const emptyStandardInput = makeTest(
  "the root task reads empty standard input as a stream that ends without chunks",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOOK_COUNTING_BYTES });
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.empty,
      );
      expect(ran.standardError).toContain("[hook] read 0 bytes with checksum 0");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

const LARGE_INPUT_CHUNK_COUNT = 50;

const BYTES_PER_KIBIBYTE = 1024;

const KIBIBYTES_PER_MEBIBYTE = 1024;

const LARGE_INPUT_CHUNK_BYTES = KIBIBYTES_PER_MEBIBYTE * BYTES_PER_KIBIBYTE;

const BYTE_VALUE_COUNT = 256;

const CHECKSUM_MODULUS = 4_294_967_296;

export const largeStandardInput = makeTest(
  "the root task reads 50 MiB of standard input intact",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOOK_COUNTING_BYTES });
      const chunkIndexes = Array.from(
        { length: LARGE_INPUT_CHUNK_COUNT },
        (_unused, index) => index,
      );
      const expectedChecksum =
        chunkIndexes.reduce(
          (sum, index) => sum + (index % BYTE_VALUE_COUNT) * LARGE_INPUT_CHUNK_BYTES,
          0,
        ) % CHECKSUM_MODULUS;
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.fromIterable(chunkIndexes).pipe(
          Stream.map((index) =>
            new Uint8Array(LARGE_INPUT_CHUNK_BYTES).fill(index % BYTE_VALUE_COUNT),
          ),
        ),
      );
      expect(ran.standardError).toContain(
        `[hook] read ${LARGE_INPUT_CHUNK_COUNT * LARGE_INPUT_CHUNK_BYTES} bytes with checksum ${expectedChecksum}`,
      );
      expect(ran.exitCode).toBe(0);
    }),
  ),
  "2 minutes",
);

export const unreadStandardInput = makeTest(
  "a root task that never reads standard input finishes while standard input stays open",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const hook = Task.make({ name: "hook", run: Effect.log("finished without reading") });
export default hook;`,
      });
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.fromQueue(yield* Queue.unbounded<Uint8Array>()),
      );
      expect(ran.standardError).toContain("[hook] finished without reading");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const nonRootTaskReadingStandardInput = makeTest(
  "a task other than the root task fails with StandardInputReadOutsideRootTask when it reads standard input",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
export const reader = Task.make({ name: "reader", run: Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  yield* Effect.log("reader started");
  return yield* Stream.mkString(Stream.decodeText(stdio.stdin));
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* reader;
}) });`,
      });
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.make(encodeText("unread")),
      );
      expect(ran.standardError).toContain("[reader] reader started");
      expect(ran.standardError).toContain(
        `[reader] error StandardInputReadOutsideRootTask: read standard input, which only the root task "main" can read`,
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const interruptWhileReading = makeTest(
  "Ctrl-C while the root task waits on standard input stops the read, runs the finalizers and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
export const hook = Task.make({ name: "hook", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  const stdio = yield* Stdio.Stdio;
  yield* Stream.runForEach(stdio.stdin, (chunk) => Effect.log(\`received \${chunk.length} bytes\`));
  yield* Effect.log("standard input ended");
}) });
export default hook;`,
      });
      const standardInputChunks = yield* Queue.unbounded<Uint8Array>();
      yield* Queue.offer(standardInputChunks, encodeText("first"));
      const command = yield* startTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.fromQueue(standardInputChunks),
      );
      yield* command.waitForLine("[hook] received 5 bytes");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain("[hook] cleaned up");
      expect(exited.standardError).not.toContain("standard input ended");
      expect(exited.standardError).toMatch(
        /^failed: 0 of 1 task, 1 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

const readsInheritedStandardInput = (taskName: string) =>
  `export const ${taskName} = Task.make({ name: "${taskName}", run: Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("cat", [], { stdin: "inherit" });
  const text = yield* Stream.mkString(Stream.decodeText(child.stdout));
  yield* child.exitCode;
  yield* Effect.log(\`input is \${text}\`);
  return text;
})) });`;

const INHERITING_TASK_IMPORTS = `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";`;

export const commandsInheritingStandardInput = makeTest(
  "a command that inherits standard input reads it only in the root task, so no other task reads the run's input",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "root.ts": `${INHERITING_TASK_IMPORTS}
${readsInheritedStandardInput("hook")}
export default hook;`,
        "dependency.ts": `${INHERITING_TASK_IMPORTS}
${readsInheritedStandardInput("reader")}
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* reader; }) });`,
      });
      const runWithInput = (file: string, input: string) =>
        runTaskProcess(project, file, {}, project.directory, [], Stream.make(encodeText(input)));
      const rootRan = yield* runWithInput("root.ts", "piped");
      expect(rootRan.standardError).toContain("[hook] input is piped");
      expect(rootRan.exitCode).toBe(0);
      const dependencyRan = yield* runWithInput("dependency.ts", "piped");
      expect(dependencyRan.standardError).toContain(
        `[reader] error StandardInputReadOutsideRootTask: read standard input, which only the root task "main" can read`,
      );
      expect(dependencyRan.standardError).not.toContain("input is");
      expect(dependencyRan.exitCode).toBe(1);
    }),
  ),
);

const HOOK_INTERRUPTING_A_PENDING_READ = `import { Effect, Fiber, Pull, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
export const hook = Task.make({ name: "hook", run: Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  const pull = yield* Stream.toPull(stdio.stdin);
  const readOnce = Pull.matchEffect(pull, {
    onSuccess: (chunks) => Effect.succeed(chunks.flatMap((chunk) => Array.from(chunk))),
    onDone: () => Effect.succeed("ended"),
    onFailure: Effect.failCause,
  });
  const pendingRead = yield* Effect.forkChild(readOnce, { startImmediately: true });
  yield* Effect.log("read pending");
  yield* Fiber.interrupt(pendingRead);
  yield* Effect.log("interrupted");
  const received = yield* readOnce;
  yield* Effect.log(\`received \${JSON.stringify(received)}\`);
}) });
export default hook;`;

export const readAfterInterruptedRead = makeTest(
  "the root task's next read receives the bytes written after it interrupted a pending read, and sends no second request",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": HOOK_INTERRUPTING_A_PENDING_READ });
      const standardInputChunks = yield* Queue.unbounded<Uint8Array>();
      const command = yield* startTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.fromQueue(standardInputChunks),
      );
      yield* command.waitForLine("[hook] interrupted");
      yield* Queue.offer(
        standardInputChunks,
        Uint8Array.from("789", (digit) => Number(digit)),
      );
      yield* command.waitForLine("[hook] received [7,8,9]");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      const standardInputRequests = trace
        ?.named("handleWorkerEvent")
        .filter(
          (span) =>
            readAttribute(span.attributes, `${TASK_TRACE_LABEL_PREFIX}.message`) ===
            "TaskFiberRequestedStandardInput",
        );
      expect(standardInputRequests).toHaveLength(1);
    }),
  ),
);
