import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Schema } from "effect";

import { CommandFailed, PrintableError } from "../../src/lib/errors/index.ts";
import {
  FAILURE_REASON_NOT_RECORDED_LINE,
  formatFailureReasonLine,
} from "../../src/shared/format-error-lines.ts";
import { read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const FEWEST_EXPECTED_ERROR_CLASSES = 50;

const TRACE_AND_REASON_LINE_COUNT = 2;

const PRINTED_ZERO_COUNT = 500;

const ZEROS_KEPT_IN_THE_REASON_LINE = 160;

const EMOJI_COUNT = 250;

const REASON_LINE_CHARACTER_LIMIT = 200;

const FAILURE_VALUE_CHARACTER_LIMIT = 200;

const EMOJIS_KEPT_IN_THE_REASON_LINE = 195;

const FAILED_COMMAND_EXIT_CODE = 3;

export const failHard = Task.make(
  "the first failure fails the run and the summary names it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const slow = Task.make("slow", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("slow cleaned up"));
  yield* Effect.log("slow started");
  yield* Effect.never;
}));
export const bad = Task.make("bad", Effect.gen(function* () {
  yield* slow.pipe(Effect.forkChild);
  return yield* Effect.fail(new TaskError({ message: "bad broke" }));
}));
export const main = Task.make("main", Effect.all([slow, bad], { concurrency: "unbounded", discard: true }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^\[bad\] \S+: error TaskError: bad broke$/mu);
      expect(ran.standardError).not.toContain("[main] passed");
      expect(ran.standardError).toMatch(/^failed: bad \(1 of 3 tasks\), 1 stopped, [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const manyFailedTasks = Task.make(
  "the summary names the first three failed tasks in the order they were made and counts the rest",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const lint = Task.make("lint", Effect.fail(new TaskError({ message: "lint broke" })));
export const build = Task.make("build", Effect.fail(new TaskError({ message: "build broke" })));
export const ok = Task.make("ok", Effect.void);
export const types = Task.make("types", Effect.fail(new TaskError({ message: "types broke" })));
export const docs = Task.make("docs", Effect.fail(new TaskError({ message: "docs broke" })));
export const e2e = Task.make("e2e", Effect.fail(new TaskError({ message: "e2e broke" })));
export const checks = Task.make("checks", Task.allSettled([e2e, docs, ok, types, build, lint]));
export const main = Task.make("main", Task.allSettled([checks]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[checks] error SettledTasksFailed: 5 of 6 tasks failed: e2e, docs, types, build, lint",
      );
      expect(ran.standardError).toContain(
        "[main] error SettledTasksFailed: 1 of 1 task failed: checks",
      );
      expect(ran.standardError).toMatch(
        /^failed: lint, build, types and 2 more \(5 of 8 tasks\), [\d.]+s$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const errorOutput = Task.make(
  "a TaskError failure or defect prints once with its tag and the user line that made it, and the trace keeps the stack",
  Effect.scoped(
    Effect.gen(function* () {
      for (const mode of ["expected", "defect"] as const) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const failure = Effect.${mode === "defect" ? "die" : "fail"}(new TaskError({ message: "dev is not running" }));
export const main = Task.make("main", failure);
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        const printedLines = ran.standardError.trimEnd().split("\n");
        expect(
          printedLines
            .slice(0, -1)
            .join("\n")
            .match(/dev is not running/gu)?.length,
        ).toBe(1);
        expect(printedLines.at(-1)).toBe("TaskError: dev is not running");
        const [, lineNumber, columnNumber] =
          /^\[main\] main\.ts:(?<lineNumber>\d+):(?<columnNumber>\d+): error TaskError: dev is not running$/mu.exec(
            ran.standardError,
          ) ?? [];
        const constructingLine = (yield* read(`${project.directory}/main.ts`)).split("\n")[
          Number(lineNumber) - 1
        ];
        expect(constructingLine?.slice(Number(columnNumber) - 1)).toStartWith("TaskError(");
        expect(ran.standardError).not.toMatch(/^\[main\]\s+at /mu);
        const tracePath = yield* Effect.fromNullishOr(
          /wrote the trace to (?<tracePath>.+)/u.exec(ran.standardError)?.[1],
        );
        const fileSystem = yield* FileSystem.FileSystem;
        const trace = yield* fileSystem.readFileString(tracePath);
        expect(trace).toContain("TaskError: dev is not running");
        expect(trace).toContain("main.ts:");
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const namedErrors = Task.make(
  "every named error a task fails with prints its message and tag without a stack, and the trace keeps the details",
  Effect.scoped(
    Effect.gen(function* () {
      const failures = [
        {
          failure: `Task.exec("sh", ["-c", "echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
          tag: "CommandFailed",
          exitCode: FAILED_COMMAND_EXIT_CODE,
          errorLines: `[main] error CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\n[main]   broken\n`,
        },
        {
          failure: `Task.exec("sh", ["-c", "echo hint: printed by the command >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
          tag: "CommandFailed",
          exitCode: FAILED_COMMAND_EXIT_CODE,
          errorLines: `[main] error CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\n[main]   hint: printed by the command\n`,
        },
        {
          failure:
            'Effect.fail(new DevServerMissing({ message: "dev server is not running", hint: "start it with bun scripts/dev.ts" }))',
          tag: "DevServerMissing",
          exitCode: 1,
          errorLines: ": error DevServerMissing: dev server is not running\n",
        },
        {
          failure: "Effect.fail(new PortTaken({ port: 3000 }))",
          tag: "PortTaken",
          exitCode: 1,
          errorLines: ": error PortTaken: port 3000 is taken\n",
        },
      ];
      for (const expected of failures) {
        const project = yield* makeProject({
          "main.ts": `import { Data, Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
class DevServerMissing extends Schema.TaggedError<DevServerMissing>()("DevServerMissing", { message: Schema.String, hint: Schema.String }) {}
class PortTaken extends Data.TaggedError("PortTaken")<{ readonly port: number }> {
  override get message() { return \`port \${this.port} is taken\`; }
  get hint() { return "stop the other server or pick another port"; }
}
const failure = ${expected.failure};
export const main = Task.make("main", failure);
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(expected.exitCode);
        expect(ran.standardError).toContain(expected.errorLines);
        expect(ran.standardError).not.toMatch(/^\[main\]\s+at /mu);
        if (expected.tag !== "CommandFailed") {
          expect(ran.standardError).toMatch(
            new RegExp(String.raw`^\[main\] main\.ts:\d+:\d+: error ${expected.tag}: `, "mu"),
          );
        }
        const tracePath = yield* Effect.fromNullishOr(
          /wrote the trace to (?<tracePath>.+)/u.exec(ran.standardError)?.[1],
        );
        const fileSystem = yield* FileSystem.FileSystem;
        const trace = yield* fileSystem.readFileString(tracePath);
        expect(trace).toContain(`${expected.tag}:`);
        expect(trace).toContain(" at ");
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failingInADependency = Task.make(
  "a body that fails inside an installed package prints no location inside node_modules",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, FileSystem } from "effect";
import { Task } from "@sshkeda/task";
const version = FileSystem.FileSystem.pipe(Effect.flatMap((fs) => fs.readFileString(".bun-version")));
export const main = Task.make("main", version);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[main\] error PlatformError: NotFound: .*\.bun-version.*$/mu,
      );
      expect(ran.standardError).not.toContain("node_modules");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const sourceFileNamedNodeModules = Task.make(
  "an error made in an extensionless source file named node_modules keeps its location in that file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "lib/node_modules": `import { TaskError } from "@sshkeda/task/errors";
export const makeError = () =>
  new TaskError({ message: "made in lib/node_modules" });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { makeError } from "./lib/node_modules";
export const main = Task.make("main", Effect.suspend(() => Effect.fail(makeError())));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[main\] lib\/node_modules:3:\d+: error TaskError: made in lib\/node_modules$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unsetConfigKey = Task.make(
  "an unset Config a task reads fails naming the key, and names the environment variable only when @sshkeda/task's environment config provider read it",
  Effect.scoped(
    Effect.gen(function* () {
      const cases = [
        {
          config: 'Config.String("HOST").pipe(Config.nested("TASK_TEST_UNSET_APP"))',
          lines:
            "error ConfigKeyNotSet: configuration key TASK_TEST_UNSET_APP.HOST is not set: the environment has no TASK_TEST_UNSET_APP_HOST\n",
        },
        {
          config: 'Config.schema(Schema.Struct({ x: Schema.String }), "TASK_TEST_UNSET_STRUCT")',
          lines: "error ConfigKeyNotSet: configuration key TASK_TEST_UNSET_STRUCT is not set\n",
        },
      ];
      for (const { config, lines } of cases) {
        const project = yield* makeProject({
          "main.ts": `import { Config, Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", ${config});
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toContain(`[main] ${lines}`);
        expect(ran.standardError).not.toContain("ConfigError");
        expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const emptyValueForDefaultedConfig = Task.make(
  "an empty environment value is set, so a defaulted Config fails with Effect's ConfigError, which keeps the schema's allowed values, instead of using the default",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("number " + (yield* Config.Int("TASK_TEST_NUMBER").pipe(Config.withDefault(3))));
  yield* Effect.log("mode " + (yield* Config.Literals(["dev", "prod"], "TASK_TEST_MODE").pipe(Config.withDefault("dev"))));
}));
if (import.meta.main) Task.main(main);`,
      });
      const unset = yield* runTaskProcess(project, "main.ts");
      expect(unset.standardError).toContain("[main] number 3");
      expect(unset.standardError).toContain("[main] mode dev");
      const emptyMode = yield* runTaskProcess(project, "main.ts", { TASK_TEST_MODE: "" });
      expect(emptyMode.exitCode).toBe(1);
      expect(emptyMode.standardError).toContain(
        '[main] error ConfigError: SchemaError(Expected "dev" | "prod"\n[main]     at ["TASK_TEST_MODE"])\n',
      );
      const emptyNumber = yield* runTaskProcess(project, "main.ts", { TASK_TEST_NUMBER: "" });
      expect(emptyNumber.exitCode).toBe(1);
      expect(emptyNumber.standardError).toContain(
        '[main] error ConfigError: SchemaError(Expected a string representing a finite number\n[main]     at ["TASK_TEST_NUMBER"])\n',
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const configSetWithWrongShape = Task.make(
  "a Config whose key is set to a value of the wrong shape, or that a task's own ConfigProvider reads, is not reported as unset",
  Effect.scoped(
    Effect.gen(function* () {
      for (const config of [
        'Effect.provide(Config.String("X"), ConfigProvider.layer(ConfigProvider.fromUnknown({ X: { a: 1 } })))',
        'Effect.provide(Config.String("X"), ConfigProvider.layer(ConfigProvider.fromUnknown({})))',
      ]) {
        const project = yield* makeProject({
          "main.ts": `import { Config, ConfigProvider, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", ${config});
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toContain("[main] error ConfigError:");
        expect(ran.standardError).not.toContain("ConfigKeyNotSet");
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const userErrorLocations = Task.make(
  "a user error, tagged error or defect prints the user line that made it, and a module that throws on import prints its line",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [failure, tag] of [
        ['Effect.fail(new Error("boom"))', "TaskBodyFailed"],
        ['Effect.fail(new Deploy({ reason: "boom" }))', "Deploy"],
        ['Effect.sync(() => { throw new TypeError("boom"); })', "TypeError"],
        ['Effect.fail(new UserNotFound({ name: "alice" }))', "UserNotFound"],
        ['Effect.fail(new BucketMissing({ location: "us-east-1" }))', "BucketMissing"],
        [
          'Effect.fail(new Wrapped({ reportedErrors: [{ tag: "Fake", message: "fake" }] }))',
          "Wrapped",
        ],
      ] as const) {
        const project = yield* makeProject({
          "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class Deploy extends Data.TaggedError("Deploy")<{ readonly reason: string }> {}
class UserNotFound extends Data.TaggedError("UserNotFound")<{ readonly name: string }> {}
class BucketMissing extends Data.TaggedError("BucketMissing")<{ readonly location: string }> {}
class Wrapped extends Data.TaggedError("Wrapped")<{ readonly reportedErrors: ReadonlyArray<unknown> }> {}
export const main = Task.make("main", ${failure});
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toMatch(
          new RegExp(String.raw`^\[main\] main\.ts:7:\d+: error ${tag}: `, "mu"),
        );
      }
      const throwing = yield* makeProject({
        "throws.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const thrown = Task.make("thrown", Effect.void);
if (!isMainThread) throw new Error("module refused");`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { thrown } from "./throws.ts";
export const main = Task.make("main", Effect.asVoid(thrown));
if (import.meta.main) Task.main(main);`,
      });
      const importFailed = yield* runTaskProcess(throwing, "main.ts");
      expect(importFailed.exitCode).toBe(1);
      expect(importFailed.standardError).toMatch(
        /^\[main\] throws\.ts:5:\d+: error TaskModuleImportFailed: could not import task module because module refused$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failuresWithoutAName = Task.make(
  "a thrown system error prints Node's text with its path, and a failure value without a message prints as JSON cut to 200 characters",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { readFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const read = Task.make("read", Effect.sync(() => readFileSync("content/contact.html", "utf8")));
export const invalid = Task.make("invalid", Effect.fail({ service: "worker", field: "port", problem: "is not a port number" }));
export const long = Task.make("long", Effect.fail({ values: Array.from({ length: 100 }, (_unused, index) => index) }));
export const main = Task.make("main", Task.allSettled([read, invalid, long]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[read\] (?:\S+ )?error TaskBodyFailed: its body failed with ENOENT: no such file or directory, open 'content\/contact\.html'$/mu,
      );
      expect(ran.standardError).toContain(
        '[invalid] error TaskBodyFailed: its body failed with {"service":"worker","field":"port","problem":"is not a port number"}\n',
      );
      const longValue = yield* encodeJson({
        values: Array.from({ length: 100 }, (_unused, index) => index),
      });
      expect(ran.standardError).toContain(
        `[long] error TaskBodyFailed: its body failed with ${longValue.slice(0, FAILURE_VALUE_CHARACTER_LIMIT - 1)}…\n`,
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const cyclicErrorCauses = Task.make(
  "an error whose cause contains a cycle keeps its original message and stack",
  Effect.scoped(
    Effect.gen(function* () {
      for (const cause of ["failure", 'new Error("inner failure", { cause: failure })']) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.sync(() => {
  const failure = new Error("original cyclic failure");
  failure.cause = ${cause};
  throw failure;
}));
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toMatch(
          /^\[main\] main\.ts:4:\d+: error TaskBodyFailed: its body failed with original cyclic failure$/mu,
        );
        expect(ran.standardError).not.toContain("RangeError");
        const tracePath = yield* Effect.fromNullishOr(
          /wrote the trace to (?<tracePath>.+)/u.exec(ran.standardError)?.[1],
        );
        const fileSystem = yield* FileSystem.FileSystem;
        const trace = yield* fileSystem.readFileString(tracePath);
        expect(trace).toContain("original cyclic failure");
        expect(trace).toContain("main.ts:4:");
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const thrownValuesWithoutMessages = Task.make(
  "an empty thrown string or Error says that the thrown value has no message",
  Effect.scoped(
    Effect.gen(function* () {
      for (const failure of ['throw "";', "throw new Error();"]) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.sync(() => { ${failure} }));
if (import.meta.main) Task.main(main);`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toContain(
          "error TaskBodyFailed: its body failed with a thrown value that has no message",
        );
        expect(ran.standardError).not.toContain("UntaggedFailure");
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const defectsWithoutSourceExcerpts = Task.make(
  "a TypeError from the engine prints without the source excerpt it appends but keeps the value it names, and a thrown message is printed as written",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const notAnEffect: any = [];
export const piped = Task.make("piped", Effect.suspend(() => notAnEffect.pipe(
  Effect.asVoid,
)));
export const deploy = Task.make("deploy", Effect.sync(() => { throw new Error("upload rejected (In 'us-east-1': quota exceeded)"); }));
export const main = Task.make("main", Task.allSettled([piped, deploy]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[piped\] main\.ts:4:\d+: error TypeError: notAnEffect\.pipe is not a function\. \('notAnEffect\.pipe' is undefined\)$/mu,
      );
      expect(ran.standardError).not.toContain("Effect.asVoid");
      expect(ran.standardError).toContain("upload rejected (In 'us-east-1': quota exceeded)");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const wrappedCauseKeepsOuterMessage = Task.make(
  "a task's own error that wraps a TaskError as its cause is printed with its own message, tag and user line, without a detecting line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
class DeployFailed extends Data.TaggedError("DeployFailed")<{ readonly cause: TaskError }> {
  override get message() { return "deploy failed"; }
}
export const main = Task.make("main", Effect.fail(new DeployFailed({
  cause: new TaskError({ message: "inner failure" }),
})));
if (import.meta.main) Task.main(main);`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(result.standardError).toMatch(
        /^\[main\] main\.ts:7:\d+: error DeployFailed: deploy failed$/mu,
      );
      expect(result.standardError).not.toContain("inner failure");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const USER_CONSTRUCTED_ERROR_CLASSES = new Set(["TaskError", "ReportedErrors"]);

export const everyTaskErrorTagIsConstructedInSource = Task.make(
  "every Task error class is tagged with its own name and constructed in src, so searching the shipped source for new <Tag>( finds where it is raised",
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const repository = yield* REPOSITORY_DIRECTORY;
    const sourceFilePaths = (yield* fileSystem.readDirectory(`${repository}/src`, {
      recursive: true,
    })).filter((sourceFilePath) => sourceFilePath.endsWith(".ts"));
    const sourceText = (yield* Effect.forEach(sourceFilePaths, (sourceFilePath) =>
      fileSystem.readFileString(`${repository}/src/${sourceFilePath}`),
    )).join("\n");
    const declarations = yield* Effect.forEach(
      sourceText.matchAll(
        /class (?<className>\w+) extends Schema\.TaggedError<\k<className>>\(\)\(\s*(?:"(?<tagLiteral>\w+)"|(?<tagConstant>\w+))/gu,
      ),
      (declaration) =>
        Schema.decodeUnknownEffect(
          Schema.Tuple([
            Schema.String,
            Schema.String,
            Schema.UndefinedOr(Schema.String),
            Schema.UndefinedOr(Schema.String),
          ]),
        )([...declaration]).pipe(
          Effect.map(([, className, tagLiteral, tagConstant]) => ({
            className,
            tagLiteral,
            tagConstant,
          })),
        ),
    );
    expect(declarations.length).toBeGreaterThan(FEWEST_EXPECTED_ERROR_CLASSES);
    expect(
      declarations.filter(({ className, tagLiteral, tagConstant }) =>
        tagLiteral === undefined
          ? !sourceText.includes(`const ${tagConstant} = "${className}";`)
          : tagLiteral !== className,
      ),
    ).toEqual([]);
    expect(
      declarations
        .map(({ className }) => className)
        .filter(
          (className) =>
            !USER_CONSTRUCTED_ERROR_CLASSES.has(className) &&
            !sourceText.includes(`new ${className}(`),
        ),
    ).toEqual([]);
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const printedErrorFormatCheck = Task.make(
  "the printed-error format check refuses a hint line under any error but CommandFailed, whose later lines are the command's output",
  Effect.sync(() => {
    expect(
      new CommandFailed({ command: "c", code: 1, standardOutput: "", standardError: "" }).message,
    ).toBe("c exited with code 1");
    expect(
      new CommandFailed({ command: "c", code: 1, standardOutput: "out", standardError: "err" })
        .message,
    ).toBe("c exited with code 1\nout\nerr");
    const commandFailure =
      "[main] error CommandFailed: sh exited with code 1\n[main]   out\n[main]   hint: printed by the command";
    for (const printed of [
      "[main] error TaskError: it broke\n[main]   fatal: broken\n[main]   hint: printed by a tool",
      `${commandFailure}\n[main] error TaskError: second\n[main]   hint: written by task`,
      "[main] error TaskError: first\n[main] \n[main]     hint: written by task",
      "error TaskError: first\n\n  hint: written by task",
    ]) {
      expect(() => {
        expect(findMisprintedErrorLines(printed)).toEqual([]);
      }, printed).toThrow();
    }
    expect(findMisprintedErrorLines(commandFailure)).toEqual([]);
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const directoryWithSpace = Task.make(
  "errors from a project directory whose name has a space keep their locations, including ReportedErrors violations",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "sp ace/package.json": '{ "name": "spaced", "type": "module" }',
        "sp ace/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ReportedErrors } from "@sshkeda/task/errors";
export const thrown = Task.make("thrown", Effect.fail(new Error("boom")));
export const checked = Task.make("checked", Effect.fail(new ReportedErrors({ reportedErrors: [
  { tag: "LintViolation", location: import.meta.dir + "/a b.ts:1:2", message: "first violation", hint: "use Effect FileSystem" },
  { tag: "OtherViolation", location: import.meta.dir + "/c d.ts:4:5", message: "second violation" },
] })));
export const main = Task.make("main", Task.allSettled([thrown, checked]));
if (import.meta.main) Task.main(main);`,
      });
      const spacedDirectory = `${project.directory}/sp ace`;
      const ran = yield* runTaskProcess(project, "sp ace/main.ts", {}, spacedDirectory);
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[thrown\] main\.ts:4:\d+: error TaskBodyFailed: its body failed with boom$/mu,
      );
      expect(ran.standardError).toContain(
        "[checked] a b.ts:1:2: error LintViolation: first violation\n[checked] c d.ts:4:5: error OtherViolation: second violation\n",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const runFailureReasonProject = (rootTaskBody: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
class EvidenceRejected extends Schema.TaggedError<EvidenceRejected>()("EvidenceRejected", { message: Schema.String }) {}
export const checkArchive = Task.make("check-archive", Effect.fail(new EvidenceRejected({ message: "archive reference is required" })));
export const checkB = Task.make("b", Effect.fail(new EvidenceRejected({ message: "b rejected the evidence" })));
export const checkLater = Task.make("check-later", Effect.sleep("2 seconds").pipe(Effect.andThen(Effect.fail(new EvidenceRejected({ message: "later failure" })))));
export const main = Task.make("main", ${rootTaskBody});
if (import.meta.main) Task.main(main);`,
    });
    const ran = yield* runTaskProcess(project, "main.ts");
    const [traceLine, reasonLine] = ran.standardError
      .trimEnd()
      .split("\n")
      .slice(-TRACE_AND_REASON_LINE_COUNT);
    return { ...ran, traceLine, reasonLine };
  });

export const failureReasonOfRootTask = Task.make(
  "a failed run ends with a reason line Tag: message after the summary and the trace line",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.fail(new EvidenceRejected({ message: "archive reference is required" }))`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), [\d.]+s\n/mu);
      expect(ran.traceLine).toStartWith("wrote the trace to ");
      expect(ran.reasonLine).toBe("EvidenceRejected: archive reference is required");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failureReasonOfAwaitedTask = Task.make(
  "the reason line names the original failure of the awaited task, not AwaitedTaskFailed wrapping it",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.all([checkArchive, checkLater], { concurrency: "unbounded", discard: true })`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] error AwaitedTaskFailed: check-archive failed");
      expect(ran.traceLine).toStartWith("wrote the trace to ");
      expect(ran.reasonLine).toBe("EvidenceRejected: archive reference is required");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const awaitedFailureWithFinalizerFailure = Task.make(
  "an awaited task failure and an independent finalizer failure in the same cause both print",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const dependency = Task.make("dependency", Effect.fail(new TaskError({ message: "dependency broke" })));
export const main = Task.make("main", Effect.scoped(Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => { throw new Error("finalizer broke independently"); }));
  yield* dependency;
})));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("error TaskError: dependency broke");
      expect(ran.standardError).toContain("[main] error AwaitedTaskFailed: dependency failed");
      expect(ran.standardError).toMatch(
        /^\[main\] main\.ts:6:\d+: error TaskBodyFailed: its body failed with finalizer broke independently$/mu,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failureReasonOfUncaughtFailure = Task.make(
  "the reason line names the failure that ended the run, not an earlier one an awaiting task caught",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.gen(function* () { yield* Effect.ignore(checkArchive); yield* checkB; })`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("error EvidenceRejected: archive reference is required");
      expect(ran.traceLine).toStartWith("wrote the trace to ");
      expect(ran.reasonLine).toBe("EvidenceRejected: b rejected the evidence");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failureReasonOfMultiLineMessage = Task.make(
  "the reason line joins the trimmed nonblank lines of a multi-line message with a pipe",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.fail(new EvidenceRejected({ message: "first line\\n  second line\\n\\nthird line\\n" }))`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.traceLine).toStartWith("wrote the trace to ");
      expect(ran.reasonLine).toBe("EvidenceRejected: first line | second line | third line");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failureReasonOfCommand = Task.make(
  "the reason line of a failed command holds the command, its exit code and its output on one line",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Task.exec("sh", ["-c", "echo checked >&1; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(ran.traceLine).toStartWith("wrote the trace to ");
      expect(ran.reasonLine).toBe(
        `CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE} | checked | broken`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failureReasonOfLongCommandOutput = Task.make(
  "the reason line is cut to 200 characters with an ellipsis",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Task.exec("sh", ["-c", "printf '%0500d' 0; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(ran.standardError).toContain("0".repeat(PRINTED_ZERO_COUNT));
      expect(ran.traceLine).toStartWith("wrote the trace to ");
      expect(ran.reasonLine).toBe(
        `CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE} | ${"0".repeat(ZEROS_KEPT_IN_THE_REASON_LINE)}…`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const failureReasonCutsBetweenCharacters = Task.make(
  "the reason line is cut between user-perceived characters, never inside an emoji",
  Effect.gen(function* () {
    const segmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
    const reason = yield* Schema.decodeEffect(PrintableError)({
      tag: "T",
      message: `a${"😀".repeat(EMOJI_COUNT)}`,
    });
    const reasonLine = formatFailureReasonLine(reason);
    expect(reasonLine.isWellFormed()).toBe(true);
    expect(reasonLine).not.toContain("\uFFFD");
    expect([...segmenter.segment(reasonLine)]).toHaveLength(REASON_LINE_CHARACTER_LIMIT);
    expect(reasonLine).toBe(`T: a${"😀".repeat(EMOJIS_KEPT_IN_THE_REASON_LINE)}…`);
    expect(FAILURE_REASON_NOT_RECORDED_LINE).toBe("TaskRunFailed: no reason recorded");
  }),
);

export const noFailureReasonOnSuccess = Task.make(
  "a passing run prints no reason line and still ends with the trace line",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject("Effect.void");
      expect(ran.exitCode).toBe(0);
      expect(ran.reasonLine).toStartWith("wrote the trace to ");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
