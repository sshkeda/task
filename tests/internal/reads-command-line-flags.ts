import { expect } from "bun:test";

import { Effect } from "effect";

import { exists, read } from "../shared/access-files.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const RECORD_FLAGS = `const record = (name: string, flags: object) =>
  Effect.promise(() => Bun.write(\`\${name}-flags\`, JSON.stringify(flags))).pipe(Effect.asVoid);`;

const DEPLOY_FILE = `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${RECORD_FLAGS}
export const migrate = Task.make({
  name: "migrate",
  flags: Schema.Struct({
    stage: Schema.Literals(["prod", "dev"]),
    batchSize: Schema.optional(Schema.Number),
  }),
  run: (flags) => record("migrate", flags),
});
export const lint = Task.make({
  name: "lint",
  run: Effect.promise(() => Bun.write("lint-ran", "")).pipe(Effect.asVoid),
});
export default Task.make({
  name: "deploy",
  flags: Schema.Struct({
    stage: Schema.Literals(["dev", "prod"]),
    since: Schema.optional(Schema.String),
    dryRun: Schema.optional(Schema.Boolean),
  }),
  run: (flags) =>
    Effect.gen(function* () {
      yield* migrate;
      yield* lint;
      yield* record("deploy", flags);
    }),
});
`;

const runWithArguments = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  scriptArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
) => runTaskProcess(project, "main.ts", environment, project.directory, scriptArguments);

const readMarker = (directory: string, marker: string) =>
  Effect.gen(function* () {
    const path = `${directory}/${marker}`;
    return (yield* exists(path)) ? yield* read(path) : null;
  });

const readDeployMarkers = (directory: string) =>
  Effect.gen(function* () {
    return {
      deploy: yield* readMarker(directory, "deploy-flags"),
      migrate: yield* readMarker(directory, "migrate-flags"),
      lint: yield* readMarker(directory, "lint-ran"),
    };
  });

export const decodedFlags = makeTest(
  "a task that declares flags runs with their decoded values, given as --name value or --name=value, spelled in kebab-case, with booleans true when present and optional flags left out",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, markers] of [
        [
          ["--stage", "prod", "--since=7d", "--dry-run", "--batch-size", "50"],
          {
            deploy: '{"stage":"prod","since":"7d","dryRun":true}',
            migrate: '{"stage":"prod","batchSize":50}',
            lint: "",
          },
        ],
        [["--stage=dev"], { deploy: '{"stage":"dev"}', migrate: '{"stage":"dev"}', lint: "" }],
        [
          ["--dry-run=false", "--batch-size=-1.5", "--stage", "dev"],
          {
            deploy: '{"stage":"dev","dryRun":false}',
            migrate: '{"stage":"dev","batchSize":-1.5}',
            lint: "",
          },
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": DEPLOY_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
        expect(ran.exitCode).toBe(0);
        expect(yield* readDeployMarkers(project.directory)).toEqual(markers);
      }
    }),
  ),
);

export const sharedFlags = makeTest(
  "every task that declares a flag sees the one value the command line gives it, even when two tasks write the same choices separately",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${RECORD_FLAGS}
export const build = Task.make({
  name: "build",
  flags: Schema.Struct({ stage: Schema.Literals(["dev", "prod"]) }),
  run: (flags) => record("build", flags),
});
export const plain = Task.make({
  name: "plain",
  run: Effect.promise(() => Bun.write("plain-ran", "")).pipe(Effect.asVoid),
});
export default Task.make({
  name: "release",
  flags: Schema.Struct({ stage: Schema.Literals(["prod", "dev"]), notes: Schema.optional(Schema.String) }),
  run: (flags) =>
    Effect.gen(function* () {
      yield* Task.all([build, plain]);
      yield* record("release", flags);
    }),
});
`,
      });
      const ran = yield* runWithArguments(project, ["--notes", "first release", "--stage", "prod"]);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(yield* read(`${project.directory}/build-flags`)).toBe('{"stage":"prod"}');
      expect(yield* read(`${project.directory}/release-flags`)).toBe(
        '{"stage":"prod","notes":"first release"}',
      );
      expect(yield* exists(`${project.directory}/plain-ran`)).toBe(true);
    }),
  ),
);

export const taskNameWithFlags = makeTest(
  "a bare word is the task to start and combines with flags, while the word after a flag that is not a boolean is that flag's value",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, markers] of [
        [["migrate", "--stage", "prod"], { deploy: null, migrate: '{"stage":"prod"}', lint: null }],
        [["--stage", "prod", "migrate"], { deploy: null, migrate: '{"stage":"prod"}', lint: null }],
        [
          ["--dry-run", "migrate", "--stage", "dev"],
          { deploy: null, migrate: '{"stage":"dev"}', lint: null },
        ],
        [
          ["--since", "lint", "--stage", "dev"],
          { deploy: '{"stage":"dev","since":"lint"}', migrate: '{"stage":"dev"}', lint: "" },
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": DEPLOY_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.exitCode, ran.standardError).toBe(0);
        expect(yield* readDeployMarkers(project.directory)).toEqual(markers);
      }
    }),
  ),
);

export const refusedCommandLines = makeTest(
  "a missing, malformed or unaccepted flag of the task the run starts with, a flag no task reads, or a second task name fails the run with one line before any task starts",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, refusal] of [
        [[], 'FlagMissing: the task "deploy" reads the flag --stage, which was not given'],
        [
          ["--stage", "staging"],
          'FlagInvalid: the flag --stage is "staging", which the task "deploy" does not accept: Expected "dev" | "prod"',
        ],
        [
          ["--stage", "dev", "--dry-run=yes"],
          'FlagInvalid: the flag --dry-run is "yes", which the task "deploy" does not accept: Expected "true" | "false"',
        ],
        [["--stage"], "FlagValueMissing: the flag --stage is given without a value"],
        [
          ["--stage", "--since", "7d"],
          "FlagValueMissing: the flag --stage is given without a value",
        ],
        [
          ["--stage", "dev", "--stage=prod"],
          "FlagGivenTwice: the flag --stage is given more than once",
        ],
        [["--stagee", "dev"], "FlagNotRead: the flag --stagee is not read by any task"],
        [["--dryRun", "--stage", "dev"], "FlagNotRead: the flag --dryRun is not read by any task"],
        [
          ["migrate", "lint", "--stage", "dev"],
          'RunArgumentsNotAccepted: the arguments "migrate" "lint" are not the name of one task to start',
        ],
        [
          ["migrate"],
          'FlagMissing: the task "migrate" reads the flag --stage, which was not given',
        ],
        [
          ["migrate", "--stage", "dev", "--batch-size", "many"],
          'FlagInvalid: the flag --batch-size is "many", which the task "migrate" does not accept: Expected a string representing a finite number',
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": DEPLOY_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.standardError).toBe(`error ${refusal}`);
        expect(ran.standardOutput).toBe("");
        expect(ran.exitCode).toBe(1);
        expect(yield* readDeployMarkers(project.directory)).toEqual({
          deploy: null,
          migrate: null,
          lint: null,
        });
      }
    }),
  ),
);

export const refusedDeclarations = makeTest(
  "two loaded tasks of which one reads a flag as a switch and the other with a value, a flag a command line cannot carry, or a task that reads --help fail the run with one line before any task starts",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [migrateFlags, refusal] of [
        [
          "{ stage: Schema.Boolean }",
          'FlagReadTwoWays: the task "migrate" reads the flag --stage as a switch without a value, but the task "deploy" reads it with a value',
        ],
        [
          "{ stage: Schema.optional(Schema.Boolean) }",
          'FlagReadTwoWays: the task "migrate" reads the flag --stage as a switch without a value, but the task "deploy" reads it with a value',
        ],
        [
          "{ targetHosts: Schema.Array(Schema.String) }",
          'FlagNotFlat: the task "migrate" reads the flag --target-hosts as a value a command line cannot carry',
        ],
        [
          "{ target: Schema.optional(Schema.Struct({ host: Schema.String })) }",
          'FlagNotFlat: the task "migrate" reads the flag --target as a value a command line cannot carry',
        ],
        [
          "{ help: Schema.optional(Schema.Boolean) }",
          'FlagReserved: the task "migrate" reads the flag --help, which is kept for printing what the file accepts',
        ],
      ] as const) {
        const project = yield* makeProject({
          "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const migrate = Task.make({
  name: "migrate",
  flags: Schema.Struct(${migrateFlags}),
  run: () => Effect.promise(() => Bun.write("migrate-ran", "")).pipe(Effect.asVoid),
});
export default Task.make({
  name: "deploy",
  flags: Schema.Struct({ stage: Schema.optional(Schema.Literals(["dev", "prod"])) }),
  run: () => Effect.promise(() => Bun.write("deploy-ran", "")).pipe(Effect.asVoid),
});
`,
        });
        const ran = yield* runWithArguments(project, []);
        expect(ran.standardError).toBe(`error ${refusal}`);
        expect(ran.exitCode).toBe(1);
        expect(yield* exists(`${project.directory}/deploy-ran`)).toBe(false);
        expect(yield* exists(`${project.directory}/migrate-ran`)).toBe(false);
      }
    }),
  ),
);

const MODE_FILE = `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${RECORD_FLAGS}
export const check = Task.make({
  name: "check",
  flags: Schema.Struct({ mode: Schema.Literals(["fast", "full"]) }),
  run: (flags) => record("check", flags),
});
export const audit = Task.make({
  name: "audit",
  flags: Schema.Struct({ mode: Schema.Literals(["strict", "loose"]), retries: Schema.optional(Schema.Number) }),
  run: (flags) => record("audit", flags),
});
export default Task.make({
  name: "release",
  flags: Schema.Struct({ mode: Schema.String }),
  run: (flags) =>
    Effect.gen(function* () {
      yield* audit;
      yield* record("release", flags);
    }),
});
`;

const readModeMarkers = (directory: string) =>
  Effect.gen(function* () {
    return {
      check: yield* readMarker(directory, "check-flags"),
      audit: yield* readMarker(directory, "audit-flags"),
      release: yield* readMarker(directory, "release-flags"),
    };
  });

export const flagReadDifferently = makeTest(
  "loaded tasks that read one flag with different schemas run when the value fits each task that runs",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, markers] of [
        [["check", "--mode", "fast"], { check: '{"mode":"fast"}', audit: null, release: null }],
        [
          ["--mode=strict", "audit", "--retries", "2"],
          { check: null, audit: '{"mode":"strict","retries":2}', release: null },
        ],
        [
          ["--mode", "loose"],
          { check: null, audit: '{"mode":"loose"}', release: '{"mode":"loose"}' },
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": MODE_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.exitCode, ran.standardError).toBe(0);
        expect(yield* readModeMarkers(project.directory)).toEqual(markers);
      }
    }),
  ),
);

export const flagFitsOneReader = makeTest(
  "when a flag's value fits one task that reads it but not another, the task it does not fit fails when it starts, or before any task starts when the run starts with it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": MODE_FILE });
      const ran = yield* runWithArguments(project, ["--mode", "fast"]);
      expect(ran.standardError).toMatch(/^\[release\] started$/mu);
      expect(ran.standardError).toMatch(/^\[audit\] started$/mu);
      expect(ran.standardError).toMatch(
        /^\[audit\] error FlagInvalid: the flag --mode is "fast", which the task "audit" does not accept: Expected "strict" \| "loose"$/mu,
      );
      expect(ran.standardError).toMatch(/^\[release\] error AwaitedTaskFailed: audit failed$/mu);
      expect(ran.standardError).toMatch(/^failed: audit \(1 of 2 tasks\), [\d.]+s$/mu);
      expect(ran.exitCode).toBe(1);
      expect(yield* readModeMarkers(project.directory)).toEqual({
        check: null,
        audit: null,
        release: null,
      });
      const refusedProject = yield* makeProject({ "main.ts": MODE_FILE });
      const refused = yield* runWithArguments(refusedProject, ["check", "--mode", "strict"]);
      expect(refused.standardError).toBe(
        'error FlagInvalid: the flag --mode is "strict", which the task "check" does not accept: Expected "fast" | "full"',
      );
      expect(refused.standardOutput).toBe("");
      expect(refused.exitCode).toBe(1);
      expect(yield* readModeMarkers(refusedProject.directory)).toEqual({
        check: null,
        audit: null,
        release: null,
      });
    }),
  ),
);

export const laterTaskFlags = makeTest(
  "a task further down that reads a flag the command line does not give fails when it starts, and the run fails like any task failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const notify = Task.make({
  name: "notify",
  flags: Schema.Struct({ channel: Schema.String }),
  run: () => Effect.promise(() => Bun.write("notify-ran", "")).pipe(Effect.asVoid),
});
export default Task.make({
  name: "deploy",
  run: Effect.gen(function* () {
    yield* notify;
    yield* Effect.promise(() => Bun.write("deploy-ran", ""));
  }),
});
`,
      });
      const ran = yield* runWithArguments(project, []);
      expect(ran.standardError).toMatch(/^\[notify\] started$/mu);
      expect(ran.standardError).toMatch(
        /^\[notify\] error FlagMissing: the task "notify" reads the flag --channel, which was not given$/mu,
      );
      expect(ran.standardError).toMatch(/^\[deploy\] error AwaitedTaskFailed: notify failed$/mu);
      expect(ran.standardError).toMatch(/^failed: notify \(1 of 2 tasks\), [\d.]+s$/mu);
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/notify-ran`)).toBe(false);
      expect(yield* exists(`${project.directory}/deploy-ran`)).toBe(false);
    }),
  ),
);

export const helpOutput = makeTest(
  "--help prints the command with any task name given, quoted for the shell, the task the run would start and every loaded task that reads flags, then exits 0 without running any task",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, printed] of [
        [
          ["--help"],
          `bun main.ts starts the task "deploy".
The task "deploy" reads:
  --stage dev | prod
  --since text (optional)
  --dry-run
The task "migrate" reads:
  --stage prod | dev
  --batch-size number (optional)
`,
        ],
        [
          ["migrate", "--help", "--stage", "dev"],
          `bun main.ts migrate starts the task "migrate".
The task "migrate" reads:
  --stage prod | dev
  --batch-size number (optional)
The task "deploy" reads:
  --stage dev | prod
  --since text (optional)
  --dry-run
`,
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": DEPLOY_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.standardOutput).toBe(printed);
        expect(ran.standardError).toBe("");
        expect(ran.exitCode).toBe(0);
        expect(yield* readDeployMarkers(project.directory)).toEqual({
          deploy: null,
          migrate: null,
          lint: null,
        });
      }
      const readDifferently = yield* makeProject({ "main.ts": MODE_FILE });
      const ranReadDifferently = yield* runWithArguments(readDifferently, ["--help"]);
      expect(ranReadDifferently.standardOutput).toBe(`bun main.ts starts the task "release".
The task "release" reads:
  --mode text
The task "check" reads:
  --mode fast | full
The task "audit" reads:
  --mode strict | loose
  --retries number (optional)
`);
      expect(ranReadDifferently.exitCode).toBe(0);
      const listed = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const lint = Task.make({ name: "lint", run: Effect.void });
export const test = Task.make({ name: "test", run: Effect.void });
export default Task.all([lint, test]);
`,
      });
      const ranListed = yield* runWithArguments(listed, ["--help"]);
      expect(ranListed.standardOutput).toBe(
        'bun main.ts starts the tasks "lint", "test".\nNo task reads flags.\n',
      );
      expect(ranListed.exitCode).toBe(0);
      const spaced = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const migrate = Task.make({ name: "db migrate", run: Effect.promise(() => Bun.write("migrate-ran", "")).pipe(Effect.asVoid) });
export default Task.make({ name: "deploy", run: migrate });
`,
      });
      const ranSpaced = yield* runWithArguments(spaced, ["db migrate", "--help"]);
      expect(ranSpaced.standardOutput).toBe(
        "bun main.ts 'db migrate' starts the task \"db migrate\".\nNo task reads flags.\n",
      );
      expect(ranSpaced.exitCode).toBe(0);
      expect(yield* exists(`${spaced.directory}/migrate-ran`)).toBe(false);
    }),
  ),
);

export const helpDescriptions = makeTest(
  "--help prints a flag's description after the flag, lined up within each task, and leaves flags without one as they are",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const check = Task.make({
  name: "check",
  flags: Schema.Struct({ mode: Schema.Literals(["fast", "full"]).annotate({ description: "how much to check" }) }),
  run: () => Effect.void,
});
export default Task.make({
  name: "deploy",
  flags: Schema.Struct({
    stage: Schema.Literals(["dev", "prod"]).annotate({ description: "which stage to deploy" }),
    since: Schema.optional(Schema.String),
    dryRun: Schema.optional(Schema.Boolean.annotate({ description: "print the commands without running them" })),
    mode: Schema.String,
  }),
  run: () => Effect.void,
});
`,
      });
      const ran = yield* runWithArguments(project, ["--help"]);
      expect(ran.standardOutput).toBe(`bun main.ts starts the task "deploy".
The task "deploy" reads:
  --stage dev | prod       which stage to deploy
  --since text (optional)
  --dry-run                print the commands without running them
  --mode text
The task "check" reads:
  --mode fast | full  how much to check
`);
      expect(ran.standardError).toBe("");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

const DEFAULTS_FILE = `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${RECORD_FLAGS}
export default Task.make({
  name: "deploy",
  flags: Schema.Struct({
    stage: Schema.String.pipe(Schema.withDecodingDefaultKey(Effect.succeed("dev"))),
    color: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(true))),
    retries: Schema.Number.pipe(Schema.withDecodingDefaultType(Effect.succeed(2))),
  }),
  run: (flags) => record("deploy", flags),
});
`;

export const defaultedFlags = makeTest(
  "a flag whose schema has a decoding default takes that default when left out, takes the given value when given, and --help prints it as optional",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, recorded] of [
        [[], '{"stage":"dev","color":true,"retries":2}'],
        [
          ["--stage", "prod", "--color=false", "--retries", "5"],
          '{"stage":"prod","color":false,"retries":5}',
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": DEFAULTS_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.exitCode, ran.standardError).toBe(0);
        expect(yield* read(`${project.directory}/deploy-flags`)).toBe(recorded);
      }
      const helped = yield* makeProject({ "main.ts": DEFAULTS_FILE });
      const ranHelp = yield* runWithArguments(helped, ["--help"]);
      expect(ranHelp.standardOutput).toBe(`bun main.ts starts the task "deploy".
The task "deploy" reads:
  --stage text (optional)
  --color
  --retries number (optional)
`);
      expect(ranHelp.exitCode).toBe(0);
    }),
  ),
);

export const innerRunFlags = makeTest(
  "a run that a task starts does not get the flags of the run that started it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "inner.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "inner",
  flags: Schema.Struct({ stage: Schema.optional(Schema.String) }),
  run: (flags) => Effect.promise(() => Bun.write("inner-flags", JSON.stringify(flags))).pipe(Effect.asVoid),
});
`,
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "outer",
  flags: Schema.Struct({ stage: Schema.String }),
  run: (flags) =>
    Effect.gen(function* () {
      yield* Task.exec(process.execPath, ["inner.ts"]);
      yield* Effect.promise(() => Bun.write("outer-flags", JSON.stringify(flags)));
    }),
});
`,
      });
      const ran = yield* runWithArguments(project, ["--stage", "prod"]);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(yield* read(`${project.directory}/outer-flags`)).toBe('{"stage":"prod"}');
      expect(yield* read(`${project.directory}/inner-flags`)).toBe("{}");
    }),
  ),
);

const makeWorkerProject = (isolated: boolean) =>
  makeProject({
    "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${RECORD_FLAGS}
const reader = (name: string) =>
  Task.make({
    name,
    isolated: ${String(isolated)},
    flags: Schema.Struct({ stage: Schema.String, retries: Schema.Number }),
    run: (flags) => record(name, flags),
  });
export const first = reader("first");
export const second = reader("second");
export const third = reader("third");
export default Task.make({ name: "main", run: Task.all([first, second, third]) });
`,
  });

export const flagsOnWorkers = makeTest(
  "flags reach tasks on a single worker and on many workers",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [isolated, environment] of [
        [false, { SSHKEDA_TASK_WORKERS: "1" }],
        [true, {}],
      ] as const) {
        const project = yield* makeWorkerProject(isolated);
        const ran = yield* runWithArguments(
          project,
          ["--stage", "prod", "--retries=3"],
          environment,
        );
        expect(ran.exitCode, ran.standardError).toBe(0);
        for (const name of ["first", "second", "third"]) {
          expect([name, yield* read(`${project.directory}/${name}-flags`)]).toEqual([
            name,
            '{"stage":"prod","retries":3}',
          ]);
        }
      }
    }),
  ),
);

export const flagTypes = makeTest(
  "run's parameter is typed from flags, so a task reads its flags with their own types",
  Effect.scoped(
    typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const migrate = Task.make({
  name: "migrate",
  flags: Schema.Struct({ stage: Schema.Literals(["dev", "prod"]), batchSize: Schema.optional(Schema.Number) }),
  run: ({ stage, batchSize }) =>
    Effect.sync(() => {
      const typedStage: "dev" | "prod" = stage;
      const typedBatchSize: number | undefined = batchSize;
      return void [typedStage, typedBatchSize];
    }),
});
export const version = Task.make({
  name: "version",
  flags: Schema.Struct({ dryRun: Schema.optional(Schema.Boolean) }),
  output: Schema.String,
  timeout: "1 minute",
  run: ({ dryRun }) => Effect.succeed(dryRun === true ? "dry" : "1.0.0"),
});
export default Task.make({
  name: "deploy",
  run: Effect.gen(function* () {
    yield* migrate;
    const text: string = yield* version;
    return void text;
  }),
});`),
  ),
);

const sourceWithDefinition = (definition: string) => `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const mistyped = Task.make(${definition});
export default Task.make({ name: "main", run: Effect.void });`;

export const mistypedFlags = makeTest(
  "a run that reads a flag the schema lacks, a run that is not a function when flags are given, a function run without flags, or flags that are not a struct do not compile",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [definition, errorStart, explanation] of [
        [
          '{ name: "misread", flags: Schema.Struct({ stage: Schema.String }), run: ({ stag }) => Effect.log(stag) }',
          "main.ts(3,110): error TS2339",
          "Property 'stag' does not exist on type '{ readonly stage: string; }'.",
        ],
        [
          '{ name: "unflagged", flags: Schema.Struct({ stage: Schema.String }), run: Effect.void }',
          "main.ts(3,104): error TS2322",
          "Type 'Effect<void, never, never>' provides no match for the signature '(flags: { readonly stage: string; }): Effect<void, never, never> & NoInfer<Effect<void, never, never>>'.",
        ],
        [
          '{ name: "called", run: () => Effect.void }',
          "main.ts(3,58): error TS2322",
          "Type '() => Effect.Effect<void, never, never>' is not assignable to type 'Effect<void, never, never>'.",
        ],
        [
          '{ name: "unstructured", flags: Schema.String, run: () => Effect.void }',
          "main.ts(3,59): error TS2322",
          "Type 'string' is not assignable to type '{ readonly [x: string]: unknown; readonly [x: number]: unknown; readonly [x: symbol]: unknown; }'.",
        ],
      ] as const) {
        const failed = yield* failureAsValue(typeCheckSource(sourceWithDefinition(definition)));
        expect(failed.message).toContain(errorStart);
        expect(failed.message).toContain(explanation);
      }
    }),
  ),
);
