import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";

const compileOutsideAnyNodeModules = (mainFileText: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({ "main.ts": mainFileText });
    const binaryDirectory = yield* makeTemporaryDirectory();
    const binaryPath = `${binaryDirectory}/main`;
    yield* Task.exec(process.execPath, ["build", "--compile", "main.ts", "--outfile", binaryPath], {
      cwd: project.directory,
    });
    return { binaryPath, binaryDirectory };
  });

export const importsWithoutRun = makeTest(
  "a bun build --compile binary that imports Task and its errors and makes a task without a default export starts and exits 0",
  Effect.scoped(
    Effect.gen(function* () {
      const { binaryPath, binaryDirectory } = yield* compileOutsideAnyNodeModules(
        `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { CommandFailed } from "@sshkeda/task/errors";
const made = Task.make({ name: "made", run: Effect.void });
console.log(made.name, new CommandFailed({ command: "c", code: 1, standardOutput: "", standardError: "" })._tag);`,
      );
      expect(yield* Task.exec(binaryPath, [], { cwd: binaryDirectory })).toBe(
        "made CommandFailed\n",
      );
    }),
  ),
);

export const locatesErrorsMadeByFilesOnDisk = makeTest(
  "a TaskError made by a file a bun build --compile binary imports from disk points at that file",
  Effect.scoped(
    Effect.gen(function* () {
      const { binaryPath, binaryDirectory } = yield* compileOutsideAnyNodeModules(
        `import { TaskError } from "@sshkeda/task/errors";
Object.assign(globalThis, { TaskError });
const { made } = await import(\`\${process.cwd()}/makes-error.js\`);
console.log(made._tag, made.location);`,
      );
      const fileSystem = yield* FileSystem.FileSystem;
      yield* fileSystem.writeFileString(
        `${binaryDirectory}/makes-error.js`,
        `export const made = new globalThis.TaskError({ message: "m" });\n`,
      );
      expect(yield* Task.exec(binaryPath, [], { cwd: binaryDirectory })).toBe(
        `TaskError ${binaryDirectory}/makes-error.js:1:36\n`,
      );
    }),
  ),
);

export const runRefused = makeTest(
  "a bun build --compile binary whose default export is a task fails with RunCalledInStandaloneExecutable before the task body runs",
  Effect.scoped(
    Effect.gen(function* () {
      const { binaryPath, binaryDirectory } = yield* compileOutsideAnyNodeModules(
        `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "root", run: Effect.log("root ran") });`,
      );
      yield* Task.exec(binaryPath, [], { cwd: binaryDirectory }).pipe(
        Effect.flatMap((output) => Effect.die(`the binary succeeded and printed ${output}`)),
        Effect.catchTag("CommandFailed", (failure) =>
          Effect.sync(() => {
            expect(failure.standardError)
              .toBe(`error RunCalledInStandaloneExecutable: @sshkeda/task can't run "root" inside a bun build --compile binary, which holds only its bundle, not the task source files it imports again to run tasks
`);
            expect(failure.code).toBe(1);
            expect(failure.standardOutput).not.toContain("root ran");
          }),
        ),
      );
    }),
  ),
);
