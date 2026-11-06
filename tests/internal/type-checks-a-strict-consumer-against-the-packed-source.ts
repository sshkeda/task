import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Path, Struct } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { encodeJson } from "../shared/encode-json.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { packPackageTarballInto } from "../shared/pack-package-tarball.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const STRICT_COMPILER_OPTIONS = {
  target: "ESNext",
  module: "NodeNext",
  strict: true,
  noUncheckedIndexedAccess: true,
  noPropertyAccessFromIndexSignature: true,
  exactOptionalPropertyTypes: true,
  noImplicitReturns: true,
  noUnusedLocals: true,
  noUnusedParameters: true,
  noImplicitOverride: true,
  noFallthroughCasesInSwitch: true,
  allowUnreachableCode: false,
  allowUnusedLabels: false,
  erasableSyntaxOnly: true,
  verbatimModuleSyntax: true,
  rewriteRelativeImportExtensions: true,
  allowImportingTsExtensions: true,
  types: ["bun"],
} as const;

const PACKAGE_FILES_LOADED_BY_PATH = [
  "src/lib/worker-thread/index.ts",
  "src/lib/preload-rewritten-imports-into-each-worker/index.ts",
  "src/lib/supervise-the-run/internal/start-watching-the-supervisor.ts",
].map((packageFile) => `node_modules/${packageJson.name}/${packageFile}`);

const CONSUMER_SOURCE_FILES = {
  "tasks.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { CommandFailed } from "@sshkeda/task/errors";
const echoOptions: Parameters<typeof Task.make<unknown, never, never>>[2] = { timeout: "1 minute", cleanup: "5 seconds" };
export const echoed = Task.make("echoed", Task.exec("echo", ["echoed"]), echoOptions);
export const main = Task.make("main", Effect.gen(function* () {
  const printed = yield* echoed;
  yield* Effect.log(\`strict consumer saw \${printed.trim()}\`);
}));
export const isCommandFailure = (failure: unknown): failure is CommandFailed =>
  failure instanceof CommandFailed;
export const echoedText = (text: string) => Task.exec("echo", [text]);
`,
  "count-lines.ts": `import { Effect } from "effect";
import { echoedText } from "./tasks.ts";
export const echoedLineCount = (text: string) =>
  echoedText(text).pipe(Effect.map((printed) => printed.split("\\n").length));
`,
  "refused.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
// @ts-expect-error
export const cached = Task.make("cached", Effect.void, { cache: true });
`,
  "run.ts": `import { Task } from "@sshkeda/task";
import { main } from "./tasks.ts";
Task.main(main);
`,
} as const;

const CONSUMER_TSCONFIG_FILES = {
  "tsconfig.json": {
    compilerOptions: {
      ...STRICT_COMPILER_OPTIONS,
      declaration: true,
      emitDeclarationOnly: true,
      outDir: "declarations",
    },
    files: PACKAGE_FILES_LOADED_BY_PATH,
    include: Object.keys(CONSUMER_SOURCE_FILES),
  },
  "tsconfig.bun-condition-node-next.json": {
    compilerOptions: { ...STRICT_COMPILER_OPTIONS, noEmit: true, customConditions: ["bun"] },
    files: PACKAGE_FILES_LOADED_BY_PATH,
    include: Object.keys(CONSUMER_SOURCE_FILES),
  },
  "tsconfig.bun-init-without-dom.json": {
    compilerOptions: {
      ...STRICT_COMPILER_OPTIONS,
      lib: ["ESNext"],
      skipLibCheck: true,
      module: "Preserve",
      moduleResolution: "Bundler",
      noEmit: true,
    },
    files: PACKAGE_FILES_LOADED_BY_PATH,
    include: Object.keys(CONSUMER_SOURCE_FILES),
  },
  "tsconfig.without-target-or-module.json": {
    compilerOptions: Struct.omit({ ...STRICT_COMPILER_OPTIONS, noEmit: true }, [
      "target",
      "module",
    ]),
    files: PACKAGE_FILES_LOADED_BY_PATH,
    include: Object.keys(CONSUMER_SOURCE_FILES),
  },
  "tsconfig.bun-condition-bundler.json": {
    compilerOptions: {
      ...STRICT_COMPILER_OPTIONS,
      module: "Preserve",
      moduleResolution: "Bundler",
      noEmit: true,
      customConditions: ["bun"],
    },
    files: PACKAGE_FILES_LOADED_BY_PATH,
    include: Object.keys(CONSUMER_SOURCE_FILES),
  },
};

export const strictConsumerUsesPackedTarball = Task.make(
  "consumers with strict tsconfigs, a bun condition, only the ESNext lib or no target or module option type-check the packed TypeScript source without errors, emit declarations that name the package, and run tasks with Bun",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repository = yield* REPOSITORY_DIRECTORY;
      const tarballDirectory = yield* fileSystem.makeTempDirectoryScoped();
      const consumer = yield* fileSystem.realPath(yield* fileSystem.makeTempDirectoryScoped());
      const tarball = yield* packPackageTarballInto(tarballDirectory);
      const consumerPackageJson = {
        name: "strict-consumer",
        type: "module",
        dependencies: {
          [packageJson.name]: `file:${tarball}`,
          effect: packageJson.devDependencies.effect,
          "@types/bun": packageJson.devDependencies["@types/bun"],
        },
      };
      for (const [fileName, json] of Object.entries({
        "package.json": consumerPackageJson,
        ...CONSUMER_TSCONFIG_FILES,
      })) {
        yield* fileSystem.writeFileString(path.join(consumer, fileName), yield* encodeJson(json));
      }
      for (const [fileName, text] of Object.entries(CONSUMER_SOURCE_FILES)) {
        yield* fileSystem.writeFileString(path.join(consumer, fileName), text);
      }
      yield* Task.exec("bun", ["install"], { cwd: consumer });
      const packedSourceDirectory = path.join(consumer, "node_modules", packageJson.name, "src");
      const shippedSourceFiles = (yield* fileSystem.readDirectory(packedSourceDirectory, {
        recursive: true,
      }))
        .filter((sourceFile) => sourceFile.endsWith(".ts"))
        .map((sourceFile) => path.join(packedSourceDirectory, sourceFile));
      for (const tsconfigFileName of Object.keys(CONSUMER_TSCONFIG_FILES)) {
        const typeCheckedFiles = (yield* Task.exec(
          path.join(repository, "node_modules/.bin/tsc"),
          ["--project", tsconfigFileName, "--listFiles"],
          { cwd: consumer },
        )).split("\n");
        const taskPackageFiles = typeCheckedFiles.filter((typeCheckedFile) =>
          typeCheckedFile.includes(`/node_modules/${packageJson.name}/`),
        );
        for (const shippedSourceFile of shippedSourceFiles) {
          expect(taskPackageFiles, tsconfigFileName).toContain(shippedSourceFile);
        }
      }
      const emittedDeclarations = yield* fileSystem.readFileString(
        path.join(consumer, "declarations/tasks.d.ts"),
      );
      expect(emittedDeclarations).toContain(`import { Task } from "${packageJson.name}";`);
      expect(emittedDeclarations).toContain(`from "${packageJson.name}/errors"`);
      expect(emittedDeclarations).not.toMatch(/import\("\.\.\/|from "\.\.\//u);
      expect(
        yield* fileSystem.readFileString(path.join(consumer, "declarations/count-lines.d.ts")),
      ).toContain(`import("${packageJson.name}").Tasks`);
      const ran = yield* runTaskProcess(
        { directory: consumer, home: yield* makeScratchDirectory, scope: yield* Effect.scope },
        "run.ts",
      );
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] strict consumer saw echoed");
    }),
  ),
  { timeout: "2 minutes", cleanup: "5 seconds" },
);
