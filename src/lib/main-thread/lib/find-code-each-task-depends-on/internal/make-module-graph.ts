import { Context, Effect, Path } from "effect";
import type { FileSystem } from "effect";

import { makeNodeModulesPackageFinder } from "../../../../../shared/find-node-modules-package-directory.ts";
import { dieOnInvariantFailure } from "../../../../../shared/invariant-errors.ts";
import { availableParallelism } from "../../../../../shared/read-environment-config.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import { RunRootTaskConfig } from "../../../shared/provide-run-root-task-config.ts";
import { makeImportFollower } from "./follow-imports-from-entry-files.ts";
import { readConfiguredLoaders } from "./read-configured-loaders.ts";
import { makeWorkerImportRewriter } from "./rewrite-worker-imports.ts";

export const makeModuleGraph = Effect.gen(function* () {
  const path = yield* Path.Path;
  const runRootTaskConfig = yield* RunRootTaskConfig;
  const context = yield* Effect.context<FileSystem.FileSystem | Path.Path>();
  const taskSourceDirectoryPath = yield* path
    .fromFileUrl(new URL("../../../../../", import.meta.url))
    .pipe(dieOnInvariantFailure("module graph", "the runtime source URL is a file URL"));
  const findNodeModulesPackageDirectoryPath = makeNodeModulesPackageFinder(
    runRootTaskConfig.rootTaskPackageJsonDirectoryPath,
  );
  const importFollower = yield* makeImportFollower({
    configuredLoaderOfExtension: yield* readConfiguredLoaders({
      labelCodeFile: (filePath) =>
        path.relative(runRootTaskConfig.rootTaskPackageJsonDirectoryPath, filePath),
      workingDirectoryPathAtStart: runRootTaskConfig.workingDirectoryPathAtStart,
    }),
    taskSourceDirectoryPath,
    findNodeModulesPackageDirectoryPath,
    concurrency: yield* availableParallelism,
  });
  const rewriteBareImports = makeWorkerImportRewriter(
    importFollower,
    findNodeModulesPackageDirectoryPath,
  );
  return {
    rewriteBareImports: (roots: readonly string[]) =>
      rewriteBareImports(roots).pipe(Effect.provide(context)),
    findReachableModules: (entryFilePath: string) =>
      importFollower.findReachableModules(entryFilePath).pipe(Effect.provide(context)),
  };
});

export class ModuleGraph extends Context.Service<
  ModuleGraph,
  Effect.Success<typeof makeModuleGraph>
>()(`${TASK_PACKAGE.name}/ModuleGraph`) {}
