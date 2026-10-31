import { Cache, Effect, FileSystem, Option, Path } from "effect";

import {
  findBunLoaderForScript,
  isDataLoader,
} from "../../../../../shared/find-bun-loader-for-script.ts";
import type { makeNodeModulesPackageFinder } from "../../../../../shared/find-node-modules-package-directory.ts";
import { CodeFileUnreadable } from "../../../../../shared/module-graph-errors.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import { findPackageJsonDirectoryPath } from "../../../shared/find-package-directories.ts";
import { listDirectoryPathsUpToRoot } from "../../../shared/list-directory-paths-up-to-root.ts";
import { ImportNotFound } from "../../../shared/main-thread-errors.ts";
import type { ModuleRefusal } from "../../../shared/main-thread-errors.ts";
import {
  PackageJsonUnreadable,
  PackageJsonWithoutName,
} from "../../../shared/package-json-errors.ts";
import { readBarePackageName } from "../../../shared/read-bare-package-name.ts";
import {
  decodePackageJsonName,
  decodePackageJsonText,
  parsePackageJsonObject,
  readPackageJsonText,
  toPackageJsonUnreadable,
} from "../../../shared/read-package-json.ts";
import { scanProjectModule } from "../../scan-module-source/index.ts";
import { loadAllReportingFirstFailure } from "./load-all-reporting-first-failure.ts";
import { resolveSpecifierToRealFilePath } from "./resolve-module-specifiers.ts";

const findPackageDirectoryNamed = Effect.fn("findPackageDirectoryNamed")(function* (
  start: string,
  name: string,
) {
  const path = yield* Path.Path;
  for (const searchDirectoryPath of listDirectoryPathsUpToRoot(path, start)) {
    const packageJsonFilePath = `${searchDirectoryPath}/package.json`;
    const packageJsonText = yield* readPackageJsonText(packageJsonFilePath).pipe(
      Effect.asSome,
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
      Effect.mapError(toPackageJsonUnreadable(packageJsonFilePath)),
    );
    if (Option.isNone(packageJsonText)) {
      continue;
    }
    const packageJsonObject = yield* parsePackageJsonObject(
      packageJsonFilePath,
      packageJsonText.value,
    );
    const packageName = yield* decodePackageJsonName(packageJsonFilePath, packageJsonObject);
    if (Option.contains(packageName, name)) {
      return searchDirectoryPath;
    }
  }
  return null;
});

const resolveProjectImport = (
  specifier: ReturnType<typeof scanProjectModule>["specifiers"][number],
  importingFilePath: string,
) =>
  resolveSpecifierToRealFilePath(importingFilePath, specifier).pipe(
    Effect.map(
      ({
        found,
        realFilePath,
      }): {
        readonly resolvedFilePath: string | null;
        readonly refusal: typeof ModuleRefusal.Type | null;
      } =>
        found
          ? { resolvedFilePath: realFilePath, refusal: null }
          : {
              resolvedFilePath: null,
              refusal: new ImportNotFound({ importingFilePath, specifier: specifier.value }),
            },
    ),
    Effect.catchTag("ImportResolutionThrew", (refusal) =>
      Effect.succeed({ resolvedFilePath: null, refusal }),
    ),
  );

const listDependencies = (
  packageJson: Effect.Success<ReturnType<ReturnType<typeof decodePackageJsonText>>>,
) =>
  Object.keys({
    ...packageJson.dependencies,
    ...packageJson.devDependencies,
    ...packageJson.peerDependencies,
    ...packageJson.optionalDependencies,
  });

const mayMakeTasks = (
  packageJson: Effect.Success<ReturnType<ReturnType<typeof decodePackageJsonText>>>,
) =>
  packageJson.name !== TASK_PACKAGE.name &&
  listDependencies(packageJson).includes(TASK_PACKAGE.name);

export const makeImportFollower = (input: {
  readonly configuredLoaderOfExtension: ReadonlyMap<string, string>;
  readonly taskSourceDirectoryPath: string;
  readonly findNodeModulesPackageDirectoryPath: ReturnType<typeof makeNodeModulesPackageFinder>;
  readonly concurrency: number;
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const { taskSourceDirectoryPath, findNodeModulesPackageDirectoryPath, concurrency } = input;

    const packageJsonCache = yield* Cache.make({
      capacity: Number.POSITIVE_INFINITY,
      lookup: (packageDirectoryPath: string) => {
        const packageJsonFilePath = path.join(packageDirectoryPath, "package.json");
        return fileSystem.readFileString(packageJsonFilePath).pipe(
          Effect.mapError(
            (cause) => new PackageJsonUnreadable({ filePath: packageJsonFilePath, cause }),
          ),
          Effect.flatMap(decodePackageJsonText(packageJsonFilePath)),
          Effect.flatMap((packageJson) => {
            const { name } = packageJson;
            return name === undefined
              ? Effect.fail(new PackageJsonWithoutName({ filePath: packageJsonFilePath }))
              : Effect.succeed({ ...packageJson, name });
          }),
        );
      },
    });

    const packageJsonDirectoryPathCache = yield* Cache.make({
      capacity: Number.POSITIVE_INFINITY,
      lookup: findPackageJsonDirectoryPath,
    });

    const parsedModuleFileCache = yield* Cache.make({
      capacity: Number.POSITIVE_INFINITY,
      lookup: Effect.fn("readAndParseModuleFile")(function* (moduleFilePath: string) {
        const script = findBunLoaderForScript(moduleFilePath, input.configuredLoaderOfExtension);
        if (script === null) {
          return null;
        }
        const text = yield* fileSystem
          .readFileString(moduleFilePath)
          .pipe(
            Effect.mapError((cause) => new CodeFileUnreadable({ filePath: moduleFilePath, cause })),
          );
        const projectModuleScan = scanProjectModule(
          moduleFilePath,
          text,
          script.isConfigured ? script.loader : null,
        );
        const resolvedSpecifiers = yield* Effect.forEach(
          projectModuleScan.specifiers,
          (specifier) =>
            resolveProjectImport(specifier, moduleFilePath).pipe(
              Effect.map((resolution) => ({ specifier, ...resolution })),
            ),
          { concurrency },
        );
        return {
          text,
          loader: script.loader,
          resolvedSpecifiers,
          madeTasks: projectModuleScan.madeTasks,
          refusals: projectModuleScan.refusals.map((refusal): typeof ModuleRefusal.Type => ({
            filePath: moduleFilePath,
            refusal,
          })),
        };
      }),
    });

    const MODULE_WITHOUT_CODE = { imports: [], madeTasks: [], refusals: [] };

    const loadModule = Effect.fn("loadModule")(function* (importedModule: {
      readonly filePath: string;
      readonly packageDirectoryPathOutsideNodeModules: string | null;
    }) {
      const moduleFilePath = importedModule.filePath;
      if (moduleFilePath.startsWith(taskSourceDirectoryPath)) {
        return MODULE_WITHOUT_CODE;
      }
      const nodeModulesPackageDirectoryPath = findNodeModulesPackageDirectoryPath(moduleFilePath);
      const isInstalled = nodeModulesPackageDirectoryPath !== null;
      const isUnfollowedInstalledModule =
        isInstalled &&
        !mayMakeTasks(yield* Cache.get(packageJsonCache, nodeModulesPackageDirectoryPath));
      if (isUnfollowedInstalledModule) {
        return MODULE_WITHOUT_CODE;
      }
      const packageDirectoryPath =
        nodeModulesPackageDirectoryPath ?? importedModule.packageDirectoryPathOutsideNodeModules;

      const parsed = yield* Cache.get(parsedModuleFileCache, moduleFilePath);
      if (parsed === null) {
        return MODULE_WITHOUT_CODE;
      }

      const followsBarePackagesOutsideNodeModules =
        !isInstalled &&
        (importedModule.packageDirectoryPathOutsideNodeModules !== null ||
          (yield* Cache.get(packageJsonDirectoryPathCache, path.dirname(moduleFilePath))) === null);
      const findPackageDirectoryPathOutsideNodeModules = (
        specifier: ReturnType<typeof scanProjectModule>["specifiers"][number],
        importedFilePath: string,
      ) => {
        const packageName = readBarePackageName(specifier.value);
        if (findNodeModulesPackageDirectoryPath(importedFilePath) !== null) {
          return Effect.succeed(null);
        }
        const followsBarePackage =
          followsBarePackagesOutsideNodeModules && packageName !== undefined;
        if (followsBarePackage) {
          return findPackageDirectoryNamed(path.dirname(importedFilePath), packageName);
        }
        const isInsideTheImportingPackage =
          !isInstalled &&
          packageDirectoryPath !== null &&
          importedFilePath.startsWith(`${packageDirectoryPath}/`);
        return Effect.succeed(isInsideTheImportingPackage ? packageDirectoryPath : null);
      };
      const refusals = [...parsed.refusals];
      const imports: (typeof importedModule)[] = [];
      for (const { specifier, resolvedFilePath, refusal } of parsed.resolvedSpecifiers) {
        if (refusal !== null) {
          refusals.push(refusal);
          continue;
        }
        const isFollowedCodeImport =
          resolvedFilePath !== null &&
          specifier.kind !== "require.resolve" &&
          !isDataLoader(specifier.loader);
        if (isFollowedCodeImport) {
          imports.push({
            filePath: resolvedFilePath,
            packageDirectoryPathOutsideNodeModules:
              yield* findPackageDirectoryPathOutsideNodeModules(specifier, resolvedFilePath),
          });
        }
      }
      return { imports, madeTasks: parsed.madeTasks, refusals };
    });

    const walkReachableModules = Effect.fn("findReachableModules")(function* (
      entryFilePath: string,
    ) {
      const reachable = new Map<string, Effect.Success<ReturnType<typeof loadModule>>>();
      const loadWave = (wave: readonly Parameters<typeof loadModule>[0][]) =>
        loadAllReportingFirstFailure(
          wave,
          (importedModule) =>
            loadModule(importedModule).pipe(
              Effect.map((module) => [importedModule.filePath, module] as const),
            ),
          concurrency,
        );
      const walkWave = (
        wave: readonly Parameters<typeof loadModule>[0][],
      ): Effect.Effect<
        void,
        Effect.Error<ReturnType<typeof loadWave>>,
        Effect.Services<ReturnType<typeof loadWave>>
      > =>
        wave.length === 0
          ? Effect.void
          : loadWave(wave).pipe(
              Effect.flatMap((loadedWave) => {
                for (const [moduleFilePath, module] of loadedWave) {
                  reachable.set(moduleFilePath, module);
                }
                const nextWave = new Map<string, Parameters<typeof loadModule>[0]>();
                for (const [, module] of loadedWave) {
                  for (const importedModule of module.imports) {
                    const queuedPackageDirectoryPath = nextWave.get(
                      importedModule.filePath,
                    )?.packageDirectoryPathOutsideNodeModules;
                    const importedPackageDirectoryPath =
                      importedModule.packageDirectoryPathOutsideNodeModules;
                    const isNewOrDeeperPackageImport =
                      !reachable.has(importedModule.filePath) &&
                      (queuedPackageDirectoryPath === undefined ||
                        queuedPackageDirectoryPath === null ||
                        (importedPackageDirectoryPath !== null &&
                          importedPackageDirectoryPath.length > queuedPackageDirectoryPath.length));
                    if (isNewOrDeeperPackageImport) {
                      nextWave.set(importedModule.filePath, importedModule);
                    }
                  }
                }
                return walkWave([...nextWave.values()]);
              }),
            );
      yield* walkWave([{ filePath: entryFilePath, packageDirectoryPathOutsideNodeModules: null }]);
      return reachable;
    });

    const reachableModulesCache = yield* Cache.make({
      capacity: Number.POSITIVE_INFINITY,
      lookup: walkReachableModules,
    });

    return {
      findReachableModules: (entryFilePath: string) =>
        Cache.get(reachableModulesCache, entryFilePath),
      readParsedModuleFile: (moduleFilePath: string) =>
        Cache.get(parsedModuleFileCache, moduleFilePath),
    };
  });
