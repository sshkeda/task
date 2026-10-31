import { Array as Arrays, Chunk, Effect, Schema } from "effect";

import { isDataLoader } from "../../../../../shared/find-bun-loader-for-script.ts";
import type { makeNodeModulesPackageFinder } from "../../../../../shared/find-node-modules-package-directory.ts";
import { dieOnInvariantFailure } from "../../../../../shared/invariant-errors.ts";
import { readBarePackageName } from "../../../shared/read-bare-package-name.ts";
import type { makeImportFollower } from "./follow-imports-from-entry-files.ts";
import { WorkerImportRefused } from "./worker-import-errors.ts";

const encodeQuotedString = Schema.encodeEffect(Schema.fromJsonString(Schema.String));

export const makeWorkerImportRewriter = (
  importFollower: Effect.Success<ReturnType<typeof makeImportFollower>>,
  findNodeModulesPackageDirectoryPath: ReturnType<typeof makeNodeModulesPackageFinder>,
) => {
  const { findReachableModules, readParsedModuleFile } = importFollower;
  return Effect.fn("rewriteBareImports")(function* (roots: readonly string[]) {
    const reachableModulesOfRoots = yield* Effect.forEach(roots, findReachableModules);
    for (const reachableModules of reachableModulesOfRoots) {
      for (const module of reachableModules.values()) {
        const [firstRefusal] = module.refusals;
        if (firstRefusal !== undefined) {
          return yield* new WorkerImportRefused({ refusal: firstRefusal });
        }
      }
    }
    const followsPackageOutsideNodeModules = reachableModulesOfRoots.some((reachableModules) =>
      [...reachableModules.values()].some((module) =>
        module.imports.some(
          (importedModule) => importedModule.packageDirectoryPathOutsideNodeModules !== null,
        ),
      ),
    );
    if (!followsPackageOutsideNodeModules) {
      return {};
    }

    const visited = new Set<string>();
    const pending = [...roots];
    const rewriteImportsOfFile = (importingFilePath: string) =>
      Effect.gen(function* () {
        const parsed = yield* readParsedModuleFile(importingFilePath);
        if (parsed === null) {
          return [];
        }
        const replacements: {
          readonly start: number;
          readonly end: number;
          readonly text: string;
        }[] = [];
        for (const { specifier, resolvedFilePath } of parsed.resolvedSpecifiers.toSorted(
          (left, right) => right.specifier.start - left.specifier.start,
        )) {
          if (resolvedFilePath === null) {
            continue;
          }
          const loadsCode = specifier.kind !== "require.resolve" && !isDataLoader(specifier.loader);
          if (loadsCode) {
            pending.push(resolvedFilePath);
          }
          const importsBarePackageOutsideNodeModules =
            readBarePackageName(specifier.value) !== undefined &&
            findNodeModulesPackageDirectoryPath(resolvedFilePath) === null;
          if (importsBarePackageOutsideNodeModules) {
            const quotedResolvedPath = yield* encodeQuotedString(resolvedFilePath).pipe(
              dieOnInvariantFailure("module graph", "a resolved import path encodes to JSON text"),
            );
            replacements.push({
              start: specifier.start,
              end: specifier.end,
              text: quotedResolvedPath,
            });
          }
        }
        const replacementsInTextOrder = replacements.toReversed();
        const keptTextStarts = Arrays.prepend(
          replacementsInTextOrder.map((replacement) => replacement.end),
          0,
        );
        const rewrittenText = [
          ...Arrays.zipWith(
            keptTextStarts,
            replacementsInTextOrder,
            (keptTextStart, replacement) =>
              `${parsed.text.slice(keptTextStart, replacement.start)}${replacement.text}`,
          ),
          parsed.text.slice(Arrays.lastNonEmpty(keptTextStarts)),
        ].join("");
        return rewrittenText === parsed.text
          ? []
          : [[importingFilePath, { contents: rewrittenText, loader: parsed.loader }] as const];
      });
    const visitNext = (
      rewrittenSoFar: Chunk.Chunk<Effect.Success<ReturnType<typeof rewriteImportsOfFile>>[number]>,
    ): Effect.Effect<
      typeof rewrittenSoFar,
      Effect.Error<ReturnType<typeof rewriteImportsOfFile>>
    > => {
      const importingFilePath = pending.pop();
      if (importingFilePath === undefined) {
        return Effect.succeed(rewrittenSoFar);
      }
      const isSkippedImportingFile =
        visited.has(importingFilePath) ||
        findNodeModulesPackageDirectoryPath(importingFilePath) !== null;
      if (isSkippedImportingFile) {
        return Effect.suspend(() => visitNext(rewrittenSoFar));
      }
      visited.add(importingFilePath);
      return rewriteImportsOfFile(importingFilePath).pipe(
        Effect.flatMap((rewrittenFile) =>
          visitNext(Chunk.appendAll(rewrittenSoFar, Chunk.fromIterable(rewrittenFile))),
        ),
      );
    };
    return Object.fromEntries(yield* Effect.suspend(() => visitNext(Chunk.empty())));
  });
};
