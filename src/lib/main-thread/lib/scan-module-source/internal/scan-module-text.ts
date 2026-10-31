import type { SCRIPT_LOADERS } from "../../../../../shared/find-bun-loader-for-script.ts";
import {
  ModuleLoadedByComputedPath,
  ModuleLoaderRefused,
  ModuleSyntaxInvalid,
} from "../../../shared/main-thread-errors.ts";
import type { ModuleScanRefusal } from "../../../shared/main-thread-errors.ts";
import { findTaskCallsBelowTopLevel, findTaskDeclarations } from "./find-task-declarations.ts";
import { parseModuleText } from "./parse-module-text.ts";
import {
  findModuleLoads,
  listSpecifiersOnce,
  listStaticImportSpecifiers,
} from "./read-module-loads.ts";

export const scanProjectModule = (
  moduleFilePath: string,
  text: string,
  configuredLoader: (typeof SCRIPT_LOADERS)[number] | null = null,
) => {
  const parsed = parseModuleText(moduleFilePath, text, { configuredLoader });
  const refusals: (typeof ModuleScanRefusal.Type)[] = [];
  const lineAt = (offset: number) => text.slice(0, offset).split("\n").length;

  for (const error of parsed.errors) {
    const [firstLabel] = error.labels;
    refusals.push(
      new ModuleSyntaxInvalid({
        line: firstLabel === undefined ? null : lineAt(firstLabel.start),
        parserMessage: error.message,
      }),
    );
  }

  const taskDeclarations = findTaskDeclarations(parsed, text, lineAt);
  refusals.push(...taskDeclarations.importViolations);
  const moduleLoads = findModuleLoads(parsed.program, text);
  for (const unfollowedLoad of moduleLoads.unfollowedLoads) {
    if (unfollowedLoad.argumentText === null) {
      refusals.push(
        new ModuleLoaderRefused({
          line: lineAt(unfollowedLoad.start),
          loader: unfollowedLoad.loader,
        }),
      );
    } else {
      refusals.push(
        new ModuleLoadedByComputedPath({
          line: lineAt(unfollowedLoad.start),
          loader: unfollowedLoad.loader,
          argumentText: unfollowedLoad.argumentText,
        }),
      );
    }
  }

  refusals.push(
    ...taskDeclarations.nameViolations,
    ...findTaskCallsBelowTopLevel(parsed.program, taskDeclarations, lineAt),
  );

  return {
    specifiers: listSpecifiersOnce([
      ...listStaticImportSpecifiers(parsed),
      ...moduleLoads.literalLoads,
    ]),
    madeTasks: taskDeclarations.madeTasks,
    refusals,
  };
};
