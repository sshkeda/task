import { createRequire } from "node:module";

import { Effect, FileSystem, Option, Path, Schema } from "effect";

import { ImportResolutionThrew } from "../../../../../shared/module-graph-errors.ts";
import type { scanProjectModule } from "../../scan-module-source/index.ts";

const isModuleNotFound = Schema.is(
  Schema.Struct({ code: Schema.Literals(["ERR_MODULE_NOT_FOUND", "MODULE_NOT_FOUND"]) }),
);

const resolveSpecifier = (
  importingFilePath: string,
  specifier: Pick<ReturnType<typeof scanProjectModule>["specifiers"][number], "kind" | "value">,
) =>
  Path.Path.use((path) =>
    specifier.kind === "import"
      ? Effect.tryPromise({
          try: () => Bun.resolve(specifier.value, path.dirname(importingFilePath)),
          catch: (cause) =>
            new ImportResolutionThrew({ importingFilePath, specifier: specifier.value, cause }),
        })
      : Effect.try({
          try: () => createRequire(importingFilePath).resolve(specifier.value),
          catch: (cause) =>
            new ImportResolutionThrew({ importingFilePath, specifier: specifier.value, cause }),
        }),
  ).pipe(
    Effect.asSome,
    Effect.catchTag("ImportResolutionThrew", (failure) =>
      isModuleNotFound(failure.cause) ? Effect.succeedNone : Effect.fail(failure),
    ),
  );

export const resolveSpecifierToRealFilePath = (
  importingFilePath: string,
  specifier: Pick<ReturnType<typeof scanProjectModule>["specifiers"][number], "kind" | "value">,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const resolvedPath = yield* resolveSpecifier(importingFilePath, specifier);
    if (Option.isNone(resolvedPath)) {
      return { found: false, realFilePath: null };
    }
    if (!path.isAbsolute(resolvedPath.value)) {
      return { found: true, realFilePath: null };
    }
    return {
      found: true,
      realFilePath: yield* fileSystem
        .realPath(resolvedPath.value)
        .pipe(
          Effect.mapError(
            (cause) =>
              new ImportResolutionThrew({ importingFilePath, specifier: specifier.value, cause }),
          ),
        ),
    };
  });
