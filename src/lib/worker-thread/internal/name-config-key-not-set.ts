import { Config, Effect, Exit, Option, Schema, SchemaAST, SchemaIssue } from "effect";

import {
  nameConfigKey,
  nameEnvironmentConfigVariable,
  observeConfigLoads,
} from "../../../shared/name-config-key.ts";
import { ConfigKeyNotSet } from "../../../shared/task-errors.ts";

const isConfigError = Schema.is(Schema.instanceOf(Config.ConfigError));

const decodeConfigErrorAtPath = Schema.decodeUnknownOption(
  Schema.Struct({ cause: Schema.Struct({ issue: Schema.instanceOf(SchemaIssue.Pointer) }) }),
);

const expectsManyVariables = (issue: SchemaIssue.Issue) =>
  issue instanceof SchemaIssue.InvalidType &&
  (SchemaAST.isObjects(issue.ast) || SchemaAST.isArrays(issue.ast));

export const nameConfigKeyNotSet = <Success, Failure, Services>(
  effect: Effect.Effect<Success, Failure, Services>,
) =>
  Effect.suspend(() => {
    const missingKeys = new Map<string, { readonly readsRuntimeConfigProvider: boolean }>();
    return effect.pipe(
      observeConfigLoads(({ configPath, readsRuntimeConfigProvider, loaded }) => {
        const loadedNothing = Exit.isSuccess(loaded) && loaded.value === undefined;
        if (loadedNothing) {
          missingKeys.set(nameConfigKey(configPath), { readsRuntimeConfigProvider });
        }
      }),
      Effect.mapError((failure): ConfigKeyNotSet | Failure => {
        if (!isConfigError(failure)) {
          return failure;
        }
        const configError = failure;
        const decodedConfigError = decodeConfigErrorAtPath(configError);
        if (Option.isNone(decodedConfigError)) {
          return configError;
        }
        const pointer = decodedConfigError.value.cause.issue;
        const configPath = pointer.path.map(String);
        const missing = missingKeys.get(nameConfigKey(configPath));
        if (missing === undefined) {
          return configError;
        }
        const namesOneEnvironmentConfigVariable =
          missing.readsRuntimeConfigProvider && !expectsManyVariables(pointer.issue);
        return new ConfigKeyNotSet({
          configKey: nameConfigKey(configPath),
          environmentConfigVariable: namesOneEnvironmentConfigVariable
            ? nameEnvironmentConfigVariable(configPath)
            : null,
        });
      }),
    );
  });
