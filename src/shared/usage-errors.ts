import { Schema } from "effect";

import {
  HomeDirectoryPathMissing,
  LogLevelInvalid,
  SetupLimitInvalid,
  StateDirectoryPathBlank,
  WorkerCountAboveMaximum,
  WorkerCountInvalid,
} from "./environment-config-errors.ts";

export const USAGE_ERROR_EXIT_CODE = 2;

export const ENVIRONMENT_USAGE_ERRORS = [
  HomeDirectoryPathMissing,
  LogLevelInvalid,
  SetupLimitInvalid,
  StateDirectoryPathBlank,
  WorkerCountAboveMaximum,
  WorkerCountInvalid,
] as const;

export const isEnvironmentUsageError = Schema.is(Schema.Union(ENVIRONMENT_USAGE_ERRORS));
