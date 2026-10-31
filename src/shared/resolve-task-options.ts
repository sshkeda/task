import type { Duration } from "effect";

export const TASK_OPTION_NAMES = [
  "timeout",
  "cleanup",
] as const satisfies readonly (keyof Parameters<typeof resolveTaskOptions>[0])[];

export const resolveTaskOptions = (options: {
  readonly timeout?: Duration.Input;
  readonly cleanup?: Duration.Input;
}) =>
  Object.freeze({
    timeout: options.timeout ?? null,
    cleanup: options.cleanup ?? null,
    unknownOptionNames: Object.freeze(
      Object.keys(options).filter(
        (optionName) => !TASK_OPTION_NAMES.some((knownName) => knownName === optionName),
      ),
    ),
  });
