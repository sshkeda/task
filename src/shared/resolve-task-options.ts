import { inspect } from "node:util";

import { Duration, Option, Schema } from "effect";

export const TASK_OPTION_NAMES = [
  "timeout",
  "cleanup",
] as const satisfies readonly (keyof Parameters<typeof resolveTaskOptions>[0])[];

export const resolveTaskOptions = (
  options: {
    readonly timeout?: Duration.Input | undefined;
    readonly cleanup?: Duration.Input | undefined;
  },
  isolated?: boolean,
) =>
  Object.freeze({
    timeout: options.timeout ?? null,
    cleanup: options.cleanup ?? null,
    isolated: isolated ?? false,
    unknownOptionNames: Object.freeze(
      Object.keys(options).filter(
        (optionName) => !TASK_OPTION_NAMES.some((knownName) => knownName === optionName),
      ),
    ),
  });

export const TaskDurationOption = Schema.TaggedUnion({
  Valid: { duration: Schema.NullOr(Schema.Duration) },
  Invalid: { text: Schema.String },
});

export const TaskOptionsRead = Schema.Struct({
  timeout: TaskDurationOption,
  cleanup: TaskDurationOption,
  isolated: Schema.Union([Schema.Boolean, Schema.Struct({ invalidText: Schema.String })]),
  unknownOptionNames: Schema.Array(Schema.String),
});

const isBoolean = Schema.is(Schema.Boolean);

const readDurationOption = (
  input: Duration.Input | null,
  isAllowed: (duration: Duration.Duration) => boolean,
) =>
  input === null
    ? TaskDurationOption.cases.Valid.make({ duration: null })
    : Option.match(Option.filter(Duration.fromInput(input), isAllowed), {
        onNone: () => TaskDurationOption.cases.Invalid.make({ text: inspect(input) }),
        onSome: (duration) => TaskDurationOption.cases.Valid.make({ duration }),
      });

export const readTaskOptions = (
  options: ReturnType<typeof resolveTaskOptions>,
): typeof TaskOptionsRead.Type => ({
  timeout: readDurationOption(options.timeout, Duration.isPositive),
  cleanup: readDurationOption(
    options.cleanup,
    (duration) => Duration.isZero(duration) || Duration.isPositive(duration),
  ),
  isolated: isBoolean(options.isolated)
    ? options.isolated
    : { invalidText: inspect(options.isolated) },
  unknownOptionNames: options.unknownOptionNames,
});
