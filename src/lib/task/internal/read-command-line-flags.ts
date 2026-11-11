import { Array as Arrays, Effect, Schema, SchemaAST, SchemaIssue } from "effect";

import { CommandLineFlags } from "../../../shared/mark-task-worker-thread-contexts.ts";
import {
  FlagGivenTwice,
  FlagInvalid,
  FlagMissing,
  FlagNotFlat,
  FlagNotRead,
  FlagReadTwoWays,
  FlagReserved,
  FlagValueMissing,
} from "./flag-errors.ts";

const HELP_FLAG_NAME = "help";

const FLAG_PREFIX = "--";

const flagNameOf = (fieldName: string) =>
  fieldName.replaceAll(/[A-Z]/gu, (letter) => `-${letter.toLowerCase()}`);

const membersOf = (ast: SchemaAST.AST): readonly SchemaAST.AST[] => {
  if (SchemaAST.isUnion(ast)) {
    return ast.types.flatMap(membersOf);
  }
  return SchemaAST.isUndefined(ast) ? [] : [ast];
};

const SINGLE_VALUE_TYPES = [
  { isType: SchemaAST.isString, shown: "text" },
  { isType: SchemaAST.isNumber, shown: "number" },
  { isType: SchemaAST.isBoolean, shown: "boolean" },
] as const;

const describeFlagType = (field: { readonly ast: SchemaAST.AST }) => {
  const members = membersOf(SchemaAST.toType(field.ast));
  const choices = members.flatMap((member) =>
    SchemaAST.isLiteral(member) ? [member.literal] : [],
  );
  const isChoiceList = Arrays.isArrayNonEmpty(choices) && choices.length === members.length;
  if (isChoiceList) {
    return { shown: choices.map(String).join(" | "), isBoolean: false };
  }
  const [onlyMember, ...otherMembers] = members;
  const isSingleValue = onlyMember !== undefined && otherMembers.length === 0;
  const singleValueType = isSingleValue
    ? SINGLE_VALUE_TYPES.find((valueType) => valueType.isType(onlyMember))
    : undefined;
  return singleValueType === undefined
    ? null
    : { shown: singleValueType.shown, isBoolean: singleValueType.shown === "boolean" };
};

const descriptionOf = (ast: SchemaAST.AST): string | null => {
  const description = SchemaAST.resolveDescription(ast);
  if (description !== undefined) {
    return description;
  }
  const [onlyMember, ...otherMembers] = SchemaAST.isUnion(ast)
    ? ast.types.filter((member) => !SchemaAST.isUndefined(member))
    : [];
  const isOnlyMember = onlyMember !== undefined && !Arrays.isArrayNonEmpty(otherMembers);
  return isOnlyMember ? descriptionOf(onlyMember) : null;
};

const readDeclaredFlags = (task: {
  readonly name: string;
  readonly flags: { readonly fields: Schema.Struct.Fields } | null;
}) =>
  Object.entries(task.flags?.fields ?? {}).map(([fieldName, field]) => ({
    taskName: task.name,
    fieldName,
    flagName: flagNameOf(fieldName),
    type: describeFlagType(field),
    isOptional: SchemaAST.isOptional(SchemaAST.toEncoded(field.ast)),
    description: descriptionOf(field.ast),
  }));

export const checkDeclaredFlags = (tasks: readonly Parameters<typeof readDeclaredFlags>[0][]) =>
  Effect.gen(function* () {
    const declaredFlags = tasks.flatMap((task) => readDeclaredFlags(task));
    const reservedFlag = declaredFlags.find((flag) => flag.flagName === HELP_FLAG_NAME);
    if (reservedFlag !== undefined) {
      return yield* new FlagReserved({
        readingTaskName: reservedFlag.taskName,
        flagName: reservedFlag.flagName,
      });
    }
    const notFlatFlag = declaredFlags.find((flag) => flag.type === null);
    if (notFlatFlag !== undefined) {
      return yield* new FlagNotFlat({
        readingTaskName: notFlatFlag.taskName,
        flagName: notFlatFlag.flagName,
      });
    }
    const flatFlags = declaredFlags.flatMap(({ type, ...flag }) =>
      type === null ? [] : [{ ...flag, type }],
    );
    const [clash] = flatFlags.flatMap((flag) =>
      flag.type.isBoolean
        ? flatFlags
            .filter((other) => other.flagName === flag.flagName && !other.type.isBoolean)
            .map((other) => ({ switchFlag: flag, valueFlag: other }))
        : [],
    );
    if (clash !== undefined) {
      return yield* new FlagReadTwoWays({
        switchTaskName: clash.switchFlag.taskName,
        valueTaskName: clash.valueFlag.taskName,
        flagName: clash.switchFlag.flagName,
      });
    }
    return flatFlags;
  });

const HELP_ARGUMENT = `${FLAG_PREFIX}${HELP_FLAG_NAME}`;

const readArguments = (
  remainingArguments: readonly string[],
  flatFlags: Effect.Success<ReturnType<typeof checkDeclaredFlags>>,
  read: {
    readonly words: readonly string[];
    readonly values: Readonly<Record<string, string>>;
    readonly asksForHelp: boolean;
  },
): Effect.Effect<typeof read, FlagGivenTwice | FlagNotRead | FlagValueMissing> =>
  Effect.gen(function* () {
    const [argument, ...laterArguments] = remainingArguments;
    if (argument === undefined) {
      return read;
    }
    if (argument === HELP_ARGUMENT) {
      return yield* readArguments(laterArguments, flatFlags, { ...read, asksForHelp: true });
    }
    const isFlag = argument.startsWith(FLAG_PREFIX) && argument.length > FLAG_PREFIX.length;
    if (!isFlag) {
      return yield* readArguments(laterArguments, flatFlags, {
        ...read,
        words: [...read.words, argument],
      });
    }
    const [flagName = "", ...valueParts] = argument.slice(FLAG_PREFIX.length).split("=");
    const flag = flatFlags.find((declaredFlag) => declaredFlag.flagName === flagName);
    if (flag === undefined) {
      return yield* new FlagNotRead({ flagName });
    }
    if (Object.hasOwn(read.values, flagName)) {
      return yield* new FlagGivenTwice({ flagName });
    }
    const [nextArgument, ...argumentsAfterNext] = laterArguments;
    const hasInlineValue = Arrays.isArrayNonEmpty(valueParts);
    const takesNextArgument =
      !hasInlineValue &&
      !flag.type.isBoolean &&
      nextArgument !== undefined &&
      !nextArgument.startsWith(FLAG_PREFIX);
    const presentValue = flag.type.isBoolean ? "true" : null;
    const inlineValue = hasInlineValue ? valueParts.join("=") : null;
    const value = inlineValue ?? (takesNextArgument ? nextArgument : presentValue);
    if (value === null) {
      return yield* new FlagValueMissing({ flagName });
    }
    return yield* readArguments(
      takesNextArgument ? argumentsAfterNext : laterArguments,
      flatFlags,
      {
        ...read,
        values: { ...read.values, [flagName]: value },
      },
    );
  });

export const readCommandLine = (
  givenArguments: readonly string[],
  flatFlags: Effect.Success<ReturnType<typeof checkDeclaredFlags>>,
) => readArguments(givenArguments, flatFlags, { words: [], values: {}, asksForHelp: false });

const formatIssue = SchemaIssue.makeFormatterStandardSchemaV1();

export const decodeTaskFlags = <
  Flags extends Schema.ConstraintDecoder<Schema.Struct.Type<Schema.Struct.Fields>> &
    Schema.Struct<Schema.Struct.Fields>,
>(
  taskName: string,
  flags: Flags,
  values: Readonly<Record<string, string>>,
) =>
  Effect.gen(function* () {
    const declaredFlags = readDeclaredFlags({ name: taskName, flags });
    const notFlatFlag = declaredFlags.find((flag) => flag.type === null);
    if (notFlatFlag !== undefined) {
      return yield* new FlagNotFlat({ readingTaskName: taskName, flagName: notFlatFlag.flagName });
    }
    const givenFields = declaredFlags.flatMap((flag) => {
      const isFalseWhenAbsent = flag.type?.isBoolean === true && !flag.isOptional;
      const absentValue = isFalseWhenAbsent ? "false" : undefined;
      const value = values[flag.flagName] ?? absentValue;
      return value === undefined ? [] : [{ ...flag, value }];
    });
    const missingFlag = declaredFlags.find(
      (flag) =>
        !flag.isOptional && !givenFields.some((given) => given.fieldName === flag.fieldName),
    );
    if (missingFlag !== undefined) {
      return yield* new FlagMissing({ readingTaskName: taskName, flagName: missingFlag.flagName });
    }
    return yield* Schema.decodeEffect(Schema.toCodecStringTree(flags))(
      Object.fromEntries(givenFields.map((given) => [given.fieldName, given.value])),
    ).pipe(
      Effect.mapError((error) => {
        const [issue] = formatIssue(error.issue).issues;
        const [fieldName] = issue?.path ?? [];
        const invalidField =
          givenFields.find((given) => given.fieldName === fieldName) ?? givenFields.at(0);
        return new FlagInvalid({
          readingTaskName: taskName,
          flagName: invalidField?.flagName ?? "",
          value: invalidField?.value ?? "",
          reason: issue?.message ?? error.message,
        });
      }),
    );
  });

export const readTaskFlags = <
  Flags extends Schema.ConstraintDecoder<Schema.Struct.Type<Schema.Struct.Fields>> &
    Schema.Struct<Schema.Struct.Fields>,
>(
  taskName: string,
  flags: Flags,
) => CommandLineFlags.use((values) => decodeTaskFlags(taskName, flags, values));

const describeFlag = (flag: Effect.Success<ReturnType<typeof checkDeclaredFlags>>[number]) =>
  flag.type.isBoolean
    ? `  --${flag.flagName}`
    : `  --${flag.flagName} ${flag.type.shown}${flag.isOptional ? " (optional)" : ""}`;

const describeTaskFlags = (taskFlags: Effect.Success<ReturnType<typeof checkDeclaredFlags>>) => {
  const described = taskFlags.map((flag) => ({
    usage: describeFlag(flag),
    description: flag.description,
  }));
  const usageWidth = Math.max(...described.map(({ usage }) => usage.length));
  return described.map(({ usage, description }) =>
    description === null ? usage : `${usage.padEnd(usageWidth)}  ${description}`,
  );
};

export const describeAcceptedFlags = (
  command: string,
  startTaskNames: readonly string[],
  flatFlags: Effect.Success<ReturnType<typeof checkDeclaredFlags>>,
) => {
  const startedTasks = startTaskNames.map((taskName) => `"${taskName}"`).join(", ");
  const readingTaskNames = Arrays.dedupe([
    ...startTaskNames.filter((taskName) => flatFlags.some((flag) => flag.taskName === taskName)),
    ...flatFlags.map((flag) => flag.taskName),
  ]);
  return [
    `${command} starts the ${startTaskNames.length === 1 ? "task" : "tasks"} ${startedTasks}.`,
    ...(Arrays.isArrayNonEmpty(readingTaskNames)
      ? readingTaskNames.flatMap((taskName) =>
          Arrays.prepend(
            describeTaskFlags(flatFlags.filter((flag) => flag.taskName === taskName)),
            `The task "${taskName}" reads:`,
          ),
        )
      : ["No task reads flags."]),
    "",
  ].join("\n");
};
