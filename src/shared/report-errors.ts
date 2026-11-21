import {
  Formatter,
  Option,
  PlatformError,
  Predicate,
  Schema,
  SchemaTransformation,
  Struct,
} from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { cutToGraphemes } from "./cut-to-graphemes.ts";
import { TASK_ERROR_FIELDS } from "./error-fields.ts";
import { readNestedRecapLines } from "./nested-recap-lines.ts";
import { selectRecapOutputLines } from "./recap-output-lines.ts";
import { TASK_PACKAGE } from "./task-package.ts";

export const attributeUnplacedToTask = (printable: PrintableError, taskName: string) => {
  const isUnplaced = printable.location === undefined && printable.taskName === undefined;
  return isUnplaced ? { ...Struct.omit(printable, ["taskName"]), taskName } : printable;
};

export const ErrorStack = Schema.Struct({ stack: Schema.String });

const ErrorMessage = Schema.Struct({ message: Schema.String });

export const readErrorMessage = (value: unknown) =>
  Option.getOrUndefined(
    Option.map(Schema.decodeUnknownOption(ErrorMessage)(value), ({ message }) => message),
  );

export const TaskRaisedError = Schema.Struct({
  raisedBy: Schema.Literal(TASK_PACKAGE.name),
  name: Schema.String,
  message: Schema.String,
  printedMessage: Schema.optional(Schema.String),
  location: Schema.optional(Schema.String),
  filePath: Schema.optional(Schema.String),
  taskName: Schema.optional(Schema.NullOr(Schema.String)),
});

const FailedCommandOutput = Schema.TaggedStruct("CommandFailed", {
  raisedBy: Schema.Literal(TASK_PACKAGE.name),
  standardOutput: Schema.String,
  standardError: Schema.String,
});

export const recapCommandOutput = (failed: { readonly cause?: unknown }) =>
  Option.map(Schema.decodeUnknownOption(FailedCommandOutput)(failed.cause), (command) => {
    const [firstLine, ...laterLines] = selectRecapOutputLines([
      command.standardOutput,
      command.standardError,
    ]);
    const nestedRecapLines = Predicate.isObject(failed.cause)
      ? readNestedRecapLines(failed.cause)
      : [];
    return firstLine === undefined
      ? nestedRecapLines
      : [firstLine, ...nestedRecapLines, ...laterLines];
  });

const printedMessageOf = (error: typeof TaskRaisedError.Type) => {
  const message = error.printedMessage ?? error.message;
  return message === "" ? error.name : message;
};

export const reportError = (
  error: typeof TaskRaisedError.Type,
  recapped: Option.Option<readonly string[]> = Option.none(),
): PrintableError => {
  const location = error.location ?? error.filePath;
  const taskName = location === undefined ? (error.taskName ?? undefined) : undefined;
  const outputLines = Option.getOrUndefined(recapped);
  return {
    tag: error.name,
    raisedBy: error.raisedBy,
    ...(location === undefined ? {} : { location }),
    ...(taskName === undefined ? {} : { taskName }),
    message: printedMessageOf(error),
    ...(outputLines === undefined ? {} : { outputLines }),
  };
};

const ErrnoCode = Schema.Struct({
  code: Schema.String,
  errno: Schema.Int,
  syscall: Schema.String,
});

const CauseWithMessage = Schema.Struct({ message: Schema.String });

const LocatedCauseMessage = Schema.Struct({ filePath: Schema.String, message: Schema.String });

const FAILURE_VALUE_MAX_GRAPHEMES = 200;

const FormattedFailureValue = Schema.Unknown.pipe(
  Schema.decodeTo(
    Schema.String,
    SchemaTransformation.transform({
      decode: (failureValue) =>
        cutToGraphemes(Formatter.format(failureValue), FAILURE_VALUE_MAX_GRAPHEMES),
      encode: (formatted) => formatted,
    }),
  ),
);

const CauseMessage = Schema.Union([
  LocatedCauseMessage,
  Schema.Struct({ message: Schema.String }),
  Schema.String,
  FormattedFailureValue,
]);

const isLocatedCauseMessage = Schema.is(LocatedCauseMessage);

export const THROWN_VALUE_WITHOUT_MESSAGE = "a thrown value that has no message";

const withMissingErrorCode = (
  errnoCode: Option.Option<typeof ErrnoCode.Type>,
  description: string,
) =>
  Option.match(errnoCode, {
    onNone: () => description,
    onSome: (systemError) => `${description} (${systemError.code})`,
  });

const describeDecodedCause = (
  decoded: typeof CauseMessage.Type,
  errnoCode: Option.Option<typeof ErrnoCode.Type>,
) => {
  const message = Predicate.isString(decoded) ? decoded : decoded.message;
  const description = withMissingErrorCode(
    errnoCode,
    message === "" ? THROWN_VALUE_WITHOUT_MESSAGE : message,
  );
  const hasLocation = !Predicate.isString(decoded) && isLocatedCauseMessage(decoded);
  return hasLocation ? `${decoded.filePath} ${description}` : description;
};

export const restatesItsCause = (reason: PlatformError.PlatformError["reason"]) =>
  reason.description === String(reason.cause) ||
  Option.exists(
    Schema.decodeUnknownOption(CauseWithMessage)(reason.cause),
    (cause) => cause.message === reason.description,
  );

export const PlatformErrorValue = Schema.instanceOf(PlatformError.PlatformError);

const describePlatformError = (platformError: PlatformError.PlatformError) =>
  Option.getOrElse(
    Option.map(
      Option.filter(
        Schema.decodeUnknownOption(CauseWithMessage)(platformError.reason.cause),
        () =>
          platformError.reason.description === undefined || restatesItsCause(platformError.reason),
      ),
      (restatedCause) => restatedCause.message,
    ),
    () => platformError.reason.description ?? platformError.message,
  );

export const describeCauseMessage = (cause: unknown) =>
  Option.match(Schema.decodeUnknownOption(PlatformErrorValue)(cause), {
    onSome: describePlatformError,
    onNone: () =>
      Option.match(Schema.decodeUnknownOption(CauseMessage)(cause), {
        onSome: (decoded) =>
          describeDecodedCause(decoded, Schema.decodeUnknownOption(ErrnoCode)(cause)),
        onNone: () => "a failure value that cannot be formatted",
      }),
  });

export const UNTAGGED_FAILURE_TAG = "UntaggedFailure";

export class CauseWithoutFailure extends Schema.TaggedError<CauseWithoutFailure>()(
  "CauseWithoutFailure",
  { ...TASK_ERROR_FIELDS, details: Schema.String },
) {
  public override get message() {
    return "was interrupted without a failure";
  }
}
