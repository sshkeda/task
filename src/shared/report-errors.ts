import { Formatter, Option, PlatformError, Predicate, Schema, SchemaTransformation } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { cutToGraphemes } from "./cut-to-graphemes.ts";
import { TASK_ERROR_FIELDS } from "./error-fields.ts";
import { TASK_PACKAGE } from "./task-package.ts";

export const withLocationOrElse = (printable: PrintableError, fallbackLocation: string) => {
  const { location, ...printableWithoutLocation } = printable;
  return { ...printableWithoutLocation, location: location ?? fallbackLocation };
};

export const ErrorStack = Schema.Struct({ stack: Schema.String });

export const TaskRaisedError = Schema.Struct({
  raisedBy: Schema.Literal(TASK_PACKAGE.name),
  name: Schema.String,
  message: Schema.String,
  messageBeforeOutput: Schema.optional(Schema.String),
  location: Schema.optional(Schema.String),
  filePath: Schema.optional(Schema.String),
  taskName: Schema.optional(Schema.NullOr(Schema.String)),
});

export const reportError = (error: typeof TaskRaisedError.Type): PrintableError => {
  const location = error.location ?? error.filePath ?? error.taskName ?? undefined;
  return {
    tag: error.name,
    ...(location === undefined ? {} : { location }),
    message: error.message === "" ? error.name : error.message,
    ...(error.messageBeforeOutput === undefined
      ? {}
      : { messageBeforeOutput: error.messageBeforeOutput }),
  };
};

const ErrnoCode = Schema.Struct({
  code: Schema.String.check(Schema.isPattern(/^E[A-Z]+$/u)),
});

const SystemErrorMessage = Schema.Struct({ ...ErrnoCode.fields, message: Schema.String });

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

const describeDecodedCause = (decoded: typeof CauseMessage.Type) => {
  const message = Predicate.isString(decoded) ? decoded : decoded.message;
  const description = message === "" ? THROWN_VALUE_WITHOUT_MESSAGE : message;
  const hasLocation = !Predicate.isString(decoded) && isLocatedCauseMessage(decoded);
  return hasLocation ? `${decoded.filePath} ${description}` : description;
};

const restatesItsCause = (reason: PlatformError.PlatformError["reason"]) =>
  reason.description === String(reason.cause) ||
  Option.exists(
    Schema.decodeUnknownOption(Schema.Struct({ message: Schema.String }))(reason.cause),
    (cause) => cause.message === reason.description,
  );

const describePlatformError = (platformError: PlatformError.PlatformError) =>
  Option.getOrElse(
    Option.map(
      Option.filter(
        Schema.decodeUnknownOption(SystemErrorMessage)(platformError.reason.cause),
        () =>
          platformError.reason.description === undefined || restatesItsCause(platformError.reason),
      ),
      (systemError) => systemError.message,
    ),
    () => platformError.reason.description ?? platformError.message,
  );

export const describeCauseMessage = (cause: unknown) =>
  Option.match(Schema.decodeUnknownOption(Schema.instanceOf(PlatformError.PlatformError))(cause), {
    onSome: describePlatformError,
    onNone: () =>
      Option.match(Schema.decodeUnknownOption(CauseMessage)(cause), {
        onSome: describeDecodedCause,
        onNone: () => "a failure value that cannot be formatted",
      }),
  });

export const UNTAGGED_FAILURE_TAG = "UntaggedFailure";

export class UntaggedFailure extends Schema.TaggedError<UntaggedFailure>()(UNTAGGED_FAILURE_TAG, {
  ...TASK_ERROR_FIELDS,
  failureMessage: Schema.String,
  location: Schema.optional(Schema.String),
}) {
  public override get message() {
    return this.failureMessage;
  }
}

export class CauseWithoutFailure extends Schema.TaggedError<CauseWithoutFailure>()(
  "CauseWithoutFailure",
  { ...TASK_ERROR_FIELDS, details: Schema.String },
) {
  public override get message() {
    return "was interrupted without a failure";
  }
}
