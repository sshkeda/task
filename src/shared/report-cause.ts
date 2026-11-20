import { Array as Arrays, Cause, Formatter, Option, Predicate, Result, Schema } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { CommandRefusal } from "./command-errors.ts";
import { PRINTABLE_ERROR_FIELDS } from "./error-fields.ts";
import { findUserLocationInStack } from "./find-user-locations-in-stack-traces.ts";
import { joinMessages } from "./join-messages.ts";
import {
  CauseWithoutFailure,
  describeCauseMessage,
  ErrorStack,
  PlatformErrorValue,
  recapCommandOutput,
  reportError,
  restatesItsCause,
  TaskRaisedError,
  UNTAGGED_FAILURE_TAG,
} from "./report-errors.ts";
import { failureValuesOfReason } from "./select-cause-reasons.ts";
import { StandardInputNotRead } from "./standard-input-not-read.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const TaskRaisedContainer = Schema.Struct({
  raisedBy: Schema.Literal(TASK_PACKAGE.name),
  reportedErrors: Schema.NonEmptyArray(Schema.Struct(PRINTABLE_ERROR_FIELDS)),
});

const UserError = Schema.Struct({ name: Schema.String, message: Schema.String });

const UNTAGGED_ERROR_NAMES = new Set(["", "Error"]);

const ErrorFields = Schema.Record(Schema.String, Schema.Unknown);

const userTagOf = (failureValue: unknown) =>
  Option.flatMap(Schema.decodeUnknownOption(ErrorFields)(failureValue), ({ _tag }) =>
    Option.liftPredicate(_tag, Predicate.isString),
  );

const describeOwnFields = (failureValue: unknown, tag: string) => {
  const fields = Object.entries(
    Option.getOrElse(Schema.decodeUnknownOption(ErrorFields)(failureValue), () => ({})),
  ).filter(([name]) => name !== "_tag");
  return fields.length === 0 ? tag : describeCauseMessage(Object.fromEntries(fields));
};

const MOST_FOLLOWED_CAUSES = 5;

const ErrorWithCause = Schema.Struct({ cause: Schema.Unknown });

const MOST_PRINTED_INNER_ERRORS = 5;

const ErrorWithInnerErrors = Schema.Struct({ errors: Schema.Array(Schema.Unknown) });

const AggregateErrorValue = Schema.instanceOf(AggregateError);

const messageToPrint = (value: unknown, message: string) =>
  Option.match(Schema.decodeUnknownOption(TaskRaisedError)(value), {
    onNone: () => message,
    onSome: (taskRaised) => taskRaised.printedMessage ?? message,
  });

const withInnerErrors = (
  innerErrors: Option.Option<typeof ErrorWithInnerErrors.Type>,
  printedText: string,
) =>
  Option.match(innerErrors, {
    onNone: () => printedText,
    onSome: ({ errors }) => {
      const innerLines = errors.slice(0, MOST_PRINTED_INNER_ERRORS).map((innerError) =>
        Option.match(Schema.decodeUnknownOption(UserError)(innerError), {
          onSome: (userError) => {
            const innerTag = Option.getOrElse(userTagOf(innerError), () => userError.name);
            const innerMessage = messageToPrint(innerError, userError.message);
            return innerMessage === "" ? `  ${innerTag}` : `  ${innerTag}: ${innerMessage}`;
          },
          onNone: () => `  ${describeCauseMessage(innerError)}`,
        }),
      );
      const unprintedCount = errors.length - innerLines.length;
      const moreLines = unprintedCount > 0 ? [`  and ${unprintedCount} more`] : [];
      return [printedText, ...innerLines, ...moreLines].join("\n");
    },
  });

const withCauseChain = (
  failureValue: unknown,
  causePrintedText: string,
  followedValues: readonly unknown[],
): string => {
  const printedText = withInnerErrors(
    Option.flatMap(
      Schema.decodeUnknownOption(AggregateErrorValue)(failureValue),
      Schema.decodeUnknownOption(ErrorWithInnerErrors),
    ),
    causePrintedText,
  );
  const followableCause = Option.filter(
    Option.map(Schema.decodeUnknownOption(ErrorWithCause)(failureValue), ({ cause }) => cause),
    (cause) => {
      const isWithinDepth = followedValues.length <= MOST_FOLLOWED_CAUSES;
      const isUnfollowed = !followedValues.includes(cause);
      return isWithinDepth && isUnfollowed;
    },
  );
  return Option.match(
    Option.flatMap(followableCause, (cause) =>
      Option.map(Schema.decodeUnknownOption(UserError)(cause), (causeError) => ({
        cause,
        causeError,
      })),
    ),
    {
      onNone: () => printedText,
      onSome: ({ cause, causeError }) => {
        const causeTag = Option.getOrElse(userTagOf(cause), () => causeError.name);
        const causeMessage = messageToPrint(cause, causeError.message);
        const causeLine =
          causeMessage === "" ? `caused by ${causeTag}` : `caused by ${causeTag}: ${causeMessage}`;
        return withCauseChain(
          cause,
          Option.exists(
            Schema.decodeUnknownOption(PlatformErrorValue)(failureValue),
            (platformError) => restatesItsCause(platformError.reason),
          )
            ? printedText
            : `${printedText}\n${causeLine}`,
          [...followedValues, cause],
        );
      },
    },
  );
};

const ErrorCausedByCommandRefusal = Schema.Struct({ cause: CommandRefusal });

const StandardInputNotReadValue = Schema.instanceOf(StandardInputNotRead);

const reportFailureValue = (
  failed: { readonly cause?: unknown },
  fallbackTag: string,
): Arrays.NonEmptyReadonlyArray<PrintableError> => {
  const failureValue = failed.cause;
  const stackLocation = Option.flatMap(
    Schema.decodeUnknownOption(ErrorStack)(failureValue),
    (withStack) => Option.fromUndefinedOr(findUserLocationInStack(withStack.stack)),
  );
  const userLocation = Option.match(stackLocation, {
    onSome: (location) => ({ location }),
    onNone: () => ({}),
  });
  const standardInputNotRead = Option.flatMap(
    Schema.decodeUnknownOption(PlatformErrorValue)(failureValue),
    ({ reason }) => Schema.decodeUnknownOption(StandardInputNotReadValue)(reason.cause),
  );
  const container = Option.orElse(standardInputNotRead, () =>
    Schema.decodeUnknownOption(TaskRaisedContainer)(failureValue),
  );
  return Option.match(container, {
    onSome: (decoded) => decoded.reportedErrors,
    onNone: () =>
      Option.match(Schema.decodeUnknownOption(ErrorCausedByCommandRefusal)(failureValue), {
        onSome: (causedByRefusal) => Arrays.of(reportError(causedByRefusal.cause)),
        onNone: () =>
          Option.match(Schema.decodeUnknownOption(TaskRaisedError)(failureValue), {
            onSome: (taskRaised) => Arrays.of(reportError(taskRaised, recapCommandOutput(failed))),
            onNone: () => {
              const userError = Schema.decodeUnknownOption(UserError)(failureValue);
              const tag = Option.getOrElse(
                Option.filter(
                  Option.map(userError, (decoded) =>
                    Option.getOrElse(userTagOf(failureValue), () => decoded.name),
                  ),
                  (userTag) => userTag !== "",
                ),
                () => fallbackTag,
              );
              const describedByItsOwnFields = Option.filter(
                userError,
                (decoded) =>
                  Option.isSome(userTagOf(failureValue)) || !UNTAGGED_ERROR_NAMES.has(decoded.name),
              );
              const message = Option.match(describedByItsOwnFields, {
                onSome: (decoded) =>
                  decoded.message === "" ? describeOwnFields(failureValue, tag) : decoded.message,
                onNone: () => describeCauseMessage(failureValue),
              });
              return Arrays.of({
                tag,
                message: withCauseChain(failureValue, message, [failureValue]),
                ...userLocation,
              });
            },
          }),
      }),
  });
};

const reportReason = (reason: Cause.Reason<unknown>): readonly PrintableError[] => {
  if (Cause.isInterruptReason(reason)) {
    return [];
  }
  const [failureValue] = failureValuesOfReason(reason);
  return reportFailureValue({ cause: failureValue }, UNTAGGED_FAILURE_TAG);
};

const reportUnderlyingErrorsOnce = (
  wrapped: { readonly cause?: unknown },
  fallbackTag: string,
  followedCauses: readonly unknown[],
): Arrays.NonEmptyReadonlyArray<PrintableError> =>
  Option.match(
    Option.filter(Schema.decodeUnknownOption(PlatformErrorValue)(wrapped.cause), ({ reason }) => {
      const isUnfollowed = !followedCauses.includes(reason.cause);
      const pointsAtItsCause =
        reason.cause !== undefined &&
        (reason.description === undefined || restatesItsCause(reason));
      return isUnfollowed && pointsAtItsCause;
    }),
    {
      onNone: () => reportFailureValue(wrapped, fallbackTag),
      onSome: (platformError) =>
        reportUnderlyingErrorsOnce(platformError.reason, fallbackTag, [
          ...followedCauses,
          platformError,
        ]),
    },
  );

export const reportUnderlyingErrors = (
  wrapped: { readonly cause?: unknown },
  fallbackTag: string,
): Arrays.NonEmptyReadonlyArray<PrintableError> =>
  reportUnderlyingErrorsOnce(wrapped, fallbackTag, []);

export const addContext = (printable: PrintableError, addedContext: string): PrintableError => {
  const { context = [], ...described } = printable;
  return { ...described, context: [...context, addedContext] };
};

export const reportWrappedCause = (
  wrapper: {
    readonly _tag: string;
    readonly cause: unknown;
    readonly location?: string | undefined;
    readonly filePath?: string | undefined;
    readonly taskName?: string | null | undefined;
  },
  context: string,
): Arrays.NonEmptyReadonlyArray<PrintableError> => {
  const wrapperLocation = "location" in wrapper ? wrapper.location : wrapper.filePath;
  const placed = (printable: PrintableError): PrintableError => {
    const { location: causeLocation, taskName: causeTaskName, ...described } = printable;
    const location = causeLocation ?? wrapperLocation;
    const taskName = location === undefined ? (wrapper.taskName ?? causeTaskName) : undefined;
    return {
      ...described,
      ...(location === undefined ? {} : { location }),
      ...(taskName === undefined ? {} : { taskName }),
    };
  };
  return Arrays.map(reportUnderlyingErrors(wrapper, UNTAGGED_FAILURE_TAG), (printable) =>
    addContext(placed(printable), context),
  );
};

export const reportTaskText = (
  wrapper: Omit<Parameters<typeof reportWrappedCause>[0], "cause">,
  text: string,
  context: string,
): Arrays.NonEmptyReadonlyArray<PrintableError> => {
  const location = wrapper.location ?? wrapper.filePath;
  const taskName = location === undefined ? (wrapper.taskName ?? undefined) : undefined;
  return Arrays.of({
    tag: wrapper._tag,
    message: text,
    context: [context],
    ...(location === undefined ? {} : { location }),
    ...(taskName === undefined ? {} : { taskName }),
  });
};

const isRangeError = Schema.is(Schema.instanceOf(RangeError));

const MOST_NESTED_PRINTED_CAUSES = 100;

const FailureWithCause = Schema.Struct({ cause: Schema.Unknown });

const withoutCause = <Link>(link: Link) =>
  Option.match(Schema.decodeUnknownOption(ErrorFields)(link), {
    onSome: (fields) =>
      Object.fromEntries(Object.entries(fields).filter(([key]) => key !== "cause")),
    onNone: () => link,
  });

const causeChainOfReason = <Failure>(reason: Cause.Reason<Failure>) => {
  const followed = new Set();
  return Arrays.unfold(Arrays.head(failureValuesOfReason(reason)), (link) =>
    Option.map(
      Option.filter(link, (linkValue) => !followed.has(linkValue)),
      (linkValue) => {
        followed.add(linkValue);
        const cause = Option.filter(
          Option.map(
            Schema.decodeUnknownOption(FailureWithCause)(linkValue),
            (withCause) => withCause.cause,
          ),
          (causeValue) => causeValue !== undefined,
        );
        return [linkValue, cause] as const;
      },
    ),
  );
};

const withFlatDeepCauseChains = <Failure>(cause: Cause.Cause<Failure>) =>
  Cause.fromReasons(
    cause.reasons.flatMap((reason): readonly Cause.Reason<Failure>[] => {
      const chain = causeChainOfReason(reason);
      if (chain.length <= MOST_NESTED_PRINTED_CAUSES) {
        return [reason];
      }
      const chainLines = chain.map((link) =>
        Option.match(Schema.decodeUnknownOption(ErrorStack)(link), {
          onSome: (withStack) => withStack.stack,
          onNone: () =>
            Option.match(Schema.decodeUnknownOption(UserError)(link), {
              onSome: (userError) => `${userError.name}: ${userError.message}`,
              onNone: () => Formatter.format(withoutCause(link)),
            }),
        }),
      );
      const [firstLink] = chain;
      const flatChain = Option.getOrElse(Schema.decodeUnknownOption(UserError)(firstLink), () => ({
        name: "Error",
        message: Formatter.format(withoutCause(firstLink)),
      }));
      return Cause.annotate(
        Cause.die({
          name: flatChain.name,
          message: `${flatChain.message}, through a chain of ${chain.length} causes:\n${chainLines.join("\n[cause]: ")}`,
        }),
        Cause.reasonAnnotations(reason),
      ).reasons;
    }),
  );

const NamedError = Schema.Struct({
  name: Schema.String,
  message: Schema.String,
  stack: Schema.optional(Schema.String),
});

const PrintedMessage = Schema.Struct({
  ...NamedError.fields,
  raisedBy: Schema.Literal(TASK_PACKAGE.name),
  printedMessage: Schema.String,
});

const copyOfError = (
  original: typeof NamedError.Type,
  message: string,
  cause: Option.Option<unknown>,
) =>
  Object.assign(
    new Error(
      message,
      Option.match(cause, { onNone: () => ({}), onSome: (causeValue) => ({ cause: causeValue }) }),
    ),
    {
      name: original.name,
      ...(original.stack === undefined ? {} : { stack: original.stack }),
    },
  );

const copyWithPrintedMessages = (
  failureValue: unknown,
  followedValues: readonly unknown[],
): Option.Option<Error | typeof ErrorFields.Type> => {
  const unfollowedCause = Option.filter(
    Option.map(
      Schema.decodeUnknownOption(ErrorWithCause)(failureValue),
      (withCause) => withCause.cause,
    ),
    (causeValue) => !followedValues.includes(causeValue),
  );
  const printedCause = Option.flatMap(unfollowedCause, (causeValue) =>
    copyWithPrintedMessages(causeValue, [...followedValues, causeValue]),
  );
  return Option.match(Schema.decodeUnknownOption(NamedError)(failureValue), {
    onSome: (named) => {
      const printedMessage = Option.match(
        Schema.decodeUnknownOption(PrintedMessage)(failureValue),
        { onNone: () => named.message, onSome: (printed) => printed.printedMessage },
      );
      const isUnchanged = printedMessage === named.message && Option.isNone(printedCause);
      return isUnchanged
        ? Option.none()
        : Option.some(
            copyOfError(
              named,
              printedMessage,
              Option.orElse(printedCause, () => unfollowedCause),
            ),
          );
    },
    onNone: () =>
      Option.flatMap(printedCause, (printed) =>
        Option.map(Schema.decodeUnknownOption(ErrorFields)(failureValue), (fields) => ({
          ...fields,
          cause: printed,
        })),
      ),
  });
};

const withPrintedMessages = <Failure>(cause: Cause.Cause<Failure>) =>
  Cause.fromReasons(
    cause.reasons.flatMap(
      (reason): readonly Cause.Reason<Error | Failure | typeof ErrorFields.Type>[] => {
        const [failureValue] = failureValuesOfReason(reason);
        return Option.match(copyWithPrintedMessages(failureValue, [failureValue]), {
          onNone: () => [reason],
          onSome: (printedValue) =>
            Cause.annotate(
              Cause.isFailReason(reason) ? Cause.fail(printedValue) : Cause.die(printedValue),
              Cause.reasonAnnotations(reason),
            ).reasons,
        });
      },
    ),
  );

export const causeWithPrintableErrors = <Failure>(failedCause: Cause.Cause<Failure>) =>
  Result.getOrElse(
    Result.try(() => {
      const cause = withFlatDeepCauseChains(failedCause);
      Cause.pretty(cause);
      return cause;
    }),
    (formattingFailure) => {
      if (!isRangeError(formattingFailure)) {
        throw formattingFailure;
      }
      return Cause.die(
        new Error(
          Formatter.format({ cause: failedCause, formattingFailure }, { ignoreToString: true }),
        ),
      );
    },
  );

export const reportCause = <Failure>(cause: Cause.Cause<Failure>) => {
  const details = Cause.pretty(withPrintedMessages(causeWithPrintableErrors(cause)));
  const reportedErrors = cause.reasons.flatMap((reason) => reportReason(reason));
  return {
    failures: Arrays.isArrayNonEmpty(reportedErrors)
      ? reportedErrors
      : Arrays.of(reportError(new CauseWithoutFailure({ details }))),
    details,
  };
};

export const describeTraceFailure = <Failure>(cause: Cause.Cause<Failure>) => {
  const [firstPrintable] = reportCause(cause).failures;
  return { errorType: firstPrintable.tag, message: joinMessages([firstPrintable]) };
};

export const describeTraceExceptions = <Failure>(cause: Cause.Cause<Failure>) =>
  cause.reasons
    .filter((reason) => !Cause.isInterruptReason(reason))
    .map((reason) => {
      const reasonCause = Cause.fromReasons([reason]);
      const [prettyError] = Cause.prettyErrors(withPrintedMessages(reasonCause), {
        includeCauseInStack: true,
      });
      const { errorType, message } = describeTraceFailure(reasonCause);
      return { errorType, message, stacktrace: prettyError?.stack ?? "No stack trace available" };
    });
