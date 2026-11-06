import { Array as Arrays, Cause, Formatter, Option, Predicate, Result, Schema } from "effect";

import { PrintableError } from "../lib/errors/index.ts";
import { CommandRefusal } from "./command-errors.ts";
import { findUserLocationInStack } from "./find-user-locations-in-stack-traces.ts";
import {
  CauseWithoutFailure,
  describeCauseMessage,
  ErrorStack,
  reportError,
  TaskRaisedError,
  UntaggedFailure,
} from "./report-errors.ts";
import { failureValuesOfReason } from "./select-cause-reasons.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const TaskRaisedContainer = Schema.Struct({
  raisedBy: Schema.Literal(TASK_PACKAGE.name),
  reportedErrors: Schema.NonEmptyArray(PrintableError),
});

const UserError = Schema.Struct({ name: Schema.String, message: Schema.String });

const UNTAGGED_ERROR_NAMES = new Set(["", "Error"]);

const ErrorFields = Schema.Record(Schema.String, Schema.Unknown);

const userTagOf = (failureValue: unknown) =>
  Option.flatMap(Schema.decodeUnknownOption(ErrorFields)(failureValue), ({ _tag }) =>
    Option.liftPredicate(_tag, Predicate.isString),
  );

const ErrorCausedByCommandRefusal = Schema.Struct({ cause: CommandRefusal });

const ENGINE_ERROR_NAMES = new Set(["TypeError", "ReferenceError"]);

const ENGINE_SOURCE_EXCERPT =
  /^(?<statement>[\s\S]*?) \(In '[\s\S]*'(?:, (?<valueNote>'[\s\S]*' is [\s\S]*))?\)$/u;

const withoutEngineSourceExcerpt = (errorName: string, defectMessage: string) => {
  const [, statement, valueNote] = ENGINE_ERROR_NAMES.has(errorName)
    ? (ENGINE_SOURCE_EXCERPT.exec(defectMessage) ?? [])
    : [];
  if (statement === undefined) {
    return defectMessage;
  }
  return valueNote === undefined ? statement : `${statement} (${valueNote})`;
};

const reportReason = (
  reason: Cause.Reason<unknown>,
  foreignMessagePrefix: string,
): readonly PrintableError[] => {
  if (Cause.isInterruptReason(reason)) {
    return [];
  }
  const [failureValue] = failureValuesOfReason(reason);
  const stackLocation = Option.flatMap(
    Schema.decodeUnknownOption(ErrorStack)(failureValue),
    (withStack) => Option.fromUndefinedOr(findUserLocationInStack(withStack.stack)),
  );
  const userLocation = Option.match(stackLocation, {
    onSome: (location) => ({ location }),
    onNone: () => ({}),
  });
  return Option.match(Schema.decodeUnknownOption(TaskRaisedContainer)(failureValue), {
    onSome: (container) => container.reportedErrors,
    onNone: () =>
      Option.match(Schema.decodeUnknownOption(ErrorCausedByCommandRefusal)(failureValue), {
        onSome: (causedByRefusal) => [reportError(causedByRefusal.cause)],
        onNone: () =>
          Option.match(Schema.decodeUnknownOption(TaskRaisedError)(failureValue), {
            onSome: (taskRaised) => [reportError(taskRaised)],
            onNone: () =>
              Option.match(
                Option.filter(
                  Schema.decodeUnknownOption(UserError)(failureValue),
                  (userError) =>
                    Option.isSome(userTagOf(failureValue)) ||
                    !UNTAGGED_ERROR_NAMES.has(userError.name),
                ),
                {
                  onSome: (userError) => {
                    const tag = Option.getOrElse(userTagOf(failureValue), () => userError.name);
                    const message = Cause.isDieReason(reason)
                      ? withoutEngineSourceExcerpt(userError.name, userError.message)
                      : userError.message;
                    return [
                      {
                        tag,
                        message: `${foreignMessagePrefix}${message === "" ? tag : message}`,
                        ...userLocation,
                      },
                    ];
                  },
                  onNone: () => [
                    reportError(
                      new UntaggedFailure({
                        failureMessage: `${foreignMessagePrefix}${describeCauseMessage(failureValue)}`,
                        ...userLocation,
                      }),
                    ),
                  ],
                },
              ),
          }),
      }),
  });
};

const isRangeError = Schema.is(Schema.instanceOf(RangeError));

const MOST_NESTED_PRINTED_CAUSES = 100;

const FailureWithCause = Schema.Struct({ cause: Schema.Unknown });

const EFFECT_INTERNAL_STACK_LINE = /Generator\.next|~effect\/(?:Effect|Utils)/u;

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
          ...flatChain,
          stack: chainLines
            .join("\n[cause]: ")
            .split("\n")
            .filter((line) => !EFFECT_INTERNAL_STACK_LINE.test(line))
            .join("\n"),
        }),
        Cause.reasonAnnotations(reason),
      ).reasons;
    }),
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

export const reportCause = <Failure>(cause: Cause.Cause<Failure>, foreignMessagePrefix = "") => {
  const details = Cause.pretty(causeWithPrintableErrors(cause));
  const reportedErrors = cause.reasons.flatMap((reason) =>
    reportReason(reason, foreignMessagePrefix),
  );
  return {
    failures: Arrays.isArrayNonEmpty(reportedErrors)
      ? reportedErrors
      : Arrays.of(reportError(new CauseWithoutFailure({ details }))),
    details,
  };
};
