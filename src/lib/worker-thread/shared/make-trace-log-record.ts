import { Array as Arrays, Cause, Clock, Option, References } from "effect";
import type { Logger } from "effect";
import { OtlpResource } from "effect/observability";

import { encodeTraceJson } from "./encode-trace-json.ts";

const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

const TRACE_ID_LENGTH = 32;

const SEVERITY_NUMBERS = {
  All: 0,
  Trace: 1,
  Debug: 5,
  Info: 9,
  Warn: 13,
  Error: 17,
  Fatal: 21,
  None: 0,
} as const;

const describeErrors = (errors: readonly Error[]) => {
  const [firstError] = errors;
  return firstError === undefined
    ? {}
    : {
        "exception.type": firstError.name,
        "exception.message": firstError.message,
        "exception.stacktrace": errors.map((error) => error.stack).join("\n"),
      };
};

export const makeTraceLogRecordJson = (options: Logger.Options<unknown>) => {
  const eventMilliseconds = options.date.getTime();
  const message = Arrays.ensure(options.message);
  const span = options.fiber.cache.span;
  const logRecord = {
    severityNumber: SEVERITY_NUMBERS[options.logLevel],
    severityText: options.logLevel,
    timeUnixNano: (BigInt(eventMilliseconds) * NANOSECONDS_PER_MILLISECOND).toString(),
    observedTimeUnixNano: options.fiber.getRef(Clock.Clock).currentTimeNanosUnsafe().toString(),
    attributes: OtlpResource.entriesToAttributes(
      Object.entries({
        ...options.fiber.getRef(References.CurrentLogAnnotations),
        "effect.fiberId": options.fiber.id,
        ...Object.fromEntries(
          options.fiber
            .getRef(References.CurrentLogSpans)
            .map(([label, startTime]) => [
              `effect.log_span.${label}`,
              eventMilliseconds - startTime,
            ]),
        ),
        ...describeErrors(Cause.prettyErrors(options.cause, { includeCauseInStack: true })),
      }),
    ),
    body: OtlpResource.unknownToAttributeValue(message.length === 1 ? message[0] : message),
    droppedAttributesCount: 0,
    ...(span === undefined
      ? {}
      : { traceId: span.traceId.padStart(TRACE_ID_LENGTH, "0"), spanId: span.spanId }),
  };
  return Option.getOrUndefined(encodeTraceJson(logRecord));
};
