import type { Cause } from "effect";
import { Array as Arrays, Clock, Logger, Option, References } from "effect";
import { OtlpResource } from "effect/observability";

import { encodeTraceJson } from "../../../shared/encode-trace-json.ts";
import { describeTraceExceptions } from "../../../shared/report-cause.ts";
import { TaskEvent } from "../../../shared/rpc.ts";

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

const describeExceptions = (cause: Cause.Cause<unknown>) => {
  const exceptions = describeTraceExceptions(cause);
  const [firstException] = exceptions;
  return firstException === undefined
    ? {}
    : {
        "exception.type": firstException.errorType,
        "exception.message": firstException.message,
        "exception.stacktrace": exceptions.map((exception) => exception.stacktrace).join("\n"),
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
        ...describeExceptions(options.cause),
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

export const makeTraceOnlyLogger = (
  reportToMainThreadNow: (event: typeof TaskEvent.Type) => void,
) =>
  Logger.make((logEntry) => {
    const traceLogRecordJson = makeTraceLogRecordJson(logEntry);
    if (traceLogRecordJson !== undefined) {
      reportToMainThreadNow(TaskEvent.cases.TraceLogRecordRequested.make({ traceLogRecordJson }));
    }
  });
