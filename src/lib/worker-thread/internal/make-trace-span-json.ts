import { Cause, Exit, Option } from "effect";
import type { Tracer } from "effect";
import { OtlpResource } from "effect/observability";

import { encodeTraceJson } from "../shared/encode-trace-json.ts";

const TRACE_ID_LENGTH = 32;

const STATUS_CODES = { Unset: 0, Ok: 1, Error: 2 } as const;

const SPAN_KINDS = {
  unspecified: 0,
  internal: 1,
  server: 2,
  client: 3,
  producer: 4,
  consumer: 5,
} as const;

const describeExit = (exit: Exit.Exit<unknown, unknown>, endTimeUnixNano: string) => {
  if (Exit.isSuccess(exit)) {
    return { status: { code: STATUS_CODES.Ok }, attributes: [], events: [] };
  }
  if (Cause.hasInterruptsOnly(exit.cause)) {
    return {
      status: { code: STATUS_CODES.Unset },
      attributes: [{ key: "effect.fiber.interrupted", value: { boolValue: true } }],
      events: [],
    };
  }
  const errors = Cause.prettyErrors(exit.cause, { includeCauseInStack: true });
  const [firstError] = errors;
  return {
    status:
      firstError === undefined
        ? { code: STATUS_CODES.Ok }
        : { code: STATUS_CODES.Error, message: firstError.message },
    attributes: [],
    events: errors.map((error) => ({
      name: "exception",
      timeUnixNano: endTimeUnixNano,
      droppedAttributesCount: 0,
      attributes: [
        { key: "exception.type", value: { stringValue: error.name } },
        { key: "exception.message", value: { stringValue: error.message } },
        {
          key: "exception.stacktrace",
          value: { stringValue: error.stack ?? "No stack trace available" },
        },
      ],
    })),
  };
};

export const makeTraceSpanJson = (
  span: Tracer.NativeSpan,
  endTime: bigint,
  exit: Exit.Exit<unknown, unknown>,
) => {
  const endTimeUnixNano = String(endTime);
  const ended = describeExit(exit, endTimeUnixNano);
  return Option.getOrUndefined(
    encodeTraceJson({
      traceId: span.traceId.padStart(TRACE_ID_LENGTH, "0"),
      spanId: span.spanId,
      parentSpanId: Option.getOrUndefined(span.parent)?.spanId,
      name: span.name,
      kind: SPAN_KINDS[span.kind],
      startTimeUnixNano: String(span.startTime),
      endTimeUnixNano,
      attributes: [
        ...(span._attributes === undefined
          ? []
          : OtlpResource.entriesToAttributes(span._attributes)),
        ...ended.attributes,
      ],
      droppedAttributesCount: 0,
      events: [
        ...(span._events ?? []).map(([name, startTime, attributes]) => ({
          name,
          timeUnixNano: String(startTime),
          attributes: OtlpResource.entriesToAttributes(Object.entries(attributes)),
          droppedAttributesCount: 0,
        })),
        ...ended.events,
      ],
      droppedEventsCount: 0,
      status: ended.status,
      links: span.links.map((link) => ({
        traceId: link.span.traceId.padStart(TRACE_ID_LENGTH, "0"),
        spanId: link.span.spanId,
        attributes: OtlpResource.entriesToAttributes(Object.entries(link.attributes)),
        droppedAttributesCount: 0,
      })),
      droppedLinksCount: 0,
    }),
  );
};
