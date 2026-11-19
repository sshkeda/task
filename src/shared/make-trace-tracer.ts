import { Cause, Context, Exit, Option, Schema, Tracer } from "effect";
import { OtlpResource } from "effect/observability";

import { encodeTraceJson } from "./encode-trace-json.ts";
import {
  causeWithPrintableErrors,
  describeTraceExceptions,
  describeTraceFailure,
} from "./report-cause.ts";
import type { RecordedTraceSpan, StartedTaskSpan } from "./rpc.ts";
import { findTaskSpan } from "./task-spans.ts";

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

const describeUserSpanExit = (exit: Exit.Exit<unknown, unknown>, endTimeUnixNano: string) => {
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
  const exceptions = describeTraceExceptions(exit.cause);
  const [firstException] = exceptions;
  return {
    status:
      firstException === undefined
        ? { code: STATUS_CODES.Ok }
        : { code: STATUS_CODES.Error, message: firstException.message },
    attributes: [],
    events: exceptions.map((exception) => ({
      name: "exception",
      timeUnixNano: endTimeUnixNano,
      droppedAttributesCount: 0,
      attributes: [
        { key: "exception.type", value: { stringValue: exception.errorType } },
        { key: "exception.message", value: { stringValue: exception.message } },
        { key: "exception.stacktrace", value: { stringValue: exception.stacktrace } },
      ],
    })),
  };
};

const describeTaskSpanFailure = (
  failure: { readonly errorType: string; readonly message: string } | null,
  exit: Exit.Exit<unknown, unknown>,
) => {
  if (failure !== null) {
    return failure;
  }
  const endedWithoutFailure = Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause);
  if (endedWithoutFailure) {
    return null;
  }
  return describeTraceFailure(exit.cause);
};

const describeTaskSpanExit = (
  failure: { readonly errorType: string; readonly message: string } | null,
  exit: Exit.Exit<unknown, unknown>,
) => {
  const ended = describeTaskSpanFailure(failure, exit);
  return ended === null
    ? { status: { code: STATUS_CODES.Unset }, attributes: [], events: [] }
    : {
        status: { code: STATUS_CODES.Error, message: ended.message },
        attributes: [{ key: "error.type", value: { stringValue: ended.errorType } }],
        events: [],
      };
};

const makeTraceSpanJson = (
  span: Tracer.NativeSpan,
  endTimeUnixNano: string,
  ended: ReturnType<typeof describeTaskSpanExit> | ReturnType<typeof describeUserSpanExit>,
) =>
  Option.getOrUndefined(
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

const makeEndedSpanJson = (
  span: Tracer.NativeSpan,
  endTime: bigint,
  exit: Exit.Exit<unknown, unknown>,
) => {
  const endTimeUnixNano = String(endTime);
  const taskSpan = findTaskSpan(span);
  return makeTraceSpanJson(
    span,
    endTimeUnixNano,
    taskSpan === undefined
      ? describeUserSpanExit(exit, endTimeUnixNano)
      : describeTaskSpanExit(taskSpan.failure, exit),
  );
};

const decodeScalarAttribute = Schema.decodeUnknownOption(
  Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]),
);

export const describeStartedTaskSpan = (span: Tracer.Span): typeof StartedTaskSpan.Type => ({
  traceId: span.traceId,
  spanId: span.spanId,
  parentSpanId: Option.getOrNull(Option.map(span.parent, (parent) => parent.spanId)),
  name: span.name,
  kind: span.kind,
  startTimeUnixNano: String(span.status.startTime),
  attributes: [...span.attributes].flatMap(([key, value]) =>
    Option.match(decodeScalarAttribute(value), {
      onNone: () => [],
      onSome: (scalar) => [[key, scalar] as const],
    }),
  ),
});

export const makeUnfinishedTaskSpanJson = (
  started: typeof StartedTaskSpan.Type,
  endTime: bigint,
  failure: { readonly errorType: string; readonly message: string },
) => {
  const span = new Tracer.NativeSpan({
    name: started.name,
    parent:
      started.parentSpanId === null
        ? Option.none()
        : Option.some(
            Tracer.externalSpan({ traceId: started.traceId, spanId: started.parentSpanId }),
          ),
    annotations: Context.empty(),
    links: [],
    startTime: BigInt(started.startTimeUnixNano),
    kind: started.kind,
    sampled: true,
  });
  span._traceId = started.traceId;
  span._spanId = started.spanId;
  for (const [key, value] of started.attributes) {
    span.attribute(key, value);
  }
  return makeTraceSpanJson(span, String(endTime), describeTaskSpanExit(failure, Exit.void));
};

export const makeTraceTracer = (
  recordSpan: (recorded: typeof RecordedTraceSpan.Type) => void,
): Tracer.Tracer =>
  Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      const ending = { hasEnded: false };
      span.end = (endTime, exit) => {
        const isExported = span.sampled && !ending.hasEnded;
        ending.hasEnded = true;
        end(endTime, exit);
        const spanJson = isExported
          ? makeEndedSpanJson(
              span,
              endTime,
              Exit.isFailure(exit) ? Exit.failCause(causeWithPrintableErrors(exit.cause)) : exit,
            )
          : undefined;
        if (spanJson !== undefined) {
          recordSpan({
            spanJson,
            spanId: span.spanId,
            isTaskSpan: findTaskSpan(span) !== undefined,
          });
        }
      };
      return span;
    },
  });
