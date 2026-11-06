import {
  Context,
  Duration,
  Effect,
  Exit,
  FileSystem,
  Option,
  Path,
  Ref,
  Schema,
  Tracer,
  Version,
} from "effect";
import type { PlatformError } from "effect";
import { HttpBody, HttpClient, HttpClientResponse } from "effect/http";
import { OtlpExporter, OtlpLogger, OtlpResource, OtlpTracer } from "effect/observability";

import { InternalInvariantBroken } from "./invariant-errors.ts";
import { TASK_STATE_DIRECTORY_NAME, TASK_TRACE_LABEL_PREFIX } from "./names-from-package-json.ts";
import { causeWithPrintableErrors } from "./report-cause.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const NEWLINE = new TextEncoder().encode("\n");

const WORKER_BATCH_SIZE = 1000;

const WORKER_EXPORT_INTERVAL = Duration.seconds(1);

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const WORKER_JSON_END = "]}]}]}";

export class TraceFile extends Context.Service<
  TraceFile,
  Effect.Success<ReturnType<typeof makeTraceFile>>
>()(`${TASK_PACKAGE.name}/TraceFile`) {}

export const makeTraceFile = Effect.fn("makeTraceFile")(function* (input: {
  readonly traceFilePath: string;
  readonly thread: "main-thread" | "worker";
  readonly maxBatchSize?: number;
  readonly onIncomplete?: (reason: string) => Effect.Effect<void>;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const whyIncomplete = yield* Ref.make<string | null>(null);
  const markIncomplete = (reason: string) =>
    Ref.modify(whyIncomplete, (firstReason) =>
      firstReason === null ? [true, reason] : [false, firstReason],
    ).pipe(
      Effect.andThen((isFirstReason) =>
        isFirstReason ? (input.onIncomplete?.(reason) ?? Effect.void) : Effect.void,
      ),
    );
  yield* fileSystem
    .makeDirectory(path.dirname(input.traceFilePath), { recursive: true })
    .pipe(
      Effect.catchTag("PlatformError", (error) =>
        markIncomplete(`${path.dirname(input.traceFilePath)} could not be made (${error.message})`),
      ),
    );
  const markUnwritable = (error: PlatformError.PlatformError) =>
    markIncomplete(`${input.traceFilePath} could not be written (${error.message})`);
  const traceFileHandle = yield* fileSystem
    .open(input.traceFilePath, { flag: "a", mode: 0o600 })
    .pipe(
      Effect.asSome,
      Effect.catchTag("PlatformError", (error) =>
        markUnwritable(error).pipe(Effect.as(Option.none())),
      ),
    );
  const appendLine = (line: Uint8Array) =>
    Option.match(traceFileHandle, {
      onNone: () => Effect.void,
      onSome: (handle) => {
        const lineWithNewline = new Uint8Array(Bun.concatArrayBuffers([line, NEWLINE]));
        return handle
          .writeAll(lineWithNewline)
          .pipe(Effect.catchTag("PlatformError", (error) => markUnwritable(error)));
      },
    });
  const otlpOptions = {
    url: `${TASK_STATE_DIRECTORY_NAME.slice(1)}:trace-file`,
    resource: {
      serviceName: TASK_PACKAGE.name,
      serviceVersion: TASK_PACKAGE.version,
      attributes: { [`${TASK_TRACE_LABEL_PREFIX}.thread`]: input.thread },
    },
    maxBatchSize: input.maxBatchSize,
    shutdownTimeout: Duration.infinity,
  };
  const workerResource = yield* OtlpResource.fromConfig({
    ...otlpOptions.resource,
    attributes: { [`${TASK_TRACE_LABEL_PREFIX}.thread`]: "worker" },
  });
  const [workerLogRecordsJsonStart, workerSpansJsonStart] = yield* Effect.all([
    encodeJson(workerResource),
    encodeJson({ name: "effect", version: Version.getCurrentVersion() }),
  ]).pipe(
    Effect.map(([resourceJson, scopeJson]) => [
      `{"resourceLogs":[{"resource":${resourceJson},"scopeLogs":[{"scope":${scopeJson},"logRecords":[`,
      `{"resourceSpans":[{"resource":${resourceJson},"scopeSpans":[{"scope":${scopeJson},"spans":[`,
    ]),
    Effect.catchTag("SchemaError", (error) => Effect.die(error)),
  );
  const exporters = yield* Effect.all({
    tracer: OtlpTracer.make(otlpOptions),
    logger: OtlpLogger.make(otlpOptions),
    workerLogRecords: OtlpExporter.make({
      label: "OtlpLogger",
      url: otlpOptions.url,
      headers: undefined,
      exportInterval: WORKER_EXPORT_INTERVAL,
      maxBatchSize: WORKER_BATCH_SIZE,
      body: (logRecordJsons: string[]) => [
        HttpBody.text(
          `${workerLogRecordsJsonStart}${logRecordJsons.join(",")}${WORKER_JSON_END}`,
          "application/json",
        ),
        Effect.void,
      ],
      shutdownTimeout: Duration.infinity,
    }),
    workerSpans: OtlpExporter.make({
      label: "OtlpTracer",
      url: otlpOptions.url,
      headers: undefined,
      exportInterval: WORKER_EXPORT_INTERVAL,
      maxBatchSize: WORKER_BATCH_SIZE,
      body: (spanJsons: string[]) => [
        HttpBody.text(
          `${workerSpansJsonStart}${spanJsons.join(",")}${WORKER_JSON_END}`,
          "application/json",
        ),
        Effect.void,
      ],
      shutdownTimeout: Duration.infinity,
    }),
  }).pipe(
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        request.body instanceof HttpBody.Uint8Array
          ? appendLine(request.body.body).pipe(
              Effect.as(HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))),
            )
          : Effect.die(
              new InternalInvariantBroken({
                area: "trace export",
                invariant: `the trace file cannot hold a ${request.body._tag} body`,
              }),
            ),
      ),
    ),
  );
  return {
    filePath: input.traceFilePath,
    tracer: Tracer.make({
      span: (options) => {
        const span = exporters.tracer.span(options);
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(
            endTime,
            Exit.isFailure(exit) ? Exit.failCause(causeWithPrintableErrors(exit.cause)) : exit,
          );
        };
        return span;
      },
      context: exporters.tracer.context,
    }),
    logger: exporters.logger,
    writeWorkerLogRecord: (logRecordJson: string) => {
      exporters.workerLogRecords.push(logRecordJson);
    },
    writeWorkerSpan: (spanJson: string) => {
      exporters.workerSpans.push(spanJson);
    },
    whyIncomplete: Ref.get(whyIncomplete),
    markIncomplete,
  };
});
