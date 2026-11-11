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

import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import {
  TASK_STATE_DIRECTORY_NAME,
  TASK_TRACE_LABEL_PREFIX,
} from "../../../shared/names-from-package-json.ts";
import { causeWithPrintableErrors } from "../../../shared/report-cause.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";

const NEWLINE = new TextEncoder().encode("\n");

const WORKER_BATCH_SIZE = 1000;

const WORKER_EXPORT_INTERVAL = Duration.seconds(1);

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const WORKER_JSON_END = "]}]}]}";

const isOwnedByCurrentUser = (information: FileSystem.File.Info) =>
  Option.exists(information.uid, (ownerId) => ownerId === process.getuid?.());

export class TraceFile extends Context.Service<
  TraceFile,
  Effect.Success<ReturnType<typeof makeTraceFile>>
>()(`${TASK_PACKAGE.name}/TraceFile`) {}

export const makeTraceFile = Effect.fn("makeTraceFile")(function* (input: {
  readonly traceFilePath: string;
  readonly createdBelow: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const whyIncomplete = yield* Ref.make<string | null>(null);
  const markIncomplete = (reason: string) =>
    Ref.update(whyIncomplete, (firstReason) => firstReason ?? reason);
  const traceDirectoryPath = path.dirname(input.traceFilePath);
  const findDirectoryNotOwned = (createdBelow: string) =>
    Effect.findFirst(
      path
        .relative(createdBelow, traceDirectoryPath)
        .split(path.sep)
        .map((_name, index, directoryNames) =>
          path.join(createdBelow, ...directoryNames.slice(0, index + 1)),
        ),
      (directoryPath) =>
        fileSystem.lstat(directoryPath).pipe(
          Effect.flatMap((information) => {
            const isLinkOwnedByCurrentUser =
              information.type === "SymbolicLink" && isOwnedByCurrentUser(information);
            return isLinkOwnedByCurrentUser
              ? Effect.flatMap(fileSystem.realPath(directoryPath), fileSystem.lstat)
              : Effect.succeed(information);
          }),
          Effect.map((information) => {
            const isDirectoryOwnedByCurrentUser =
              information.type === "Directory" && isOwnedByCurrentUser(information);
            return !isDirectoryOwnedByCurrentUser;
          }),
        ),
    );
  const markUnwritable = (error: PlatformError.PlatformError) =>
    markIncomplete(`${input.traceFilePath} could not be written (${error.message})`);
  const openTraceFile = (flag: FileSystem.OpenFlag) =>
    fileSystem.open(input.traceFilePath, { flag, mode: 0o600 }).pipe(
      Effect.asSome,
      Effect.catchTag("PlatformError", (error) =>
        markUnwritable(error).pipe(Effect.as(Option.none())),
      ),
    );
  const skipTraceFile = (reason: string) => markIncomplete(reason).pipe(Effect.as(Option.none()));
  const traceFileHandle = yield* fileSystem
    .makeDirectory(traceDirectoryPath, { recursive: true, mode: 0o700 })
    .pipe(
      Effect.andThen(findDirectoryNotOwned(input.createdBelow)),
      Effect.flatMap(
        Option.match({
          onNone: () => openTraceFile("ax"),
          onSome: (directoryPath) =>
            skipTraceFile(`${directoryPath} is not a directory owned by the current user`),
        }),
      ),
      Effect.catchTag("PlatformError", (error) =>
        skipTraceFile(`${traceDirectoryPath} could not be made (${error.message})`),
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
      attributes: { [`${TASK_TRACE_LABEL_PREFIX}.thread`]: "main-thread" },
    },
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
  };
});
