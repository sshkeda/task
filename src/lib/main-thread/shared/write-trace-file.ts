import { Context, Duration, Effect, FileSystem, Option, Path, Ref, Schema, Version } from "effect";
import type { PlatformError } from "effect";
import { HttpBody, HttpClient, HttpClientResponse } from "effect/http";
import { OtlpExporter, OtlpLogger, OtlpResource } from "effect/observability";

import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import { makeTraceTracer, makeUnfinishedTaskSpanJson } from "../../../shared/make-trace-tracer.ts";
import {
  TASK_STATE_DIRECTORY_NAME,
  TASK_TRACE_LABEL_PREFIX,
} from "../../../shared/names-from-package-json.ts";
import type { RecordedTraceSpan, StartedTaskSpan } from "../../../shared/rpc.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { TraceFileIncomplete } from "./main-thread-errors.ts";

const NEWLINE = new TextEncoder().encode("\n");

const BATCH_SIZE = 1000;

const EXPORT_INTERVAL = Duration.seconds(1);

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const encodeLogRecordsNow = Schema.encodeOption(Schema.fromJsonString(Schema.Unknown));

const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

const TRACE_ID_LENGTH = 32;

const LOG_SEVERITY_NUMBER = 9;

const makeConsoleTraceLogRecord = (
  entry: {
    readonly taskName: string;
    readonly streamId: number;
    readonly traceId: string | null;
    readonly spanId: string | null;
    readonly writtenAtMilliseconds: number;
  },
  text: string,
) => {
  const timeUnixNano = (
    BigInt(entry.writtenAtMilliseconds) * NANOSECONDS_PER_MILLISECOND
  ).toString();
  const hasSpan = entry.traceId !== null && entry.spanId !== null;
  return {
    severityNumber: LOG_SEVERITY_NUMBER,
    severityText: "Info",
    timeUnixNano,
    observedTimeUnixNano: timeUnixNano,
    attributes: OtlpResource.entriesToAttributes([
      [`${TASK_TRACE_LABEL_PREFIX}.task`, entry.taskName],
      [`${TASK_TRACE_LABEL_PREFIX}.streamId`, entry.streamId],
    ]),
    body: OtlpResource.unknownToAttributeValue(text),
    droppedAttributesCount: 0,
    ...(hasSpan
      ? { traceId: entry.traceId.padStart(TRACE_ID_LENGTH, "0"), spanId: entry.spanId }
      : {}),
  };
};

const isOwnedByCurrentUser = (information: FileSystem.File.Info) =>
  Option.exists(information.uid, (ownerId) => ownerId === process.getuid?.());

export class TraceFile extends Context.Service<
  TraceFile,
  Effect.Success<ReturnType<typeof makeTraceFile>>
>()(`${TASK_PACKAGE.name}/TraceFile`) {}

export const makeTraceFile = Effect.fn(function* (input: {
  readonly traceFilePath: string;
  readonly createdBelow: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const whyIncomplete = yield* Ref.make<TraceFileIncomplete | null>(null);
  const markIncomplete = (failedStep: string, cause: PlatformError.PlatformError | null) =>
    Ref.update(
      whyIncomplete,
      (firstReason) =>
        firstReason ??
        new TraceFileIncomplete({ traceFilePath: input.traceFilePath, failedStep, cause }),
    );
  const runDirectoryPath = path.dirname(input.traceFilePath);
  const findDirectoryNotOwned = (createdBelow: string) =>
    Effect.findFirst(
      path
        .relative(createdBelow, runDirectoryPath)
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
    markIncomplete(`could not write ${input.traceFilePath}`, error);
  const openTraceFile = (flag: FileSystem.OpenFlag) =>
    fileSystem.open(input.traceFilePath, { flag, mode: 0o600 }).pipe(
      Effect.asSome,
      Effect.catchTag("PlatformError", (error) =>
        markUnwritable(error).pipe(Effect.as(Option.none())),
      ),
    );
  const skipTraceFile = (failedStep: string, cause: PlatformError.PlatformError | null) =>
    markIncomplete(failedStep, cause).pipe(Effect.as(Option.none()));
  const traceFileHandle = yield* fileSystem
    .makeDirectory(runDirectoryPath, { recursive: true, mode: 0o700 })
    .pipe(
      Effect.andThen(findDirectoryNotOwned(input.createdBelow)),
      Effect.flatMap(
        Option.match({
          onNone: () => openTraceFile("ax"),
          onSome: (directoryPath) =>
            skipTraceFile(`${directoryPath} is not a directory owned by the current user`, null),
        }),
      ),
      Effect.catchTag("PlatformError", (error) =>
        skipTraceFile(`could not make ${runDirectoryPath}`, error),
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
      attributes: {
        [`${TASK_TRACE_LABEL_PREFIX}.thread`]: "main-thread",
        "process.runtime.name": "bun",
        "process.runtime.version": Bun.version,
      },
    },
    shutdownTimeout: Duration.infinity,
  };
  const encodeResourceJson = (thread: "main-thread" | "worker") =>
    OtlpResource.fromConfig({
      ...otlpOptions.resource,
      attributes: {
        ...otlpOptions.resource.attributes,
        [`${TASK_TRACE_LABEL_PREFIX}.thread`]: thread,
      },
    }).pipe(Effect.flatMap(encodeJson));
  const [mainThreadResourceJson, workerResourceJson, userScopeJson, taskScopeJson] =
    yield* Effect.all([
      encodeResourceJson("main-thread"),
      encodeResourceJson("worker"),
      encodeJson({ name: "effect", version: Version.getCurrentVersion() }),
      encodeJson({ name: TASK_PACKAGE.name, version: TASK_PACKAGE.version }),
    ]).pipe(Effect.catchTag("SchemaError", (error) => Effect.die(error)));
  const makeSpansJson = (
    resourceJson: string,
    recordedSpans: readonly (typeof RecordedTraceSpan.Type)[],
  ) => {
    const scopeSpans = [
      { scopeJson: userScopeJson, isTaskSpan: false },
      { scopeJson: taskScopeJson, isTaskSpan: true },
    ].flatMap(({ scopeJson, isTaskSpan }) => {
      const spanJsons = recordedSpans
        .filter((recorded) => recorded.isTaskSpan === isTaskSpan)
        .map((recorded) => recorded.spanJson);
      return spanJsons.length === 0
        ? []
        : [`{"scope":${scopeJson},"spans":[${spanJsons.join(",")}]}`];
    });
    return `{"resourceSpans":[{"resource":${resourceJson},"scopeSpans":[${scopeSpans.join(",")}]}]}`;
  };
  const makeSpansExporter = (resourceJson: string) =>
    OtlpExporter.make({
      label: "OtlpTracer",
      url: otlpOptions.url,
      headers: undefined,
      exportInterval: EXPORT_INTERVAL,
      maxBatchSize: BATCH_SIZE,
      body: (recordedSpans: (typeof RecordedTraceSpan.Type)[]) => [
        HttpBody.text(makeSpansJson(resourceJson, recordedSpans), "application/json"),
        Effect.void,
      ],
      shutdownTimeout: Duration.infinity,
    });
  const exporters = yield* Effect.all({
    mainThreadSpans: makeSpansExporter(mainThreadResourceJson),
    workerSpans: makeSpansExporter(workerResourceJson),
    logger: OtlpLogger.make(otlpOptions),
    workerLogRecords: OtlpExporter.make({
      label: "OtlpLogger",
      url: otlpOptions.url,
      headers: undefined,
      exportInterval: EXPORT_INTERVAL,
      maxBatchSize: BATCH_SIZE,
      body: (logRecordJsons: string[]) => [
        HttpBody.text(
          `{"resourceLogs":[{"resource":${workerResourceJson},"scopeLogs":[{"scope":${userScopeJson},"logRecords":[${logRecordJsons.join(",")}]}]}]}`,
          "application/json",
        ),
        Effect.void,
      ],
      shutdownTimeout: Duration.infinity,
    }),
    consoleLogRecords: OtlpExporter.make({
      label: "OtlpLogger",
      url: otlpOptions.url,
      headers: undefined,
      exportInterval: EXPORT_INTERVAL,
      maxBatchSize: BATCH_SIZE,
      body: (logRecords: ReturnType<typeof makeConsoleTraceLogRecord>[]) =>
        Option.match(encodeLogRecordsNow(logRecords), {
          onNone: () => [
            HttpBody.empty,
            Effect.die(
              new InternalInvariantBroken({
                area: "trace export",
                invariant: "console log records encode as JSON",
              }),
            ),
          ],
          onSome: (logRecordsJson) => [
            HttpBody.text(
              `{"resourceLogs":[{"resource":${workerResourceJson},"scopeLogs":[{"scope":${userScopeJson},"logRecords":${logRecordsJson}}]}]}`,
              "application/json",
            ),
            Effect.void,
          ],
        }),
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
  const openWorkerTaskSpans = new Map<
    string,
    { readonly workerId: number; readonly started: typeof StartedTaskSpan.Type }
  >();
  const workerEndings = new Map<
    number,
    {
      readonly endTime: bigint;
      readonly failure: { readonly errorType: string; readonly message: string };
    }
  >();
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const open of openWorkerTaskSpans.values()) {
        const ending = workerEndings.get(open.workerId);
        const spanJson =
          ending === undefined
            ? undefined
            : makeUnfinishedTaskSpanJson(open.started, ending.endTime, ending.failure);
        if (spanJson !== undefined) {
          exporters.workerSpans.push({ spanJson, isTaskSpan: true });
        }
      }
    }),
  );
  return {
    filePath: input.traceFilePath,
    tracer: makeTraceTracer((recorded) => {
      exporters.mainThreadSpans.push(recorded);
    }),
    logger: exporters.logger,
    writeWorkerLogRecord: (logRecordJson: string) => {
      exporters.workerLogRecords.push(logRecordJson);
    },
    writeConsoleLogRecord: (...described: Parameters<typeof makeConsoleTraceLogRecord>) => {
      exporters.consoleLogRecords.push(makeConsoleTraceLogRecord(...described));
    },
    writeWorkerSpan: (recordedSpan: typeof RecordedTraceSpan.Type) => {
      openWorkerTaskSpans.delete(recordedSpan.spanId);
      exporters.workerSpans.push(recordedSpan);
    },
    recordWorkerTaskSpanStart: (workerId: number, started: typeof StartedTaskSpan.Type) => {
      openWorkerTaskSpans.set(started.spanId, { workerId, started });
    },
    recordWorkerTaskSpanUpdate: (updated: typeof StartedTaskSpan.Type) => {
      const open = openWorkerTaskSpans.get(updated.spanId);
      if (open !== undefined) {
        openWorkerTaskSpans.set(updated.spanId, { workerId: open.workerId, started: updated });
      }
    },
    recordWorkerTaskSpanEnd: (spanId: string) => {
      openWorkerTaskSpans.delete(spanId);
    },
    recordWorkerEnded: (
      workerId: number,
      endTime: bigint,
      failure: { readonly errorType: string; readonly message: string },
    ) => {
      workerEndings.set(workerId, { endTime, failure });
    },
    whyIncomplete: Ref.get(whyIncomplete),
  };
});
