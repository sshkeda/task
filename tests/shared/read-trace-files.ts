import { Effect, FileSystem, Schema } from "effect";

import { read } from "./access-files.ts";
import type { makeProjectUsingTaskFrom } from "./make-projects.ts";
import { TASK_STATE_DIRECTORY_NAME } from "./run-task-processes.ts";

const PERMISSION_BITS = 0o777;

const TASK_TRACE_LABEL_PREFIX = "sshkeda_task";

const Scalar = Schema.Struct({
  stringValue: Schema.optionalKey(Schema.String),
  intValue: Schema.optionalKey(Schema.Union([Schema.Finite, Schema.String])),
  doubleValue: Schema.optionalKey(Schema.Finite),
  boolValue: Schema.optionalKey(Schema.Boolean),
});

const AnyValue = Schema.Struct({
  ...Scalar.fields,
  arrayValue: Schema.optionalKey(Schema.Struct({ values: Schema.Array(Scalar) })),
});

const Attributes = Schema.Array(Schema.Struct({ key: Schema.String, value: AnyValue }));

const Span = Schema.Struct({
  traceId: Schema.String,
  spanId: Schema.String,
  parentSpanId: Schema.optionalKey(Schema.String),
  name: Schema.String,
  startTimeUnixNano: Schema.String,
  endTimeUnixNano: Schema.String,
  attributes: Attributes,
  status: Schema.Struct({ code: Schema.Finite, message: Schema.optionalKey(Schema.String) }),
});

const LogRecord = Schema.Struct({
  timeUnixNano: Schema.String,
  body: AnyValue,
  attributes: Attributes,
  traceId: Schema.optionalKey(Schema.String),
  spanId: Schema.optionalKey(Schema.String),
});

const Resource = Schema.Struct({ attributes: Attributes });

const Line = Schema.Union([
  Schema.Struct({
    resourceSpans: Schema.Array(
      Schema.Struct({
        resource: Resource,
        scopeSpans: Schema.Array(Schema.Struct({ spans: Schema.Array(Span) })),
      }),
    ),
  }),
  Schema.Struct({
    resourceLogs: Schema.Array(
      Schema.Struct({
        resource: Resource,
        scopeLogs: Schema.Array(Schema.Struct({ logRecords: Schema.Array(LogRecord) })),
      }),
    ),
  }),
]);

const readScalar = (value: typeof Scalar.Type) =>
  value.stringValue ??
  (value.intValue === undefined ? undefined : Number(value.intValue)) ??
  value.doubleValue ??
  value.boolValue;

export const readAttribute = (attributes: typeof Attributes.Type, key: string) => {
  const value = attributes.find((entry) => entry.key === key)?.value;
  if (value?.arrayValue !== undefined) {
    return value.arrayValue.values.map(readScalar);
  }
  return value === undefined ? undefined : readScalar(value);
};

export const readTraceDirectory = (
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
  ran: { readonly standardError: string; readonly exitCode: number },
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const stateDirectory = `${project.home}/${TASK_STATE_DIRECTORY_NAME}`;
    const directory = `${stateDirectory}/traces/project`;
    const directoryModes = yield* Effect.forEach(
      [stateDirectory, `${stateDirectory}/traces`, directory],
      (path) =>
        Effect.map(
          fileSystem.stat(path),
          (directoryInformation) => directoryInformation.mode & PERMISSION_BITS,
        ),
    );
    const files = yield* Effect.forEach(
      (yield* fileSystem.readDirectory(directory, { recursive: true })).toSorted(),
      (path) =>
        Effect.gen(function* () {
          const fileInformation = yield* fileSystem.stat(`${directory}/${path}`);
          if (fileInformation.type !== "File") {
            return [];
          }

          return [
            {
              path,
              mode: fileInformation.mode & PERMISSION_BITS,
              text: yield* read(`${directory}/${path}`),
            },
          ];
        }),
    );
    return {
      home: project.home,
      directory: project.directory,
      ...ran,
      directoryModes,
      files: files.flat(),
    };
  });

export const readTraces = (traceDirectory: Effect.Success<ReturnType<typeof readTraceDirectory>>) =>
  Effect.forEach(
    traceDirectory.files.filter((file) => file.path.endsWith(".jsonl")),
    (file) =>
      Effect.gen(function* () {
        const lines = yield* Effect.forEach(file.text.trimEnd().split("\n"), (line) =>
          Schema.decodeEffect(Schema.fromJsonString(Line))(line),
        );
        const spans = lines.flatMap((line) =>
          "resourceSpans" in line
            ? line.resourceSpans.flatMap((resource) =>
                resource.scopeSpans.flatMap((scope) =>
                  scope.spans.map((span) => ({
                    ...span,
                    label:
                      span.name === "executeTaskFiber"
                        ? `executeTaskFiber ${String(readAttribute(span.attributes, `${TASK_TRACE_LABEL_PREFIX}.task`))}`
                        : span.name,
                    thread: readAttribute(
                      resource.resource.attributes,
                      `${TASK_TRACE_LABEL_PREFIX}.thread`,
                    ),
                    serviceName: readAttribute(resource.resource.attributes, "service.name"),
                  })),
                ),
              )
            : [],
        );
        const logs = lines.flatMap((line) =>
          "resourceLogs" in line
            ? line.resourceLogs.flatMap((resource) =>
                resource.scopeLogs.flatMap((scope) => scope.logRecords),
              )
            : [],
        );
        const named = (label: string) => spans.filter((span) => span.label === label);
        const findOnlySpan = (label: string) => {
          const [only, ...others] = named(label);
          return others.length === 0 ? only : undefined;
        };
        const findParentSpanNames = (span: (typeof spans)[number] | undefined): string[] => {
          const parent = spans.find((candidate) => candidate.spanId === span?.parentSpanId);
          return parent === undefined ? [] : [parent.label, ...findParentSpanNames(parent)];
        };
        return {
          ...file,
          file: `${traceDirectory.home}/${TASK_STATE_DIRECTORY_NAME}/traces/project/${file.path}`,
          spans,
          logs,
          named,
          findOnlySpan,
          findParentSpanNames,
        };
      }),
  );
