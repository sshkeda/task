import { Effect, FileSystem, Schema } from "effect";

import { read } from "./access-files.ts";
import type { makeProjectUsingTaskFrom } from "./make-projects.ts";
import { TASK_STATE_DIRECTORY_NAME } from "./run-task-processes.ts";

const PERMISSION_BITS = 0o777;

const TASK_TRACE_LABEL_PREFIX = "sshkeda_task";

const TASK_SCOPE_NAME = "@sshkeda/task";

const TASK_SPAN_ROLE_ATTRIBUTE = `${TASK_TRACE_LABEL_PREFIX}.span.role`;

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
  kind: Schema.Finite,
  startTimeUnixNano: Schema.String,
  endTimeUnixNano: Schema.String,
  attributes: Attributes,
  events: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ name: Schema.String, timeUnixNano: Schema.String, attributes: Attributes }),
    ),
  ),
  status: Schema.Struct({ code: Schema.Finite, message: Schema.optionalKey(Schema.String) }),
  links: Schema.Array(
    Schema.Struct({ traceId: Schema.String, spanId: Schema.String, attributes: Attributes }),
  ),
});

const Scope = Schema.Struct({ name: Schema.String, version: Schema.optionalKey(Schema.String) });

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
        scopeSpans: Schema.Array(Schema.Struct({ scope: Scope, spans: Schema.Array(Span) })),
      }),
    ),
  }),
  Schema.Struct({
    resourceLogs: Schema.Array(
      Schema.Struct({
        resource: Resource,
        scopeLogs: Schema.Array(
          Schema.Struct({ scope: Scope, logRecords: Schema.Array(LogRecord) }),
        ),
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

export const readProjectRunsDirectory = (
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
  ran: { readonly standardError: string; readonly exitCode: number },
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const stateDirectory = `${project.home}/${TASK_STATE_DIRECTORY_NAME}`;
    const directory = `${stateDirectory}/runs/project`;
    const directoryModes = yield* Effect.forEach(
      [stateDirectory, `${stateDirectory}/runs`, directory],
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

const TRACE_FILE_NAME = "trace.jsonl";

const findOnly = <Found>(found: readonly Found[]) => {
  const [only, ...others] = found;
  return others.length === 0 ? only : undefined;
};

export const readTraces = (
  projectRunsDirectory: Effect.Success<ReturnType<typeof readProjectRunsDirectory>>,
) =>
  Effect.forEach(
    projectRunsDirectory.files.filter((file) => {
      const [runDirectoryName, fileName, ...deeper] = file.path.split("/");
      return runDirectoryName !== undefined && fileName === TRACE_FILE_NAME && deeper.length === 0;
    }),
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
                    scope: scope.scope,
                    role:
                      scope.scope.name === TASK_SCOPE_NAME
                        ? readAttribute(span.attributes, TASK_SPAN_ROLE_ATTRIBUTE)
                        : undefined,
                    resource: resource.resource.attributes,
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
        const userSpans = spans.filter((span) => span.role === undefined);
        const named = (name: string) => userSpans.filter((span) => span.name === name);
        const findOnlySpan = (name: string) => findOnly(named(name));
        const withRole = (role: string) => spans.filter((span) => span.role === role);
        const withTaskRole = (role: string, taskName: string) =>
          withRole(role).filter(
            (span) =>
              readAttribute(span.attributes, `${TASK_TRACE_LABEL_PREFIX}.task`) === taskName,
          );
        const findRunSpan = () => findOnly(withRole("run"));
        const findTaskSpan = (role: string, taskName: string) =>
          findOnly(withTaskRole(role, taskName));
        const findTaskEventNames = (role: string, taskName: string) =>
          withTaskRole(role, taskName).flatMap((span) =>
            (span.events ?? []).map((event) => event.name),
          );
        const findParentSpans = (span: (typeof spans)[number] | undefined): typeof spans => {
          const parent = spans.find((candidate) => candidate.spanId === span?.parentSpanId);
          return parent === undefined ? [] : [parent, ...findParentSpans(parent)];
        };
        const findChildSpans = (span: (typeof spans)[number] | undefined) =>
          spans.filter((candidate) => candidate.parentSpanId === span?.spanId);
        return {
          ...file,
          file: `${projectRunsDirectory.home}/${TASK_STATE_DIRECTORY_NAME}/runs/project/${file.path}`,
          spans,
          logs,
          named,
          findOnlySpan,
          withRole,
          findRunSpan,
          findTaskSpan,
          findTaskEventNames,
          findParentSpans,
          findChildSpans,
        };
      }),
  );

const TRACED_FAILURE_PREFIX = "failed: ";

export const readTracedFailures = (
  project: Parameters<typeof readProjectRunsDirectory>[0],
  ran: Parameters<typeof readProjectRunsDirectory>[1],
) =>
  Effect.gen(function* () {
    const traces = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
    return traces.flatMap((trace) =>
      trace.logs.flatMap((log) => {
        const body = log.body.stringValue ?? "";
        return body.startsWith(TRACED_FAILURE_PREFIX)
          ? [
              {
                taskName: readAttribute(log.attributes, `${TASK_TRACE_LABEL_PREFIX}.task`),
                failure: body.slice(TRACED_FAILURE_PREFIX.length),
              },
            ]
          : [];
      }),
    );
  });
