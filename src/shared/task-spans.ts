import { Clock, Context, Effect, Exit, Option, Tracer } from "effect";

import { TASK_TRACE_LABEL_PREFIX } from "./names-from-package-json.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const KIND_OF_ROLE = {
  body: "internal",
  cleanup: "internal",
  import: "internal",
  process: "client",
  run: "internal",
  setup: "internal",
  task: "internal",
} as const satisfies Readonly<Record<string, Tracer.SpanKind>>;

export const TASK_SPAN_ROLE_ATTRIBUTE = `${TASK_TRACE_LABEL_PREFIX}.span.role`;

export class TaskSpanAnnouncements extends Context.Service<
  TaskSpanAnnouncements,
  {
    readonly started: (span: Tracer.Span) => void;
    readonly updated: (span: Tracer.Span) => void;
    readonly ended: (span: Tracer.Span) => void;
  }
>()(`${TASK_PACKAGE.name}/TaskSpanAnnouncements`) {}

const ownedTaskSpans = new WeakMap<
  Tracer.AnySpan,
  {
    failure: { readonly errorType: string; readonly message: string } | null;
    hasEnded: boolean;
  }
>();

const announce = (tell: (announcements: TaskSpanAnnouncements["Service"]) => void) =>
  Effect.flatMap(Effect.serviceOption(TaskSpanAnnouncements), (announcements) =>
    Effect.sync(() => {
      if (Option.isSome(announcements)) {
        tell(announcements.value);
      }
    }),
  );

export const startTaskSpan = (
  role: keyof typeof KIND_OF_ROLE,
  name: string,
  options: {
    readonly parent: Tracer.AnySpan | null;
    readonly attributes?: Tracer.SpanOptionsNoTrace["attributes"];
    readonly startTime?: bigint | undefined;
  },
) =>
  Effect.gen(function* () {
    const tracer = yield* Effect.tracer;
    const startTime = options.startTime ?? (yield* Clock.currentTimeNanos);
    const span = tracer.span({
      name,
      parent: Option.fromNullishOr(options.parent),
      annotations: Context.empty(),
      links: [],
      startTime,
      kind: KIND_OF_ROLE[role],
      root: options.parent === null,
      sampled: true,
    });
    ownedTaskSpans.set(span, { failure: null, hasEnded: false });
    span.attribute(TASK_SPAN_ROLE_ATTRIBUTE, role);
    for (const [key, value] of Object.entries(options.attributes ?? {})) {
      span.attribute(key, value);
    }
    yield* announce((announcements) => {
      announcements.started(span);
    });
    return span;
  });

export const announceTaskSpanUpdate = (span: Tracer.Span) =>
  announce((announcements) => {
    announcements.updated(span);
  });

export const endTaskSpan = (span: Tracer.Span, exit: Exit.Exit<unknown, unknown> = Exit.void) =>
  Effect.gen(function* () {
    const owned = ownedTaskSpans.get(span);
    const isOwnedAndOpen = owned !== undefined && !owned.hasEnded;
    if (!isOwnedAndOpen) {
      return;
    }
    owned.hasEnded = true;
    span.end(yield* Clock.currentTimeNanos, exit);
    yield* announce((announcements) => {
      announcements.ended(span);
    });
  });

export const withTaskSpan =
  (
    role: keyof typeof KIND_OF_ROLE,
    name: string,
    attributes?: Tracer.SpanOptionsNoTrace["attributes"],
  ) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    Effect.flatMap(Effect.serviceOption(Tracer.ParentSpan), (parent) =>
      Effect.acquireUseRelease(
        startTaskSpan(role, name, { parent: Option.getOrNull(parent), attributes }),
        (span) => Effect.withParentSpan(effect, span),
        (span, exit) => endTaskSpan(span, exit),
      ),
    );

export const recordTaskSpanFailure = (
  span: Tracer.AnySpan,
  failure: { readonly errorType: string; readonly message: string },
) => {
  const owned = ownedTaskSpans.get(span);
  if (owned !== undefined) {
    owned.failure ??= failure;
  }
};

export const findTaskSpan = (span: Tracer.AnySpan) => ownedTaskSpans.get(span);
