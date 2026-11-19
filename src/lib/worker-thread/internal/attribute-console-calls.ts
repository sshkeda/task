import { AsyncLocalStorage } from "node:async_hooks";
import { Console } from "node:console";
import { EventEmitter } from "node:events";

import { Context, Effect, Fiber, Option, Tracer } from "effect";

import { splitIntoPieces, startWithNoLineOpen } from "../../../shared/label-output.ts";
import { TaskLines } from "../../../shared/mark-task-worker-thread-contexts.ts";
import { takeOutputStreamId } from "../../../shared/output-stream-ids.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";

const UNOWNED_WRITES = {
  standardOutput: console.log,
  standardError: console.error,
} as const;

const writeUnowned = (write: (text: string) => void) => (text: string) => {
  write(text.endsWith("\n") ? text.slice(0, -1) : text);
};

const writeTo = (print: (text: string) => void) => {
  const stream = Object.assign(new EventEmitter(), {
    writable: true,
    write: (chunk: Uint8Array | string) => {
      print(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString());
      return true;
    },
    end: () => stream,
  });
  return stream;
};

const readSpanIdsAsStrings = Option.liftThrowable((span: Tracer.AnySpan) => {
  const { traceId, spanId } = span;
  const areBothStrings = typeof traceId === "string" && typeof spanId === "string";
  return areBothStrings ? Option.some({ traceId, spanId }) : Option.none();
});

const spansBeingRead = new Set<Tracer.AnySpan>();

const readSpanIdsOf = (span: Tracer.AnySpan) => {
  const isAnotherReadInProgress = spansBeingRead.size > 0;
  if (isAnotherReadInProgress) {
    return Option.none();
  }
  spansBeingRead.add(span);
  try {
    return Option.flatten(readSpanIdsAsStrings(span));
  } finally {
    spansBeingRead.delete(span);
  }
};

const readSpanIdsOnce = (context: Context.Context<never>) =>
  Option.flatMap(
    Option.fromNullishOr(Context.getOrUndefined(context, Tracer.ParentSpan)),
    readSpanIdsOf,
  );

const readSpanIds = (context: Context.Context<never>) =>
  Option.getOrUndefined(readSpanIdsOnce(context));

const makeConsoleOwner = (
  streamIds: { readonly standardOutput: number; readonly standardError: number },
  launchContext: Context.Context<never>,
  hasEnded: () => boolean,
) => {
  const call = { context: Option.none<Context.Context<never>>() };
  const { postStreamOutputNow } = Context.get(launchContext, TaskLines);
  const printAsLines = (streamId: number, printUnowned: (text: string) => void) => {
    const lines = { state: startWithNoLineOpen() };
    return (text: string) => {
      if (hasEnded()) {
        printUnowned(text);
        return;
      }
      const [nextState, pieces] = splitIntoPieces(lines.state, text);
      lines.state = nextState;
      if (pieces.length === 0) {
        return;
      }
      const context = Option.getOrElse(call.context, () => launchContext);
      postStreamOutputNow(streamId, pieces, readSpanIds(context));
    };
  };
  const ownConsole = new Console({
    stdout: writeTo(
      printAsLines(streamIds.standardOutput, writeUnowned(UNOWNED_WRITES.standardOutput)),
    ),
    stderr: writeTo(
      printAsLines(streamIds.standardError, writeUnowned(UNOWNED_WRITES.standardError)),
    ),
  });
  return { ownConsole, launchContext, hasEnded, call };
};

const CurrentConsoleOwner = Context.Reference<ReturnType<typeof makeConsoleOwner> | null>(
  `${TASK_PACKAGE.name}/CurrentConsoleOwner`,
  { defaultValue: () => null },
);

const consoleOwners = new AsyncLocalStorage<ReturnType<typeof makeConsoleOwner>>();

const findConsoleOwner = () => {
  const fiber = Fiber.getCurrent();
  const fiberOwner = fiber?.getRef(CurrentConsoleOwner) ?? null;
  const isOwnedByTheFiber = fiber !== undefined && fiberOwner !== null;
  if (isOwnedByTheFiber) {
    return { owner: fiberOwner, context: fiber.context };
  }
  const continuedOwner = consoleOwners.getStore();
  return continuedOwner === undefined
    ? null
    : { owner: continuedOwner, context: continuedOwner.launchContext };
};

const replaceMethod = <Values extends readonly unknown[]>(
  originalMethod: (...values: Values) => void,
  callOwnMethod: (ownConsole: Console, values: Values) => void,
  replace: (replacement: (...values: Values) => void) => void,
) => {
  replace((...values) => {
    const found = findConsoleOwner();
    if (found === null) {
      originalMethod(...values);
      return;
    }
    const { owner } = found;
    const outerContext = owner.call.context;
    owner.call.context = Option.some(found.context);
    try {
      callOwnMethod(owner.ownConsole, values);
    } finally {
      owner.call.context = outerContext;
    }
  });
};

export const attributeConsoleCalls = () => {
  replaceMethod(
    console.log,
    (ownConsole, values) => {
      ownConsole.log.apply(ownConsole, values);
    },
    (replacement) => {
      console.log = replacement;
    },
  );
  replaceMethod(
    console.warn,
    (ownConsole, values) => {
      ownConsole.warn.apply(ownConsole, values);
    },
    (replacement) => {
      console.warn = replacement;
    },
  );
  replaceMethod(
    console.error,
    (ownConsole, values) => {
      ownConsole.error.apply(ownConsole, values);
    },
    (replacement) => {
      console.error = replacement;
    },
  );
  replaceMethod(
    console.info,
    (ownConsole, values) => {
      ownConsole.info.apply(ownConsole, values);
    },
    (replacement) => {
      console.info = replacement;
    },
  );
  replaceMethod(
    console.debug,
    (ownConsole, values) => {
      ownConsole.debug.apply(ownConsole, values);
    },
    (replacement) => {
      console.debug = replacement;
    },
  );
  replaceMethod(
    console.dir,
    (ownConsole, values) => {
      ownConsole.dir.apply(ownConsole, values);
    },
    (replacement) => {
      console.dir = replacement;
    },
  );
  replaceMethod(
    console.dirxml,
    (ownConsole, values) => {
      ownConsole.dirxml.apply(ownConsole, values);
    },
    (replacement) => {
      console.dirxml = replacement;
    },
  );
  replaceMethod(
    console.table,
    (ownConsole, values) => {
      ownConsole.table.apply(ownConsole, values);
    },
    (replacement) => {
      console.table = replacement;
    },
  );
  replaceMethod(
    console.trace,
    (ownConsole, values) => {
      ownConsole.trace.apply(ownConsole, values);
    },
    (replacement) => {
      console.trace = replacement;
    },
  );
  replaceMethod(
    console.assert,
    (ownConsole, values) => {
      ownConsole.assert.apply(ownConsole, values);
    },
    (replacement) => {
      console.assert = replacement;
    },
  );
  replaceMethod(
    console.clear,
    (ownConsole, values) => {
      ownConsole.clear.apply(ownConsole, values);
    },
    (replacement) => {
      console.clear = replacement;
    },
  );
  replaceMethod(
    console.count,
    (ownConsole, values) => {
      ownConsole.count.apply(ownConsole, values);
    },
    (replacement) => {
      console.count = replacement;
    },
  );
  replaceMethod(
    console.countReset,
    (ownConsole, values) => {
      ownConsole.countReset.apply(ownConsole, values);
    },
    (replacement) => {
      console.countReset = replacement;
    },
  );
  replaceMethod(
    console.group,
    (ownConsole, values) => {
      ownConsole.group.apply(ownConsole, values);
    },
    (replacement) => {
      console.group = replacement;
    },
  );
  replaceMethod(
    console.groupCollapsed,
    (ownConsole, values) => {
      ownConsole.groupCollapsed.apply(ownConsole, values);
    },
    (replacement) => {
      console.groupCollapsed = replacement;
    },
  );
  replaceMethod(
    console.groupEnd,
    (ownConsole, values) => {
      ownConsole.groupEnd.apply(ownConsole, values);
    },
    (replacement) => {
      console.groupEnd = replacement;
    },
  );
  replaceMethod(
    console.time,
    (ownConsole, values) => {
      ownConsole.time.apply(ownConsole, values);
    },
    (replacement) => {
      console.time = replacement;
    },
  );
  replaceMethod(
    console.timeEnd,
    (ownConsole, values) => {
      ownConsole.timeEnd.apply(ownConsole, values);
    },
    (replacement) => {
      console.timeEnd = replacement;
    },
  );
  replaceMethod(
    console.timeLog,
    (ownConsole, values) => {
      ownConsole.timeLog.apply(ownConsole, values);
    },
    (replacement) => {
      console.timeLog = replacement;
    },
  );
};

export const ownConsoleCalls =
  (hasEnded: () => boolean) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    Effect.gen(function* () {
      const streamIds = {
        standardOutput: yield* takeOutputStreamId,
        standardError: yield* takeOutputStreamId,
      };
      const launchContext = yield* Effect.context<Services>();
      const owner = makeConsoleOwner(streamIds, launchContext, hasEnded);
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.flatMap(
          Effect.sync(() =>
            consoleOwners.run(owner, () =>
              Effect.runForkWith(Context.add(launchContext, CurrentConsoleOwner, owner))(
                restore(effect),
                { uninterruptible: true },
              ),
            ),
          ),
          (ownedFiber) =>
            Effect.interruptible(Fiber.join(ownedFiber)).pipe(
              Effect.onInterrupt(() =>
                Effect.andThen(Fiber.interrupt(ownedFiber), Fiber.join(ownedFiber)),
              ),
            ),
        ),
      );
    });
