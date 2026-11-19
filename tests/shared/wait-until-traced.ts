import { dlopen, FFIType, ptr } from "bun:ffi";

import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as BunServices from "@effect/platform-bun/BunServices";
import { Deferred, Effect, Exit, FileSystem, Queue, Stream } from "effect";
import type { PlatformError } from "effect";
import { Argument, Command } from "effect/cli";

const KEVENT_BYTES = 32;
const KEVENT_FILTER_OFFSET = 8;
const KEVENT_FLAGS_OFFSET = 10;
const KEVENT_FILTER_FLAGS_OFFSET = 12;
const EVFILT_VNODE = -4;
const EV_ADD = 0x1;
const EV_CLEAR = 0x20;
const NOTE_WRITE = 0x2;
const NOTE_EXTEND = 0x4;
const O_EVTONLY = 0x80_00;

const TRACE_FILE_NAME = "trace.jsonl";

const watchWithKqueue = Effect.sync(() => {
  const library = dlopen("/usr/lib/libSystem.B.dylib", {
    open: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
    kqueue: { args: [], returns: FFIType.i32 },
    kevent: {
      args: [FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.ptr],
      returns: FFIType.i32,
    },
  });
  const queue = library.symbols.kqueue();
  const received = new Uint8Array(KEVENT_BYTES);
  return {
    watch: (path: string) =>
      Effect.sync(() => {
        const change = new DataView(new ArrayBuffer(KEVENT_BYTES));
        const opened = library.symbols.open(ptr(new TextEncoder().encode(`${path}\0`)), O_EVTONLY);
        change.setBigUint64(0, BigInt(opened), true);
        change.setInt16(KEVENT_FILTER_OFFSET, EVFILT_VNODE, true);
        change.setUint16(KEVENT_FLAGS_OFFSET, EV_ADD | EV_CLEAR, true);
        change.setUint32(KEVENT_FILTER_FLAGS_OFFSET, NOTE_WRITE | NOTE_EXTEND, true);
        library.symbols.kevent(queue, ptr(change), 1, null, 0, null);
      }),
    waitForChange: Effect.sync(() => {
      library.symbols.kevent(queue, null, 0, ptr(received), 1, null);
    }),
  };
});

const watchWithFileSystem = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const changes = yield* Queue.unbounded<FileSystem.WatchEvent, PlatformError.PlatformError>();
  return {
    watch: (path: string) =>
      Effect.gen(function* () {
        const watching = yield* Deferred.make<void, PlatformError.PlatformError>();
        yield* fileSystem
          .watch(path, {
            onReady: () => {
              Deferred.doneUnsafe(watching, Exit.void);
            },
          })
          .pipe(
            Stream.runForEach((change) => Queue.offer(changes, change)),
            Effect.tapCause((cause) =>
              Effect.andThen(Deferred.failCause(watching, cause), Queue.failCause(changes, cause)),
            ),
            Effect.forkScoped,
          );
        yield* Deferred.await(watching);
      }),
    waitForChange: Queue.take(changes),
  };
});

const waitUntilTraced = Effect.fn("waitUntilTraced")(function* ({
  directory,
  text,
}: {
  readonly directory: string;
  readonly text: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const { watch, waitForChange } = yield* process.platform === "darwin"
    ? watchWithKqueue
    : watchWithFileSystem;
  const watched = new Set<string>();

  const watchOnce = (path: string) =>
    watched.has(path)
      ? Effect.void
      : watch(path).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              watched.add(path);
            }),
          ),
        );

  yield* watchOnce(directory);
  const tracesIncludeTextOrWaitForChange = Effect.gen(function* () {
    const runDirectories = (yield* fileSystem.readDirectory(directory)).map(
      (runDirectoryName) => `${directory}/${runDirectoryName}`,
    );
    yield* Effect.forEach(runDirectories, watchOnce, { discard: true });
    const traces = yield* Effect.filter(
      runDirectories.map((runDirectory) => `${runDirectory}/${TRACE_FILE_NAME}`),
      (trace) => fileSystem.exists(trace),
    );
    yield* Effect.forEach(traces, watchOnce, { discard: true });
    const texts = yield* Effect.forEach(traces, (trace) => fileSystem.readFileString(trace));
    if (texts.some((traced) => traced.includes(text))) {
      return true;
    }
    yield* waitForChange;
    return false;
  });
  yield* Stream.fromEffectRepeat(tracesIncludeTextOrWaitForChange).pipe(
    Stream.takeUntil((found) => found),
    Stream.runDrain,
  );
}, Effect.scoped);

const program = Command.make(
  "wait-until-traced",
  { directory: Argument.String("directory"), text: Argument.String("text") },
  waitUntilTraced,
).pipe(Command.run({ version: "0.0.0" }));

BunRuntime.runMain(program.pipe(Effect.provide(BunServices.layer)));
