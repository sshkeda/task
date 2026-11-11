import { dlopen, FFIType } from "bun:ffi";

import { Effect, Stream } from "effect";

import { isCommandInTerminal } from "../../../shared/run-in-terminal.ts";
import { listenToSignals } from "../../../shared/stop-signals.ts";

const C_LIBRARY_PATH = "libc.so.6";
const PR_SET_CHILD_SUBREAPER = 36;
const PRCTL_ENABLE = 1n;
const PRCTL_UNUSED_ARGUMENT = 0n;
const P_ALL = 0;
const WNOHANG = 1;
const WEXITED = 4;
const WNOWAIT = 0x01_00_00_00;
const SIGINFO_BYTES = 128;
const SIGINFO_PROCESS_ID_OFFSET = 16;

const openCLibrary = Effect.acquireRelease(
  Effect.sync(() =>
    dlopen(C_LIBRARY_PATH, {
      prctl: {
        args: [FFIType.i32, FFIType.u64, FFIType.u64, FFIType.u64, FFIType.u64],
        returns: FFIType.i32,
      },
      waitid: { args: [FFIType.i32, FFIType.i32, FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
      waitpid: { args: [FFIType.i32, FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
    }),
  ),
  (library) =>
    Effect.sync(() => {
      library.close();
    }),
).pipe(Effect.map((library) => library.symbols));

const reapExitedOrphans = (
  symbols: Effect.Success<typeof openCLibrary>,
  runProcessId: number,
): void => {
  const information = new Uint8Array(SIGINFO_BYTES);
  const peeked = symbols.waitid(P_ALL, 0, information, WEXITED | WNOHANG | WNOWAIT);
  const processId = new DataView(information.buffer).getInt32(SIGINFO_PROCESS_ID_OFFSET, true);
  const isExitedOrphan =
    peeked === 0 &&
    processId !== 0 &&
    processId !== runProcessId &&
    !isCommandInTerminal(processId);
  if (isExitedOrphan) {
    symbols.waitpid(processId, null, 0);
    reapExitedOrphans(symbols, runProcessId);
  }
};

export const adoptOrphans = Effect.gen(function* () {
  if (process.platform !== "linux") {
    return () => Effect.void;
  }
  const symbols = yield* openCLibrary;
  symbols.prctl(
    PR_SET_CHILD_SUBREAPER,
    PRCTL_ENABLE,
    PRCTL_UNUSED_ARGUMENT,
    PRCTL_UNUSED_ARGUMENT,
    PRCTL_UNUSED_ARGUMENT,
  );
  const childSignals = yield* listenToSignals(["SIGCHLD"]);
  return (runProcessId: number) =>
    Stream.fromQueue(childSignals).pipe(
      Stream.runForEach(() =>
        Effect.sync(() => {
          reapExitedOrphans(symbols, runProcessId);
        }),
      ),
      Effect.interruptible,
      Effect.forkScoped,
      Effect.asVoid,
    );
});
