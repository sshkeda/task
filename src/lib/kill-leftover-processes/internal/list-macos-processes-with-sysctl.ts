import { read, dlopen, FFIType } from "bun:ffi";
import { getSystemErrorName } from "node:util";

import { Effect } from "effect";

import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import {
  SystemCallFailed,
  ProcessArgumentsMalformed,
  ProcessCommandLineNotRead,
  ProcessTableNotRead,
  SystemLibraryNotLoaded,
} from "../../../shared/process-errors.ts";
import { programNameOf } from "./command-words.ts";

const SYSTEM_LIBRARY_PATH = "/usr/lib/libSystem.B.dylib";

const KERNEL_PROCESS_ID = 0;

const makeSystemCallFailed = (call: string, processId: number | null, errorNumber: number) =>
  new SystemCallFailed({
    call,
    processId,
    errorNumber,
    errorName: getSystemErrorName(-errorNumber),
  });

const readErrorNumberAt = (errorNumberAddress: bigint | number | null) =>
  errorNumberAddress === null
    ? Effect.die(
        new InternalInvariantBroken({
          area: "system calls",
          invariant: "the C library returns the address of the calling thread's errno",
        }),
      )
    : Effect.succeed(read.i32(errorNumberAddress, 0));

const CTL_KERN = 1;
const KERN_ARGMAX = 8;
const KERN_PROC = 14;
const KERN_PROC_ALL = 0;
const KERN_PROC_PID = 1;
const KERN_PROCARGS2 = 49;
const KINFO_PROC_SIZE = 648;
const KINFO_PROC_START_SECONDS_OFFSET = 0;
const KINFO_PROC_START_MICROSECONDS_OFFSET = 8;
const KINFO_PROC_FLAGS_OFFSET = 32;
const KINFO_PROC_STATUS_OFFSET = 36;
const KINFO_PROC_PROCESS_ID_OFFSET = 40;
const KINFO_PROC_COMMAND_NAME_OFFSET = 243;
const KINFO_PROC_PARENT_PROCESS_ID_OFFSET = 560;
const KINFO_PROC_PROCESS_GROUP_ID_OFFSET = 564;
const MAXCOMLEN = 16;
const SIDL = 1;
const SRUN = 2;
const SSLEEP = 3;
const SSTOP = 4;
const SZOMB = 5;
const P_WEXIT = 0x20_00;
const ENOMEM = 12;
const EINVAL = 22;
const EXECUTABLE_KEY = "executable_path=";
const SIXTY_FOUR_BIT_PROCESS_POINTER_SIZE = 8;

const processStatusNames = new Map([
  [SIDL, "created"],
  [SRUN, "running"],
  [SSLEEP, "sleeping"],
  [SSTOP, "stopped"],
  [SZOMB, "zombie"],
]);

const openSystemLibrary = (taskName: string | null) =>
  Effect.acquireRelease(
    Effect.try({
      try: () =>
        dlopen(SYSTEM_LIBRARY_PATH, {
          sysctl: {
            args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64],
            returns: FFIType.i32,
          },
          __error: { args: [], returns: FFIType.ptr },
          getsid: { args: [FFIType.i32], returns: FFIType.i32 },
        }),
      catch: (cause) =>
        new SystemLibraryNotLoaded({ taskName, libraryPath: SYSTEM_LIBRARY_PATH, cause }),
    }),
    (systemLibrary) =>
      Effect.sync(() => {
        systemLibrary.close();
      }),
  ).pipe(Effect.map((systemLibrary) => systemLibrary.symbols));

const callSysctl = (
  symbols: Effect.Success<ReturnType<typeof openSystemLibrary>>,
  managementInformationBaseName: readonly number[],
  value: Uint8Array | null,
) =>
  Effect.suspend(() => {
    const name = Int32Array.from(managementInformationBaseName);
    const valueSize = new BigUint64Array([BigInt(value === null ? 0 : value.byteLength)]);
    const returned = symbols.sysctl(name, name.length, value, valueSize, null, 0);
    return readErrorNumberAt(symbols.__error()).pipe(
      Effect.map((errorNumber) => ({
        succeeded: returned === 0,
        errorNumber,
        filledSize: Number(valueSize[0]),
      })),
    );
  });

const decodeKernelProcess = (kernelProcesses: Uint8Array, offset: number) => {
  const fields = new DataView(kernelProcesses.buffer, offset, KINFO_PROC_SIZE);
  const status = processStatusNames.get(fields.getInt8(KINFO_PROC_STATUS_OFFSET));
  const commandNameBytes = kernelProcesses.subarray(
    offset + KINFO_PROC_COMMAND_NAME_OFFSET,
    offset + KINFO_PROC_COMMAND_NAME_OFFSET + MAXCOMLEN + 1,
  );
  const commandNameEnd = commandNameBytes.indexOf(0);
  return status === undefined
    ? Effect.die(
        new InternalInvariantBroken({
          area: "macOS process listing",
          invariant: `kinfo_proc p_stat is one of the states in sys/proc.h, not ${fields.getInt8(KINFO_PROC_STATUS_OFFSET)}`,
        }),
      )
    : Effect.succeed({
        processId: fields.getInt32(KINFO_PROC_PROCESS_ID_OFFSET, true),
        parentProcessId: fields.getInt32(KINFO_PROC_PARENT_PROCESS_ID_OFFSET, true),
        processGroupId: fields.getInt32(KINFO_PROC_PROCESS_GROUP_ID_OFFSET, true),
        status,
        startTime: `${fields.getBigInt64(KINFO_PROC_START_SECONDS_OFFSET, true)}.${fields.getInt32(KINFO_PROC_START_MICROSECONDS_OFFSET, true)}`,
        isExiting: (fields.getInt32(KINFO_PROC_FLAGS_OFFSET, true) & P_WEXIT) !== 0,
        commandName: new TextDecoder().decode(
          commandNameEnd === -1 ? commandNameBytes : commandNameBytes.subarray(0, commandNameEnd),
        ),
      });
};

const readKernelProcesses = (
  symbols: Effect.Success<ReturnType<typeof openSystemLibrary>>,
  call: string,
  managementInformationBaseName: readonly number[],
): Effect.Effect<
  readonly Effect.Success<ReturnType<typeof decodeKernelProcess>>[],
  SystemCallFailed
> =>
  Effect.gen(function* () {
    const sizeQuery = yield* callSysctl(symbols, managementInformationBaseName, null);
    if (!sizeQuery.succeeded) {
      return yield* makeSystemCallFailed(call, null, sizeQuery.errorNumber);
    }
    if (sizeQuery.filledSize === 0) {
      return [];
    }
    const kernelProcesses = new Uint8Array(sizeQuery.filledSize);
    const filled = yield* callSysctl(symbols, managementInformationBaseName, kernelProcesses);
    const bufferOutgrownByNewProcesses = !filled.succeeded && filled.errorNumber === ENOMEM;
    if (bufferOutgrownByNewProcesses) {
      return yield* readKernelProcesses(symbols, call, managementInformationBaseName);
    }
    if (!filled.succeeded) {
      return yield* makeSystemCallFailed(call, null, filled.errorNumber);
    }
    return yield* Effect.forEach(
      Array.from({ length: filled.filledSize / KINFO_PROC_SIZE }, (_unused, index) => index),
      (index) => decodeKernelProcess(kernelProcesses, index * KINFO_PROC_SIZE),
    );
  });

const decodeArgumentWords = (processId: number, processArguments: Uint8Array) => {
  const argumentCount = new DataView(processArguments.buffer).getInt32(0, true);
  const executablePathStart = Int32Array.BYTES_PER_ELEMENT;
  const executablePathEnd = processArguments.indexOf(0, executablePathStart);
  const executablePrefixLength =
    EXECUTABLE_KEY.length + executablePathEnd + 1 - executablePathStart;
  const paddedExecutablePathLength =
    Math.ceil(executablePrefixLength / SIXTY_FOUR_BIT_PROCESS_POINTER_SIZE) *
      SIXTY_FOUR_BIT_PROCESS_POINTER_SIZE -
    EXECUTABLE_KEY.length;
  const argumentWords =
    executablePathEnd === -1
      ? []
      : new TextDecoder("utf-8", { ignoreBOM: true })
          .decode(processArguments.subarray(executablePathStart + paddedExecutablePathLength))
          .split("\0")
          .slice(0, -1)
          .slice(0, Math.max(argumentCount, 0));
  if (argumentWords.length < argumentCount) {
    return Effect.fail(
      new ProcessArgumentsMalformed({
        processId,
        argumentCount,
        argumentsFound: argumentWords.length,
      }),
    );
  }
  return Effect.succeed(argumentWords);
};

const readCommandLineUnlessGoneWith = (
  symbols: Effect.Success<ReturnType<typeof openSystemLibrary>>,
  processId: number,
  commandName: string,
) =>
  Effect.gen(function* () {
    const argumentMaximumValue = new Uint8Array(Int32Array.BYTES_PER_ELEMENT);
    const argumentMaximum = yield* callSysctl(
      symbols,
      [CTL_KERN, KERN_ARGMAX],
      argumentMaximumValue,
    );
    if (!argumentMaximum.succeeded) {
      return yield* makeSystemCallFailed("sysctl kern.argmax", null, argumentMaximum.errorNumber);
    }
    const processArguments = new Uint8Array(
      new DataView(argumentMaximumValue.buffer).getInt32(0, true),
    );
    const readProcessArguments = yield* callSysctl(
      symbols,
      [CTL_KERN, KERN_PROCARGS2, processId],
      processArguments,
    );
    if (readProcessArguments.succeeded) {
      return programNameOf(
        yield* decodeArgumentWords(
          processId,
          processArguments.subarray(0, readProcessArguments.filledSize),
        ),
        commandName,
      );
    }
    if (readProcessArguments.errorNumber !== EINVAL) {
      return yield* makeSystemCallFailed(
        "sysctl kern.procargs2",
        processId,
        readProcessArguments.errorNumber,
      );
    }
    const [stillListed] = yield* readKernelProcesses(symbols, "sysctl kern.proc.pid", [
      CTL_KERN,
      KERN_PROC,
      KERN_PROC_PID,
      processId,
    ]);
    const isGone =
      stillListed === undefined || stillListed.status === "zombie" || stillListed.isExiting;
    return isGone ? null : commandName;
  });

const readCommandLineUnlessGone = (
  taskName: string | null,
  processId: number,
  commandName: string,
) =>
  Effect.scoped(
    openSystemLibrary(taskName).pipe(
      Effect.flatMap((symbols) =>
        readCommandLineUnlessGoneWith(symbols, processId, commandName).pipe(
          Effect.mapError((cause) => new ProcessCommandLineNotRead({ taskName, processId, cause })),
        ),
      ),
    ),
  );

const readKernelProcessTable = (taskName: string | null) =>
  Effect.scoped(
    openSystemLibrary(taskName).pipe(
      Effect.flatMap((symbols) =>
        readKernelProcesses(symbols, "sysctl kern.proc.all", [
          CTL_KERN,
          KERN_PROC,
          KERN_PROC_ALL,
        ]).pipe(
          Effect.mapBoth({
            onSuccess: (kernelProcesses) =>
              kernelProcesses.map((kernelProcess) => ({
                ...kernelProcess,
                sessionId:
                  kernelProcess.processId === KERNEL_PROCESS_ID
                    ? KERNEL_PROCESS_ID
                    : symbols.getsid(kernelProcess.processId),
              })),
            onFailure: (cause) => new ProcessTableNotRead({ taskName, cause }),
          }),
        ),
      ),
    ),
  );

export const listProcessesWithSysctl = (taskName: string | null) =>
  readKernelProcessTable(taskName).pipe(
    Effect.map(
      (kernelProcesses) =>
        new Map(
          kernelProcesses.map((kernelProcess) => [
            kernelProcess.processId,
            {
              parentProcessId: kernelProcess.parentProcessId,
              processGroupId: kernelProcess.processGroupId,
              sessionId: kernelProcess.sessionId,
              startTime: kernelProcess.startTime,
              hasEnded: kernelProcess.status === "zombie" || kernelProcess.isExiting,
              readCommandLineUnlessGone: readCommandLineUnlessGone(
                taskName,
                kernelProcess.processId,
                kernelProcess.commandName,
              ),
            },
          ]),
        ),
    ),
  );

export const readProcessWithSysctl = (taskName: string | null, processId: number) =>
  Effect.scoped(
    openSystemLibrary(taskName).pipe(
      Effect.flatMap((symbols) =>
        readKernelProcesses(symbols, "sysctl kern.proc.pid", [
          CTL_KERN,
          KERN_PROC,
          KERN_PROC_PID,
          processId,
        ]).pipe(Effect.mapError((cause) => new ProcessTableNotRead({ taskName, cause }))),
      ),
      Effect.map(([kernelProcess]) =>
        kernelProcess === undefined
          ? null
          : {
              processGroupId: kernelProcess.processGroupId,
              startTime: kernelProcess.startTime,
              hasEnded: kernelProcess.status === "zombie" || kernelProcess.isExiting,
            },
      ),
    ),
  );
