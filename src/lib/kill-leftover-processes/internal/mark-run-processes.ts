import { dlopen, FFIType, toArrayBuffer } from "bun:ffi";
import type { FFIFunction } from "bun:ffi";
import { getSystemErrorName } from "node:util";

import { Effect, FileSystem, Schema } from "effect";
import type { PlatformError } from "effect";
import { SecureRandom } from "effect/crypto";

import { processIdentityKey } from "../../../shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import {
  ProcRunMarkerUnreadable,
  RunMarkerNotCreated,
  SystemCallFailed,
  SystemLibraryNotLoaded,
} from "../../../shared/process-errors.ts";
import {
  availableParallelism,
  RUN_MARKERS_VARIABLE_NAME,
} from "../../../shared/read-environment-config.ts";
import { findTaskOfMarkedCommand, RunMarker } from "../../../shared/run-marker.ts";
import { decodeProcessArguments } from "./decode-process-arguments.ts";
import {
  readProcessFromProcDirectory,
  readProcFileUnlessGone,
} from "./list-linux-processes-from-proc.ts";
import { readProcessWithSysctl } from "./list-macos-processes-with-sysctl.ts";
import { readProcessUnlessGone } from "./list-processes.ts";
import { sendSignal } from "./send-signal.ts";

const MACOS_SYSTEM_LIBRARY_PATH = "/usr/lib/libSystem.B.dylib";
const LINUX_C_LIBRARY_PATH = "libc.so.6";
const PROC_DIRECTORY_PATH = "/proc";
const LINUX_PIPE_LINK_PREFIX = "pipe:";

const SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR = 3;

const CTL_KERN = 1;
const KERN_ARGMAX = 8;
const KERN_PROCARGS2 = 49;
const PROC_PIDLISTFDS = 1;
const PROC_PIDFDPIPEINFO = 6;
const PROX_FDTYPE_PIPE = 6;
const PROC_FDINFO_BYTES = 8;
const PROC_FDINFO_TYPE_OFFSET = 4;
const PIPE_FDINFO_BYTES = 184;
const PIPE_FDINFO_HANDLE_OFFSET = 160;
const PIPE_READ_END = 0;
const PIPE_WRITE_END = 1;
const PIPE_END_COUNT = 2;
const NO_FLAVOR_ARGUMENT = 0n;
const NO_ERROR_NUMBER = 0;
const CALLER_PROCESS_GROUP_ID = 0;
const ESRCH = 3;
const EBADF = 9;
const ARGUMENT_MAXIMUM_NAME = [CTL_KERN, KERN_ARGMAX];
const TOKEN_READ_CHUNK_BYTES = 4096;

const isMarkedPlatform = process.platform === "darwin" || process.platform === "linux";

const PIPE_SYMBOLS = {
  pipe: { args: [FFIType.ptr], returns: FFIType.i32 },
  close: { args: [FFIType.i32], returns: FFIType.i32 },
  read: { args: [FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
  write: { args: [FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
} as const satisfies Record<string, FFIFunction>;

const MACOS_PROCESS_SYMBOLS = {
  __error: { args: [], returns: FFIType.ptr },
  proc_pidinfo: {
    args: [FFIType.i32, FFIType.i32, FFIType.u64, FFIType.ptr, FFIType.i32],
    returns: FFIType.i32,
  },
  proc_pidfdinfo: {
    args: [FFIType.i32, FFIType.i32, FFIType.i32, FFIType.ptr, FFIType.i32],
    returns: FFIType.i32,
  },
  sysctl: {
    args: [FFIType.ptr, FFIType.u32, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.u64],
    returns: FFIType.i32,
  },
} as const satisfies Record<string, FFIFunction>;

const LINUX_ERROR_NUMBER_SYMBOLS = {
  __errno_location: { args: [], returns: FFIType.ptr },
} as const satisfies Record<string, FFIFunction>;

export const MarkerReading = Schema.TaggedUnion({
  Marked: { ownerTaskName: Schema.NullOr(Schema.String) },
  Unmarked: {},
  Unreadable: { cause: Schema.Defect() },
});

const UNMARKED = MarkerReading.cases.Unmarked.make({});

const MICROSECONDS_PER_SECOND = 1_000_000n;
const START_TIME_FRACTION_SEPARATOR = ".";

const startTimeValue = (startTime: string) => {
  const [seconds = "0", fraction = "0"] = startTime.split(START_TIME_FRACTION_SEPARATOR);
  return BigInt(seconds) * MICROSECONDS_PER_SECOND + BigInt(fraction);
};

const MARKER_REREADS_OF_A_LIVE_PROCESS = 1;

const readMarkersOfCandidates = <Failure, Services, ProcessFailure, ProcessServices>(
  candidates: readonly (typeof ProcessIdentity.Type)[],
  runStartTime: string | null,
  readMarker: (processId: number) => Effect.Effect<typeof MarkerReading.Type, Failure, Services>,
  readProcessNow: (
    processId: number,
  ) => Effect.Effect<{ readonly hasEnded: boolean } | null, ProcessFailure, ProcessServices>,
  concurrency: number,
) => {
  const settleReading = (
    processId: number,
    reading: typeof MarkerReading.Type,
    rereadsLeft: number,
  ): Effect.Effect<
    typeof MarkerReading.Type,
    Failure | ProcessFailure,
    ProcessServices | Services
  > =>
    MarkerReading.guards.Unreadable(reading)
      ? Effect.flatMap(readProcessNow(processId), (processNow) => {
          const isEnding = processNow === null || processNow.hasEnded;
          if (isEnding) {
            return Effect.succeed(UNMARKED);
          }
          return rereadsLeft > 0
            ? Effect.flatMap(readMarker(processId), (reread) =>
                settleReading(processId, reread, rereadsLeft - 1),
              )
            : Effect.succeed(reading);
        })
      : Effect.succeed(reading);
  return Effect.forEach(
    candidates,
    ({ processId, startTime }) => {
      const startedBeforeTheRun =
        runStartTime !== null && startTimeValue(startTime) < startTimeValue(runStartTime);
      return startedBeforeTheRun
        ? Effect.succeed([processId, UNMARKED] as const)
        : readMarker(processId).pipe(
            Effect.flatMap((reading) =>
              settleReading(processId, reading, MARKER_REREADS_OF_A_LIVE_PROCESS),
            ),
            Effect.map((reading) => [processId, reading] as const),
          );
    },
    { concurrency },
  );
};

const openLibrary = <Symbols extends Record<string, FFIFunction>>(
  libraryPath: string,
  symbols: Symbols,
) =>
  Effect.acquireRelease(
    Effect.try({
      try: () => dlopen(libraryPath, symbols),
      catch: (cause) => new SystemLibraryNotLoaded({ taskName: null, libraryPath, cause }),
    }),
    (library) =>
      Effect.sync(() => {
        library.close();
      }),
  ).pipe(Effect.map((library) => library.symbols));

const openMacOsLibrary = openLibrary(MACOS_SYSTEM_LIBRARY_PATH, {
  ...PIPE_SYMBOLS,
  ...MACOS_PROCESS_SYMBOLS,
});

const openPipeLibrary =
  process.platform === "darwin"
    ? openMacOsLibrary
    : openLibrary(LINUX_C_LIBRARY_PATH, { ...PIPE_SYMBOLS, ...LINUX_ERROR_NUMBER_SYMBOLS });

const callReadingErrorNumber = (
  symbols: Effect.Success<typeof openPipeLibrary>,
  call: () => bigint | number,
) => {
  const errorNumberAddress = "__error" in symbols ? symbols.__error() : symbols.__errno_location();
  if (errorNumberAddress === null) {
    return Effect.die(
      new InternalInvariantBroken({
        area: "system calls",
        invariant: "the C library returns the address of the calling thread's errno",
      }),
    );
  }
  const errorNumber = new Int32Array(
    toArrayBuffer(errorNumberAddress, 0, Int32Array.BYTES_PER_ELEMENT),
  );
  errorNumber.fill(NO_ERROR_NUMBER);
  const returned = Number(call());
  return Effect.succeed({ returned, errorNumber: errorNumber[0] ?? NO_ERROR_NUMBER });
};

const systemCallFailed = (call: string, processId: number | null, errorNumber: number) =>
  new SystemCallFailed({
    call,
    processId,
    errorNumber,
    errorName: errorNumber === NO_ERROR_NUMBER ? "no errno" : getSystemErrorName(-errorNumber),
  });

const readMacOsPipeHandle = (
  symbols: Effect.Success<typeof openMacOsLibrary>,
  processId: number,
  fileDescriptor: number,
) => {
  const pipeInformation = new Uint8Array(PIPE_FDINFO_BYTES);
  return callReadingErrorNumber(symbols, () =>
    symbols.proc_pidfdinfo(
      processId,
      fileDescriptor,
      PROC_PIDFDPIPEINFO,
      pipeInformation,
      PIPE_FDINFO_BYTES,
    ),
  ).pipe(
    Effect.map(({ returned, errorNumber }) => {
      if (returned === PIPE_FDINFO_BYTES) {
        return {
          handle: String(
            new DataView(pipeInformation.buffer).getBigUint64(PIPE_FDINFO_HANDLE_OFFSET, true),
          ),
          cause: null,
        };
      }
      const isClosedOrGone = errorNumber === EBADF || errorNumber === ESRCH;
      return {
        handle: null,
        cause: isClosedOrGone
          ? null
          : systemCallFailed("proc_pidfdinfo PROC_PIDFDPIPEINFO", processId, errorNumber),
      };
    }),
  );
};

const readProcFile = <Success>(
  filePath: string,
  readFile: (filePath: string) => Effect.Effect<Success, PlatformError.PlatformError>,
) =>
  readProcFileUnlessGone(readFile(filePath)).pipe(
    Effect.mapError(
      (cause) =>
        new ProcRunMarkerUnreadable({ taskName: null, filePath, whatIsWrong: null, cause }),
    ),
  );

const readLinuxPipeLink = (
  fileSystem: FileSystem.FileSystem,
  processId: number,
  fileDescriptor: string,
) =>
  readProcFile(
    `${PROC_DIRECTORY_PATH}/${processId}/fd/${fileDescriptor}`,
    fileSystem.readLink,
  ).pipe(
    Effect.map(({ content, hiddenBy }) => ({
      link: content?.startsWith(LINUX_PIPE_LINK_PREFIX) === true ? content : null,
      hiddenBy,
    })),
  );

const readOwnPipeIdentity = (
  fileDescriptor: number,
): Effect.Effect<
  string | null,
  ProcRunMarkerUnreadable | SystemLibraryNotLoaded,
  FileSystem.FileSystem
> =>
  process.platform === "darwin"
    ? Effect.scoped(
        openMacOsLibrary.pipe(
          Effect.flatMap((symbols) => readMacOsPipeHandle(symbols, process.pid, fileDescriptor)),
          Effect.map(({ handle }) => handle),
        ),
      )
    : FileSystem.FileSystem.use((fileSystem) =>
        readLinuxPipeLink(fileSystem, process.pid, String(fileDescriptor)).pipe(
          Effect.map(({ link }) => link),
        ),
      );

const requireOwnPipeIdentity = (fileDescriptor: number) =>
  readOwnPipeIdentity(fileDescriptor).pipe(
    Effect.filterOrFail(
      (identity) => identity !== null,
      () =>
        new ProcRunMarkerUnreadable({
          taskName: null,
          filePath: `/dev/fd/${fileDescriptor}`,
          whatIsWrong: "it is not an open pipe",
          cause: null,
        }),
    ),
  );

const markerOf = (fileDescriptor: number, tokens: readonly string[]) => {
  const token = tokens.at(-1);
  return token === undefined
    ? null
    : RunMarker.make({ fileDescriptor, token, environmentValue: tokens.join(" ") });
};

export const createRunMarker = (inheritedTokens: readonly string[]) =>
  isMarkedPlatform
    ? Effect.scoped(
        Effect.gen(function* () {
          const secureRandom = yield* SecureRandom.SecureRandom;
          const tokens = [...inheritedTokens, yield* secureRandom.uuidV4()];
          const symbols = yield* openPipeLibrary;
          const pipeEnds = new Int32Array(PIPE_END_COUNT);
          const piped = yield* callReadingErrorNumber(symbols, () => symbols.pipe(pipeEnds));
          if (piped.returned !== 0) {
            return yield* new RunMarkerNotCreated({
              taskName: null,
              cause: systemCallFailed("pipe", null, piped.errorNumber),
            });
          }
          const readEnd = pipeEnds[PIPE_READ_END] ?? -1;
          const writeEnd = pipeEnds[PIPE_WRITE_END] ?? -1;
          const tokenBytes = new TextEncoder().encode(tokens.join(" "));
          const written = yield* callReadingErrorNumber(symbols, () =>
            symbols.write(writeEnd, tokenBytes, tokenBytes.byteLength),
          );
          symbols.close(writeEnd);
          if (written.returned !== tokenBytes.byteLength) {
            symbols.close(readEnd);
            return yield* new RunMarkerNotCreated({
              taskName: null,
              cause:
                written.returned < 0
                  ? systemCallFailed("write", null, written.errorNumber)
                  : `write put ${written.returned} of the marker's ${tokenBytes.byteLength} bytes into the pipe`,
            });
          }
          return markerOf(readEnd, tokens);
        }),
      )
    : Effect.succeed(null);

const readUntilTheEnd = (
  symbols: Effect.Success<typeof openPipeLibrary>,
  fileDescriptor: number,
  readSoFar = "",
): string | null => {
  const chunk = new Uint8Array(TOKEN_READ_CHUNK_BYTES);
  const filled = Number(symbols.read(fileDescriptor, chunk, chunk.byteLength));
  if (filled < 0) {
    return null;
  }
  return filled > 0
    ? readUntilTheEnd(
        symbols,
        fileDescriptor,
        readSoFar + new TextDecoder().decode(chunk.subarray(0, filled)),
      )
    : readSoFar;
};

export const adoptSupervisorRunMarker = isMarkedPlatform
  ? Effect.scoped(
      Effect.gen(function* () {
        yield* requireOwnPipeIdentity(SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR);
        const symbols = yield* openPipeLibrary;
        const tokens = readUntilTheEnd(symbols, SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR);
        if (tokens === null) {
          return yield* new ProcRunMarkerUnreadable({
            taskName: null,
            filePath: `/dev/fd/${SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR}`,
            whatIsWrong: "read failed",
            cause: null,
          });
        }
        return markerOf(
          SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR,
          tokens.split(" ").filter((token) => token !== ""),
        );
      }),
    )
  : Effect.succeed(null);

const readEnvironmentMarker = (
  marker: typeof RunMarker.Type,
  environmentEntries: readonly string[],
) => {
  const variablePrefix = `${RUN_MARKERS_VARIABLE_NAME}=`;
  const markerTokens = environmentEntries
    .filter((entry) => entry.startsWith(variablePrefix))
    .map((entry) => entry.slice(variablePrefix.length).split(" "))
    .find((tokens) => tokens.includes(marker.token));
  return markerTokens === undefined
    ? UNMARKED
    : MarkerReading.cases.Marked.make({
        ownerTaskName: findTaskOfMarkedCommand(marker, markerTokens),
      });
};

const combineReadings = (
  environmentReading: typeof MarkerReading.Type,
  pipeReading: typeof MarkerReading.Type,
) => {
  const isDecidedByThePipe =
    MarkerReading.guards.Marked(pipeReading) ||
    !MarkerReading.guards.Unreadable(environmentReading);
  return isDecidedByThePipe ? pipeReading : environmentReading;
};

const findMacOsMarkerPipe = (
  symbols: Effect.Success<typeof openMacOsLibrary>,
  processId: number,
  pipeIdentity: string,
): Effect.Effect<typeof MarkerReading.Type> =>
  Effect.gen(function* () {
    const sized = yield* callReadingErrorNumber(symbols, () =>
      symbols.proc_pidinfo(processId, PROC_PIDLISTFDS, NO_FLAVOR_ARGUMENT, null, 0),
    );
    if (sized.returned <= 0) {
      return MarkerReading.cases.Unreadable.make({
        cause: systemCallFailed("proc_pidinfo PROC_PIDLISTFDS", processId, sized.errorNumber),
      });
    }
    const fileDescriptors = new Uint8Array(sized.returned);
    const filled = yield* callReadingErrorNumber(symbols, () =>
      symbols.proc_pidinfo(
        processId,
        PROC_PIDLISTFDS,
        NO_FLAVOR_ARGUMENT,
        fileDescriptors,
        fileDescriptors.byteLength,
      ),
    );
    const listingFailed = filled.returned <= 0 && filled.errorNumber !== NO_ERROR_NUMBER;
    if (listingFailed) {
      return MarkerReading.cases.Unreadable.make({
        cause: systemCallFailed("proc_pidinfo PROC_PIDLISTFDS", processId, filled.errorNumber),
      });
    }
    if (filled.returned >= fileDescriptors.byteLength) {
      return yield* findMacOsMarkerPipe(symbols, processId, pipeIdentity);
    }
    const fields = new DataView(fileDescriptors.buffer);
    const pipeHandles = yield* Effect.forEach(
      Array.from(
        { length: Math.max(filled.returned, 0) / PROC_FDINFO_BYTES },
        (_unused, index) => index * PROC_FDINFO_BYTES,
      ).filter(
        (offset) => fields.getUint32(offset + PROC_FDINFO_TYPE_OFFSET, true) === PROX_FDTYPE_PIPE,
      ),
      (offset) => readMacOsPipeHandle(symbols, processId, fields.getInt32(offset, true)),
    );
    if (pipeHandles.some(({ handle }) => handle === pipeIdentity)) {
      return MarkerReading.cases.Marked.make({ ownerTaskName: null });
    }
    const unreadablePipeCause = pipeHandles.find(({ cause }) => cause !== null)?.cause ?? null;
    return unreadablePipeCause === null
      ? UNMARKED
      : MarkerReading.cases.Unreadable.make({ cause: unreadablePipeCause });
  });

const readArgumentMaximum = (symbols: Effect.Success<typeof openMacOsLibrary>) => {
  const value = new Int32Array(1);
  const valueSize = new BigUint64Array([BigInt(value.byteLength)]);
  const name = Int32Array.from(ARGUMENT_MAXIMUM_NAME);
  return callReadingErrorNumber(symbols, () =>
    symbols.sysctl(name, name.length, value, valueSize, null, 0),
  ).pipe(
    Effect.flatMap(({ returned, errorNumber }) =>
      returned === 0
        ? Effect.succeed(value[0] ?? 0)
        : Effect.fail(
            new ProcRunMarkerUnreadable({
              taskName: null,
              filePath: "kern.argmax",
              whatIsWrong: null,
              cause: systemCallFailed("sysctl kern.argmax", null, errorNumber),
            }),
          ),
    ),
  );
};

const readMacOsEnvironmentMarker = (
  symbols: Effect.Success<typeof openMacOsLibrary>,
  processArguments: Uint8Array,
  processId: number,
  marker: typeof RunMarker.Type,
) => {
  const valueSize = new BigUint64Array([BigInt(processArguments.byteLength)]);
  const name = Int32Array.from([CTL_KERN, KERN_PROCARGS2, processId]);
  return callReadingErrorNumber(symbols, () =>
    symbols.sysctl(name, name.length, processArguments, valueSize, null, 0),
  ).pipe(
    Effect.flatMap(({ returned, errorNumber }) =>
      returned === 0
        ? decodeProcessArguments(
            processId,
            processArguments.subarray(0, Number(valueSize[0])),
          ).pipe(
            Effect.map(({ environmentEntries }) =>
              readEnvironmentMarker(marker, environmentEntries),
            ),
            Effect.catchTag("ProcessArgumentsMalformed", (cause) =>
              Effect.succeed(MarkerReading.cases.Unreadable.make({ cause })),
            ),
          )
        : Effect.succeed(
            MarkerReading.cases.Unreadable.make({
              cause: systemCallFailed("sysctl kern.procargs2", processId, errorNumber),
            }),
          ),
    ),
  );
};

const SEQUENTIAL = 1;

const readMacOsMarkers = (
  marker: typeof RunMarker.Type,
  candidates: readonly (typeof ProcessIdentity.Type)[],
  runStartTime: string | null,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const symbols = yield* openMacOsLibrary;
      const pipeIdentity = yield* requireOwnPipeIdentity(marker.fileDescriptor);
      const processArguments = new Uint8Array(yield* readArgumentMaximum(symbols));
      return yield* readMarkersOfCandidates(
        candidates,
        runStartTime,
        (processId) =>
          Effect.gen(function* () {
            const environmentReading = yield* readMacOsEnvironmentMarker(
              symbols,
              processArguments,
              processId,
              marker,
            );
            if (MarkerReading.guards.Marked(environmentReading)) {
              return environmentReading;
            }
            const pipeReading = yield* findMacOsMarkerPipe(symbols, processId, pipeIdentity);
            return combineReadings(environmentReading, pipeReading);
          }),
        (processId) => readProcessWithSysctl(null, processId),
        SEQUENTIAL,
      );
    }),
  );

const findLinuxMarkerPipe = (
  fileSystem: FileSystem.FileSystem,
  processId: number,
  pipeIdentity: string,
) =>
  Effect.gen(function* () {
    const fileDescriptors = yield* readProcFile(
      `${PROC_DIRECTORY_PATH}/${processId}/fd`,
      (directoryPath) => fileSystem.readDirectory(directoryPath),
    );
    if (fileDescriptors.hiddenBy !== null) {
      return MarkerReading.cases.Unreadable.make({ cause: fileDescriptors.hiddenBy });
    }
    const links = yield* Effect.forEach(fileDescriptors.content ?? [], (fileDescriptor) =>
      readLinuxPipeLink(fileSystem, processId, fileDescriptor),
    );
    if (links.some(({ link }) => link === pipeIdentity)) {
      return MarkerReading.cases.Marked.make({ ownerTaskName: null });
    }
    const hiddenBy = links.find((link) => link.hiddenBy !== null)?.hiddenBy ?? null;
    return hiddenBy === null ? UNMARKED : MarkerReading.cases.Unreadable.make({ cause: hiddenBy });
  });

const readLinuxEnvironmentMarker = (
  fileSystem: FileSystem.FileSystem,
  processId: number,
  marker: typeof RunMarker.Type,
) =>
  readProcFile(`${PROC_DIRECTORY_PATH}/${processId}/environ`, fileSystem.readFile).pipe(
    Effect.map(({ content, hiddenBy }) => {
      if (hiddenBy !== null) {
        return MarkerReading.cases.Unreadable.make({ cause: hiddenBy });
      }
      return content === null
        ? UNMARKED
        : readEnvironmentMarker(
            marker,
            new TextDecoder("utf-8", { ignoreBOM: true }).decode(content).split("\0"),
          );
    }),
  );

export const readLinuxMarkers = (
  marker: typeof RunMarker.Type,
  pipeIdentity: string,
  candidates: readonly (typeof ProcessIdentity.Type)[],
  runStartTime: string | null,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    return yield* readMarkersOfCandidates(
      candidates,
      runStartTime,
      (processId) =>
        Effect.gen(function* () {
          const environmentReading = yield* readLinuxEnvironmentMarker(
            fileSystem,
            processId,
            marker,
          );
          if (MarkerReading.guards.Marked(environmentReading)) {
            return environmentReading;
          }
          const pipeReading = yield* findLinuxMarkerPipe(fileSystem, processId, pipeIdentity);
          return combineReadings(environmentReading, pipeReading);
        }),
      (processId) =>
        readProcessFromProcDirectory(null, processId).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
        ),
      yield* availableParallelism,
    );
  });

export const findMarkedProcesses = Effect.fn(function* (
  marker: typeof RunMarker.Type | null,
  listedProcesses: ReadonlyMap<
    number,
    { readonly hasEnded: boolean; readonly processGroupId: number; readonly startTime: string }
  >,
) {
  if (marker === null) {
    return {
      markedProcesses: new Map<number, string | null>(),
      markedIdentities: new Map<string, string | null>(),
      unreadableProcesses: new Map<number, unknown>(),
    };
  }
  const isStillTheListedProcess = (processId: number) =>
    readProcessUnlessGone(null, processId).pipe(
      Effect.map((listedNow) => {
        const listedThen = listedProcesses.get(processId);
        return (
          listedNow !== null &&
          listedThen !== undefined &&
          !listedNow.hasEnded &&
          listedNow.startTime === listedThen.startTime &&
          listedNow.processGroupId === listedThen.processGroupId
        );
      }),
    );
  const runProcessIds = new Set([process.pid, process.ppid]);
  const signalableCandidates = yield* Effect.filter(
    [...listedProcesses]
      .filter(
        ([processId, listedProcess]) =>
          processId > CALLER_PROCESS_GROUP_ID &&
          !listedProcess.hasEnded &&
          !runProcessIds.has(processId),
      )
      .map(([processId, listedProcess]) => ({ processId, startTime: listedProcess.startTime })),
    ({ processId }) =>
      sendSignal(null, processId, 0).pipe(Effect.map((result) => result === "sent")),
  );
  const runStartTime = (yield* readProcessUnlessGone(null, process.pid))?.startTime ?? null;
  const readings =
    process.platform === "darwin"
      ? yield* readMacOsMarkers(marker, signalableCandidates, runStartTime)
      : yield* readLinuxMarkers(
          marker,
          yield* requireOwnPipeIdentity(marker.fileDescriptor),
          signalableCandidates,
          runStartTime,
        );
  const unreadable = yield* Effect.filter(
    readings.flatMap(([processId, reading]) =>
      MarkerReading.guards.Unreadable(reading) ? [{ processId, cause: reading.cause }] : [],
    ),
    ({ processId }) => isStillTheListedProcess(processId),
  );
  const marked = yield* Effect.filter(
    readings.flatMap(([processId, reading]) =>
      MarkerReading.guards.Marked(reading)
        ? [{ processId, ownerTaskName: reading.ownerTaskName }]
        : [],
    ),
    ({ processId }) => isStillTheListedProcess(processId),
  );
  return {
    markedProcesses: new Map(
      marked.map(({ processId, ownerTaskName }) => [processId, ownerTaskName] as const),
    ),
    markedIdentities: new Map(
      marked.flatMap(({ processId, ownerTaskName }) => {
        const listedProcess = listedProcesses.get(processId);
        return listedProcess === undefined
          ? []
          : [
              [
                processIdentityKey({ processId, startTime: listedProcess.startTime }),
                ownerTaskName,
              ] as const,
            ];
      }),
    ),
    unreadableProcesses: new Map<number, unknown>(
      unreadable.map(({ processId, cause }) => [processId, cause] as const),
    ),
  };
});
