import { dlopen, FFIType } from "bun:ffi";
import type { FFIFunction } from "bun:ffi";

import { Effect, FileSystem } from "effect";
import type { PlatformError } from "effect";
import { SecureRandom } from "effect/crypto";

import { ProcRunMarkerUnreadable, SystemLibraryNotLoaded } from "../../../shared/process-errors.ts";
import {
  availableParallelism,
  RUN_MARKERS_VARIABLE_NAME,
} from "../../../shared/read-environment-config.ts";
import { RunMarker } from "../../../shared/run-marker.ts";
import { nullWhenProcessGoneOrHidden } from "./list-linux-processes-from-proc.ts";

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
const LISTED_FILE_DESCRIPTOR_LIMIT = 4096;
const PIPE_READ_END = 0;
const PIPE_WRITE_END = 1;
const PIPE_END_COUNT = 2;
const NO_FLAVOR_ARGUMENT = 0n;
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
    : openLibrary(LINUX_C_LIBRARY_PATH, PIPE_SYMBOLS);

const readMacOsPipeHandle = (
  symbols: Effect.Success<typeof openMacOsLibrary>,
  processId: number,
  fileDescriptor: number,
) => {
  const pipeInformation = new Uint8Array(PIPE_FDINFO_BYTES);
  const filled = symbols.proc_pidfdinfo(
    processId,
    fileDescriptor,
    PROC_PIDFDPIPEINFO,
    pipeInformation,
    PIPE_FDINFO_BYTES,
  );
  return filled === PIPE_FDINFO_BYTES
    ? String(new DataView(pipeInformation.buffer).getBigUint64(PIPE_FDINFO_HANDLE_OFFSET, true))
    : null;
};

const readProcFileUnlessGone = <Success>(
  filePath: string,
  readProcFile: (filePath: string) => Effect.Effect<Success, PlatformError.PlatformError>,
) =>
  nullWhenProcessGoneOrHidden(readProcFile(filePath), Effect.void).pipe(
    Effect.mapError((cause) => new ProcRunMarkerUnreadable({ taskName: null, filePath, cause })),
  );

const readLinuxPipeLink = (
  fileSystem: FileSystem.FileSystem,
  processId: number,
  fileDescriptor: string,
) =>
  readProcFileUnlessGone(
    `${PROC_DIRECTORY_PATH}/${processId}/fd/${fileDescriptor}`,
    fileSystem.readLink,
  ).pipe(Effect.map((link) => (link?.startsWith(LINUX_PIPE_LINK_PREFIX) === true ? link : null)));

const readOwnPipeIdentity = (fileDescriptor: number) =>
  process.platform === "darwin"
    ? Effect.scoped(
        openMacOsLibrary.pipe(
          Effect.map((symbols) => readMacOsPipeHandle(symbols, process.pid, fileDescriptor)),
        ),
      )
    : FileSystem.FileSystem.use((fileSystem) =>
        readLinuxPipeLink(fileSystem, process.pid, String(fileDescriptor)),
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
          if (symbols.pipe(pipeEnds) !== 0) {
            return null;
          }
          const readEnd = pipeEnds[PIPE_READ_END] ?? -1;
          const writeEnd = pipeEnds[PIPE_WRITE_END] ?? -1;
          const tokenBytes = new TextEncoder().encode(tokens.join(" "));
          const written = Number(symbols.write(writeEnd, tokenBytes, tokenBytes.byteLength));
          symbols.close(writeEnd);
          if (written !== tokenBytes.byteLength) {
            symbols.close(readEnd);
            return null;
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
        if ((yield* readOwnPipeIdentity(SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR)) === null) {
          return null;
        }
        const symbols = yield* openPipeLibrary;
        const tokens = readUntilTheEnd(symbols, SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR);
        if (tokens === null) {
          return yield* new ProcRunMarkerUnreadable({
            taskName: null,
            filePath: `/dev/fd/${SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR}`,
            cause: "read failed",
          });
        }
        return markerOf(
          SUPERVISED_RUN_MARKER_FILE_DESCRIPTOR,
          tokens.split(" ").filter((token) => token !== ""),
        );
      }),
    )
  : Effect.succeed(null);

const holdsToken = (environment: Uint8Array, token: string) => {
  const variablePrefix = `${RUN_MARKERS_VARIABLE_NAME}=`;
  const mentionsToken = Buffer.from(
    environment.buffer,
    environment.byteOffset,
    environment.byteLength,
  ).includes(token, "latin1");
  return (
    mentionsToken &&
    new TextDecoder("latin1")
      .decode(environment)
      .split("\0")
      .some(
        (entry) =>
          entry.startsWith(variablePrefix) &&
          entry.slice(variablePrefix.length).split(" ").includes(token),
      )
  );
};

const macOsHoldsPipe = (
  symbols: Effect.Success<typeof openMacOsLibrary>,
  fileDescriptors: Uint8Array,
  processId: number,
  pipeIdentity: string,
) => {
  const filled = symbols.proc_pidinfo(
    processId,
    PROC_PIDLISTFDS,
    NO_FLAVOR_ARGUMENT,
    fileDescriptors,
    fileDescriptors.byteLength,
  );
  const fields = new DataView(fileDescriptors.buffer);
  return Array.from(
    { length: Math.max(filled, 0) / PROC_FDINFO_BYTES },
    (_unused, index) => index * PROC_FDINFO_BYTES,
  ).some(
    (offset) =>
      fields.getUint32(offset + PROC_FDINFO_TYPE_OFFSET, true) === PROX_FDTYPE_PIPE &&
      readMacOsPipeHandle(symbols, processId, fields.getInt32(offset, true)) === pipeIdentity,
  );
};

const readArgumentMaximum = (symbols: Effect.Success<typeof openMacOsLibrary>) => {
  const value = new Int32Array(1);
  const valueSize = new BigUint64Array([BigInt(value.byteLength)]);
  const name = Int32Array.from(ARGUMENT_MAXIMUM_NAME);
  return symbols.sysctl(name, name.length, value, valueSize, null, 0) === 0 ? (value[0] ?? 0) : 0;
};

const macOsEnvironmentHoldsToken = (
  symbols: Effect.Success<typeof openMacOsLibrary>,
  processArguments: Uint8Array,
  processId: number,
  token: string,
) => {
  const valueSize = new BigUint64Array([BigInt(processArguments.byteLength)]);
  const name = Int32Array.from([CTL_KERN, KERN_PROCARGS2, processId]);
  return (
    symbols.sysctl(name, name.length, processArguments, valueSize, null, 0) === 0 &&
    holdsToken(processArguments.subarray(0, Number(valueSize[0])), token)
  );
};

const findMacOsMarkedProcessIds = (
  marker: typeof RunMarker.Type,
  candidateProcessIds: readonly number[],
) =>
  Effect.scoped(
    openMacOsLibrary.pipe(
      Effect.map((symbols) => {
        const pipeIdentity = readMacOsPipeHandle(symbols, process.pid, marker.fileDescriptor);
        const fileDescriptors = new Uint8Array(PROC_FDINFO_BYTES * LISTED_FILE_DESCRIPTOR_LIMIT);
        const processArguments = new Uint8Array(readArgumentMaximum(symbols));
        return candidateProcessIds.filter(
          (processId) =>
            (pipeIdentity !== null &&
              macOsHoldsPipe(symbols, fileDescriptors, processId, pipeIdentity)) ||
            macOsEnvironmentHoldsToken(symbols, processArguments, processId, marker.token),
        );
      }),
    ),
  );

const linuxHoldsPipe = (
  fileSystem: FileSystem.FileSystem,
  processId: number,
  pipeIdentity: string,
) =>
  readProcFileUnlessGone(`${PROC_DIRECTORY_PATH}/${processId}/fd`, (directoryPath) =>
    fileSystem.readDirectory(directoryPath),
  ).pipe(
    Effect.flatMap((fileDescriptors) =>
      Effect.forEach(fileDescriptors ?? [], (fileDescriptor) =>
        readLinuxPipeLink(fileSystem, processId, fileDescriptor),
      ),
    ),
    Effect.map((links) => links.includes(pipeIdentity)),
  );

const linuxEnvironmentHoldsToken = (
  fileSystem: FileSystem.FileSystem,
  processId: number,
  token: string,
) =>
  readProcFileUnlessGone(`${PROC_DIRECTORY_PATH}/${processId}/environ`, fileSystem.readFile).pipe(
    Effect.map((environment) => environment !== null && holdsToken(environment, token)),
  );

const findLinuxMarkedProcessIds = (
  marker: typeof RunMarker.Type,
  candidateProcessIds: readonly number[],
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const pipeIdentity = yield* readLinuxPipeLink(
      fileSystem,
      process.pid,
      String(marker.fileDescriptor),
    );
    const holdsMarkerPipe = (processId: number) =>
      pipeIdentity === null
        ? Effect.succeed(false)
        : linuxHoldsPipe(fileSystem, processId, pipeIdentity);
    return yield* Effect.filter(
      candidateProcessIds,
      (processId) =>
        Effect.gen(function* () {
          return (
            (yield* linuxEnvironmentHoldsToken(fileSystem, processId, marker.token)) ||
            (yield* holdsMarkerPipe(processId))
          );
        }),
      { concurrency: yield* availableParallelism },
    );
  });

export const findMarkedProcessIds = (
  marker: typeof RunMarker.Type | null,
  candidateProcessIds: Iterable<number>,
) => {
  const runProcessIds = new Set([process.pid, process.ppid]);
  const otherProcessIds = [...candidateProcessIds].filter(
    (processId) => !runProcessIds.has(processId),
  );
  if (marker === null) {
    return Effect.succeed(new Set<number>());
  }
  return Effect.gen(function* () {
    return new Set(
      process.platform === "darwin"
        ? yield* findMacOsMarkedProcessIds(marker, otherProcessIds)
        : yield* findLinuxMarkedProcessIds(marker, otherProcessIds),
    );
  });
};
