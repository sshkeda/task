import { dlopen, FFIType, ptr } from "bun:ffi";
import type { FFIFunction } from "bun:ffi";
import { getSystemErrorName } from "node:util";

import { Deferred, Effect, Exit, Option, Schema } from "effect";
import type { Duration } from "effect";

import {
  forgetCommandInTerminal,
  notStartedBecauseSpawnThrew,
  runInTerminal,
  TerminalCommandEnded,
  TerminalCommandNotSpawned,
  trackCommandInTerminal,
} from "../../../shared/run-in-terminal.ts";
import type { TerminalCommand } from "../../../shared/run-in-terminal.ts";
import { STOP_GRACE_PERIOD } from "../../../shared/stop-signals.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { CommandNotStarted } from "../../errors/index.ts";
import { numberSignal, readWaitStatus } from "./read-wait-statuses.ts";

const MACOS_SYSTEM_LIBRARY_PATH = "/usr/lib/libSystem.B.dylib";
const LINUX_C_LIBRARY_PATH = "libc.so.6";

const isMacOs = process.platform === "darwin";

const POSIX_SPAWN_SETPGROUP = 0x02;
const POSIX_SPAWN_SETSIGDEF = 0x04;
const POSIX_SPAWN_SETSIGMASK = 0x08;
const POSIX_SPAWN_START_SUSPENDED = 0x80;
const POSIX_SPAWN_CLOEXEC_DEFAULT = 0x40_00;
const SPAWN_FLAGS = POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_SETSIGDEF | POSIX_SPAWN_SETSIGMASK;
const OWN_PROCESS_GROUP = 0;
const SPAWNED = 0;
const ENOENT = 2;

const MACOS_SIG_BLOCK = 1;
const MACOS_SIG_SETMASK = 3;
const LINUX_SIG_BLOCK = 0;
const LINUX_SIG_SETMASK = 2;
const SIG_BLOCK = isMacOs ? MACOS_SIG_BLOCK : LINUX_SIG_BLOCK;
const SIG_SETMASK = isMacOs ? MACOS_SIG_SETMASK : LINUX_SIG_SETMASK;
const WNOHANG = 1;
const WUNTRACED = 2;
const SET_FOREGROUND_SUCCEEDED = 0;
const NO_PROCESS_GROUP = 0;
const PROBE_SIGNAL = 0;

const SPAWN_ATTRIBUTES_BYTES = 512;
const SPAWN_FILE_ACTIONS_BYTES = 512;
const SIGNAL_SET_BYTES = 128;

const STANDARD_INPUT_FILE_DESCRIPTOR = 0;
const STANDARD_OUTPUT_FILE_DESCRIPTOR = 1;
const STANDARD_ERROR_FILE_DESCRIPTOR = 2;
const STANDARD_STREAM_FILE_DESCRIPTORS = [
  STANDARD_INPUT_FILE_DESCRIPTOR,
  STANDARD_OUTPUT_FILE_DESCRIPTOR,
  STANDARD_ERROR_FILE_DESCRIPTOR,
] as const;
const RUN_MARKER_FILE_DESCRIPTOR = 3;

const DEFAULT_SEARCH_PATH = "/usr/bin:/bin";

const isStopSignalFromTheTerminal = Schema.is(
  Schema.Literals(["SIGHUP", "SIGINT"] satisfies (typeof STOP_SIGNALS)[number][]),
);

const isLibraryWithoutTheForeground = Schema.is(
  Schema.Struct({ code: Schema.Literals(["ERR_DLOPEN_FAILED", "ERR_INVALID_ARG_TYPE"]) }),
);

const SYMBOLS = {
  posix_spawn: {
    args: [FFIType.ptr, FFIType.cstring, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr],
    returns: FFIType.i32,
  },
  posix_spawnattr_init: { args: [FFIType.ptr], returns: FFIType.i32 },
  posix_spawnattr_destroy: { args: [FFIType.ptr], returns: FFIType.i32 },
  posix_spawnattr_setflags: { args: [FFIType.ptr, FFIType.i16], returns: FFIType.i32 },
  posix_spawnattr_setpgroup: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
  posix_spawnattr_setsigdefault: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
  posix_spawnattr_setsigmask: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
  posix_spawn_file_actions_init: { args: [FFIType.ptr], returns: FFIType.i32 },
  posix_spawn_file_actions_destroy: { args: [FFIType.ptr], returns: FFIType.i32 },
  posix_spawn_file_actions_adddup2: {
    args: [FFIType.ptr, FFIType.i32, FFIType.i32],
    returns: FFIType.i32,
  },
  posix_spawn_file_actions_addchdir_np: {
    args: [FFIType.ptr, FFIType.cstring],
    returns: FFIType.i32,
  },
  sigemptyset: { args: [FFIType.ptr], returns: FFIType.i32 },
  sigfillset: { args: [FFIType.ptr], returns: FFIType.i32 },
  sigaddset: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
  pthread_sigmask: { args: [FFIType.i32, FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
  tcgetpgrp: { args: [FFIType.i32], returns: FFIType.i32 },
  tcsetpgrp: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
  getpgrp: { args: [], returns: FFIType.i32 },
  waitpid: { args: [FFIType.i32, FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
  strerror: { args: [FFIType.i32], returns: FFIType.cstring },
} as const satisfies Record<string, FFIFunction>;

const MACOS_SYMBOLS = {
  ...SYMBOLS,
  posix_spawn_file_actions_addinherit_np: {
    args: [FFIType.ptr, FFIType.i32],
    returns: FFIType.i32,
  },
} as const satisfies Record<string, FFIFunction>;

const LINUX_SYMBOLS = {
  ...SYMBOLS,
  posix_spawn_file_actions_addclosefrom_np: {
    args: [FFIType.ptr, FFIType.i32],
    returns: FFIType.i32,
  },
  posix_spawn_file_actions_addtcsetpgrp_np: {
    args: [FFIType.ptr, FFIType.i32],
    returns: FFIType.i32,
  },
} as const satisfies Record<string, FFIFunction>;

const isNoSuchProcess = Schema.is(Schema.Struct({ code: Schema.Literal("ESRCH") }));

const signalUnlessGone = (processId: number, signal: NodeJS.Signals) => {
  try {
    process.kill(processId, signal);
  } catch (error) {
    if (!isNoSuchProcess(error)) {
      throw error;
    }
  }
};

const processGroupExists = (processGroupId: number) => {
  try {
    process.kill(-processGroupId, PROBE_SIGNAL);
    return true;
  } catch (error) {
    return !isNoSuchProcess(error);
  }
};

const openMacOsLibrary = () => {
  const library = dlopen(MACOS_SYSTEM_LIBRARY_PATH, MACOS_SYMBOLS);
  const { symbols } = library;
  return {
    symbols,
    spawnFlags: SPAWN_FLAGS | POSIX_SPAWN_CLOEXEC_DEFAULT | POSIX_SPAWN_START_SUSPENDED,
    keepOnly: (fileActions: Uint8Array, keptFileDescriptors: readonly number[]) => {
      for (const fileDescriptor of keptFileDescriptors) {
        symbols.posix_spawn_file_actions_addinherit_np(fileActions, fileDescriptor);
      }
    },
    handTheTerminal: (terminal: number) => (processId: number) => {
      const holdsTheTerminal = symbols.tcsetpgrp(terminal, processId) === SET_FOREGROUND_SUCCEEDED;
      signalUnlessGone(processId, "SIGCONT");
      return holdsTheTerminal;
    },
    close: () => {
      library.close();
    },
  };
};

const openLinuxLibrary = () => {
  const library = dlopen(LINUX_C_LIBRARY_PATH, LINUX_SYMBOLS);
  const { symbols } = library;
  return {
    symbols,
    spawnFlags: SPAWN_FLAGS,
    keepOnly: (fileActions: Uint8Array, keptFileDescriptors: readonly number[]) => {
      symbols.posix_spawn_file_actions_addclosefrom_np(fileActions, keptFileDescriptors.length);
    },
    handTheTerminal: (terminal: number, fileActions: Uint8Array) => {
      symbols.posix_spawn_file_actions_addtcsetpgrp_np(fileActions, terminal);
      return () => true;
    },
    close: () => {
      library.close();
    },
  };
};

const openLibraryUnlessMissing = () => {
  try {
    return isMacOs ? openMacOsLibrary() : openLinuxLibrary();
  } catch (error) {
    if (isLibraryWithoutTheForeground(error)) {
      return null;
    }
    throw error;
  }
};

const openCLibrary = Effect.acquireRelease(Effect.sync(openLibraryUnlessMissing), (library) =>
  Effect.sync(() => {
    library?.close();
  }),
);

const encoder = new TextEncoder();

const NULL_BYTE = "\0";

const findExecutable = (command: typeof TerminalCommand.Type) => {
  if (command.command.includes("/")) {
    return command.command;
  }
  const searchPath = command.env["PATH"] ?? "";
  return Bun.which(command.command, {
    PATH: searchPath === "" ? DEFAULT_SEARCH_PATH : searchPath,
    cwd: command.cwd,
  });
};

const spawnStringPart = (text: string, subject: string, isReceivedEscaped: boolean) => ({
  text,
  subject,
  isReceivedEscaped,
});

const wholeSpawnString = (text: string, subject: string, isReceivedEscaped: boolean) => ({
  text,
  parts: [spawnStringPart(text, subject, isReceivedEscaped)],
});

const environmentSpawnString = (name: string, value: string) => ({
  text: `${name}=${value}`,
  parts: [name, value].map((text) =>
    spawnStringPart(text, `property 'options.env['${name}']'`, false),
  ),
});

const argumentSpawnString = (word: string, index: number) =>
  wholeSpawnString(word, `argument 'args[${index}]'`, index === 0);

const refuseNullByte = (part: ReturnType<typeof spawnStringPart>) =>
  new TerminalCommandNotSpawned({
    cause: Object.assign(
      new TypeError(
        `The ${part.subject} must be a string without null bytes. Received ${part.isReceivedEscaped ? Bun.inspect(part.text) : `"${part.text}"`}`,
      ),
      { code: "ERR_INVALID_ARG_VALUE" },
    ),
  });

const encodeSpawnStrings = (spawnStrings: readonly ReturnType<typeof wholeSpawnString>[]) => {
  const refused = spawnStrings
    .flatMap((spawnString) => spawnString.parts)
    .find((part) => part.text.includes(NULL_BYTE));
  return refused === undefined
    ? {
        isRefused: false as const,
        encoded: spawnStrings.map(({ text }) => encoder.encode(`${text}${NULL_BYTE}`)),
      }
    : {
        isRefused: true as const,
        notStarted: notStartedBecauseSpawnThrew(refuseNullByte(refused)),
      };
};

const packSpawnStrings = (command: typeof TerminalCommand.Type) => {
  const environmentEntries = Object.entries(command.env).map(([name, value]) =>
    environmentSpawnString(name, value),
  );
  const argumentWords = [command.command, ...command.args].map((word, index) =>
    argumentSpawnString(word, index),
  );
  const [commandWord, ...laterArgumentWords] = argumentWords;
  const beforeTheSearch = encodeSpawnStrings([
    wholeSpawnString(command.cwd, "property 'options.cwd'", true),
    ...environmentEntries,
    ...(commandWord === undefined ? [] : [commandWord]),
  ]);
  if (beforeTheSearch.isRefused) {
    return beforeTheSearch;
  }
  const executable = findExecutable(command);
  if (executable === null) {
    return {
      isRefused: true as const,
      notStarted: TerminalCommandEnded.cases.NotStarted.make({
        notFound: true,
        cause: {
          tag: CommandNotStarted.name,
          message: `Executable not found in $PATH: "${command.command}"`,
        },
      }),
    };
  }
  const afterTheSearch = encodeSpawnStrings([
    ...laterArgumentWords,
    wholeSpawnString(executable, "argument 'args[0]'", true),
  ]);
  if (afterTheSearch.isRefused) {
    return afterTheSearch;
  }
  const encodedExecutable = afterTheSearch.encoded.at(-1) ?? new Uint8Array();
  const [encodedDirectory = new Uint8Array(), ...encodedEnvironmentAndCommand] =
    beforeTheSearch.encoded;
  const texts = [
    encodedDirectory,
    ...encodedEnvironmentAndCommand,
    ...afterTheSearch.encoded.slice(0, -1),
  ];
  const argumentPointerCount = argumentWords.length + 1;
  const pointerCount = argumentPointerCount + environmentEntries.length + 1;
  const pointerBytes = pointerCount * BigUint64Array.BYTES_PER_ELEMENT;
  const starts: number[] = [];
  const ends: number[] = [];
  for (const text of texts) {
    const start = ends.at(-1) ?? pointerBytes;
    starts.push(start);
    ends.push(start + text.length);
  }
  const memory = new Uint8Array(ends.at(-1) ?? pointerBytes);
  for (const [index, text] of texts.entries()) {
    memory.set(text, starts[index]);
  }
  const address = ptr(memory);
  const pointers = new BigUint64Array(memory.buffer, 0, pointerCount);
  const firstArgumentString = 1 + environmentEntries.length;
  const slots = [
    ...argumentWords.map((_word, index) => [index, firstArgumentString + index] as const),
    ...environmentEntries.map(
      (_entry, index) => [argumentPointerCount + index, 1 + index] as const,
    ),
  ];
  for (const [slot, stringIndex] of slots) {
    pointers[slot] = BigInt(address + (starts[stringIndex] ?? 0));
  }
  return {
    isRefused: false as const,
    executable,
    encodedExecutable,
    directory: memory.subarray(starts[0], ends[0]),
    argumentPointers: pointers.subarray(0, argumentPointerCount),
    environmentPointers: pointers.subarray(argumentPointerCount),
  };
};

const makeCommandHoldingTheTerminal = (
  processId: number,
  terminal: number,
  holdsTheTerminal: boolean,
) => ({
  processId,
  terminal,
  holdsTheTerminal,
  ended:
    Deferred.makeUnsafe<
      Exclude<typeof TerminalCommandEnded.Type, { readonly _tag: "NotStarted" }>
    >(),
});

const withoutTheForeground = {
  run:
    (
      runMarkerFileDescriptor: number | null,
      _requestStop: (signal: (typeof STOP_SIGNALS)[number]) => Effect.Effect<void>,
      stopReceived: Effect.Effect<void>,
    ) =>
    (command: typeof TerminalCommand.Type) =>
      runInTerminal(stopReceived, runMarkerFileDescriptor)(command),
  collectEnded: Effect.void,
  findStopped: Effect.succeed(false),
  handTheTerminalBack: Effect.void,
};

const withTheForeground = (library: NonNullable<ReturnType<typeof openLibraryUnlessMissing>>) => {
  const { symbols, spawnFlags, keepOnly, handTheTerminal } = library;
  const running = new Map<number, ReturnType<typeof makeCommandHoldingTheTerminal>>();

  const signalSetOf = (signalNumbers: readonly number[]) => {
    const signalSet = new Uint8Array(SIGNAL_SET_BYTES);
    symbols.sigemptyset(signalSet);
    for (const signalNumber of signalNumbers) {
      symbols.sigaddset(signalSet, signalNumber);
    }
    return signalSet;
  };

  const takeTheTerminalBack = (terminal: number, commandProcessGroupId: number | null) => {
    const foreground = symbols.tcgetpgrp(terminal);
    const isLeftWithTheCommand =
      foreground === commandProcessGroupId ||
      foreground <= NO_PROCESS_GROUP ||
      !processGroupExists(foreground);
    const isTakenBack = foreground !== symbols.getpgrp() && isLeftWithTheCommand;
    if (!isTakenBack) {
      return;
    }
    const previousMask = new Uint8Array(SIGNAL_SET_BYTES);
    symbols.pthread_sigmask(SIG_BLOCK, signalSetOf([numberSignal("SIGTTOU")]), previousMask);
    symbols.tcsetpgrp(terminal, symbols.getpgrp());
    symbols.pthread_sigmask(SIG_SETMASK, previousMask, null);
  };

  const collect = (command: ReturnType<typeof makeCommandHoldingTheTerminal>, options: number) => {
    const status = new Int32Array(1);
    if (symbols.waitpid(command.processId, status, options) !== command.processId) {
      return null;
    }
    const read = readWaitStatus(status[0] ?? 0);
    if ("stoppedBy" in read) {
      return read.stoppedBy;
    }
    running.delete(command.processId);
    takeTheTerminalBack(command.terminal, command.processId);
    Deferred.doneUnsafe(command.ended, Exit.succeed(read.ended));
    return null;
  };

  const spawnInItsOwnProcessGroup = (
    strings: Extract<ReturnType<typeof packSpawnStrings>, { isRefused: false }>,
    terminal: number,
    runMarkerFileDescriptor: number | null,
  ):
    | { readonly notStarted: typeof TerminalCommandEnded.cases.NotStarted.Type }
    | { readonly processId: number; readonly holdsTheTerminal: boolean } => {
    const attributes = new Uint8Array(SPAWN_ATTRIBUTES_BYTES);
    const fileActions = new Uint8Array(SPAWN_FILE_ACTIONS_BYTES);
    symbols.posix_spawnattr_init(attributes);
    symbols.posix_spawn_file_actions_init(fileActions);
    const everySignal = new Uint8Array(SIGNAL_SET_BYTES);
    symbols.sigfillset(everySignal);
    symbols.posix_spawnattr_setflags(attributes, spawnFlags);
    symbols.posix_spawnattr_setpgroup(attributes, OWN_PROCESS_GROUP);
    symbols.posix_spawnattr_setsigdefault(attributes, everySignal);
    symbols.posix_spawnattr_setsigmask(attributes, signalSetOf([]));
    if (runMarkerFileDescriptor !== null) {
      symbols.posix_spawn_file_actions_adddup2(
        fileActions,
        runMarkerFileDescriptor,
        RUN_MARKER_FILE_DESCRIPTOR,
      );
    }
    keepOnly(fileActions, [
      ...STANDARD_STREAM_FILE_DESCRIPTORS,
      ...(runMarkerFileDescriptor === null ? [] : [RUN_MARKER_FILE_DESCRIPTOR]),
    ]);
    symbols.posix_spawn_file_actions_addchdir_np(fileActions, strings.directory);
    const holdTheTerminal = handTheTerminal(terminal, fileActions);
    const processId = new Int32Array(1);
    const errorNumber = symbols.posix_spawn(
      processId,
      strings.encodedExecutable,
      fileActions,
      attributes,
      strings.argumentPointers,
      strings.environmentPointers,
    );
    symbols.posix_spawn_file_actions_destroy(fileActions);
    symbols.posix_spawnattr_destroy(attributes);
    if (errorNumber !== SPAWNED) {
      takeTheTerminalBack(terminal, null);
      return {
        notStarted: TerminalCommandEnded.cases.NotStarted.make({
          notFound: errorNumber === ENOENT,
          cause: {
            tag: CommandNotStarted.name,
            message: `${getSystemErrorName(-errorNumber)}: ${String(symbols.strerror(errorNumber))}, posix_spawn '${strings.executable}'`,
          },
        }),
      };
    }
    const spawnedProcessId = processId[0] ?? 0;
    return { processId: spawnedProcessId, holdsTheTerminal: holdTheTerminal(spawnedProcessId) };
  };

  const start = (
    command: typeof TerminalCommand.Type,
    terminal: number,
    runMarkerFileDescriptor: number | null,
  ) =>
    Effect.sync<
      | { readonly notStarted: typeof TerminalCommandEnded.cases.NotStarted.Type }
      | { readonly started: ReturnType<typeof makeCommandHoldingTheTerminal> }
    >(() => {
      const strings = packSpawnStrings(command);
      if (strings.isRefused) {
        return { notStarted: strings.notStarted };
      }
      const spawned = spawnInItsOwnProcessGroup(strings, terminal, runMarkerFileDescriptor);
      if ("notStarted" in spawned) {
        return { notStarted: spawned.notStarted };
      }
      const started = makeCommandHoldingTheTerminal(
        spawned.processId,
        terminal,
        spawned.holdsTheTerminal,
      );
      running.set(started.processId, started);
      trackCommandInTerminal(command.id, started.processId, Deferred.await(started.ended));
      return { started };
    });

  const signalUnlessEnded = (
    command: ReturnType<typeof makeCommandHoldingTheTerminal>,
    signal: NodeJS.Signals,
  ) =>
    Effect.sync(() => {
      if (!Deferred.isDoneUnsafe(command.ended)) {
        signalUnlessGone(command.processId, signal);
      }
    });

  const terminate = (
    command: ReturnType<typeof makeCommandHoldingTheTerminal>,
    cleanup: Duration.Duration,
  ) =>
    signalUnlessEnded(command, "SIGTERM").pipe(
      Effect.andThen(Deferred.await(command.ended).pipe(Effect.timeoutOption(cleanup))),
      Effect.flatMap((ended) =>
        Option.isSome(ended)
          ? Effect.void
          : signalUnlessEnded(command, "SIGKILL").pipe(
              Effect.andThen(
                Deferred.await(command.ended).pipe(Effect.timeoutOption(STOP_GRACE_PERIOD)),
              ),
              Effect.flatMap((killed) =>
                Option.isSome(killed)
                  ? Effect.void
                  : Effect.sync(() => {
                      takeTheTerminalBack(command.terminal, command.processId);
                    }),
              ),
            ),
      ),
    );

  const decideEnd =
    (
      requestStop: (signal: (typeof STOP_SIGNALS)[number]) => Effect.Effect<void>,
      stopReceived: Effect.Effect<void>,
    ) =>
    (
      command: ReturnType<typeof makeCommandHoldingTheTerminal>,
    ): Effect.Effect<typeof TerminalCommandEnded.Type> =>
      Deferred.await(command.ended).pipe(
        Effect.flatMap((ended) => {
          const signal = TerminalCommandEnded.guards.Signaled(ended) ? ended.signal : null;
          const isStoppedFromTheTerminal =
            command.holdsTheTerminal && isStopSignalFromTheTerminal(signal);
          return isStoppedFromTheTerminal
            ? requestStop(signal).pipe(Effect.andThen(stopReceived), Effect.andThen(Effect.never))
            : Effect.succeed(ended);
        }),
      );

  const runHoldingTheTerminal = (
    terminal: number,
    runMarkerFileDescriptor: number | null,
    requestStop: (signal: (typeof STOP_SIGNALS)[number]) => Effect.Effect<void>,
    stopReceived: Effect.Effect<void>,
    command: typeof TerminalCommand.Type,
  ) =>
    Effect.acquireUseRelease(
      start(command, terminal, runMarkerFileDescriptor),
      (spawned) =>
        "notStarted" in spawned
          ? Effect.succeed(spawned.notStarted)
          : decideEnd(requestStop, stopReceived)(spawned.started),
      (spawned, exit) =>
        "notStarted" in spawned
          ? Effect.void
          : (Exit.isSuccess(exit) ? Effect.void : terminate(spawned.started, command.cleanup)).pipe(
              Effect.ensuring(forgetCommandInTerminal(command.id)),
            ),
    );

  return {
    run:
      (
        runMarkerFileDescriptor: number | null,
        requestStop: (signal: (typeof STOP_SIGNALS)[number]) => Effect.Effect<void>,
        stopReceived: Effect.Effect<void>,
      ) =>
      (command: typeof TerminalCommand.Type) => {
        const terminal =
          STANDARD_STREAM_FILE_DESCRIPTORS.find(
            (fileDescriptor) => symbols.tcgetpgrp(fileDescriptor) === symbols.getpgrp(),
          ) ?? null;
        return terminal === null
          ? withoutTheForeground.run(runMarkerFileDescriptor, requestStop, stopReceived)(command)
          : runHoldingTheTerminal(
              terminal,
              runMarkerFileDescriptor,
              requestStop,
              stopReceived,
              command,
            );
      },
    collectEnded: Effect.sync(() => {
      for (const command of running.values()) {
        collect(command, WNOHANG);
      }
    }),
    findStopped: Effect.sync(() =>
      [...running.values()]
        .map((command) => collect(command, WNOHANG | WUNTRACED))
        .some((stoppedBy) => stoppedBy !== null),
    ),
    handTheTerminalBack: Effect.sync(() => {
      for (const command of running.values()) {
        const isOursToHand =
          command.holdsTheTerminal && symbols.tcgetpgrp(command.terminal) === symbols.getpgrp();
        if (isOursToHand) {
          symbols.tcsetpgrp(command.terminal, command.processId);
        }
        signalUnlessGone(-command.processId, "SIGCONT");
      }
    }),
  };
};

export const makeTerminalForeground = openCLibrary.pipe(
  Effect.map((library) => (library === null ? withoutTheForeground : withTheForeground(library))),
);
