import { dlopen, FFIType } from "bun:ffi";

import { Effect } from "effect";

const F_GETFL = 3;
const F_SETFL = 4;
const STANDARD_INPUT_FILE_DESCRIPTOR = 0;
const STANDARD_OUTPUT_FILE_DESCRIPTOR = 1;
const STANDARD_ERROR_FILE_DESCRIPTOR = 2;
const STANDARD_STREAM_FILE_DESCRIPTORS = [
  STANDARD_INPUT_FILE_DESCRIPTOR,
  STANDARD_OUTPUT_FILE_DESCRIPTOR,
  STANDARD_ERROR_FILE_DESCRIPTOR,
] as const;
const FILE_CONTROL = {
  args: [FFIType.i32, FFIType.i32, FFIType.i32],
  returns: FFIType.i32,
} as const;

const loadFileControl = () => {
  if (process.platform === "darwin") {
    const library = dlopen("/usr/lib/libSystem.B.dylib", { __fcntl: FILE_CONTROL });
    return {
      fileControl: library.symbols.__fcntl,
      close: () => {
        library.close();
      },
    };
  }
  const library = dlopen("libc.so.6", { fcntl: FILE_CONTROL });
  return {
    fileControl: library.symbols.fcntl,
    close: () => {
      library.close();
    },
  };
};

export const saveStandardStreamFlags = Effect.acquireRelease(
  Effect.sync(loadFileControl),
  (library) =>
    Effect.sync(() => {
      library.close();
    }),
).pipe(
  Effect.map(({ fileControl }) => {
    const savedFlags = STANDARD_STREAM_FILE_DESCRIPTORS.map((fileDescriptor) => ({
      fileDescriptor,
      flags: fileControl(fileDescriptor, F_GETFL, 0),
    }));
    return Effect.sync(() => {
      for (const { fileDescriptor, flags } of savedFlags) {
        fileControl(fileDescriptor, F_SETFL, flags);
      }
    });
  }),
);
