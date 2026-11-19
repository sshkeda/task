import { Effect } from "effect";

import { ProcessArgumentsMalformed } from "../../../shared/process-errors.ts";

const EXECUTABLE_KEY = "executable_path=";
const SIXTY_FOUR_BIT_PROCESS_POINTER_SIZE = 8;

export const decodeProcessArguments = (
  processId: number,
  processArguments: Uint8Array,
): Effect.Effect<
  { readonly argumentWords: readonly string[]; readonly environmentEntries: readonly string[] },
  ProcessArgumentsMalformed
> => {
  const argumentCount = new DataView(
    processArguments.buffer,
    processArguments.byteOffset,
    processArguments.byteLength,
  ).getInt32(0, true);
  const executablePathStart = Int32Array.BYTES_PER_ELEMENT;
  const executablePathEnd = processArguments.indexOf(0, executablePathStart);
  const executablePrefixLength =
    EXECUTABLE_KEY.length + executablePathEnd + 1 - executablePathStart;
  const paddedExecutablePathLength =
    Math.ceil(executablePrefixLength / SIXTY_FOUR_BIT_PROCESS_POINTER_SIZE) *
      SIXTY_FOUR_BIT_PROCESS_POINTER_SIZE -
    EXECUTABLE_KEY.length;
  const terminatedStrings =
    executablePathEnd === -1
      ? []
      : new TextDecoder("utf-8", { ignoreBOM: true })
          .decode(processArguments.subarray(executablePathStart + paddedExecutablePathLength))
          .split("\0")
          .slice(0, -1);
  const argumentWords = terminatedStrings.slice(0, Math.max(argumentCount, 0));
  if (argumentWords.length < argumentCount) {
    return Effect.fail(
      new ProcessArgumentsMalformed({
        processId,
        argumentCount,
        argumentsFound: argumentWords.length,
      }),
    );
  }
  return Effect.succeed({
    argumentWords,
    environmentEntries: terminatedStrings.slice(argumentWords.length),
  });
};
