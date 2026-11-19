const HELD_MEBIBYTES = 1;

const BYTES_PER_MEBIBYTE = 1_048_576;

export const HELD_COMMAND_OUTPUT_BYTES = HELD_MEBIBYTES * BYTES_PER_MEBIBYTE;

export const describeCutCommandOutput = (filePath: string | null) =>
  `only the last ${String(HELD_MEBIBYTES)} MiB of its commands' output prints${
    filePath === null ? "" : `, and the rest is in ${filePath}`
  }`;
