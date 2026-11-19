const ENDED_LINE_PREFIX = "ended: ";

export const separateEndedLines = (lines: readonly string[]) => ({
  endedLines: lines.filter((line) => line.startsWith(ENDED_LINE_PREFIX)),
  otherLines: lines.filter((line) => !line.startsWith(ENDED_LINE_PREFIX)),
});
