import { Array as Arrays, String as Strings } from "effect";

const TASK_LINE_PREFIX = String.raw`(?:\[[^\]]+\] )?`;

const ERROR_LIKE_LINE = new RegExp(
  String.raw`^${TASK_LINE_PREFIX}(?:(?:\S[^\n]*?: )?error\b|\w*Error: )`,
  "u",
);

const PRINTED_ERROR_LINE = new RegExp(
  String.raw`^${TASK_LINE_PREFIX}(?:\S[^\n]*?: )?error [A-Za-z][\w-]*: \S`,
  "u",
);

const TASK_PREFIX_REPEATED_AS_LOCATION = /^\[(?<taskName>[^\]]+)\] \k<taskName>: error /u;

const BUN_STACK_FRAME = /^\s+at (?<framePath>\/.+):\d+:\d+$/u;

const TASK_SOURCE_DIRECTORY_PATH = new URL("../../src/", import.meta.url).pathname;

const isBunRuntimeErrorOutsideTask = (line: string, nextLine: string | undefined) => {
  const isNotFollowedRuntimeError = !line.startsWith("error: ") || nextLine === undefined;
  if (isNotFollowedRuntimeError) {
    return false;
  }
  if (nextLine.startsWith("Bun v")) {
    return true;
  }
  const [, framePath] = BUN_STACK_FRAME.exec(nextLine) ?? [];
  return framePath !== undefined && !framePath.startsWith(TASK_SOURCE_DIRECTORY_PATH);
};

const HINT_LINE = new RegExp(String.raw`^${TASK_LINE_PREFIX}\s*hint:`, "u");

const CONTINUATION_LINE = new RegExp(String.raw`^${TASK_LINE_PREFIX}(?:\s.*)?$`, "u");

const COMMAND_FAILED_LINE = new RegExp(
  String.raw`^${TASK_LINE_PREFIX}(?:\S[^\n]*?: )?error CommandFailed: `,
  "u",
);

const FAILURE_REASON_LINE = /^[A-Za-z][\w-]*: \S[^\n]*$/u;

export const findMisprintedErrorLines = (standardError: string) => {
  const printedLines = Strings.split(standardError.trimEnd(), "\n");
  const lines = FAILURE_REASON_LINE.test(Arrays.lastNonEmpty(printedLines))
    ? printedLines.slice(0, -1)
    : printedLines;
  const misprinted: { readonly line: string; readonly fault: string }[] = [];
  const printedByAfterEachLine: ("command" | "nothing" | "task")[] = [];
  for (const [lineIndex, line] of lines.entries()) {
    const printedBy = printedByAfterEachLine.at(-1) ?? "nothing";
    const isPrintedErrorLine =
      ERROR_LIKE_LINE.test(line) && !isBunRuntimeErrorOutsideTask(line, lines[lineIndex + 1]);
    if (isPrintedErrorLine) {
      if (!PRINTED_ERROR_LINE.test(line)) {
        misprinted.push({ line, fault: "does not print error, a tag, a colon and a message" });
      }
      if (TASK_PREFIX_REPEATED_AS_LOCATION.test(line)) {
        misprinted.push({ line, fault: "repeats the task prefix as its location" });
      }
      printedByAfterEachLine.push(COMMAND_FAILED_LINE.test(line) ? "command" : "task");
      continue;
    }
    const continuesError = printedBy !== "nothing" && CONTINUATION_LINE.test(line);
    const isTaskWrittenHint = continuesError && printedBy === "task" && HINT_LINE.test(line);
    if (isTaskWrittenHint) {
      misprinted.push({ line, fault: "prints a hint under an error Task wrote" });
    }
    printedByAfterEachLine.push(continuesError ? printedBy : "nothing");
  }
  return misprinted;
};
