import { String as Strings } from "effect";

const TASK_PREFIX = String.raw`\[(?<taskName>(?:[^\]\\]|\\.)+)\]`;

const ERROR_LINE = new RegExp(String.raw`^error:(?: ${TASK_PREFIX})?(?: (?<value>.*))?$`, "u");

const ERROR_HEADLINE_VALUE = /^(?:[^[\n][^\n]*?: )?(?<tag>[A-Za-z][\w-]*): \S/u;

const ERROR_LIKE_LINE_WITHOUT_LABEL = /^(?:(?:\S[^\n]*?: )?error\b(?!:)|\w*Error: )/u;

const TASK_LINE = new RegExp(String.raw`^${TASK_PREFIX}(?: |$)`, "u");

const HINT_VALUE = /^\s*hint:/u;

const OUTPUT_FILE_POINTER_START = "its output is in /";

const RECAP_LINE_START = "recap: ";

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

export const findMisprintedErrorLines = (standardError: string) => {
  const lines = Strings.split(standardError.trimEnd(), "\n");
  const misprinted: { readonly line: string; readonly fault: string }[] = [];
  const previousErrors = new Map<
    "previous",
    { readonly taskName: string | undefined; readonly tag: string }
  >();
  for (const [lineIndex, line] of lines.entries()) {
    const isTaskOrRuntimeLine =
      TASK_LINE.test(line) || isBunRuntimeErrorOutsideTask(line, lines[lineIndex + 1]);
    if (isTaskOrRuntimeLine) {
      previousErrors.delete("previous");
      continue;
    }
    const errorLine = ERROR_LINE.exec(line);
    if (line.startsWith(RECAP_LINE_START)) {
      previousErrors.delete("previous");
      continue;
    }
    if (errorLine === null) {
      if (ERROR_LIKE_LINE_WITHOUT_LABEL.test(line)) {
        misprinted.push({ line, fault: "does not print the error label" });
      }
      previousErrors.delete("previous");
      continue;
    }
    const { taskName, value = "" } = errorLine.groups ?? {};
    if (value.startsWith(OUTPUT_FILE_POINTER_START)) {
      continue;
    }
    const previousError = previousErrors.get("previous");
    const isDetailOfThePreviousError =
      previousError !== undefined && previousError.taskName === taskName;
    if (isDetailOfThePreviousError) {
      if (HINT_VALUE.test(value)) {
        misprinted.push({ line, fault: "prints a hint under an error Task wrote" });
      }
      const nextTag = value.startsWith(" ")
        ? undefined
        : ERROR_HEADLINE_VALUE.exec(value)?.groups?.["tag"];
      if (nextTag !== undefined) {
        previousErrors.set("previous", { taskName, tag: nextTag });
      }
      continue;
    }
    const tag = ERROR_HEADLINE_VALUE.exec(value)?.groups?.["tag"];
    if (tag === undefined) {
      misprinted.push({ line, fault: "does not print a tag, a colon and a message" });
      previousErrors.delete("previous");
      continue;
    }
    const repeatsTheTaskName = taskName !== undefined && value.startsWith(`${taskName}: `);
    if (repeatsTheTaskName) {
      misprinted.push({ line, fault: "repeats the task name as its location" });
    }
    previousErrors.set("previous", { taskName, tag });
  }
  return misprinted;
};
