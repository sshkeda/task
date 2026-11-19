import { String as Strings } from "effect";

export const LINE_LABELS = [
  "started",
  "ended",
  "error",
  "recap",
  "warning",
  "serving",
  "closed",
  "running",
  "stopping",
  "killed",
  "passed",
  "failed",
  "stopped",
  "run",
  "output",
] as const;

const LINE_BREAK = /\r\n|\r|\n/u;

export const splitLines = (text: string) => Strings.split(text, LINE_BREAK);

const ESCAPED_NAME_CHARACTER = /[\\\]\p{Cc}\p{Zl}\p{Zp}]/gu;

const NAME_CHARACTER_ESCAPES = new Map([
  ["\\", String.raw`\\`],
  ["]", String.raw`\]`],
  ["\n", String.raw`\n`],
  ["\r", String.raw`\r`],
  ["\t", String.raw`\t`],
]);

const HEXADECIMAL = 16;

const CONTROL_CHARACTER_HEX_DIGITS = 4;

const escapeNameCharacter = (character: string) =>
  NAME_CHARACTER_ESCAPES.get(character) ??
  String.raw`\u${(character.codePointAt(0) ?? 0)
    .toString(HEXADECIMAL)
    .toUpperCase()
    .padStart(CONTROL_CHARACTER_HEX_DIGITS, "0")}`;

const CONTROL_OR_LINE_BREAK_CHARACTER = /[\p{Cc}\p{Zl}\p{Zp}]/gu;

export const escapeControlAndLineBreakCharacters = (text: string) =>
  text.replaceAll(CONTROL_OR_LINE_BREAK_CHARACTER, escapeNameCharacter);

const LINE_SEPARATOR_CHARACTER = /[\p{Zl}\p{Zp}]/gu;

export const escapeLineSeparators = (text: string) =>
  text.replaceAll(LINE_SEPARATOR_CHARACTER, escapeNameCharacter);

export const nameTask = (taskName: string) =>
  `[${taskName.replaceAll(ESCAPED_NAME_CHARACTER, escapeNameCharacter)}]`;

export const labelLine = (
  label: (typeof LINE_LABELS)[number],
  taskName: string | null,
  text: string,
) => {
  const value = [taskName === null ? "" : nameTask(taskName), escapeLineSeparators(text)]
    .filter((part) => part !== "")
    .join(" ");
  return value === "" ? `${label}:` : `${label}: ${value}`;
};

export const labelLines = (
  label: (typeof LINE_LABELS)[number],
  taskName: string | null,
  text: string,
) => splitLines(text).map((line) => labelLine(label, taskName, line));

export const prefixTaskLines = (taskName: string, text: string) =>
  splitLines(text).map((line) => `${nameTask(taskName)} ${escapeLineSeparators(line)}`);
