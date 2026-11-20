import { counted } from "./counted.ts";
import { escapeControlAndLineBreakCharacters } from "./label-lines.ts";

const MOST_CODE_UNITS_OF_A_RECAP_LINE = 200;

const describeNotShown = (count: number, noun: string) =>
  `(${counted(count, noun)} ${count === 1 ? "is" : "are"} not shown)`;

export const describeLinesNotShown = (count: number) => describeNotShown(count, "line");

export const describeNestedLinesNotShown = (count: number) =>
  describeNotShown(count, "nested recap line");

const describeMoreCharacters = (count: number) => describeNotShown(count, "more character");

const escapeWithinLimit = (
  text: string,
  from: number,
  end: number,
  limit: number,
  shown = "",
): { readonly shown: string; readonly sourceEnd: number } => {
  const codePoint = from < end ? text.codePointAt(from) : undefined;
  if (codePoint === undefined) {
    return { shown, sourceEnd: from };
  }
  const character = String.fromCodePoint(codePoint);
  const escaped = escapeControlAndLineBreakCharacters(character);
  return shown.length + escaped.length > limit
    ? { shown, sourceEnd: from }
    : escapeWithinLimit(text, from + character.length, end, limit, `${shown}${escaped}`);
};

export const recapLine = (text: string, start = 0, end = text.length) => {
  const whole = escapeWithinLimit(text, start, end, MOST_CODE_UNITS_OF_A_RECAP_LINE);
  if (whole.sourceEnd === end) {
    return whole.shown;
  }
  const longestNote = ` ${describeMoreCharacters(end - start)}`;
  const { shown, sourceEnd } = escapeWithinLimit(
    text,
    start,
    end,
    MOST_CODE_UNITS_OF_A_RECAP_LINE - longestNote.length,
  );
  return `${shown} ${describeMoreCharacters(end - sourceEnd)}`;
};
