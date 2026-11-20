import { countLineBreaks } from "./count-line-breaks.ts";
import { describeLinesNotShown, recapLine } from "./recap-entries.ts";

const placeLineAround = (text: string, stream: number, index: number) => ({
  stream,
  start: Math.max(text.lastIndexOf("\n", index), text.lastIndexOf("\r", index)) + 1,
  end: Math.min(
    ...[text.indexOf("\n", index), text.indexOf("\r", index), text.length].filter(
      (lineEnd) => lineEnd >= 0,
    ),
  ),
});

const findNextLine = (
  outputs: readonly string[],
  stream: number,
  from: number,
): ReturnType<typeof placeLineAround> | null => {
  if (stream >= outputs.length) {
    return null;
  }
  const text = outputs[stream] ?? "";
  const nonBlank = /\S/gu;
  nonBlank.lastIndex = from;
  const found = nonBlank.exec(text);
  return found === null
    ? findNextLine(outputs, stream + 1, 0)
    : placeLineAround(text, stream, found.index);
};

const findLineBefore = (
  outputs: readonly string[],
  stream: number,
  before: number,
): ReturnType<typeof placeLineAround> | null => {
  if (stream < 0) {
    return null;
  }
  const text = outputs[stream] ?? "";
  const found = /\S(?=\s*$)/u.exec(before === text.length ? text : text.slice(0, before));
  return found === null
    ? findLineBefore(outputs, stream - 1, (outputs[stream - 1] ?? "").length)
    : placeLineAround(text, stream, found.index);
};

const isSameLine = (
  left: ReturnType<typeof placeLineAround>,
  right: ReturnType<typeof placeLineAround>,
) => left.stream === right.stream && left.start === right.start;

const isLaterLine = (
  line: ReturnType<typeof placeLineAround> | null,
  than: ReturnType<typeof placeLineAround>,
) =>
  line !== null &&
  (line.stream > than.stream || (line.stream === than.stream && line.start > than.start));

const countLines = (text: string) =>
  text === "" ? 0 : countLineBreaks(text) + (/[\r\n]$/u.test(text) ? 0 : 1);

const countLinesBetween = (
  outputs: readonly string[],
  after: ReturnType<typeof placeLineAround>,
  before: ReturnType<typeof placeLineAround>,
) => {
  const textOf = (stream: number) => outputs[stream] ?? "";
  if (after.stream === before.stream) {
    return countLineBreaks(textOf(after.stream).slice(after.end, before.start)) - 1;
  }
  const restOfItsStream = Math.max(countLines(textOf(after.stream).slice(after.end)) - 1, 0);
  const streamsBetween = outputs
    .slice(after.stream + 1, before.stream)
    .reduce((lineCount, text) => lineCount + countLines(text), 0);
  return (
    restOfItsStream + streamsBetween + countLineBreaks(textOf(before.stream).slice(0, before.start))
  );
};

export const selectRecapOutputLines = (outputs: readonly string[]): readonly string[] => {
  const first = findNextLine(outputs, 0, 0);
  if (first === null) {
    return [];
  }
  const lastStream = outputs.length - 1;
  const lastFound = findLineBefore(outputs, lastStream, outputs[lastStream]?.length ?? 0);
  const last = isLaterLine(lastFound, first) ? lastFound : null;
  const beforeLastFound = last === null ? null : findLineBefore(outputs, last.stream, last.start);
  const beforeLast = isLaterLine(beforeLastFound, first) ? beforeLastFound : null;
  const lastLines = [beforeLast, last].filter((line) => line !== null);
  const afterFirst = findNextLine(outputs, first.stream, first.end);
  const skipsALine =
    beforeLast !== null && afterFirst !== null && !isSameLine(afterFirst, beforeLast);
  const skipped = skipsALine
    ? [describeLinesNotShown(countLinesBetween(outputs, first, beforeLast))]
    : [];
  const take = (line: ReturnType<typeof placeLineAround>) =>
    recapLine(outputs[line.stream] ?? "", line.start, line.end);
  return [take(first), ...skipped, ...lastLines.map((line) => take(line))];
};
