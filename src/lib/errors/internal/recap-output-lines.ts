import {
  cutRecapOutputLine,
  RECAPPED_OUTPUT_LINE_READ_LIMIT,
} from "../../../shared/cut-recap-output-line.ts";

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

export const selectRecapOutputLines = (outputs: readonly string[]) => {
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
  const hasSkippedLines =
    beforeLast !== null && afterFirst !== null && !isSameLine(afterFirst, beforeLast);
  return [first, ...(hasSkippedLines ? [null] : []), ...lastLines].map((line) =>
    line === null
      ? null
      : cutRecapOutputLine(
          (outputs[line.stream] ?? "").slice(
            line.start,
            Math.min(line.end, line.start + RECAPPED_OUTPUT_LINE_READ_LIMIT),
          ),
        ),
  );
};
