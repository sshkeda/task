import { cutRecapOutputLine } from "../../../shared/cut-recap-output-line.ts";
import { escapeControlAndLineBreakCharacters, labelLine } from "../../../shared/label-lines.ts";

const SKIPPED_LINES_MARKER = "…";

export const formatRecapLines = (
  taskName: string,
  recap: {
    readonly text: string;
    readonly outputLines: readonly (string | null)[];
  },
  fileName: string | undefined,
) => [
  labelLine("recap", taskName, fileName === undefined ? recap.text : `${fileName} | ${recap.text}`),
  ...recap.outputLines.map((outputLine) =>
    labelLine(
      "recap",
      taskName,
      outputLine === null
        ? SKIPPED_LINES_MARKER
        : cutRecapOutputLine(escapeControlAndLineBreakCharacters(cutRecapOutputLine(outputLine))),
    ),
  ),
];
