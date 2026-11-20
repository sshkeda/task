import { labelLine } from "../../../shared/label-lines.ts";
import { recapLine } from "../../../shared/recap-entries.ts";

export const formatRecapLines = (
  taskName: string,
  recap: {
    readonly text: string;
    readonly outputLines: readonly string[];
  },
  fileName: string | undefined,
) => [
  labelLine("recap", taskName, fileName === undefined ? recap.text : `${fileName} | ${recap.text}`),
  ...recap.outputLines.map((outputLine) => labelLine("recap", taskName, recapLine(outputLine))),
];
