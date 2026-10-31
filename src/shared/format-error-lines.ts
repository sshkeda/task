import { Cause, String as Strings } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { cutToGraphemes } from "./cut-to-graphemes.ts";
import { reportCause } from "./report-cause.ts";

export const relativeToWorkingDirectory = (location: string) => {
  const workingDirectoryPrefix = `${process.cwd()}/`;
  return location.startsWith(workingDirectoryPrefix)
    ? location.slice(workingDirectoryPrefix.length)
    : location;
};

const indentContinuation = (continuationLine: string) =>
  continuationLine === "" ? "" : `  ${continuationLine}`;

const formatErrorLines = (printable: PrintableError) => {
  const [firstLine, ...continuationLines] = Strings.split(printable.message, "\n");
  const locationPrefix =
    printable.location === undefined ? "" : `${relativeToWorkingDirectory(printable.location)}: `;
  return [
    `${locationPrefix}error ${printable.tag}: ${firstLine}`,
    ...continuationLines.map((continuationLine) => indentContinuation(continuationLine)),
  ];
};

const omitTaskNameLocation = (printable: PrintableError, taskName: string | null) => {
  const { location, ...printableWithoutLocation } = printable;
  const isLocationRedundant = location === undefined || location === taskName;
  return isLocationRedundant ? printableWithoutLocation : { ...printableWithoutLocation, location };
};

export const formatCauseLines = <Failure>(cause: Cause.Cause<Failure>, taskName: string | null) =>
  reportCause(cause)
    .failures.flatMap((reported) => formatErrorLines(omitTaskNameLocation(reported, taskName)))
    .join("\n");

export const formatFailureLines = (failure: Error, taskName: string | null) =>
  formatCauseLines(Cause.fail(failure), taskName);

const FAILURE_REASON_LINE_MAX_LENGTH = 200;

export const FAILURE_REASON_NOT_RECORDED_LINE = "TaskRunFailed: no reason recorded";

export const formatFailureReasonLine = (reason: PrintableError) =>
  cutToGraphemes(
    `${reason.tag}: ${reason.message
      .split("\n")
      .map((messageLine) => messageLine.trim())
      .filter((messageLine) => messageLine !== "")
      .join(" | ")}`,
    FAILURE_REASON_LINE_MAX_LENGTH,
  );
