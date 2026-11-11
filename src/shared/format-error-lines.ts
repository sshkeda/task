import { Cause, String as Strings } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { cutToGraphemes } from "./cut-to-graphemes.ts";
import { reportCause } from "./report-cause.ts";

const relativeToWorkingDirectory = (location: string) => {
  const workingDirectoryPrefix = `${process.cwd()}/`;
  return location.startsWith(workingDirectoryPrefix)
    ? location.slice(workingDirectoryPrefix.length)
    : location;
};

const LINE_BREAK = /\r\n|\r|\n/u;

const indentContinuation = (continuationLine: string) =>
  continuationLine === "" ? "" : `  ${continuationLine}`;

const formatErrorLines = (
  printable: PrintableError,
  severity: string,
  loggedOutput: "left out" | "repeated",
) => {
  const message =
    loggedOutput === "left out"
      ? (printable.messageBeforeOutput ?? printable.message)
      : printable.message;
  const [firstLine, ...continuationLines] = Strings.split(message, LINE_BREAK);
  const locationPrefix =
    printable.location === undefined ? "" : `${relativeToWorkingDirectory(printable.location)}: `;
  return [
    `${locationPrefix}${severity} ${printable.tag}: ${firstLine}`,
    ...continuationLines.map((continuationLine) => indentContinuation(continuationLine)),
  ];
};

const omitTaskNameLocation = (printable: PrintableError, taskName: string | null) => {
  const { location, ...printableWithoutLocation } = printable;
  const isLocationRedundant = location === undefined || location === taskName;
  return isLocationRedundant ? printableWithoutLocation : { ...printableWithoutLocation, location };
};

export const formatCauseLines = <Failure>(
  cause: Cause.Cause<Failure>,
  taskName: string | null,
  severity = "error",
  loggedOutput: Parameters<typeof formatErrorLines>[2] = "repeated",
) =>
  reportCause(cause)
    .failures.flatMap((reported) =>
      formatErrorLines(omitTaskNameLocation(reported, taskName), severity, loggedOutput),
    )
    .join("\n");

export const formatFailureLines = (failure: Error, taskName: string | null) =>
  formatCauseLines(Cause.fail(failure), taskName);

const FAILURE_REASON_LINE_MAX_LENGTH = 200;

export const FAILURE_REASON_NOT_RECORDED_LINE = "TaskRunFailed: no reason recorded";

export const formatFailureReasonLine = (reason: PrintableError) =>
  cutToGraphemes(
    `${reason.tag}: ${reason.message
      .split(LINE_BREAK)
      .map((messageLine) => messageLine.trim())
      .filter((messageLine) => messageLine !== "")
      .join(" | ")}`,
    FAILURE_REASON_LINE_MAX_LENGTH,
  );
