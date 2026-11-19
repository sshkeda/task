import { Cause } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { labelLine, splitLines } from "./label-lines.ts";
import { reportCause } from "./report-cause.ts";
import { relativeToStartDirectory } from "./start-directory-path.ts";

const indentContinuation = (continuationLine: string) =>
  continuationLine === "" ? "" : `  ${continuationLine}`;

const splitContextLines = (printable: PrintableError) =>
  (printable.context ?? []).flatMap((context) => splitLines(context));

const formatLoggedErrorLines = (printable: PrintableError, severity: string) => {
  const place =
    printable.location === undefined
      ? printable.taskName
      : relativeToStartDirectory(printable.location);
  const locationPrefix = place === undefined ? "" : `${place}: `;
  const [firstLine, ...continuationLines] = [
    ...splitLines(`${locationPrefix}${severity} ${printable.tag}: ${printable.message}`),
    ...splitContextLines(printable),
  ];
  return [
    firstLine,
    ...continuationLines.map((continuationLine) => indentContinuation(continuationLine)),
  ];
};

const omitOwnTaskName = (printable: PrintableError, taskName: string | null) => {
  const { taskName: attributedTaskName, ...printableWithoutTaskName } = printable;
  const isOwnOrNoTaskName = attributedTaskName === undefined || attributedTaskName === taskName;
  return isOwnOrNoTaskName ? printableWithoutTaskName : printable;
};

export const formatCauseLines = <Failure>(
  cause: Cause.Cause<Failure>,
  taskName: string | null,
  severity = "error",
) =>
  reportCause(cause)
    .failures.flatMap((reported) =>
      formatLoggedErrorLines(omitOwnTaskName(reported, taskName), severity),
    )
    .join("\n");

export const describeErrorLines = (printable: PrintableError) => {
  const locationPrefix =
    printable.location === undefined ? "" : `${relativeToStartDirectory(printable.location)}: `;
  return [
    ...splitLines(`${locationPrefix}${printable.tag}: ${printable.message.trimEnd()}`),
    ...splitContextLines(printable),
  ];
};

export const listErrorLines = <TaskName extends string | null>(
  printables: readonly PrintableError[],
  taskName: TaskName,
) =>
  printables.flatMap((printable) => {
    const printedTaskName: TaskName | string = printable.taskName ?? taskName;
    return describeErrorLines(printable).map((text) => ({ printedTaskName, text }));
  });

export const formatErrorLines = (printables: readonly PrintableError[], taskName: string | null) =>
  listErrorLines(printables, taskName)
    .map(({ printedTaskName, text }) => labelLine("error", printedTaskName, text))
    .join("\n");

export const formatCauseErrorLines = <Failure>(cause: Cause.Cause<Failure>) =>
  formatErrorLines(reportCause(cause).failures, null);

export const formatFailureLines = (failure: Error) => formatCauseErrorLines(Cause.fail(failure));

export const FAILURE_REASON_NOT_RECORDED_LINE = labelLine(
  "error",
  null,
  "TaskRunFailed: no reason recorded",
);

export const describeFailureLines = (failure: Error) =>
  reportCause(Cause.fail(failure))
    .failures.flatMap((printable) => describeErrorLines(printable))
    .join("\n");
