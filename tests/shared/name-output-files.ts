import type { Effect } from "effect";

import type { makeProjectUsingTaskFrom } from "./make-projects.ts";
import { TASK_STATE_DIRECTORY_NAME } from "./run-task-processes.ts";

const withRunDirectoryPathsNamed = (
  printed: string,
  runDirectoryPaths: readonly string[],
): string => {
  const [runDirectoryPath, ...laterRunDirectoryPaths] = runDirectoryPaths;
  return runDirectoryPath === undefined
    ? printed
    : withRunDirectoryPathsNamed(
        printed.replaceAll(runDirectoryPath, "<run folder>/"),
        laterRunDirectoryPaths,
      );
};

export const withRunDirectoryNamed = (
  printed: string,
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
) => {
  const runLinePrefix = `run: ${project.home}/${TASK_STATE_DIRECTORY_NAME}/runs/project/`;
  const runDirectoryPaths = new Set(
    printed
      .split("\n")
      .filter((line) => line.startsWith(runLinePrefix) && line.endsWith("/"))
      .map((line) => line.slice("run: ".length)),
  );
  return withRunDirectoryPathsNamed(printed, [...runDirectoryPaths]);
};

const RUN_LINE_PREFIX = "run: ";

export const findRunLinePath = (printed: string) =>
  printed
    .split("\n")
    .findLast((line) => line.startsWith(RUN_LINE_PREFIX))
    ?.slice(RUN_LINE_PREFIX.length);

export const withRunLinePathNamed = (text: string, printed = text) => {
  const runLinePath = findRunLinePath(printed);
  return runLinePath === undefined ? text : text.replaceAll(runLinePath, "<run folder>/");
};

export const findRunDirectoryPath = (printed: string) =>
  findRunLinePath(printed)?.replace(/\/$/u, "") ?? null;

export const findTraceFilePath = (printed: string) => {
  const runDirectoryPath = findRunDirectoryPath(printed);
  return runDirectoryPath === null ? null : `${runDirectoryPath}/trace.jsonl`;
};

export const withSecondsNamed = (printed: string) =>
  printed.replaceAll(/\d+\.\d+s(?=$|, (?:\d+\.output|passed|failed|stopped)$)/gmu, "<seconds>");
