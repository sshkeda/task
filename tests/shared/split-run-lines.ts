import { expect } from "bun:test";

import { Effect } from "effect";

import { exists } from "./access-files.ts";
import { findRunLinePath, withRunLinePathNamed, withSecondsNamed } from "./name-output-files.ts";

export const linesBetweenTheRunLines = (printed: string) => {
  const lines = withRunLinePathNamed(withSecondsNamed(printed.trimEnd())).split("\n");
  expect(lines[0], printed).toStartWith("run: ");
  expect(lines.at(-1), printed).toBe(lines[0]);
  return lines.slice(1, -1);
};

export const withoutTheFirstRunLine = (printed: string) => {
  const [firstLine, ...laterLines] = printed.split("\n");
  expect(firstLine, printed).toBe(`run: ${findRunLinePath(printed)}`);
  return laterLines.join("\n");
};

export const expectNoRunDirectoryOrBothRunLines = (printed: string) =>
  Effect.gen(function* () {
    const lines = printed.trimEnd().split("\n");
    const [firstRunLine, ...laterRunLines] = lines.filter((line) => line.startsWith("run: "));
    if (firstRunLine === undefined) {
      return;
    }
    expect(laterRunLines, printed).toEqual([firstRunLine]);
    expect(lines.at(-1), printed).toBe(firstRunLine);
    const runDirectory = firstRunLine.slice("run: ".length);
    expect(yield* exists(`${runDirectory}trace.jsonl`), printed).toBe(true);
  });
