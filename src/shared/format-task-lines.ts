import { DateTime } from "effect";

const MILLISECONDS_PER_SECOND = 1000;

export const formatSeconds = (elapsedMilliseconds: number) =>
  `${(elapsedMilliseconds / MILLISECONDS_PER_SECOND).toFixed(1)}s`;

export const formatIsoTimestamp = (millisecondsSinceEpoch: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(millisecondsSinceEpoch));

export const formatTaskPassed = (elapsedMilliseconds: number) =>
  `passed in ${formatSeconds(elapsedMilliseconds)}`;

export const formatTaskServing = (elapsedMilliseconds: number) =>
  `serving after ${formatSeconds(elapsedMilliseconds)}`;

export const prefixLines = (taskName: string, text: string) =>
  text
    .split("\n")
    .map((line) => `[${taskName}] ${line}`)
    .join("\n");
