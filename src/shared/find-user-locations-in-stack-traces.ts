import { Ref } from "effect";

import { makeNodeModulesPackageFinder } from "./find-node-modules-package-directory.ts";

const TASK_SOURCE_DIRECTORY_PATH = Bun.fileURLToPath(new URL("../", import.meta.url));

const EFFECT_PACKAGE_JSON_PATH = Bun.isStandaloneExecutable
  ? null
  : Bun.fileURLToPath(import.meta.resolve("effect/package.json"));
const EFFECT_PACKAGE_DIRECTORY_PATH =
  EFFECT_PACKAGE_JSON_PATH === null
    ? null
    : EFFECT_PACKAGE_JSON_PATH.slice(0, EFFECT_PACKAGE_JSON_PATH.lastIndexOf("/") + 1);

const STACK_FRAME_LOCATION =
  /^\s*at (?:.* \()?(?<filePath>\/.+?):(?<lineAndColumn>\d+(?::\d+)?)\)?$/u;

const parseStackFrames = (stack: string) =>
  stack.split("\n").flatMap((stackLine) => {
    const [, filePath, lineAndColumn] = STACK_FRAME_LOCATION.exec(stackLine) ?? [];
    const isFrameWithoutLocation = filePath === undefined || lineAndColumn === undefined;
    return isFrameWithoutLocation ? [] : [{ filePath, lineAndColumn }];
  });

export const readStackAbove = (frameAboveConstruction: (...parameters: never) => unknown) => {
  const stackHolder: { stack?: string } = {};
  const previousStackTraceLimit = Error.stackTraceLimit;
  Error.stackTraceLimit = Number.POSITIVE_INFINITY;
  try {
    Error.captureStackTrace(stackHolder, frameAboveConstruction);
  } finally {
    Error.stackTraceLimit = previousStackTraceLimit;
  }
  return stackHolder.stack;
};

const findNodeModulesPackageDirectoryPathRef = Ref.makeUnsafe(makeNodeModulesPackageFinder(null));

export const setProjectDirectoryPath = (directoryPath: string) =>
  Ref.set(findNodeModulesPackageDirectoryPathRef, makeNodeModulesPackageFinder(directoryPath));

const isInsideInstalledPackage = (filePath: string) =>
  Ref.getUnsafe(findNodeModulesPackageDirectoryPathRef)(filePath) !== null;

const isUserFrame = (frame: { readonly filePath: string }) =>
  !frame.filePath.startsWith(TASK_SOURCE_DIRECTORY_PATH) &&
  !isInsideInstalledPackage(frame.filePath) &&
  (EFFECT_PACKAGE_DIRECTORY_PATH === null ||
    !frame.filePath.startsWith(EFFECT_PACKAGE_DIRECTORY_PATH));

export const findUserLocationInStack = (stack: string): string | undefined => {
  const userFrame = parseStackFrames(stack).find((frame) => isUserFrame(frame));
  return userFrame === undefined ? undefined : `${userFrame.filePath}:${userFrame.lineAndColumn}`;
};

export const findConstructingLocation = (): string | undefined => {
  const stack = readStackAbove(findConstructingLocation);
  return stack === undefined ? undefined : findUserLocationInStack(stack);
};

export const findLocationInFile = (stack: string, filePath: string): string | undefined => {
  const frameInFile = parseStackFrames(stack).find((frame) => frame.filePath === filePath);
  return frameInFile === undefined ? undefined : `${filePath}:${frameInFile.lineAndColumn}`;
};
