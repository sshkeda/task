import { Ref } from "effect";

import {
  isInsideProjectOutsideNodeModules,
  isOutsideNodeModules,
} from "./is-inside-project-outside-node-modules.ts";

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

const ALL_CALL_SITES_LIMIT = Number.MAX_SAFE_INTEGER;

const ABSOLUTE_PATH_START = "/";

const parseStackFrames = (stack: string) =>
  stack.split("\n").flatMap((stackLine) => {
    const [, filePath, lineAndColumn] = STACK_FRAME_LOCATION.exec(stackLine) ?? [];
    const isFrameWithoutLocation = filePath === undefined || lineAndColumn === undefined;
    return isFrameWithoutLocation ? [] : [{ filePath, lineAndColumn }];
  });

const frameInFileOf = (callSite: NodeJS.CallSite) => {
  const filePath = callSite.getFileName();
  const line = callSite.getLineNumber();
  const column = callSite.getColumnNumber();
  const isInFile =
    filePath !== null &&
    filePath.startsWith(ABSOLUTE_PATH_START) &&
    !callSite.isNative() &&
    !callSite.isEval() &&
    line !== null &&
    column !== null;
  return isInFile ? [{ filePath, lineAndColumn: `${line}:${column}` }] : [];
};

const isWritableValue = (descriptor: PropertyDescriptor | undefined) =>
  descriptor?.writable === true;

const captureCallSitesAbove = (
  frameAboveCapture: (...parameters: never) => unknown,
  stackTraceLimit: PropertyDescriptor | undefined,
) => {
  const callSites: NodeJS.CallSite[] = [];
  Error.prepareStackTrace = (_error, capturedCallSites) => {
    callSites.push(...capturedCallSites);
  };
  if (!isWritableValue(stackTraceLimit)) {
    Error.captureStackTrace({}, frameAboveCapture);
    return callSites;
  }
  try {
    Error.stackTraceLimit = ALL_CALL_SITES_LIMIT;
    Error.captureStackTrace({}, frameAboveCapture);
  } finally {
    Reflect.set(Error, "stackTraceLimit", stackTraceLimit?.value);
  }
  return callSites;
};

const readFramesAbove = (frameAboveCapture: (...parameters: never) => unknown) => {
  const prepareStackTrace = Object.getOwnPropertyDescriptor(Error, "prepareStackTrace");
  const stackTraceLimit = Object.getOwnPropertyDescriptor(Error, "stackTraceLimit");
  if (!isWritableValue(prepareStackTrace)) {
    return [];
  }
  try {
    return captureCallSitesAbove(frameAboveCapture, stackTraceLimit).flatMap((callSite) =>
      frameInFileOf(callSite),
    );
  } finally {
    Reflect.set(Error, "prepareStackTrace", prepareStackTrace?.value);
  }
};

const projectDirectoryPathRef = Ref.makeUnsafe<string | null>(null);

export const setProjectDirectoryPath = (directoryPath: string) =>
  Ref.set(projectDirectoryPathRef, directoryPath);

export const readProjectDirectoryPath = () => Ref.getUnsafe(projectDirectoryPathRef);

export const isProjectFile = (filePath: string) =>
  isInsideProjectOutsideNodeModules(Ref.getUnsafe(projectDirectoryPathRef), filePath);

const LINE_AND_COLUMN = /(?::\d+){1,2}$/u;

export const filePathOfLocation = (location: string) => location.replace(LINE_AND_COLUMN, "");

export const findProjectLocation = (location: string | undefined) => {
  const isInTheProject = location !== undefined && isProjectFile(filePathOfLocation(location));
  return isInTheProject ? location : undefined;
};

const isOutsideTask = (frame: { readonly filePath: string }) =>
  !frame.filePath.startsWith(TASK_SOURCE_DIRECTORY_PATH);

const isOutsideTaskAndEffect = (frame: { readonly filePath: string }) =>
  isOutsideTask(frame) &&
  (EFFECT_PACKAGE_DIRECTORY_PATH === null ||
    !frame.filePath.startsWith(EFFECT_PACKAGE_DIRECTORY_PATH));

const isUserFrame = (frame: { readonly filePath: string }) =>
  isOutsideTaskAndEffect(frame) && isOutsideNodeModules(frame.filePath);

const describeFrame = (frame: { readonly filePath: string; readonly lineAndColumn: string }) =>
  `${frame.filePath}:${frame.lineAndColumn}`;

export const findUserLocationInStack = (stack: string): string | undefined => {
  const userFrame = parseStackFrames(stack).find((frame) => isUserFrame(frame));
  return userFrame === undefined ? undefined : describeFrame(userFrame);
};

export const findCallingLocation = (): string | undefined => {
  const callingFrame = readFramesAbove(findCallingLocation).find((frame) =>
    isOutsideTaskAndEffect(frame),
  );
  return callingFrame === undefined ? undefined : describeFrame(callingFrame);
};

export const findCallingUserLocation = (): string | undefined => {
  const callingFrame = readFramesAbove(findCallingUserLocation).find((frame) =>
    isOutsideTask(frame),
  );
  const isCalledFromUserCode = callingFrame !== undefined && isUserFrame(callingFrame);
  return isCalledFromUserCode ? describeFrame(callingFrame) : undefined;
};

export const findConstructingLocation = (): string | undefined => {
  const userFrame = readFramesAbove(findConstructingLocation).find((frame) => isUserFrame(frame));
  return userFrame === undefined ? undefined : describeFrame(userFrame);
};

export const findMakingLocation = (): string | undefined => {
  const frames = readFramesAbove(findMakingLocation);
  const makingFrame =
    frames.find((frame) => isUserFrame(frame)) ??
    frames.find((frame) => isOutsideTaskAndEffect(frame));
  return makingFrame === undefined ? undefined : describeFrame(makingFrame);
};

export const findLocationInFile = (stack: string, filePath: string): string | undefined => {
  const frameInFile = parseStackFrames(stack).find((frame) => frame.filePath === filePath);
  return frameInFile === undefined ? undefined : `${filePath}:${frameInFile.lineAndColumn}`;
};
