import { Ref } from "effect";

import { readProjectDirectoryPath } from "./find-user-locations-in-stack-traces.ts";

const startDirectoryPathRef = Ref.makeUnsafe<string | null>(null);

export const setStartDirectoryPath = (directoryPath: string) =>
  Ref.set(startDirectoryPathRef, directoryPath);

const isInside = (directoryPath: string, path: string) => path.startsWith(`${directoryPath}/`);

const climbTo = (fromDirectoryPath: string, toPath: string) => {
  const fromParts = fromDirectoryPath.split("/");
  const toParts = toPath.split("/");
  const firstDifferentIndex = fromParts.findIndex((part, index) => part !== toParts[index]);
  return [
    ...fromParts.slice(firstDifferentIndex).map(() => ".."),
    ...toParts.slice(firstDifferentIndex),
  ].join("/");
};

export const relativeToStartDirectory = (location: string) => {
  const startDirectoryPath = Ref.getUnsafe(startDirectoryPathRef) ?? process.cwd();
  if (isInside(startDirectoryPath, location)) {
    return location.slice(startDirectoryPath.length + 1);
  }
  const projectDirectoryPath = readProjectDirectoryPath();
  const isInsideTheProjectTheRunStartedIn =
    projectDirectoryPath !== null &&
    isInside(projectDirectoryPath, location) &&
    (startDirectoryPath === projectDirectoryPath ||
      isInside(projectDirectoryPath, startDirectoryPath));
  return isInsideTheProjectTheRunStartedIn ? climbTo(startDirectoryPath, location) : location;
};
