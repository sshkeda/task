export const baseNameOf = (filePath: string) => filePath.slice(filePath.lastIndexOf("/") + 1);

export const nameWithoutExecutable = (name: string) => `${name}, executable unknown`;

export const nameFromArgumentsOf = (argumentWords: readonly string[], commandName: string) => {
  const [programPath = ""] = argumentWords;
  const programName = baseNameOf(programPath);
  return nameWithoutExecutable(programName === "" ? commandName : programName);
};
