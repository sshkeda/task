export const findKilledProcesses = (standardError: string) =>
  Array.from(
    standardError.matchAll(
      /^killed (?<processId>\d+) \((?<command>.*)\), started by (?<by>.*)$/gmu,
    ),
    ([, processId, command, by]) => ({
      processId: Number(processId),
      command: command ?? "",
      by: by ?? "",
    }),
  );
