const KILLED_LINE =
  /^killed: (?:\[(?<owner>(?:[^\]\\]|\\.)+)\] (?<ownedProcessId>\d+) \((?<ownedCommand>.*)\)|(?<processId>\d+) \((?<command>.*)\), started by no known task)$/gmu;

export const findKilledProcesses = (standardError: string) =>
  Array.from(standardError.matchAll(KILLED_LINE), ({ groups = {} }) =>
    groups["owner"] === undefined
      ? {
          processId: Number(groups["processId"]),
          command: groups["command"] ?? "",
          by: "no known task",
        }
      : {
          processId: Number(groups["ownedProcessId"]),
          command: groups["ownedCommand"] ?? "",
          by: `[${groups["owner"]}]`,
        },
  );
