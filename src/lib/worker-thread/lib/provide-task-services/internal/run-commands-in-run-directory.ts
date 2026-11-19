import type { Path } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

const resolveWorkingDirectories = (
  path: Path.Path,
  runDirectoryPath: string,
  command: ChildProcess.Command,
): ChildProcess.Command =>
  ChildProcess.isStandardCommand(command)
    ? ChildProcess.setCwd(command, path.resolve(runDirectoryPath, command.options.cwd ?? "."))
    : ChildProcess.pipeTo(
        resolveWorkingDirectories(path, runDirectoryPath, command.left),
        resolveWorkingDirectories(path, runDirectoryPath, command.right),
        command.options,
      );

export const runCommandsInRunDirectory = (
  path: Path.Path,
  runDirectoryPath: string,
  spawn: ChildProcessSpawner.ChildProcessSpawner["Service"]["spawn"],
) =>
  ChildProcessSpawner.make((command) =>
    spawn(resolveWorkingDirectories(path, runDirectoryPath, command)),
  );
