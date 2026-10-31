import type { Path } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

const resolveWorkingDirectories = (
  path: Path.Path,
  packageDirectoryPath: string,
  command: ChildProcess.Command,
): ChildProcess.Command =>
  ChildProcess.isStandardCommand(command)
    ? ChildProcess.setCwd(command, path.resolve(packageDirectoryPath, command.options.cwd ?? "."))
    : ChildProcess.pipeTo(
        resolveWorkingDirectories(path, packageDirectoryPath, command.left),
        resolveWorkingDirectories(path, packageDirectoryPath, command.right),
        command.options,
      );

export const runCommandsInPackageDirectory = (
  path: Path.Path,
  packageDirectoryPath: string,
  spawn: ChildProcessSpawner.ChildProcessSpawner["Service"]["spawn"],
) =>
  ChildProcessSpawner.make((command) =>
    spawn(resolveWorkingDirectories(path, packageDirectoryPath, command)),
  );
