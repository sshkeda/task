import { FileSystem, Path } from "effect";

export const resolvePathsFromPackageDirectory = (
  path: Path.Path,
  fileSystem: FileSystem.FileSystem,
  packageDirectoryPath: string,
) => {
  const resolve = (...pathSegments: readonly string[]) =>
    path.resolve(packageDirectoryPath, ...pathSegments);
  const resolveTemporaryDirectory = <Options extends { readonly directory?: string | undefined }>(
    temporaryOptions: Options | undefined,
  ) =>
    temporaryOptions?.directory === undefined
      ? temporaryOptions
      : { ...temporaryOptions, directory: resolve(temporaryOptions.directory) };
  const packagePath = Path.Path.of({
    ...path,
    resolve,
    relative: (fromPath, toPath) => path.relative(resolve(fromPath), resolve(toPath)),
    toFileUrl: (filePath) => path.toFileUrl(resolve(filePath)),
  });
  const packageFileSystem = FileSystem.make({
    ...fileSystem,
    access: (filePath, accessOptions) => fileSystem.access(resolve(filePath), accessOptions),
    copy: (fromPath, toPath, copyOptions) =>
      fileSystem.copy(resolve(fromPath), resolve(toPath), copyOptions),
    copyFile: (fromPath, toPath) => fileSystem.copyFile(resolve(fromPath), resolve(toPath)),
    chmod: (filePath, mode) => fileSystem.chmod(resolve(filePath), mode),
    chown: (filePath, userId, groupId) => fileSystem.chown(resolve(filePath), userId, groupId),
    link: (fromPath, toPath) => fileSystem.link(resolve(fromPath), resolve(toPath)),
    makeDirectory: (directoryPath, directoryOptions) =>
      fileSystem.makeDirectory(resolve(directoryPath), directoryOptions),
    makeTempDirectory: (temporaryOptions) =>
      fileSystem.makeTempDirectory(resolveTemporaryDirectory(temporaryOptions)),
    makeTempDirectoryScoped: (temporaryOptions) =>
      fileSystem.makeTempDirectoryScoped(resolveTemporaryDirectory(temporaryOptions)),
    makeTempFile: (temporaryOptions) =>
      fileSystem.makeTempFile(resolveTemporaryDirectory(temporaryOptions)),
    makeTempFileScoped: (temporaryOptions) =>
      fileSystem.makeTempFileScoped(resolveTemporaryDirectory(temporaryOptions)),
    open: (filePath, openOptions) => fileSystem.open(resolve(filePath), openOptions),
    readDirectory: (directoryPath, readDirectoryOptions) =>
      fileSystem.readDirectory(resolve(directoryPath), readDirectoryOptions),
    readDirectoryEntries: (directoryPath) =>
      fileSystem.readDirectoryEntries(resolve(directoryPath)),
    readFile: (filePath) => fileSystem.readFile(resolve(filePath)),
    readLink: (filePath) => fileSystem.readLink(resolve(filePath)),
    realPath: (filePath) => fileSystem.realPath(resolve(filePath)),
    remove: (filePath, removeOptions) => fileSystem.remove(resolve(filePath), removeOptions),
    rename: (fromPath, toPath) => fileSystem.rename(resolve(fromPath), resolve(toPath)),
    stat: (filePath) => fileSystem.stat(resolve(filePath)),
    symlink: (targetPath, linkPath) => fileSystem.symlink(targetPath, resolve(linkPath)),
    truncate: (filePath, length) => fileSystem.truncate(resolve(filePath), length),
    utimes: (filePath, accessed, modified) =>
      fileSystem.utimes(resolve(filePath), accessed, modified),
    watch: (watchedPath, watchOptions) => fileSystem.watch(resolve(watchedPath), watchOptions),
    writeFile: (filePath, contents, writeOptions) =>
      fileSystem.writeFile(resolve(filePath), contents, writeOptions),
  });
  return { path: packagePath, fileSystem: packageFileSystem };
};
