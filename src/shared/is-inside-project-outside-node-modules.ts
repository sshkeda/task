const NODE_MODULES_DIRECTORY_NAME = "node_modules";

const isInsideProject = (projectDirectoryPath: string | null, filePath: string) =>
  projectDirectoryPath === null || filePath.startsWith(`${projectDirectoryPath}/`);

const isInsideNodeModules = (filePath: string) =>
  filePath.split("/").slice(0, -1).includes(NODE_MODULES_DIRECTORY_NAME);

export const isOutsideNodeModules = (filePath: string) => !isInsideNodeModules(filePath);

export const isInsideProjectOutsideNodeModules = (
  projectDirectoryPath: string | null,
  filePath: string,
) => isInsideProject(projectDirectoryPath, filePath) && !isInsideNodeModules(filePath);
