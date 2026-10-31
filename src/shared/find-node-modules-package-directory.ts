const NODE_MODULES_DIRECTORY_NAME = "node_modules";

const SCOPED_PACKAGE_NAME_SEGMENT_COUNT = 2;

const UNSCOPED_PACKAGE_NAME_SEGMENT_COUNT = 1;

export const makeNodeModulesPackageFinder =
  (projectDirectoryPath: string | null) =>
  (filePath: string): string | null => {
    const segments = filePath.split("/");
    const isInsideTheProject =
      projectDirectoryPath !== null && filePath.startsWith(`${projectDirectoryPath}/`);
    const firstSegmentIndexBelowProject = isInsideTheProject
      ? projectDirectoryPath.split("/").length
      : 0;
    const nodeModulesIndex = segments.lastIndexOf(NODE_MODULES_DIRECTORY_NAME);
    const packageNameSegmentCount =
      segments[nodeModulesIndex + 1]?.startsWith("@") === true
        ? SCOPED_PACKAGE_NAME_SEGMENT_COUNT
        : UNSCOPED_PACKAGE_NAME_SEGMENT_COUNT;
    const hasPackageNameAfterNodeModulesBelowProject =
      nodeModulesIndex >= firstSegmentIndexBelowProject &&
      nodeModulesIndex + packageNameSegmentCount < segments.length;
    if (!hasPackageNameAfterNodeModulesBelowProject) {
      return null;
    }
    const packageDirectoryPath = segments
      .slice(0, nodeModulesIndex + 1 + packageNameSegmentCount)
      .join("/");
    const holdsTheProject =
      projectDirectoryPath !== null &&
      (projectDirectoryPath === packageDirectoryPath ||
        projectDirectoryPath.startsWith(`${packageDirectoryPath}/`));
    return holdsTheProject ? null : packageDirectoryPath;
  };
