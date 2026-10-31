export const SCRIPT_LOADERS = ["js", "jsx", "ts", "tsx"] as const;

const LOADER_OF_SCRIPT_EXTENSION = new Map<string, (typeof SCRIPT_LOADERS)[number]>([
  [".ts", "ts"],
  [".mts", "ts"],
  [".cts", "ts"],
  [".tsx", "tsx"],
  [".js", "js"],
  [".mjs", "js"],
  [".cjs", "js"],
  [".jsx", "jsx"],
]);

export const isDataLoader = (loader: string | null) =>
  loader !== null && !SCRIPT_LOADERS.some((scriptLoader) => scriptLoader === loader);

export const findBunLoaderForScript = (
  filePath: string,
  configuredLoaderOfExtension: ReadonlyMap<string, string>,
) => {
  const extension = /\.[^./]+$/u.exec(filePath)?.[0];
  if (extension === undefined) {
    return null;
  }
  const configuredLoader = configuredLoaderOfExtension.get(extension);
  if (configuredLoader === undefined) {
    const loader = LOADER_OF_SCRIPT_EXTENSION.get(extension);
    return loader === undefined ? null : { loader, isConfigured: false };
  }
  const loader = SCRIPT_LOADERS.find((scriptLoader) => scriptLoader === configuredLoader);
  return loader === undefined ? null : { loader, isConfigured: true };
};
