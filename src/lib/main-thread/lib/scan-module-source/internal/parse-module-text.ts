import { parseSync } from "oxc-parser";

import type { SCRIPT_LOADERS } from "../../../../../shared/find-bun-loader-for-script.ts";

const PARSER_LANGUAGE_OF_LOADER = {
  js: "jsx",
  jsx: "jsx",
  ts: "ts",
  tsx: "tsx",
} as const satisfies Record<(typeof SCRIPT_LOADERS)[number], string>;

const findParserLanguage = (
  moduleFilePath: string,
  configuredLoader: (typeof SCRIPT_LOADERS)[number] | null,
) => {
  if (configuredLoader !== null) {
    return { lang: PARSER_LANGUAGE_OF_LOADER[configuredLoader] };
  }
  return moduleFilePath.endsWith(".js") ? { lang: "jsx" as const } : {};
};

export const parseModuleText = (
  moduleFilePath: string,
  text: string,
  options: {
    readonly configuredLoader: (typeof SCRIPT_LOADERS)[number] | null;
    readonly sourceType?: "commonjs";
  },
) =>
  parseSync(moduleFilePath, text, {
    preserveParens: false,
    ...findParserLanguage(moduleFilePath, options.configuredLoader),
    ...(options.sourceType === undefined ? {} : { sourceType: options.sourceType }),
  });
