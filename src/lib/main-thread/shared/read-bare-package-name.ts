const BARE_PACKAGE = /^(?![./]|[a-z]+:)(?<packageName>(?:@[^/@]+\/)?[^/@]+)/u;

export const readBarePackageName = (specifier: string) => BARE_PACKAGE.exec(specifier)?.[1];
