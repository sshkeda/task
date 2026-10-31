import { Visitor } from "oxc-parser";
import type {
  Argument,
  CallExpression,
  Expression,
  ImportAttribute,
  parseSync,
  Program,
  TSImportEqualsDeclaration,
} from "oxc-parser";

import { readLiteralString } from "./read-literal-string.ts";
import { visitNonReferenceIdentifiers } from "./visit-non-reference-identifiers.ts";

const REQUIRE_FUNCTION_NAMES = new Set(["require", "__require"]);

const MODULE_LOADER_NAMES = new Set([...REQUIRE_FUNCTION_NAMES, "createRequire"]);

const readAttributeLoader = (attributes: readonly ImportAttribute[]) =>
  attributes.find((attribute) =>
    attribute.key.type === "Identifier"
      ? attribute.key.name === "type"
      : attribute.key.value === "type",
  )?.value.value ?? null;

const readObjectProperty = (expression: Expression | null, name: string) => {
  if (expression?.type !== "ObjectExpression") {
    return;
  }
  const properties = expression.properties;
  const hasComputedOrSpreadProperties = properties.some(
    (property) => property.type === "SpreadElement" || property.computed,
  );
  if (hasComputedOrSpreadProperties) {
    return;
  }
  for (const property of properties.toReversed()) {
    const isNamedProperty =
      property.type === "Property" &&
      (property.key.type === "Identifier"
        ? property.key.name === name
        : property.key.type === "Literal" && property.key.value === name);
    if (isNamedProperty) {
      return property.value;
    }
  }
  return null;
};

const readDynamicImportLoader = (options: Expression | null) => {
  if (options === null) {
    return null;
  }
  const attributes = readObjectProperty(options, "with");
  const hasNoAttributes = attributes === undefined || attributes === null;
  if (hasNoAttributes) {
    return attributes;
  }
  const loader = readObjectProperty(attributes, "type");
  const hasNoLoader = loader === undefined || loader === null;
  if (hasNoLoader) {
    return loader;
  }
  return readLiteralString(loader) ?? undefined;
};

const LITERAL_PATH_PREFIX = /^\.{0,2}\//u;

const readLiteralPrefix = (argument: Argument | undefined): string | null => {
  if (argument?.type === "TemplateLiteral") {
    return argument.quasis[0]?.value.cooked ?? null;
  }
  const isConcatenation = argument?.type === "BinaryExpression" && argument.operator === "+";
  if (isConcatenation) {
    return readLiteralPrefix(argument.left);
  }
  return readLiteralString(argument);
};

const readLiteralDirectory = (argument: Argument | undefined) => {
  const prefix = readLiteralPrefix(argument);
  const namesPathDirectory = prefix !== null && LITERAL_PATH_PREFIX.test(prefix);
  return namesPathDirectory ? prefix.slice(0, prefix.lastIndexOf("/") + 1) : null;
};

const readRequireResolveName = (callee: CallExpression["callee"]) => {
  const isResolveMember =
    callee.type === "MemberExpression" && !callee.computed && callee.property.name === "resolve";
  if (!isResolveMember) {
    return null;
  }
  const owner = callee.object;
  const isRequire = owner.type === "Identifier" && owner.name === "require";
  if (isRequire) {
    return "require.resolve";
  }
  const isGlobalRequire =
    owner.type === "MemberExpression" &&
    !owner.computed &&
    owner.object.type === "Identifier" &&
    owner.object.name === "globalThis" &&
    owner.property.name === "require";
  return isGlobalRequire ? "globalThis.require.resolve" : null;
};

export const readRequireFunctionName = (call: CallExpression) => {
  const { callee } = call;
  const isRequireFunction = callee.type === "Identifier" && REQUIRE_FUNCTION_NAMES.has(callee.name);
  return isRequireFunction ? callee.name : null;
};

const readValueImportEqualsPath = (declaration: TSImportEqualsDeclaration) => {
  const { moduleReference } = declaration;
  const isValueExternalModuleImport =
    declaration.importKind !== "type" && moduleReference.type === "TSExternalModuleReference";
  return isValueExternalModuleImport ? moduleReference.expression : null;
};

export const listValueImports = (parsed: ReturnType<typeof parseSync>) => {
  const typeOnlyImportDeclarationStarts = new Set(
    parsed.program.body.flatMap((statement) => {
      const isTypeOnlyImport =
        statement.type === "ImportDeclaration" && statement.importKind === "type";
      return isTypeOnlyImport ? [statement.start] : [];
    }),
  );
  return parsed.module.staticImports.flatMap((declaration) => {
    const valueEntries = declaration.entries.filter((importEntry) => !importEntry.isType);
    const importsOnlyTypes =
      typeOnlyImportDeclarationStarts.has(declaration.start) ||
      (declaration.entries.length > 0 && valueEntries.length === 0);
    return importsOnlyTypes ? [] : [{ declaration, valueEntries }];
  });
};

export const listStaticImportSpecifiers = (parsed: ReturnType<typeof parseSync>) => {
  const loaders = new Map(
    parsed.program.body.flatMap((statement) => {
      const hasImportAttributes =
        statement.type === "ImportDeclaration" ||
        statement.type === "ExportNamedDeclaration" ||
        statement.type === "ExportAllDeclaration";
      if (!hasImportAttributes) {
        return [];
      }
      const { source, attributes } = statement;
      return source === null ? [] : [[source.start, readAttributeLoader(attributes)] as const];
    }),
  );
  return [
    ...listValueImports(parsed).map(({ declaration }) => declaration.moduleRequest),
    ...parsed.module.staticExports.flatMap((declaration) =>
      declaration.entries.flatMap(({ moduleRequest, isType }) => {
        const reexportsValues = moduleRequest !== null && !isType;
        return reexportsValues ? [moduleRequest] : [];
      }),
    ),
  ].map((moduleRequest) => ({
    value: moduleRequest.value,
    start: moduleRequest.start,
    end: moduleRequest.end,
    kind: "import" as const,
    loader: loaders.get(moduleRequest.start) ?? null,
  }));
};

export const findModuleLoads = (program: Program, text: string) => {
  const literalLoads: {
    readonly value: string;
    readonly start: number;
    readonly end: number;
    readonly kind: "import" | "require.resolve" | "require";
    readonly loader: string | null;
  }[] = [];
  const unfollowedLoads: {
    readonly start: number;
    readonly loader: string;
    readonly argumentText: string | null;
    readonly literalDirectory: string | null;
  }[] = [];
  const nonReferenceStarts = new Set<number>();
  const followedLoaderStarts = new Set<number>();
  new Visitor({
    ...visitNonReferenceIdentifiers(nonReferenceStarts),
    MemberExpression: (member) => {
      if (member.computed) {
        return;
      }
      nonReferenceStarts.add(member.property.start);
      const resolveName = readRequireResolveName(member);
      const isUnfollowedResolver = resolveName !== null && !followedLoaderStarts.has(member.start);
      if (isUnfollowedResolver) {
        unfollowedLoads.push({
          start: member.start,
          loader: resolveName,
          argumentText: null,
          literalDirectory: null,
        });
      }
      const isModuleOrImportMetaRequire =
        member.property.name === "require" &&
        ((member.object.type === "Identifier" && member.object.name === "module") ||
          member.object.type === "MetaProperty");
      if (isModuleOrImportMetaRequire) {
        unfollowedLoads.push({
          start: member.start,
          loader: member.object.type === "MetaProperty" ? "import.meta.require" : "module.require",
          argumentText: null,
          literalDirectory: null,
        });
      }
    },
    ImportExpression: (dynamicImport) => {
      const importedPath = readLiteralString(dynamicImport.source);
      const loader = readDynamicImportLoader(dynamicImport.options);
      if (loader === undefined) {
        unfollowedLoads.push({
          start: dynamicImport.start,
          loader: "import attributes",
          argumentText: null,
          literalDirectory: null,
        });
      } else if (importedPath === null) {
        unfollowedLoads.push({
          start: dynamicImport.start,
          loader: "import",
          argumentText: text.slice(dynamicImport.source.start, dynamicImport.source.end),
          literalDirectory: readLiteralDirectory(dynamicImport.source),
        });
      } else {
        literalLoads.push({
          value: importedPath,
          start: dynamicImport.source.start,
          end: dynamicImport.source.end,
          kind: "import",
          loader,
        });
      }
    },
    CallExpression: (call) => {
      const { callee } = call;
      const resolveName = readRequireResolveName(callee);
      const requireFunctionName = resolveName ?? readRequireFunctionName(call);
      if (requireFunctionName === null) {
        return;
      }
      followedLoaderStarts.add(callee.start);
      const isResolveCall = resolveName !== null && callee.type === "MemberExpression";
      if (isResolveCall) {
        followedLoaderStarts.add(callee.object.start);
        if (call.arguments.length !== 1) {
          unfollowedLoads.push({
            start: call.start,
            loader: resolveName,
            argumentText: null,
            literalDirectory: null,
          });
          return;
        }
      }
      const [requiredPathArgument] = call.arguments;
      const requiredPath = readLiteralString(requiredPathArgument);
      const isUnfollowedRequire = requiredPathArgument === undefined || requiredPath === null;
      if (isUnfollowedRequire) {
        unfollowedLoads.push({
          start: call.start,
          loader: requireFunctionName,
          argumentText:
            requiredPathArgument === undefined
              ? ""
              : text.slice(requiredPathArgument.start, requiredPathArgument.end),
          literalDirectory: readLiteralDirectory(requiredPathArgument),
        });
      } else {
        literalLoads.push({
          value: requiredPath,
          start: requiredPathArgument.start,
          end: requiredPathArgument.end,
          kind: resolveName === null ? "require" : "require.resolve",
          loader: null,
        });
      }
    },
    Identifier: (identifier) => {
      const isUnfollowedLoaderReference =
        MODULE_LOADER_NAMES.has(identifier.name) &&
        !nonReferenceStarts.has(identifier.start) &&
        !followedLoaderStarts.has(identifier.start);
      if (isUnfollowedLoaderReference) {
        unfollowedLoads.push({
          start: identifier.start,
          loader: identifier.name,
          argumentText: null,
          literalDirectory: null,
        });
      }
    },
    TSImportEqualsDeclaration: (declaration) => {
      const requiredPath = readValueImportEqualsPath(declaration);
      if (requiredPath !== null) {
        literalLoads.push({
          value: requiredPath.value,
          start: requiredPath.start,
          end: requiredPath.end,
          kind: "require",
          loader: null,
        });
      }
    },
  }).visit(program);
  return { literalLoads, unfollowedLoads };
};

export const listSpecifiersOnce = <Specifier extends { readonly start: number }>(
  specifiers: readonly Specifier[],
) => [...new Map(specifiers.map((specifier) => [specifier.start, specifier])).values()];
