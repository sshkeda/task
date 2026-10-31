import { Effect, FileSystem, Formatter, Option, Predicate, Schema, Struct } from "effect";
import type { PlatformError } from "effect";

import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { describeCauseMessage } from "../../../shared/report-errors.ts";
import {
  PackageJsonFieldInvalid,
  PackageJsonNameBlank,
  PackageJsonNameInvalid,
  PackageJsonNameNotString,
  PackageJsonNotJson,
  PackageJsonNotObject,
  PackageJsonUnreadable,
  PackageJsonWithoutName,
} from "./package-json-errors.ts";

const DependencyVersions = Schema.Record(Schema.String, Schema.String).annotate({
  expected: "an object of strings",
});

const CHECKED_PACKAGE_JSON_FIELDS = Schema.Struct({
  version: Schema.String.annotate({ expected: "a string" }),
  dependencies: DependencyVersions,
  devDependencies: DependencyVersions,
  peerDependencies: DependencyVersions,
  optionalDependencies: DependencyVersions,
});

const PackageJsonObject = Schema.Record(Schema.String, Schema.Unknown);

const PackageJsonFields = CHECKED_PACKAGE_JSON_FIELDS.mapFields(Struct.map(Schema.optional));

const NON_BLANK_TEXT = /\S/u;

const isPackageName = Schema.is(
  Schema.String.check(Schema.isPattern(/^(?:@[^./][^/]*\/)?[^./][^/]*$/u)),
);

export const readPackageJsonText = (packageJsonFilePath: string) =>
  FileSystem.FileSystem.use((fileSystem) => fileSystem.readFileString(packageJsonFilePath));

export const toPackageJsonUnreadable =
  (packageJsonFilePath: string) => (cause: PlatformError.PlatformError) =>
    new PackageJsonUnreadable({ filePath: packageJsonFilePath, cause });

export const parsePackageJsonObject = (packageJsonFilePath: string, packageJsonText: string) =>
  Effect.gen(function* () {
    const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
      packageJsonText,
    ).pipe(
      Effect.catchTag("SchemaError", () =>
        Effect.try({
          try: () => Bun.JSONC.parse(packageJsonText),
          catch: (cause) =>
            new PackageJsonNotJson({
              filePath: packageJsonFilePath,
              parserMessage: describeCauseMessage(cause),
            }),
        }).pipe(
          Effect.andThen(
            Effect.fail(
              new PackageJsonNotJson({ filePath: packageJsonFilePath, parserMessage: null }),
            ),
          ),
        ),
      ),
    );
    return yield* Schema.decodeUnknownEffect(PackageJsonObject)(parsed).pipe(
      Effect.mapError(
        () =>
          new PackageJsonNotObject({
            filePath: packageJsonFilePath,
            valueText: Formatter.format(parsed),
          }),
      ),
    );
  });

export const decodePackageJsonName = (
  packageJsonFilePath: string,
  packageJsonObject: typeof PackageJsonObject.Type,
) => {
  const { name } = packageJsonObject;
  if (name === undefined) {
    return Effect.succeedNone;
  }
  if (!Predicate.isString(name)) {
    return Effect.fail(
      new PackageJsonNameNotString({
        filePath: packageJsonFilePath,
        nameText: Formatter.format(name),
      }),
    );
  }
  return NON_BLANK_TEXT.test(name)
    ? Effect.succeedSome(name)
    : Effect.fail(new PackageJsonNameBlank({ filePath: packageJsonFilePath, packageName: name }));
};

export const decodePackageJsonText = (packageJsonFilePath: string) => (packageJsonText: string) =>
  Effect.gen(function* () {
    const packageJsonObject = yield* parsePackageJsonObject(packageJsonFilePath, packageJsonText);
    const name = yield* decodePackageJsonName(packageJsonFilePath, packageJsonObject);
    for (const [fieldName, fieldSchema] of Object.entries(CHECKED_PACKAGE_JSON_FIELDS.fields)) {
      const value = packageJsonObject[fieldName];
      const isInvalidFieldValue = value !== undefined && !Schema.is(fieldSchema)(value);
      if (isInvalidFieldValue) {
        return yield* new PackageJsonFieldInvalid({
          filePath: packageJsonFilePath,
          fieldName,
          valueText: Formatter.format(value),
          expected: yield* Effect.fromNullishOr(
            Schema.resolveAnnotations(fieldSchema)?.expected,
          ).pipe(
            dieOnInvariantFailure(
              "package.json decoding",
              "each checked field names what it expects",
            ),
          ),
        });
      }
    }
    const fields = yield* Schema.decodeEffect(PackageJsonFields)(packageJsonObject).pipe(
      dieOnInvariantFailure("package.json decoding", "fields that passed their checks decode"),
    );
    return { ...fields, name: Option.getOrUndefined(name) };
  });

export const readPackageJson = (packageJsonDirectoryPath: string) => {
  const packageJsonFilePath = `${packageJsonDirectoryPath}/package.json`;
  return readPackageJsonText(packageJsonFilePath).pipe(
    Effect.mapError(toPackageJsonUnreadable(packageJsonFilePath)),
    Effect.flatMap(decodePackageJsonText(packageJsonFilePath)),
    Effect.flatMap(({ name, ...packageJson }) =>
      Effect.gen(function* () {
        if (name === undefined) {
          return yield* new PackageJsonWithoutName({ filePath: packageJsonFilePath });
        }
        if (!isPackageName(name)) {
          return yield* new PackageJsonNameInvalid({
            filePath: packageJsonFilePath,
            packageName: name,
          });
        }
        return { ...packageJson, name };
      }),
    ),
  );
};
