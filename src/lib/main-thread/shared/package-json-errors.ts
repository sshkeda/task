import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";
import { describeCauseMessage } from "../../../shared/report-errors.ts";

export class PackageJsonUnreadable extends Schema.TaggedError<PackageJsonUnreadable>()(
  "PackageJsonUnreadable",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not read it because ${describeCauseMessage(this.cause)}`;
  }
}

export class PackageJsonNotJson extends Schema.TaggedError<PackageJsonNotJson>()(
  "PackageJsonNotJson",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, parserMessage: Schema.NullOr(Schema.String) },
) {
  public override get message() {
    return this.parserMessage === null
      ? "is not JSON"
      : `is not JSON because ${this.parserMessage}`;
  }
}

export class PackageJsonNotObject extends Schema.TaggedError<PackageJsonNotObject>()(
  "PackageJsonNotObject",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, valueText: Schema.String },
) {
  public override get message() {
    return `holds ${this.valueText}, not an object`;
  }
}

export class PackageJsonNameNotString extends Schema.TaggedError<PackageJsonNameNotString>()(
  "PackageJsonNameNotString",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, nameText: Schema.String },
) {
  public override get message() {
    return `has the "name" ${this.nameText}, which is not a string`;
  }
}

export class PackageJsonNameBlank extends Schema.TaggedError<PackageJsonNameBlank>()(
  "PackageJsonNameBlank",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, packageName: Schema.String },
) {
  public override get message() {
    return `has the "name" "${this.packageName}", which is blank`;
  }
}

export class PackageJsonNameInvalid extends Schema.TaggedError<PackageJsonNameInvalid>()(
  "PackageJsonNameInvalid",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, packageName: Schema.String },
) {
  public override get message() {
    return `has the "name" "${this.packageName}", which is not a package name: "name" or "@scope/name" with no part starting with "."`;
  }
}

export class PackageJsonFieldInvalid extends Schema.TaggedError<PackageJsonFieldInvalid>()(
  "PackageJsonFieldInvalid",
  {
    ...TASK_ERROR_FIELDS,
    filePath: Schema.String,
    fieldName: Schema.String,
    valueText: Schema.String,
    expected: Schema.String,
  },
) {
  public override get message() {
    return `has the "${this.fieldName}" ${this.valueText}, which is not ${this.expected}`;
  }
}

export class PackageJsonWithoutName extends Schema.TaggedError<PackageJsonWithoutName>()(
  "PackageJsonWithoutName",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String },
) {
  public override get message() {
    return `has no "name"`;
  }
}

export class PackageJsonSearchFailed extends Schema.TaggedError<PackageJsonSearchFailed>()(
  "PackageJsonSearchFailed",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not check for package.json because ${describeCauseMessage(this.cause)}`;
  }
}
