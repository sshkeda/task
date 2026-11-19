import { Array as Arrays, Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { reportWrappedCause } from "../../../shared/report-cause.ts";
import type { PrintableError } from "../../errors/index.ts";

export class PackageJsonUnreadable extends Schema.TaggedError<PackageJsonUnreadable>()(
  "PackageJsonUnreadable",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public get reportedErrors() {
    return reportWrappedCause(this, "could not read package.json");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class PackageJsonNotJson extends Schema.TaggedError<PackageJsonNotJson>()(
  "PackageJsonNotJson",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.NullOr(Schema.Defect()) },
) {
  public get reportedErrors(): Arrays.NonEmptyReadonlyArray<PrintableError> {
    return this.cause === null
      ? Arrays.of({ tag: this._tag, location: this.filePath, message: "is not JSON" })
      : reportWrappedCause(this, "could not parse package.json");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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

export class PackageJsonSearchFailed extends Schema.TaggedError<PackageJsonSearchFailed>()(
  "PackageJsonSearchFailed",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public get reportedErrors() {
    return reportWrappedCause(this, "could not check for package.json");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}
