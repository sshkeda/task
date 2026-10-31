import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "./error-fields.ts";
import { describeCauseMessage } from "./report-errors.ts";

export class CodeFileUnreadable extends Schema.TaggedError<CodeFileUnreadable>()(
  "CodeFileUnreadable",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not read it because ${describeCauseMessage(this.cause)}`;
  }
}

export class BunfigUnparsable extends Schema.TaggedError<BunfigUnparsable>()("BunfigUnparsable", {
  ...TASK_ERROR_FIELDS,
  filePath: Schema.String,
  cause: Schema.Defect(),
}) {
  public override get message() {
    return `could not read the loaders it configures because ${describeCauseMessage(this.cause)}`;
  }
}

export class ImportResolutionThrew extends Schema.TaggedError<ImportResolutionThrew>()(
  "ImportResolutionThrew",
  {
    ...TASK_ERROR_FIELDS,
    importingFilePath: Schema.String,
    specifier: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public get location() {
    return this.importingFilePath;
  }
  public override get message() {
    return `resolving the import "${this.specifier}" threw ${describeCauseMessage(this.cause)}`;
  }
}

export class RewrittenModuleUnloadable extends Schema.TaggedError<RewrittenModuleUnloadable>()(
  "RewrittenModuleUnloadable",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String },
) {
  public override get message() {
    return "is not a script among the rewritten worker imports";
  }
}
