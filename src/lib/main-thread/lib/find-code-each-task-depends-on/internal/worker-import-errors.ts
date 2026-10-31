import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../../../shared/error-fields.ts";
import {
  ModuleRefusal,
  reportTaskDefinitionViolation,
} from "../../../shared/main-thread-errors.ts";

export class WorkerImportRefused extends Schema.TaggedError<WorkerImportRefused>()(
  "WorkerImportRefused",
  { ...TASK_ERROR_FIELDS, refusal: ModuleRefusal },
) {
  public get location() {
    return reportTaskDefinitionViolation(this.refusal).location;
  }
  public override get message() {
    return `could not rewrite the imports of worker modules because ${reportTaskDefinitionViolation(this.refusal).message}`;
  }
}
