import { Schema } from "effect";

import { PRINTABLE_ERROR_FIELDS, TASK_ERROR_FIELDS } from "./error-fields.ts";
import { joinMessages } from "./join-messages.ts";

export class StandardInputNotRead extends Schema.TaggedError<StandardInputNotRead>()(
  "StandardInputNotRead",
  {
    ...TASK_ERROR_FIELDS,
    reportedErrors: Schema.NonEmptyArray(Schema.Struct(PRINTABLE_ERROR_FIELDS)),
  },
) {
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}
