import { Array as Arrays } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { addContext } from "./report-cause.ts";
import { attributeUnplacedToTask } from "./report-errors.ts";

export const attributeFailures = (
  carrier: {
    readonly taskName: string;
    readonly failures: Arrays.NonEmptyReadonlyArray<PrintableError>;
  },
  context: string | null,
): Arrays.NonEmptyReadonlyArray<PrintableError> =>
  Arrays.map(carrier.failures, (failure) =>
    attributeUnplacedToTask(
      context === null ? failure : addContext(failure, context),
      carrier.taskName,
    ),
  );
