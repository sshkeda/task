import { Array as Arrays, Cause, Exit, Option, PlatformError, Schema } from "effect";

import { signalThatKilled } from "../../../shared/killed-by-signal.ts";
import { CommandStandardInputFailed } from "../../errors/index.ts";

const StandardInputFailures = Schema.Array(CommandStandardInputFailed);

export const CommandEnd = Schema.TaggedUnion({
  Passed: {},
  ExitedWithCode: { code: Schema.Int, standardInputFailures: StandardInputFailures },
  KilledBySignal: { signal: Schema.String },
  ExitFailedWithoutSignal: {
    cause: Schema.Cause(Schema.instanceOf(PlatformError.PlatformError), Schema.Defect()),
  },
  StandardInputFailed: { standardInputFailures: StandardInputFailures },
});

export const decideCommandEnd = (
  exitCodeExit: Exit.Exit<number, PlatformError.PlatformError>,
  standardInputFailures: readonly CommandStandardInputFailed[],
) => {
  const standardInputFailed = CommandEnd.cases.StandardInputFailed.make({ standardInputFailures });
  const hasStandardInputFailures = Arrays.isReadonlyArrayNonEmpty(standardInputFailures);
  if (Exit.isSuccess(exitCodeExit)) {
    if (exitCodeExit.value !== 0) {
      return CommandEnd.cases.ExitedWithCode.make({
        code: exitCodeExit.value,
        standardInputFailures,
      });
    }
    return hasStandardInputFailures ? standardInputFailed : CommandEnd.cases.Passed.make({});
  }
  if (hasStandardInputFailures) {
    return standardInputFailed;
  }
  const signal = Option.getOrNull(
    Option.flatMapNullishOr(Cause.findErrorOption(exitCodeExit.cause), signalThatKilled),
  );
  return signal === null
    ? CommandEnd.cases.ExitFailedWithoutSignal.make({ cause: exitCodeExit.cause })
    : CommandEnd.cases.KilledBySignal.make({ signal });
};
