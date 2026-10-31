import {
  Array as Arrays,
  Effect,
  Config,
  ConfigProvider,
  Duration,
  Option,
  Path,
  Schema,
} from "effect";

import {
  BunVersionUnsupported,
  HomeDirectoryPathMissing,
  SetupLimitInvalid,
  StateDirectoryPathBlank,
  WorkerCountAboveMaximum,
  WorkerCountInvalid,
} from "./environment-config-errors.ts";
import { dieOnInvariantFailure } from "./invariant-errors.ts";
import { markRuntimeConfigProvider } from "./name-config-key.ts";
import { TASK_STATE_DIRECTORY_NAME, packageNameParts } from "./names-from-package-json.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const TASK_ENVIRONMENT_VARIABLE_PREFIX = packageNameParts.join("_").toUpperCase();

export const availableParallelism = Effect.sync(() => navigator.hardwareConcurrency);

export const MAXIMUM_WORKER_COUNT = 1024;

export const workingDirectoryPath = Effect.sync(() => process.cwd());

const FIRST_SCRIPT_ARGUMENT_INDEX = 2;

export const commandLineArguments = Effect.sync(() =>
  process.argv.slice(FIRST_SCRIPT_ARGUMENT_INDEX),
);

export const bunArguments = Effect.sync(() => [...process.execArgv]);

const BUN_WATCH_MODE_FLAGS = ["--watch", "--hot"] as const;

export const bunWatchModeFlag = Effect.sync(() =>
  Option.fromUndefinedOr(BUN_WATCH_MODE_FLAGS.find((flag) => process.execArgv.includes(flag))),
);

const isWorkingDirectoryFlag = (flag: string) => flag === "--cwd" || flag.startsWith("--cwd=");

const SUPERVISOR_SOCKET_ARGUMENT_PREFIX = `--${packageNameParts.join("-")}-supervisor-socket=`;

export const supervisedRunCommand = (preloadFlag: string, supervisorSocketPath: string) =>
  Effect.sync(() => [
    process.execPath,
    preloadFlag,
    ...process.execArgv,
    ...(process.execArgv.some((flag) => isWorkingDirectoryFlag(flag))
      ? [`--cwd=${process.cwd()}`]
      : []),
    ...process.argv.slice(1, FIRST_SCRIPT_ARGUMENT_INDEX),
    `${SUPERVISOR_SOCKET_ARGUMENT_PREFIX}${supervisorSocketPath}`,
    ...process.argv.slice(FIRST_SCRIPT_ARGUMENT_INDEX),
  ]);

export const takeSupervisorSocketPath = Effect.sync(() => {
  const supervisorSocketArgument = process.argv[FIRST_SCRIPT_ARGUMENT_INDEX] ?? "";
  if (!supervisorSocketArgument.startsWith(SUPERVISOR_SOCKET_ARGUMENT_PREFIX)) {
    return Option.none();
  }
  process.argv.splice(FIRST_SCRIPT_ARGUMENT_INDEX, 1);
  return Option.some(supervisorSocketArgument.slice(SUPERVISOR_SOCKET_ARGUMENT_PREFIX.length));
});

const NON_BLANK_TEXT = Schema.String.check(Schema.isPattern(/\S/u));

const readEnvironmentKeepingEmptyValues = () =>
  ConfigProvider.fromEnv({ preserveEmptyStrings: true });

const taskOwnConfigProvider = readEnvironmentKeepingEmptyValues();

export const runtimeConfigProviderLayer = ConfigProvider.layer(
  Effect.sync(() => markRuntimeConfigProvider(readEnvironmentKeepingEmptyValues())),
);

const STATE_DIRECTORY_VARIABLE_NAME = `${TASK_ENVIRONMENT_VARIABLE_PREFIX}_STATE_DIRECTORY`;

const WORKER_COUNT_VARIABLE_NAME = `${TASK_ENVIRONMENT_VARIABLE_PREFIX}_WORKERS`;

const readEnvironmentVariable = (variableName: string) =>
  Config.String(variableName)
    .pipe(Config.option)
    .parse(taskOwnConfigProvider)
    .pipe(
      dieOnInvariantFailure(
        "configuration",
        `the environment provides ${variableName} or omits it`,
      ),
    );

const readEnvironmentVariableNow = (variableName: string) =>
  Effect.suspend(() =>
    Config.String(variableName).pipe(Config.option).parse(readEnvironmentKeepingEmptyValues()),
  ).pipe(
    dieOnInvariantFailure("configuration", `the environment provides ${variableName} or omits it`),
  );

const isNonBlankText = Schema.is(NON_BLANK_TEXT);

const isWholeDecimalNumberAboveZero = Schema.is(
  Schema.String.check(Schema.isPattern(/^0*[1-9]\d*$/u)),
);

export const readStateDirectory = Effect.fn("readStateDirectory")(function* () {
  const path = yield* Path.Path;
  const configuredStateDirectoryPath = yield* readEnvironmentVariable(
    STATE_DIRECTORY_VARIABLE_NAME,
  );
  if (Option.isSome(configuredStateDirectoryPath)) {
    if (!isNonBlankText(configuredStateDirectoryPath.value)) {
      return yield* new StateDirectoryPathBlank({
        variableName: STATE_DIRECTORY_VARIABLE_NAME,
        value: configuredStateDirectoryPath.value,
      });
    }
    const stateDirectoryPath = path.resolve(configuredStateDirectoryPath.value);
    return { stateDirectoryPath, configuredStateDirectoryPath: stateDirectoryPath };
  }
  const homeDirectoryPath = yield* readEnvironmentVariable("HOME");
  const isHomeDirectoryPathMissing =
    Option.isNone(homeDirectoryPath) || !isNonBlankText(homeDirectoryPath.value);
  if (isHomeDirectoryPathMissing) {
    return yield* new HomeDirectoryPathMissing({
      stateDirectoryVariableName: STATE_DIRECTORY_VARIABLE_NAME,
      homeValue: Option.getOrNull(homeDirectoryPath),
    });
  }
  return {
    stateDirectoryPath: path.resolve(path.join(homeDirectoryPath.value, TASK_STATE_DIRECTORY_NAME)),
    configuredStateDirectoryPath: null,
  };
});

export const readWorkerCount = Effect.fn("readWorkerCount")(function* () {
  const configuredWorkerCount = yield* readEnvironmentVariable(WORKER_COUNT_VARIABLE_NAME);
  if (Option.isNone(configuredWorkerCount)) {
    return yield* availableParallelism;
  }
  if (!isWholeDecimalNumberAboveZero(configuredWorkerCount.value)) {
    return yield* new WorkerCountInvalid({
      variableName: WORKER_COUNT_VARIABLE_NAME,
      value: configuredWorkerCount.value,
    });
  }
  const workerCount = Number(configuredWorkerCount.value);
  if (workerCount > MAXIMUM_WORKER_COUNT) {
    return yield* new WorkerCountAboveMaximum({
      variableName: WORKER_COUNT_VARIABLE_NAME,
      value: configuredWorkerCount.value,
      maximum: MAXIMUM_WORKER_COUNT,
    });
  }
  return workerCount;
});

const SETUP_LIMIT_VARIABLE_NAME = `${TASK_ENVIRONMENT_VARIABLE_PREFIX}_SETUP_LIMIT`;

const DEFAULT_SETUP_LIMIT = Duration.minutes(1);

const decodeDurationOption = Schema.decodeUnknownOption(Schema.DurationFromString);

export const readSetupLimit = Effect.fn("readSetupLimit")(function* () {
  const configuredSetupLimit = yield* readEnvironmentVariable(SETUP_LIMIT_VARIABLE_NAME);
  if (Option.isNone(configuredSetupLimit)) {
    return DEFAULT_SETUP_LIMIT;
  }
  const setupLimit = decodeDurationOption(configuredSetupLimit.value);
  const isFiniteDurationAboveZero =
    Option.isSome(setupLimit) &&
    Duration.isFinite(setupLimit.value) &&
    Duration.isGreaterThan(setupLimit.value, Duration.zero);
  if (!isFiniteDurationAboveZero) {
    return yield* new SetupLimitInvalid({
      variableName: SETUP_LIMIT_VARIABLE_NAME,
      value: configuredSetupLimit.value,
    });
  }
  return setupLimit.value;
});

export const inheritedEnvironment = Effect.sync(() => ({ ...process.env }));

export const RUN_MARKERS_VARIABLE_NAME = `${TASK_ENVIRONMENT_VARIABLE_PREFIX}_RUN_MARKERS`;

export const inheritedRunMarkers = readEnvironmentVariableNow(RUN_MARKERS_VARIABLE_NAME).pipe(
  Effect.map((runMarkers) =>
    Option.getOrElse(runMarkers, () => "")
      .split(" ")
      .filter((token) => token !== ""),
  ),
);

const VARIABLES_EVERY_COMMAND_SEES = ["HOME", "PATH", "TMPDIR"] as const;

const TASK_CONFIG_EVERY_COMMAND_SEES = [
  STATE_DIRECTORY_VARIABLE_NAME,
  WORKER_COUNT_VARIABLE_NAME,
  SETUP_LIMIT_VARIABLE_NAME,
] as const;

export const environmentEveryCommandSees = Effect.forEach(
  [...VARIABLES_EVERY_COMMAND_SEES, ...TASK_CONFIG_EVERY_COMMAND_SEES],
  (variableName) =>
    readEnvironmentVariableNow(variableName).pipe(
      Effect.map((value) =>
        Option.map(value, (presentValue) => [variableName, presentValue] as const),
      ),
    ),
).pipe(Effect.map((entries) => Object.fromEntries(Arrays.getSomes(entries))));

export const passStateDirectoryToCommands = (configuredStateDirectoryPath: string | null) =>
  Effect.sync(() => {
    if (configuredStateDirectoryPath !== null) {
      process.env[STATE_DIRECTORY_VARIABLE_NAME] = configuredStateDirectoryPath;
    }
  });

export const requireSupportedBunVersion = (
  bunVersion: string,
  supportedRange: string = TASK_PACKAGE.engines.bun,
) =>
  Bun.semver.satisfies(bunVersion.replace(/[-+].*$/u, ""), supportedRange)
    ? Effect.void
    : Effect.fail(
        new BunVersionUnsupported({
          bunVersion,
          supportedRange,
          taskPackageName: TASK_PACKAGE.name,
          taskVersion: TASK_PACKAGE.version,
        }),
      );
