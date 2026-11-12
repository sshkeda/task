import { inspect } from "node:util";

import { Array as Arrays, Option, Predicate, Result, Schema } from "effect";
import type { Effect } from "effect";

import type {
  bunEvaluationFlag,
  bunWatchModeFlag,
} from "../../../shared/read-environment-config.ts";
import type { findDefinitionNotGiven } from "../../../shared/record-tasks-made.ts";
import {
  RootTaskGroup,
  TASK_GROUP_KEY,
  TaskGroup,
  TaskListNotRecorded,
} from "../../../shared/root-task-group.ts";
import { TASK_BODY } from "../../../shared/task-body-key.ts";
import {
  DefaultExportNotRunnable,
  RunArgumentsNotAccepted,
  RunCalledInBunEvaluation,
  RunCalledInBunWatchMode,
  RunCalledInStandaloneExecutable,
  StartTaskNotMade,
  TaskDefinitionNotGiven,
  TaskListNotGiven,
  TaskNameNotString,
} from "../../../shared/task-errors.ts";
import {
  FlagGivenTwice,
  FlagInvalid,
  FlagMissing,
  FlagNotFlat,
  FlagNotRead,
  FlagReadTwoWays,
  FlagReserved,
  FlagValueMissing,
} from "./flag-errors.ts";
import { Task } from "./make-tasks.ts";
import { describeAcceptedFlags } from "./read-command-line-flags.ts";
import type { checkDeclaredFlags, readCommandLine } from "./read-command-line-flags.ts";

const EntryStartRefusal = Schema.Union([
  DefaultExportNotRunnable,
  FlagGivenTwice,
  FlagInvalid,
  FlagMissing,
  FlagNotFlat,
  FlagNotRead,
  FlagReadTwoWays,
  FlagReserved,
  FlagValueMissing,
  RunArgumentsNotAccepted,
  RunCalledInBunEvaluation,
  RunCalledInBunWatchMode,
  RunCalledInStandaloneExecutable,
  StartTaskNotMade,
  TaskDefinitionNotGiven,
  TaskListNotGiven,
  TaskNameNotString,
]);

export const EntryStart = Schema.TaggedUnion({
  NotStarted: {},
  Refused: { refusal: EntryStartRefusal },
  HelpAsked: { text: Schema.String },
  Supervised: {
    name: Schema.String,
    listedTasks: Schema.Array(Schema.instanceOf(Task)),
    group: Schema.NullOr(RootTaskGroup),
    checksForgottenTasks: Schema.Boolean,
    flags: Schema.Record(Schema.String, Schema.String),
  },
});

const NOT_STARTED = EntryStart.cases.NotStarted.make({});

const refuse = (refusal: typeof EntryStartRefusal.Type) =>
  EntryStart.cases.Refused.make({ refusal });

const decodeGroup = Schema.decodeUnknownOption(Schema.Struct({ [TASK_GROUP_KEY]: TaskGroup }));

const decodeListNotRecorded = Schema.decodeUnknownOption(
  Schema.Struct({ [TASK_GROUP_KEY]: TaskListNotRecorded }),
);

const FUNCTION_OF_LIST_MODE = { all: "Task.all", allSettled: "Task.allSettled" } as const;

const SHELL_SAFE_WORD = /^[\w%+,./:=@-]+$/u;

const quoteForShell = (word: string) =>
  SHELL_SAFE_WORD.test(word) ? word : `'${word.replaceAll("'", String.raw`'\''`)}'`;

const rootOfTask = (task: Task, checksForgottenTasks: boolean) => ({
  name: task.name,
  listedTasks: [task],
  group: null,
  checksForgottenTasks,
});

export const decideEntryStart = (facts: {
  readonly entryFile: Option.Option<unknown>;
  readonly entryFilePath: string;
  readonly madeTasks: readonly {
    readonly task: Task;
    readonly location: string | undefined;
    readonly definitionNotGiven: ReturnType<typeof findDefinitionNotGiven>;
  }[];
  readonly makesATask: boolean;
  readonly isListMadeHere: (list: WeakKey) => boolean;
  readonly commandLine: Result.Result<
    Effect.Success<ReturnType<typeof readCommandLine>> & {
      readonly flatFlags: Effect.Success<ReturnType<typeof checkDeclaredFlags>>;
    },
    | Effect.Error<ReturnType<typeof checkDeclaredFlags>>
    | Effect.Error<ReturnType<typeof readCommandLine>>
  >;
  readonly entryCommand: readonly string[];
  readonly isStandaloneExecutable: boolean;
  readonly watchModeFlag: Effect.Success<typeof bunWatchModeFlag>;
  readonly evaluationFlag: Effect.Success<typeof bunEvaluationFlag>;
}) => {
  if (Option.isNone(facts.entryFile)) {
    return NOT_STARTED;
  }
  const entryFile = facts.entryFile.value;
  const hasDefaultExport = Predicate.hasProperty(entryFile, "default");
  const defaultExport = hasDefaultExport ? entryFile.default : null;
  const madeTasks = facts.madeTasks.map((madeTask) => madeTask.task);
  const defaultTaskMade = facts.madeTasks.find((madeTask) => madeTask.task === defaultExport);
  const isMadeByAnotherCopy = Predicate.hasProperty(defaultExport, TASK_GROUP_KEY)
    ? !facts.isListMadeHere(defaultExport)
    : defaultTaskMade === undefined && Predicate.hasProperty(defaultExport, TASK_BODY);
  if (isMadeByAnotherCopy) {
    return NOT_STARTED;
  }
  if (defaultTaskMade?.definitionNotGiven) {
    const { given, location } = defaultTaskMade.definitionNotGiven;
    return refuse(new TaskDefinitionNotGiven({ given, location: location ?? facts.entryFilePath }));
  }
  const defaultTaskName: unknown = defaultTaskMade?.task.name;
  const isDefaultTaskUnnamed =
    defaultTaskMade !== undefined && !Predicate.isString(defaultTaskName);
  if (isDefaultTaskUnnamed) {
    return refuse(
      new TaskNameNotString({
        location: defaultTaskMade.location ?? facts.entryFilePath,
        nameText: inspect(defaultTaskName),
      }),
    );
  }
  const listNotRecorded = decodeListNotRecorded(defaultExport);
  if (Option.isSome(listNotRecorded)) {
    const { mode, given } = listNotRecorded.value[TASK_GROUP_KEY];
    return refuse(
      new TaskListNotGiven({
        calledFunction: FUNCTION_OF_LIST_MODE[mode],
        given,
        location: facts.entryFilePath,
      }),
    );
  }
  const defaultRoot =
    defaultTaskMade === undefined
      ? Option.getOrNull(
          Option.map(decodeGroup(defaultExport), ({ [TASK_GROUP_KEY]: taskGroup }) => {
            const name = facts.entryFilePath.slice(facts.entryFilePath.lastIndexOf("/") + 1);
            return {
              name,
              listedTasks: taskGroup.taskNames.flatMap((taskName) =>
                madeTasks.filter((madeTask) => madeTask.name === taskName).slice(0, 1),
              ),
              group: { name, ...taskGroup },
              checksForgottenTasks: true,
            };
          }),
        )
      : rootOfTask(defaultTaskMade.task, true);
  if (defaultRoot === null) {
    const exportedValues: readonly unknown[] = Predicate.isObject(entryFile)
      ? Object.values(entryFile)
      : [];
    const exportsATask = madeTasks.some((madeTask) => exportedValues.includes(madeTask));
    if (Predicate.hasProperty(defaultExport, TASK_GROUP_KEY)) {
      return refuse(
        new DefaultExportNotRunnable({
          location: facts.entryFilePath,
          defaultExport: "unreadable list",
          tasksInFile: null,
        }),
      );
    }
    const hasATask = facts.makesATask || exportsATask;
    return hasATask
      ? refuse(
          new DefaultExportNotRunnable({
            location: facts.entryFilePath,
            defaultExport: hasDefaultExport ? "not a task" : "missing",
            tasksInFile: facts.makesATask ? "makes" : "exports",
          }),
        )
      : NOT_STARTED;
  }
  if (Result.isFailure(facts.commandLine)) {
    return refuse(facts.commandLine.failure);
  }
  const { words, values, asksForHelp, flatFlags } = facts.commandLine.success;
  const isOneTaskName =
    words.length === 1 && words.every((word) => word.trim() !== "" && !word.startsWith("-"));
  const areArgumentsRefused = Arrays.isReadonlyArrayNonEmpty(words) && !isOneTaskName;
  if (areArgumentsRefused) {
    return refuse(new RunArgumentsNotAccepted({ givenArguments: words }));
  }
  const [startTaskName] = words;
  const startTask =
    startTaskName === undefined
      ? undefined
      : madeTasks.find((madeTask) => madeTask.name === startTaskName);
  const isStartTaskMissing = startTaskName !== undefined && startTask === undefined;
  if (isStartTaskMissing) {
    return refuse(new StartTaskNotMade({ startTaskName }));
  }
  const root = startTask === undefined ? defaultRoot : rootOfTask(startTask, false);
  if (facts.isStandaloneExecutable) {
    return refuse(new RunCalledInStandaloneExecutable({ rootTaskName: root.name }));
  }
  if (Option.isSome(facts.watchModeFlag)) {
    return refuse(new RunCalledInBunWatchMode({ flag: facts.watchModeFlag.value }));
  }
  if (Option.isSome(facts.evaluationFlag)) {
    return refuse(new RunCalledInBunEvaluation({ flag: facts.evaluationFlag.value }));
  }
  if (asksForHelp) {
    const command = [...facts.entryCommand, ...(startTask === undefined ? [] : [startTask.name])]
      .map((word) => quoteForShell(word))
      .join(" ");
    const startTaskNames = root.listedTasks.map((listedTask) => listedTask.name);
    return EntryStart.cases.HelpAsked.make({
      text: describeAcceptedFlags(command, startTaskNames, flatFlags),
    });
  }
  return EntryStart.cases.Supervised.make({ ...root, flags: values });
};
