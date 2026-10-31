import { TASK_PACKAGE } from "./task-package.ts";

export const packageNameParts = TASK_PACKAGE.name.replace(/^@/u, "").split("/");

export const TASK_STATE_DIRECTORY_NAME = `.${packageNameParts.join("-")}`;

export const TASK_TRACE_LABEL_PREFIX = packageNameParts.join("_");
