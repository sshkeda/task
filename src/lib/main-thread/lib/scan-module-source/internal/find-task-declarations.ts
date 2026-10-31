import { Visitor } from "oxc-parser";
import type { parseSync, CallExpression, Expression, Program } from "oxc-parser";

import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import {
  TaskCalledBelowTopLevel,
  TaskNameEmpty,
  TaskNameComputed,
  TaskNameMissing,
  TaskNameNotString,
  TaskPackageImportedWithoutTask,
} from "../../../shared/main-thread-errors.ts";
import type { ModuleScanRefusal } from "../../../shared/main-thread-errors.ts";
import { readBarePackageName } from "../../../shared/read-bare-package-name.ts";
import { findLexicalBindings } from "./find-lexical-bindings.ts";
import { readLiteralString } from "./read-literal-string.ts";
import { listValueImports, readRequireFunctionName } from "./read-module-loads.ts";
import { unwrapTypeAssertions } from "./unwrap-type-assertions.ts";

const unwrapPipeCalls = (
  expression: Expression | null | undefined,
): Expression | null | undefined => {
  const isMissing = expression === null || expression === undefined;
  if (isMissing) {
    return expression;
  }
  const unwrapped = unwrapTypeAssertions(expression);
  if (unwrapped.type !== "CallExpression") {
    return unwrapped;
  }
  const callee = unwrapped.callee;
  const callsPipeMethod =
    callee.type === "MemberExpression" && !callee.computed && callee.property.name === "pipe";
  return callsPipeMethod ? unwrapPipeCalls(callee.object) : unwrapped;
};

const listInitializers = (
  statement: Program["body"][number],
): readonly (Expression | null | undefined)[] => {
  const declared = statement.type === "ExportNamedDeclaration" ? statement.declaration : statement;
  if (declared?.type === "VariableDeclaration") {
    return declared.declarations.map((declarator) => declarator.init);
  }
  if (declared?.type === "ExpressionStatement") {
    return [declared.expression];
  }
  if (declared?.type !== "ExportDefaultDeclaration") {
    return [];
  }
  const declaration = declared.declaration;
  const isDeclaration =
    declaration.type === "FunctionDeclaration" ||
    declaration.type === "ClassDeclaration" ||
    declaration.type === "TSInterfaceDeclaration";
  return isDeclaration ? [] : [declaration];
};

const readRequiredModule = (initializer: Expression | null) => {
  if (initializer === null) {
    return null;
  }
  const required = unwrapTypeAssertions(initializer);
  const isRequireCall =
    required.type === "CallExpression" && readRequireFunctionName(required) !== null;
  return isRequireCall ? readLiteralString(required.arguments[0]) : null;
};

const listTaskPackageRequires = (program: Program) =>
  program.body.flatMap((statement) =>
    statement.type === "VariableDeclaration"
      ? statement.declarations.flatMap(({ id, init: initializer }) => {
          const requiredModule = readRequiredModule(initializer);
          const requiresTaskPackage =
            requiredModule !== null && readBarePackageName(requiredModule) === TASK_PACKAGE.name;
          return requiresTaskPackage ? [id] : [];
        })
      : [],
  );

export const findTaskDeclarations = (
  parsed: ReturnType<typeof parseSync>,
  text: string,
  lineAt: (offset: number) => number,
) => {
  const importViolations: (typeof ModuleScanRefusal.Type)[] = [];
  const nameViolations: (typeof ModuleScanRefusal.Type)[] = [];
  const bindingFor = findLexicalBindings(parsed.program);
  const taskNamespaceBindings = new Set<number>();
  const moduleNamespaceBindings = new Set<number>();
  for (const { declaration, valueEntries } of listValueImports(parsed)) {
    const importedModule = declaration.moduleRequest.value;
    if (readBarePackageName(importedModule) !== TASK_PACKAGE.name) {
      continue;
    }

    for (const importEntry of valueEntries) {
      if (importEntry.importName.name === "Task") {
        taskNamespaceBindings.add(importEntry.localName.start);
      } else if (importEntry.importName.kind === "NamespaceObject") {
        moduleNamespaceBindings.add(importEntry.localName.start);
      } else if (importEntry.importName.name === null) {
        importViolations.push(
          new TaskPackageImportedWithoutTask({ line: lineAt(declaration.start) }),
        );
      }
    }
  }

  for (const binding of listTaskPackageRequires(parsed.program)) {
    if (binding.type === "Identifier") {
      moduleNamespaceBindings.add(binding.start);
      continue;
    }
    if (binding.type !== "ObjectPattern") {
      continue;
    }
    for (const property of binding.properties) {
      const bindsTaskNamespace =
        property.type === "Property" &&
        !property.computed &&
        property.key.type === "Identifier" &&
        property.key.name === "Task" &&
        property.value.type === "Identifier";
      if (bindsTaskNamespace) {
        taskNamespaceBindings.add(property.value.start);
      }
    }
  }

  const isTaskNamespace = (expression: Expression): boolean =>
    expression.type === "Identifier"
      ? taskNamespaceBindings.has(bindingFor(expression) ?? -1)
      : expression.type === "MemberExpression" &&
        !expression.computed &&
        expression.object.type === "Identifier" &&
        moduleNamespaceBindings.has(bindingFor(expression.object) ?? -1) &&
        expression.property.name === "Task";

  const callsTask = (call: CallExpression) => {
    const callee = call.callee;
    const isNamedMemberCall = callee.type === "MemberExpression" && !callee.computed;
    return isNamedMemberCall && callee.property.name === "make" && isTaskNamespace(callee.object);
  };

  const madeTasks: { readonly name: string; readonly line: number }[] = [];
  const topLevelTaskCalls = new Set<number>();
  for (const statement of parsed.program.body) {
    for (const pipedInitializer of listInitializers(statement)) {
      const initializer = unwrapPipeCalls(pipedInitializer);
      const isTaskCall = initializer?.type === "CallExpression" && callsTask(initializer);
      if (!isTaskCall) {
        continue;
      }

      topLevelTaskCalls.add(initializer.start);
      const [nameArgument] = initializer.arguments;
      const taskName = readLiteralString(nameArgument);
      const isNonStringLiteralName = taskName === null && nameArgument?.type === "Literal";
      if (nameArgument === undefined) {
        nameViolations.push(new TaskNameMissing({ line: lineAt(initializer.start) }));
      } else if (isNonStringLiteralName) {
        nameViolations.push(
          new TaskNameNotString({
            line: lineAt(initializer.start),
            nameText: text.slice(nameArgument.start, nameArgument.end),
          }),
        );
      } else if (taskName === null) {
        nameViolations.push(
          new TaskNameComputed({
            line: lineAt(initializer.start),
            nameText: text.slice(nameArgument.start, nameArgument.end),
          }),
        );
      } else if (taskName === "") {
        nameViolations.push(new TaskNameEmpty({ line: lineAt(initializer.start) }));
      } else {
        madeTasks.push({ name: taskName, line: lineAt(initializer.start) });
      }
    }
  }

  return { madeTasks, importViolations, nameViolations, callsTask, topLevelTaskCalls };
};

export const findTaskCallsBelowTopLevel = (
  program: Program,
  taskDeclarations: Pick<
    ReturnType<typeof findTaskDeclarations>,
    "callsTask" | "topLevelTaskCalls"
  >,
  lineAt: (offset: number) => number,
) => {
  const violations: (typeof ModuleScanRefusal.Type)[] = [];
  new Visitor({
    CallExpression: (call) => {
      const isTaskCallBelowTopLevel =
        taskDeclarations.callsTask(call) && !taskDeclarations.topLevelTaskCalls.has(call.start);
      if (isTaskCallBelowTopLevel) {
        violations.push(new TaskCalledBelowTopLevel({ line: lineAt(call.start) }));
      }
    },
  }).visit(program);
  return violations;
};
