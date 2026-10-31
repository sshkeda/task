import { Visitor } from "oxc-parser";
import type { BindingPattern, ParamPattern, Program } from "oxc-parser";

class LexicalScope {
  public readonly bindings = new Map<string, number>();

  public readonly parent: LexicalScope | null;

  public readonly holdsVarDeclarations: boolean;

  public constructor(parent: LexicalScope | null, holdsVarDeclarations: boolean) {
    this.parent = parent;
    this.holdsVarDeclarations = holdsVarDeclarations;
  }

  public bindingFor(name: string): number | null {
    return this.bindings.get(name) ?? this.parent?.bindingFor(name) ?? null;
  }

  public scopeForVar(): LexicalScope {
    return this.holdsVarDeclarations ? this : (this.parent?.scopeForVar() ?? this);
  }
}

const declarePattern = (scope: LexicalScope, pattern: BindingPattern | ParamPattern): void => {
  if (pattern.type === "Identifier") {
    if (!scope.bindings.has(pattern.name)) {
      scope.bindings.set(pattern.name, pattern.start);
    }
  } else if (pattern.type === "AssignmentPattern") {
    declarePattern(scope, pattern.left);
  } else if (pattern.type === "RestElement") {
    declarePattern(scope, pattern.argument);
  } else if (pattern.type === "TSParameterProperty") {
    declarePattern(scope, pattern.parameter);
  } else if (pattern.type === "ObjectPattern") {
    for (const property of pattern.properties) {
      declarePattern(scope, property.type === "Property" ? property.value : property.argument);
    }
  } else {
    for (const element of pattern.elements) {
      if (element !== null) {
        declarePattern(scope, element);
      }
    }
  }
};

export const findLexicalBindings = (program: Program) => {
  const current = { scope: new LexicalScope(null, true) };
  const scopesAtIdentifierStarts = new Map<number, LexicalScope>();
  const enterScope = () => {
    current.scope = new LexicalScope(current.scope, false);
  };
  const leaveScope = () => {
    current.scope = current.scope.parent ?? current.scope;
  };
  const enterFunction = (parameters: readonly ParamPattern[]) => {
    current.scope = new LexicalScope(current.scope, true);
    for (const parameter of parameters) {
      declarePattern(current.scope, parameter);
    }
  };
  new Visitor({
    Identifier: (identifier) => {
      scopesAtIdentifierStarts.set(identifier.start, current.scope);
    },
    VariableDeclaration: (declaration) => {
      if (declaration.declare === true) {
        return;
      }
      const declarationScope =
        declaration.kind === "var" ? current.scope.scopeForVar() : current.scope;
      for (const declarator of declaration.declarations) {
        declarePattern(declarationScope, declarator.id);
      }
    },
    ImportDeclaration: (declaration) => {
      if (declaration.importKind === "type") {
        return;
      }
      for (const specifier of declaration.specifiers) {
        const importsValue =
          specifier.type !== "ImportSpecifier" || specifier.importKind !== "type";
        if (importsValue) {
          declarePattern(current.scope, specifier.local);
        }
      }
    },
    TSImportEqualsDeclaration: (declaration) => {
      if (declaration.importKind !== "type") {
        declarePattern(current.scope, declaration.id);
      }
    },
    FunctionDeclaration: (declaration) => {
      if (declaration.id !== null) {
        declarePattern(current.scope, declaration.id);
      }
      enterFunction(declaration.params);
    },
    "FunctionDeclaration:exit": leaveScope,
    FunctionExpression: (expression) => {
      enterFunction(expression.params);
      if (expression.id !== null) {
        declarePattern(current.scope, expression.id);
      }
    },
    "FunctionExpression:exit": leaveScope,
    ArrowFunctionExpression: (expression) => {
      enterFunction(expression.params);
    },
    "ArrowFunctionExpression:exit": leaveScope,
    BlockStatement: enterScope,
    "BlockStatement:exit": leaveScope,
    ForStatement: enterScope,
    "ForStatement:exit": leaveScope,
    ForInStatement: enterScope,
    "ForInStatement:exit": leaveScope,
    ForOfStatement: enterScope,
    "ForOfStatement:exit": leaveScope,
    SwitchStatement: enterScope,
    "SwitchStatement:exit": leaveScope,
    CatchClause: (clause) => {
      enterScope();
      if (clause.param !== null) {
        declarePattern(current.scope, clause.param);
      }
    },
    "CatchClause:exit": leaveScope,
    ClassDeclaration: (declaration) => {
      const { id, declare: declaresOnlyType } = declaration;
      const declaresValue = id !== null && declaresOnlyType !== true;
      if (declaresValue) {
        declarePattern(current.scope, id);
      }
      enterScope();
    },
    "ClassDeclaration:exit": leaveScope,
    ClassExpression: (expression) => {
      enterScope();
      if (expression.id !== null) {
        declarePattern(current.scope, expression.id);
      }
    },
    "ClassExpression:exit": leaveScope,
    StaticBlock: () => {
      current.scope = new LexicalScope(current.scope, true);
    },
    "StaticBlock:exit": leaveScope,
  }).visit(program);

  return (identifier: { readonly name: string; readonly start: number }) =>
    scopesAtIdentifierStarts.get(identifier.start)?.bindingFor(identifier.name) ?? null;
};
