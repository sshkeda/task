import type { Expression } from "oxc-parser";

export const unwrapTypeAssertions = (expression: Expression): Expression => {
  const isTypeAssertion =
    expression.type === "TSAsExpression" ||
    expression.type === "TSSatisfiesExpression" ||
    expression.type === "TSNonNullExpression" ||
    expression.type === "TSTypeAssertion";
  return isTypeAssertion ? unwrapTypeAssertions(expression.expression) : expression;
};
