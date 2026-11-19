const nestedRecapLinesByFailure = new WeakMap<WeakKey, readonly string[]>();

export const recapNestedRunIn = <Failure extends WeakKey>(
  failure: Failure,
  nestedRecapLines: readonly string[],
) => {
  nestedRecapLinesByFailure.set(failure, nestedRecapLines);
  return failure;
};

export const readNestedRecapLines = (failure: WeakKey) =>
  nestedRecapLinesByFailure.get(failure) ?? [];
