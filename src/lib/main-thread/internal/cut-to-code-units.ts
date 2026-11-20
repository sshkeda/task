const SURROGATE_LEFT_UNPAIRED_AT_THE_END = /\p{Cs}$/u;

export const cutToCodeUnits = (text: string, mostCodeUnits: number) =>
  text.length <= mostCodeUnits
    ? text
    : `${text.slice(0, mostCodeUnits - 1).replace(SURROGATE_LEFT_UNPAIRED_AT_THE_END, "")}…`;
