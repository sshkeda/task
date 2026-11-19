const MOST_CODE_UNITS_OF_A_RECAPPED_OUTPUT_LINE = 200;

const SURROGATE_LEFT_UNPAIRED_AT_THE_END = /\p{Cs}$/u;

export const RECAPPED_OUTPUT_LINE_READ_LIMIT = MOST_CODE_UNITS_OF_A_RECAPPED_OUTPUT_LINE + 1;

export const cutToCodeUnits = (text: string, mostCodeUnits: number) =>
  text.length <= mostCodeUnits
    ? text
    : `${text.slice(0, mostCodeUnits - 1).replace(SURROGATE_LEFT_UNPAIRED_AT_THE_END, "")}…`;

export const cutRecapOutputLine = (line: string) =>
  cutToCodeUnits(line, MOST_CODE_UNITS_OF_A_RECAPPED_OUTPUT_LINE);
