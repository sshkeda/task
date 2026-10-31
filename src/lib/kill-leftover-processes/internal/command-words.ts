const BYTE_ORDER_MARK = "\uFEFF";

export const commandWordsOf = (argumentWords: readonly string[]) =>
  argumentWords
    .map((argumentWord) =>
      argumentWord.startsWith(BYTE_ORDER_MARK)
        ? argumentWord.slice(BYTE_ORDER_MARK.length)
        : argumentWord,
    )
    .filter((commandWord) => commandWord !== "");

export const programNameOf = (argumentWords: readonly string[], commandName: string) => {
  const [programPath] = commandWordsOf(argumentWords.slice(0, 1));
  const programName = programPath?.slice(programPath.lastIndexOf("/") + 1) ?? "";
  return programName === "" ? commandName : programName;
};
