const graphemeSegmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });

export const cutToGraphemes = (text: string, maxGraphemes: number) => {
  const graphemes: string[] = [];
  for (const { segment } of graphemeSegmenter.segment(text)) {
    graphemes.push(segment);
    if (graphemes.length > maxGraphemes) {
      return `${graphemes.slice(0, maxGraphemes - 1).join("")}…`;
    }
  }
  return text;
};
