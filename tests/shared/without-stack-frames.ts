const ERROR_LABEL = /^error: \[(?:[^\]\\]|\\.)*\] /u;

const STACK_FRAMES_HEADER = "stack frames of ";

const STACK_FRAME_TEXT = /^(?:at |\d+ more stack frames left out, past the first \d+$)/u;

const textAfterLabel = (line: string) => {
  const label = ERROR_LABEL.exec(line)?.[0];
  return label === undefined ? undefined : line.slice(label.length);
};

export const findStackFrameBlocks = (text: string) => {
  const blocks: { readonly error: string; readonly frames: string[] }[] = [];
  for (const line of text.split("\n")) {
    const afterLabel = textAfterLabel(line);
    const isFrame = afterLabel !== undefined && STACK_FRAME_TEXT.test(afterLabel);
    if (afterLabel?.startsWith(STACK_FRAMES_HEADER) === true) {
      blocks.push({ error: afterLabel.slice(STACK_FRAMES_HEADER.length), frames: [] });
    } else if (isFrame) {
      blocks.at(-1)?.frames.push(afterLabel);
    }
  }
  return blocks;
};

export const withoutStackFrames = (text: string) => {
  const kept: string[] = [];
  const inBlock: boolean[] = [];
  for (const line of text.split("\n")) {
    const afterLabel = textAfterLabel(line);
    const startsBlock = afterLabel?.startsWith(STACK_FRAMES_HEADER) === true;
    const isFrame = afterLabel !== undefined && STACK_FRAME_TEXT.test(afterLabel);
    const staysInBlock = startsBlock || (inBlock.at(-1) === true && isFrame);
    inBlock.push(staysInBlock);
    if (!staysInBlock) {
      kept.push(line);
    }
  }
  return kept.join("\n");
};
