import { splitIntoPieces, startWithNoLineOpen } from "../../../shared/label-output.ts";
import type { OutputPiece } from "../../../shared/label-output.ts";
import { HELD_COMMAND_OUTPUT_BYTES } from "./cut-command-output.ts";

const BLOCK_BYTES = 65_536;

export const RUN_COST_BYTES = 16;

const LINE_FEED = 0x0a;

const CARRIAGE_RETURN = 0x0d;

const isLineBreak = (byte: number) => byte === LINE_FEED || byte === CARRIAGE_RETURN;

const LINE_CLOSED = { text: "", continuesLine: true, endsLine: true } as const;

export const makeHeldCommandOutput = ({
  boundBytes = HELD_COMMAND_OUTPUT_BYTES,
  blockBytes = BLOCK_BYTES,
} = {}) => {
  const blocks: Uint8Array[] = [];
  const runStreamIds: number[] = [];
  const runEnds: number[] = [];
  const runsEndingTheirStream: boolean[] = [];
  const held = { firstBlockStart: 0, start: 0, end: 0 };
  const lastDroppedBytes = new Map<number, number>();
  const runCountsByStreamId = new Map<number, number>();
  const failedStreamIds = new Set<number>();
  const cut = { hasCutFailedStreamsWithoutRuns: false };
  const forgetFailedStreamWithoutRuns = (streamId: number) => {
    failedStreamIds.delete(streamId);
    if (lastDroppedBytes.delete(streamId)) {
      cut.hasCutFailedStreamsWithoutRuns = true;
    }
  };
  const countRun = (streamId: number) => {
    runCountsByStreamId.set(streamId, (runCountsByStreamId.get(streamId) ?? 0) + 1);
  };
  const uncountRun = (streamId: number) => {
    const runCount = (runCountsByStreamId.get(streamId) ?? 0) - 1;
    if (runCount > 0) {
      runCountsByStreamId.set(streamId, runCount);
      return;
    }
    runCountsByStreamId.delete(streamId);
    if (failedStreamIds.has(streamId)) {
      forgetFailedStreamWithoutRuns(streamId);
    }
  };
  const byteAt = (offset: number) => {
    const fromFirstBlock = offset - held.firstBlockStart;
    return blocks[Math.floor(fromFirstBlock / blockBytes)]?.[fromFirstBlock % blockBytes] ?? 0;
  };
  const copyIn = (bytes: Uint8Array): void => {
    if (bytes.length === 0) {
      return;
    }
    const fromFirstBlock = held.end - held.firstBlockStart;
    const blockIndex = Math.floor(fromFirstBlock / blockBytes);
    const block = blocks[blockIndex] ?? new Uint8Array(blockBytes);
    blocks[blockIndex] = block;
    const into = fromFirstBlock % blockBytes;
    const copied = bytes.subarray(0, blockBytes - into);
    block.set(copied, into);
    held.end += copied.length;
    copyIn(bytes.subarray(copied.length));
  };
  const dropUpTo = (streamId: number, offset: number) => {
    held.start = offset;
    lastDroppedBytes.set(streamId, byteAt(offset - 1));
  };
  const firstStartThatFits = (keptRuns: number) =>
    held.end + keptRuns * RUN_COST_BYTES - boundBytes;
  const dropOverBound = () => {
    if (firstStartThatFits(runEnds.length) <= held.start) {
      return;
    }
    const firstKeptRun = runEnds.findIndex(
      (runEnd, index) => firstStartThatFits(runEnds.length - index) < runEnd,
    );
    const newStart = firstStartThatFits(runEnds.length - firstKeptRun);
    for (const droppedRunEnd of runEnds.slice(0, firstKeptRun)) {
      const droppedStreamId = runStreamIds.shift() ?? 0;
      runEnds.shift();
      if (runsEndingTheirStream.shift() !== true) {
        dropUpTo(droppedStreamId, droppedRunEnd);
      }
      uncountRun(droppedStreamId);
    }
    if (newStart > held.start) {
      dropUpTo(runStreamIds[0] ?? 0, newStart);
    }
    const droppedBlocks = Math.floor((held.start - held.firstBlockStart) / blockBytes);
    blocks.splice(0, droppedBlocks);
    held.firstBlockStart += droppedBlocks * blockBytes;
  };
  const copyOut = (start: number, end: number) => {
    const bytes = new Uint8Array(end - start);
    const firstBlock = Math.floor((start - held.firstBlockStart) / blockBytes);
    const lastBlock = Math.ceil((end - held.firstBlockStart) / blockBytes);
    for (const [index, block] of blocks.slice(firstBlock, lastBlock).entries()) {
      const blockStart = held.firstBlockStart + (firstBlock + index) * blockBytes;
      const from = Math.max(start, blockStart);
      const to = Math.min(end, blockStart + blockBytes);
      bytes.set(block.subarray(from - blockStart, to - blockStart), from - start);
    }
    return bytes;
  };
  const runBoundsOf = (index: number) => ({
    start: Math.max(runEnds[index - 1] ?? held.start, held.start),
    end: runEnds[index] ?? held.end,
  });
  const replay = function* (): Generator<
    { readonly streamId: number; readonly pieces: readonly (typeof OutputPiece.Type)[] },
    void,
    undefined
  > {
    const streams = new Map<
      number,
      {
        readonly decoder: TextDecoder;
        hasEnded: boolean;
        isSkippingToALine: boolean;
        split: ReturnType<typeof startWithNoLineOpen>;
      }
    >();
    const streamOf = (streamId: number) => {
      const known = streams.get(streamId);
      if (known !== undefined) {
        return known;
      }
      const lastDroppedByte = lastDroppedBytes.get(streamId);
      const started = {
        decoder: new TextDecoder("utf-8", { ignoreBOM: lastDroppedByte !== undefined }),
        hasEnded: false,
        isSkippingToALine: lastDroppedByte !== undefined && !isLineBreak(lastDroppedByte),
        split: {
          ...startWithNoLineOpen(),
          endedWithCarriageReturn: lastDroppedByte === CARRIAGE_RETURN,
        },
      };
      streams.set(streamId, started);
      return started;
    };
    const splitText = (stream: ReturnType<typeof streamOf>, text: string) => {
      if (text === "") {
        return [];
      }
      const [split, pieces] = splitIntoPieces(stream.split, text);
      stream.split = split;
      return pieces;
    };
    const textOf = (stream: ReturnType<typeof streamOf>, bytes: Uint8Array) => {
      if (!stream.isSkippingToALine) {
        return stream.decoder.decode(bytes, { stream: true });
      }
      const lineBreak = bytes.findIndex((byte) => isLineBreak(byte));
      if (lineBreak === -1) {
        return "";
      }
      stream.isSkippingToALine = false;
      stream.split = {
        ...startWithNoLineOpen(),
        endedWithCarriageReturn: bytes[lineBreak] === CARRIAGE_RETURN,
      };
      return stream.decoder.decode(bytes.subarray(lineBreak + 1), { stream: true });
    };
    const piecesAtTheEndOf = (stream: ReturnType<typeof streamOf>) => {
      stream.hasEnded = true;
      const pieces = splitText(stream, stream.decoder.decode());
      return stream.split.isLineOpen ? [...pieces, LINE_CLOSED] : pieces;
    };
    for (const [index, streamId] of runStreamIds.entries()) {
      const stream = streamOf(streamId);
      if (runsEndingTheirStream[index] === true) {
        const pieces = piecesAtTheEndOf(stream);
        if (pieces.length > 0) {
          yield { streamId, pieces };
        }
        continue;
      }
      const { start: runStart, end: runEnd } = runBoundsOf(index);
      const sliceCount = Math.ceil((runEnd - runStart) / blockBytes);
      for (const slice of Array.from({ length: sliceCount }).keys()) {
        const sliceStart = runStart + slice * blockBytes;
        const pieces = splitText(
          stream,
          textOf(stream, copyOut(sliceStart, Math.min(sliceStart + blockBytes, runEnd))),
        );
        if (pieces.length > 0) {
          yield { streamId, pieces };
        }
      }
    }
    for (const [streamId, stream] of streams) {
      const pieces = stream.hasEnded ? [] : piecesAtTheEndOf(stream);
      if (pieces.length > 0) {
        yield { streamId, pieces };
      }
    }
  };
  const release = (streamIds: ReadonlySet<number>) => {
    for (const streamId of streamIds) {
      lastDroppedBytes.delete(streamId);
      failedStreamIds.delete(streamId);
    }
    if (!runStreamIds.some((streamId) => streamIds.has(streamId))) {
      return;
    }
    const keptRuns = runStreamIds.flatMap((streamId, index) => {
      if (streamIds.has(streamId)) {
        return [];
      }
      const { start, end } = runBoundsOf(index);
      return [{ streamId, bytes: copyOut(start, end), endsStream: runsEndingTheirStream[index] }];
    });
    blocks.length = 0;
    runCountsByStreamId.clear();
    runStreamIds.length = 0;
    runEnds.length = 0;
    runsEndingTheirStream.length = 0;
    Object.assign(held, { firstBlockStart: 0, start: 0, end: 0 });
    for (const { streamId, bytes, endsStream } of keptRuns) {
      copyIn(bytes);
      countRun(streamId);
      runStreamIds.push(streamId);
      runEnds.push(held.end);
      runsEndingTheirStream.push(endsStream === true);
    }
  };
  return {
    hold: (streamId: number, bytes: Uint8Array) => {
      if (bytes.length === 0) {
        return;
      }
      copyIn(bytes);
      const continuesTheLastRun =
        runStreamIds.at(-1) === streamId && runsEndingTheirStream.at(-1) === false;
      if (continuesTheLastRun) {
        runEnds[runEnds.length - 1] = held.end;
      } else {
        countRun(streamId);
        runStreamIds.push(streamId);
        runEnds.push(held.end);
        runsEndingTheirStream.push(false);
      }
      dropOverBound();
    },
    endStream: (streamId: number) => {
      countRun(streamId);
      runStreamIds.push(streamId);
      runEnds.push(held.end);
      runsEndingTheirStream.push(true);
      dropOverBound();
    },
    measure: () => ({
      heldBytes: held.end - held.start,
      runs: runEnds.length,
      blocks: blocks.length,
      trackedStreams: new Set([
        ...lastDroppedBytes.keys(),
        ...failedStreamIds,
        ...runCountsByStreamId.keys(),
      ]).size,
    }),
    isCut: () => lastDroppedBytes.size > 0 || cut.hasCutFailedStreamsWithoutRuns,
    keepFailed: (streamIds: ReadonlySet<number>) => {
      for (const streamId of streamIds) {
        if (runCountsByStreamId.has(streamId)) {
          failedStreamIds.add(streamId);
        } else {
          forgetFailedStreamWithoutRuns(streamId);
        }
      }
    },
    isEmpty: () => runEnds.length === 0,
    release,
    replay,
  };
};
