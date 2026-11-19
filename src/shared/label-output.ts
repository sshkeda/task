import { Schema, Stream } from "effect";

import { escapeLineSeparators, labelLine, nameTask } from "./label-lines.ts";

const HEXADECIMAL = 16;

const BYTE_HEX_DIGITS = 2;

const LAST_SINGLE_BYTE = 0x7f;

const FIRST_TWO_BYTE_LEAD = 0xc2;

const LAST_TWO_BYTE_LEAD = 0xdf;

const THREE_BYTE_LEAD_BELOW_U_0800 = 0xe0;

const THREE_BYTE_LEAD_OF_SURROGATES = 0xed;

const LAST_THREE_BYTE_LEAD = 0xef;

const FOUR_BYTE_LEAD_BELOW_U_10000 = 0xf0;

const LAST_FOUR_BYTE_LEAD_IN_RANGE = 0xf3;

const FOUR_BYTE_LEAD_UP_TO_U_10FFFF = 0xf4;

const FIRST_CONTINUATION_BYTE = 0x80;

const LAST_CONTINUATION_BYTE = 0xbf;

const FIRST_SECOND_BYTE_AFTER_E0 = 0xa0;

const LAST_SECOND_BYTE_AFTER_ED = 0x9f;

const FIRST_SECOND_BYTE_AFTER_F0 = 0x90;

const LAST_SECOND_BYTE_AFTER_F4 = 0x8f;

const CONTINUATIONS_AFTER_THREE_BYTE_LEAD = 2;

const CONTINUATIONS_AFTER_FOUR_BYTE_LEAD = 3;

const textDecoder = new TextDecoder("utf-8", { ignoreBOM: true });

const BYTE_VALUES = 256;

const ESCAPED_BYTES = Array.from(
  { length: BYTE_VALUES },
  (_unused, byte) =>
    String.raw`\x${byte.toString(HEXADECIMAL).toUpperCase().padStart(BYTE_HEX_DIGITS, "0")}`,
);

const sequence = (
  missing: number,
  low = FIRST_CONTINUATION_BYTE,
  high = LAST_CONTINUATION_BYTE,
) => ({
  missing,
  low,
  high,
});

const sequenceStartedBy = (lead: number) => {
  if (lead <= LAST_SINGLE_BYTE) {
    return sequence(0);
  }
  if (lead < FIRST_TWO_BYTE_LEAD) {
    return null;
  }
  if (lead <= LAST_TWO_BYTE_LEAD) {
    return sequence(1);
  }
  if (lead === THREE_BYTE_LEAD_BELOW_U_0800) {
    return sequence(CONTINUATIONS_AFTER_THREE_BYTE_LEAD, FIRST_SECOND_BYTE_AFTER_E0);
  }
  if (lead === THREE_BYTE_LEAD_OF_SURROGATES) {
    return sequence(
      CONTINUATIONS_AFTER_THREE_BYTE_LEAD,
      FIRST_CONTINUATION_BYTE,
      LAST_SECOND_BYTE_AFTER_ED,
    );
  }
  if (lead <= LAST_THREE_BYTE_LEAD) {
    return sequence(CONTINUATIONS_AFTER_THREE_BYTE_LEAD);
  }
  if (lead === FOUR_BYTE_LEAD_BELOW_U_10000) {
    return sequence(CONTINUATIONS_AFTER_FOUR_BYTE_LEAD, FIRST_SECOND_BYTE_AFTER_F0);
  }
  if (lead <= LAST_FOUR_BYTE_LEAD_IN_RANGE) {
    return sequence(CONTINUATIONS_AFTER_FOUR_BYTE_LEAD);
  }
  if (lead === FOUR_BYTE_LEAD_UP_TO_U_10FFFF) {
    return sequence(
      CONTINUATIONS_AFTER_FOUR_BYTE_LEAD,
      FIRST_CONTINUATION_BYTE,
      LAST_SECOND_BYTE_AFTER_F4,
    );
  }
  return null;
};

const decodeBytes = (bytes: Uint8Array, isLastChunk: boolean) => {
  const scan = { runStart: 0, sequenceStart: 0, ...sequence(0), texts: new Array<string>() };
  const endRun = (end: number) => {
    if (end > scan.runStart) {
      scan.texts.push(textDecoder.decode(bytes.subarray(scan.runStart, end)));
    }
    scan.runStart = end;
  };
  const escapeBytes = (start: number, end: number) => {
    endRun(start);
    for (const byte of bytes.subarray(start, end)) {
      scan.texts.push(ESCAPED_BYTES[byte] ?? "");
    }
    scan.runStart = end;
  };
  const startSequence = (index: number, lead: number) => {
    const started = sequenceStartedBy(lead);
    Object.assign(scan, started ?? sequence(0), { sequenceStart: index });
    if (started === null) {
      escapeBytes(index, index + 1);
    }
  };
  for (const index of bytes.keys()) {
    const byte = bytes[index] ?? 0;
    const isSingleByteCharacter = scan.missing === 0 && byte <= LAST_SINGLE_BYTE;
    if (isSingleByteCharacter) {
      continue;
    }
    if (scan.missing === 0) {
      startSequence(index, byte);
      continue;
    }
    const continuesTheSequence = byte >= scan.low && byte <= scan.high;
    if (continuesTheSequence) {
      scan.missing -= 1;
      scan.low = FIRST_CONTINUATION_BYTE;
      scan.high = LAST_CONTINUATION_BYTE;
      continue;
    }
    escapeBytes(scan.sequenceStart, index);
    startSequence(index, byte);
  }
  const isSequenceCut = scan.missing > 0;
  const isCutForGood = isSequenceCut && isLastChunk;
  if (isCutForGood) {
    escapeBytes(scan.sequenceStart, bytes.length);
  }
  const waitsForTheRest = isSequenceCut && !isLastChunk;
  const pendingStart = waitsForTheRest ? scan.sequenceStart : bytes.length;
  endRun(pendingStart);
  return { text: scan.texts.join(""), pending: Uint8Array.from(bytes.subarray(pendingStart)) };
};

const joinBytes = (first: Uint8Array, second: Uint8Array) => {
  if (first.length === 0) {
    return second;
  }
  const joined = new Uint8Array(first.length + second.length);
  joined.set(first);
  joined.set(second, first.length);
  return joined;
};

export const decodeOutputText = <Failure, Services>(
  bytes: Stream.Stream<Uint8Array, Failure, Services>,
) =>
  bytes.pipe(
    Stream.mapAccum(
      (): Uint8Array => new Uint8Array(),
      (pending: Uint8Array, chunk: Uint8Array): readonly [Uint8Array, readonly string[]] => {
        const decoded = decodeBytes(joinBytes(pending, chunk), false);
        return [decoded.pending, decoded.text === "" ? [] : [decoded.text]];
      },
      {
        onHalt: (pending: Uint8Array) =>
          pending.length === 0 ? [] : [decodeBytes(pending, true).text],
      },
    ),
  );

export const OutputPiece = Schema.Struct({
  text: Schema.String,
  continuesLine: Schema.Boolean,
  endsLine: Schema.Boolean,
});

const LINE_BREAK = /\r\n|\r|\n/u;

export const startWithNoLineOpen = () => ({ isLineOpen: false, endedWithCarriageReturn: false });

export const splitIntoPieces = (state: ReturnType<typeof startWithNoLineOpen>, text: string) => {
  const isRestOfABreak = state.endedWithCarriageReturn && text.startsWith("\n");
  const rest = isRestOfABreak ? text.slice(1) : text;
  if (rest === "") {
    return [
      { ...state, endedWithCarriageReturn: state.endedWithCarriageReturn && !isRestOfABreak },
      [],
    ] as const;
  }
  const segments = rest.split(LINE_BREAK);
  const pieces = segments.map((segment, index): typeof OutputPiece.Type => ({
    text: escapeLineSeparators(segment),
    continuesLine: index === 0 && state.isLineOpen,
    endsLine: index < segments.length - 1,
  }));
  const lastPiece = pieces.at(-1);
  const endsWithABreak = lastPiece?.text === "";
  return [
    { isLineOpen: !endsWithABreak, endedWithCarriageReturn: rest.endsWith("\r") },
    endsWithABreak ? pieces.slice(0, -1) : pieces,
  ] as const;
};

export const splitOutputPieces = <Failure, Services>(
  texts: Stream.Stream<string, Failure, Services>,
) =>
  texts.pipe(
    Stream.mapAccum(startWithNoLineOpen, splitIntoPieces, {
      onHalt: (state): readonly (typeof OutputPiece.Type)[] =>
        state.isLineOpen ? [{ text: "", continuesLine: true, endsLine: true }] : [],
    }),
  );

export const PRINTED_TO = ["terminal and file", "held"] as const;

export const OutputToJoin = Schema.TaggedUnion({
  Line: { text: Schema.String },
  Piece: { source: Schema.String, taskName: Schema.NullOr(Schema.String), piece: OutputPiece },
});

const startLine = (taskName: string | null, text: string) =>
  taskName === null ? labelLine("output", null, text) : `${nameTask(taskName)} ${text}`;

const joinPiece = (
  openSource: string | null,
  { source, taskName, piece }: typeof OutputToJoin.cases.Piece.Type,
) => {
  const ending = openSource === null ? "" : "\n";
  const continues = piece.continuesLine && openSource === source;
  const closesALineAlreadyClosed = piece.continuesLine && !continues && piece.text === "";
  if (closesALineAlreadyClosed) {
    return [openSource, []] as const;
  }
  const start = continues ? piece.text : `${ending}${startLine(taskName, piece.text)}`;
  return piece.endsLine ? ([null, [`${start}\n`]] as const) : ([source, [start]] as const);
};

export const joinOutput = (
  openSource: string | null,
  output: typeof OutputToJoin.Type,
): readonly [string | null, readonly string[]] =>
  OutputToJoin.match(output, {
    Line: ({ text }) => [null, [`${openSource === null ? "" : "\n"}${text}\n`]] as const,
    Piece: (piece) => joinPiece(openSource, piece),
  });
