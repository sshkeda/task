import { Iterable } from "effect";

const LINE_BREAK = /\r\n|\r|\n/gu;

export const countLineBreaks = (text: string) => Iterable.size(text.matchAll(LINE_BREAK));

const LINE_FEED = /\n/gu;

export const countLineFeeds = (text: string) => Iterable.size(text.matchAll(LINE_FEED));
