const MILLISECONDS_PER_SECOND = 1000;

export const formatSeconds = (elapsedMilliseconds: number) =>
  `${(elapsedMilliseconds / MILLISECONDS_PER_SECOND).toFixed(1)}s`;
