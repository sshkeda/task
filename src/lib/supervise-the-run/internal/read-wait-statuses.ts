import { TerminalCommandEnded } from "../../../shared/run-in-terminal.ts";

const LINUX_SIGNAL_NAMES = [
  "SIGHUP",
  "SIGINT",
  "SIGQUIT",
  "SIGILL",
  "SIGTRAP",
  "SIGABRT",
  "SIGBUS",
  "SIGFPE",
  "SIGKILL",
  "SIGUSR1",
  "SIGSEGV",
  "SIGUSR2",
  "SIGPIPE",
  "SIGALRM",
  "SIGTERM",
  "SIGSTKFLT",
  "SIGCHLD",
  "SIGCONT",
  "SIGSTOP",
  "SIGTSTP",
  "SIGTTIN",
  "SIGTTOU",
  "SIGURG",
  "SIGXCPU",
  "SIGXFSZ",
  "SIGVTALRM",
  "SIGPROF",
  "SIGWINCH",
  "SIGIO",
  "SIGPWR",
  "SIGSYS",
] as const;

const MACOS_SIGNAL_NAMES = [
  "SIGHUP",
  "SIGINT",
  "SIGQUIT",
  "SIGILL",
  "SIGTRAP",
  "SIGABRT",
  "SIGEMT",
  "SIGFPE",
  "SIGKILL",
  "SIGBUS",
  "SIGSEGV",
  "SIGSYS",
  "SIGPIPE",
  "SIGALRM",
  "SIGTERM",
  "SIGURG",
  "SIGSTOP",
  "SIGTSTP",
  "SIGCONT",
  "SIGCHLD",
  "SIGTTIN",
  "SIGTTOU",
  "SIGIO",
  "SIGXCPU",
  "SIGXFSZ",
  "SIGVTALRM",
  "SIGPROF",
  "SIGWINCH",
  "SIGINFO",
  "SIGUSR1",
  "SIGUSR2",
] as const;

const SIGNAL_NAMES: readonly string[] =
  process.platform === "darwin" ? MACOS_SIGNAL_NAMES : LINUX_SIGNAL_NAMES;

const FIRST_SIGNAL_NUMBER = 1;

const TERMINATING_SIGNAL_BITS = 0x7f;
const STOPPED_MARK = 0x7f;
const BITS_PER_BYTE = 8;
const BYTE_MASK = 0xff;

const nameSignal = (signalNumber: number) =>
  SIGNAL_NAMES[signalNumber - FIRST_SIGNAL_NUMBER] ?? `signal ${signalNumber}`;

export const numberSignal = (
  signal: (typeof LINUX_SIGNAL_NAMES)[number] & (typeof MACOS_SIGNAL_NAMES)[number],
) => SIGNAL_NAMES.indexOf(signal) + FIRST_SIGNAL_NUMBER;

export const readWaitStatus = (status: number) => {
  const terminatingSignal = status & TERMINATING_SIGNAL_BITS;
  const highByte = (status >> BITS_PER_BYTE) & BYTE_MASK;
  if (terminatingSignal === 0) {
    return { ended: TerminalCommandEnded.cases.Exited.make({ code: highByte }) };
  }
  if (terminatingSignal === STOPPED_MARK) {
    return { stoppedBy: nameSignal(highByte) };
  }
  return {
    ended: TerminalCommandEnded.cases.Signaled.make({ signal: nameSignal(terminatingSignal) }),
  };
};
