const verboseConsole =
  (process.env.ENABLE_VERBOSE_CONSOLE || process.env.CONSOLE_LOG_LEVEL || 'false')
    .toLowerCase()
    .trim() === 'true' ||
  (process.env.CONSOLE_LOG_LEVEL || '').toLowerCase().trim() === 'verbose' ||
  (process.env.LOG_LEVEL || '').toLowerCase().trim() === 'debug';

const baseConsoleLog = console.log.bind(console);

const DEFAULT_VISIBLE_PREFIXES = [
  '[OPERATOR]',
  '[STARTUP]',
];

function shouldKeepConsoleLog(args: any[]): boolean {
  if (verboseConsole) {
    return true;
  }

  const first = args?.[0];
  if (typeof first !== 'string') {
    return false;
  }

  return DEFAULT_VISIBLE_PREFIXES.some((prefix) => first.startsWith(prefix));
}

export function installOperatorConsoleFilter(): void {
  if ((console as any).__operatorConsoleFilterInstalled) {
    return;
  }

  (console as any).__operatorConsoleFilterInstalled = true;
  console.log = (...args: any[]) => {
    if (!shouldKeepConsoleLog(args)) {
      return;
    }
    baseConsoleLog(...args);
  };
}

export function truncateLogValue(value: unknown, maxChars = 160): string {
  const normalized = String(value ?? '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!normalized) {
    return 'n/a';
  }

  return normalized.length > maxChars
    ? `${normalized.slice(0, maxChars)}...`
    : normalized;
}

export function formatOperatorFields(fields: Record<string, unknown>): string {
  return Object.entries(fields)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `  ${key}: ${truncateLogValue(value)}`)
    .join('\n');
}

export function operatorLog(title: string, fields: Record<string, unknown> = {}): void {
  const body = formatOperatorFields(fields);
  console.log(`[OPERATOR] ${title}${body ? `\n${body}` : ''}`);
}

installOperatorConsoleFilter();
