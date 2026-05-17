/**
 * Structured logger for rocketmq services.
 *
 * Produces timestamped, leveled log lines:
 *   2026-05-17T09:00:00.123Z [INFO]  [order-service] queue 'orders' ready
 */

export type LogLevel = "DEBUG" | "INFO" | "WARN" | "ERROR";

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
};

export class Logger {
  private readonly name: string;
  private readonly minLevel: LogLevel;

  constructor(name: string, minLevel: LogLevel = "DEBUG") {
    this.name = name;
    this.minLevel = minLevel;
  }

  private shouldLog(level: LogLevel): boolean {
    return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[this.minLevel];
  }

  private format(level: LogLevel, msg: string): string {
    const ts = new Date().toISOString();
    return `${ts} [${level.padEnd(5)}] [${this.name}] ${msg}`;
  }

  debug(msg: string, ...args: unknown[]): void {
    if (!this.shouldLog("DEBUG")) return;
    console.debug(this.format("DEBUG", msg), ...args);
  }

  info(msg: string, ...args: unknown[]): void {
    if (!this.shouldLog("INFO")) return;
    console.info(this.format("INFO", msg), ...args);
  }

  warn(msg: string, ...args: unknown[]): void {
    if (!this.shouldLog("WARN")) return;
    console.warn(this.format("WARN", msg), ...args);
  }

  error(msg: string, ...args: unknown[]): void {
    if (!this.shouldLog("ERROR")) return;
    console.error(this.format("ERROR", msg), ...args);
  }
}
