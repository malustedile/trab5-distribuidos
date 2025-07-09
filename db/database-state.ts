// leader/leader-state.ts

import { RedisManager } from "./redis";

export interface LogEntry {
  epoch: number;
  offset: number;
  key: string;
  value: string;
  committed: boolean;
}

export class DatabaseState {
  epoch: number = 1;
  offset: number = 0;
  private database: RedisManager;

  constructor(entity: string) {
    this.database = new RedisManager(entity);
    this.loadState(entity);
  }

  async loadState(entity: string) {
    this.epoch = await this.database.getStoredEpoch(entity);
    console.log(`${entity} state loaded with epoch: ${this.epoch}`);
  }

  async setEpoch(epoch: number): Promise<void> {
    await this.database.setEpoch(epoch);
  }

  getEpoch(): number {
    return this.epoch;
  }

  getOffset(): number {
    return this.offset;
  }

  async getLog(): Promise<LogEntry[]> {
    return await this.database.getAllLogs();
  }

  async setLog(logEntry: LogEntry[]): Promise<LogEntry[]> {
    return await this.database.setNewStateFromLog(logEntry);
  }

  async appendEntry(key: string, value: string): Promise<LogEntry> {
    const entry: LogEntry = {
      epoch: +this.epoch,
      offset: +this.offset,
      key: key,
      value: value,
      committed: false,
    };
    this.offset += 1;
    await this.database.saveLogEntry(entry);
    return entry;
  }

  async markCommitted(epoch: number, offset: number) {
    await this.database.commitLogEntry(epoch, offset);
  }

  async read(key: string): Promise<string | undefined> {
    const entry = await this.database.getLastCommittedValue(key);
    return entry ? entry.value : undefined;
  }
}
