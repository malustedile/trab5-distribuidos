// leader/leader-state.ts

import { RedisManager } from "../db/redis";

export interface LogEntry {
  epoch: number;
  offset: number;
  key: string;
  value: string;
  committed: boolean;
}

export class LeaderState {
  private epoch: number = 1;
  private offset: number = 0;
  private database: RedisManager = new RedisManager("leader");

  constructor() {
    this.loadState();
  }

  async loadState() {
    this.epoch = await this.database.getLeaderEpoch();
    console.log(`Leader state loaded with epoch: ${this.epoch}`);
  }

  getEpoch(): number {
    return this.epoch;
  }

  getOffset(): number {
    return this.offset;
  }

  getLog(): LogEntry[] {
    return this.getLog();
  }

  appendEntry(key: string, value: string): LogEntry {
    const entry: LogEntry = {
      epoch: this.epoch,
      offset: this.offset,
      key,
      value,
      committed: false,
    };
    this.database.saveLogEntry(entry);
    return entry;
  }

  markCommitted(epoch: number, offset: number) {
    this.database.commitLogEntry(epoch, offset);
  }

  async read(key: string): Promise<string | undefined> {
    const entry = await this.database.getLastCommittedValue(key);
    return entry ? entry.value : undefined;
  }
}

export const leaderState = new LeaderState();
