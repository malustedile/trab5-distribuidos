// leader/leader-state.ts

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
  private log: LogEntry[] = [];
  private database: Record<string, string> = {};

  getEpoch(): number {
    return this.epoch;
  }

  getOffset(): number {
    return this.offset;
  }

  getLog(): LogEntry[] {
    return this.log;
  }

  appendEntry(key: string, value: string): LogEntry {
    const entry: LogEntry = {
      epoch: this.epoch,
      offset: this.offset,
      key,
      value,
      committed: false,
    };
    this.log.push(entry);
    return entry;
  }

  markCommitted(entry: LogEntry) {
    const index = this.log.findIndex(
      (e) => e.epoch === entry.epoch && e.offset === entry.offset
    );
    if (index !== -1) {
      this.log[index].committed = true;
      this.database[entry.key] = entry.value;
      this.offset++;
    }
  }

  read(key: string): string | undefined {
    return this.database[key];
  }
}
