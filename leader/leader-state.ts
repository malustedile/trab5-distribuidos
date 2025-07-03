// leader/leader-state.ts

import { REDIS_KEYS, RedisManager } from "../database/redis";

export interface LogEntry {
  epoch: number;
  offset: number;
  key: string;
  value: string;
  committed?: boolean;
  superseded?: boolean;
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

  setEpoch(newEpoch: number): void {
    this.epoch = newEpoch;
  }

  incrementOffset(): void {
    this.offset++;
  }

  appendEntry(key: string, value: string): LogEntry {
    const entry: LogEntry = {
      epoch: this.epoch,
      offset: this.offset,
      key,
      value,
      committed: false,
      superseded: false,
    };
    this.log.push(entry);
    return entry;
  }

  removeLastEntry(): void {
    this.log.pop();
  }

  findExistingEntry(key: string): number {
    return this.log.findIndex(
      (entry) => entry.key === key && !entry.superseded
    );
  }

  markEntrySuperseded(index: number): void {
    if (index >= 0 && index < this.log.length) {
      this.log[index].superseded = true;
    }
  }

  restoreEntry(index: number): void {
    if (index >= 0 && index < this.log.length) {
      this.log[index].superseded = false;
    }
  }

  markCommitted(entry: LogEntry) {
    const index = this.log.findIndex(
      (e) => e.epoch === entry.epoch && e.offset === entry.offset
    );
    if (index !== -1) {
      this.log[index].committed = true;
      this.database[entry.key] = entry.value;
    }
  }

  loadLogEntries(entries: LogEntry[]): void {
    this.log.push(...entries);
  }

  filterEntriesFromOffset(startOffset: number): LogEntry[] {
    return this.log.filter((entry) => entry.offset >= startOffset);
  }

  findLatestEntryForKey(key: string): LogEntry | undefined {
    return this.log
      .slice()
      .reverse()
      .find((entry) => entry.key === key && !entry.superseded);
  }

  read(key: string): string | undefined {
    return this.database[key];
  }
  async persistData(key: string, value: string, epoch: number, offset: number) {
    try {
      const dataKey = `${REDIS_KEYS.DATA}${key}`;
      const dataValue = JSON.stringify({
        value,
        epoch,
        offset,
        timestamp: Date.now(),
      });
      await RedisManager.set(dataKey, dataValue);
      console.log(
        `[Leader] Dados persistidos no Redis - Chave: ${key}, Valor: ${value}`
      );
    } catch (error) {
      console.error("[Leader] Erro ao persistir dados no Redis:", error);
    }
  }

  async persistState() {
    try {
      await RedisManager.set(REDIS_KEYS.EPOCH, this.getEpoch().toString());
      await RedisManager.set(REDIS_KEYS.OFFSET, this.getOffset().toString());
      await RedisManager.set(REDIS_KEYS.LOG, JSON.stringify(this.getLog()));
    } catch (error) {
      console.error("[Leader] Erro ao persistir estado no Redis:", error);
    }
  }

  async initialize() {
    try {
      const savedEpoch = await RedisManager.get(REDIS_KEYS.EPOCH);
      const savedOffset = await RedisManager.get(REDIS_KEYS.OFFSET);
      const savedLog = await RedisManager.get(REDIS_KEYS.LOG);
      console.log({ savedEpoch, savedOffset, savedLog });

      // Incrementar época a cada inicialização do líder
      if (savedEpoch) {
        const newEpoch = parseInt(savedEpoch) + 1;
        this.setEpoch(newEpoch);
        console.log(
          `[Leader] Nova época iniciada: ${newEpoch} (anterior: ${savedEpoch})`
        );
      } else {
        console.log(
          `[Leader] Primeira inicialização - Época: ${this.getEpoch()}`
        );
      }

      // Reset offset para nova época - LeaderState starts with offset 0

      // Carregar log anterior mas não usar para nova época
      if (savedLog) {
        const parsedLog = JSON.parse(savedLog);
        this.loadLogEntries(parsedLog);
        console.log(
          `[Leader] Log anterior carregado: ${parsedLog.length} entradas`
        );
      }

      // Persistir novo estado
      await this.persistState();

      const log = this.getLog();
      console.log(
        `[Leader] Estado inicializado - Época: ${this.getEpoch()}, Offset: ${this.getOffset()}, Log entries: ${
          log.length
        }`
      );
    } catch (error) {
      console.error("[Leader] Erro ao inicializar estado do Redis:", error);
    }
  }
}

export const leaderState = new LeaderState();
