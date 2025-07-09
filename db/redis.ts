import Redis from "ioredis";

const redis = new Redis({
  host: "localhost",
  port: 6379,
});

export interface LogEntry {
  epoch: number;
  offset: number;
  key: string;
  value: string;
  committed: boolean;
}

export class RedisManager {
  constructor(private readonly entity: string) {}

  async setEpoch(epoch: number): Promise<void> {
    await redis.set(`${this.entity}_epoch`, epoch);
  }
  async getStoredEpoch(entity: string): Promise<number> {
    const epoch = await redis.get(`${entity}_epoch`);

    if (!epoch) {
      await redis.set(`${entity}_epoch`, 0);
      return 0;
    }

    if (entity !== "leader") {
      await redis.set(`${entity}_epoch`, 0);
      return parseInt(epoch, 10);
    }

    const newEpoch = epoch ? parseInt(epoch, 10) + 1 : 0;
    await redis.set(`${entity}_epoch`, newEpoch);
    return newEpoch;
  }

  async saveLogEntry(entry: LogEntry) {
    const redisKey = `${this.entity}:${entry.epoch}:${entry.offset}`;

    await redis.hset(redisKey, {
      key: entry.key,
      value: entry.value,
      committed: entry.committed ? "1" : "0",
    });

    await redis.rpush(
      `${this.entity}:log_offsets:${entry.epoch}`,
      entry.offset.toString()
    );
    await this.setEpoch(entry.epoch);
  }

  async getLastCommittedValue(keyToFind: string): Promise<LogEntry | null> {
    const keys = await redis.keys(`${this.entity}:log_offsets:*`);
    const epochs = keys
      .map((k) => parseInt(k.split(":").at(-1) as string))
      .sort((a, b) => b - a);

    for (const epoch of epochs) {
      const logOffsetKey = `${this.entity}:log_offsets:${epoch}`;
      const offsets = await redis.lrange(logOffsetKey, 0, -1);
      const sortedOffsets = offsets.map(Number).sort((a, b) => b - a);

      for (const offset of sortedOffsets) {
        const entry = await this.getLogEntry(epoch, offset);
        if (entry && entry.committed && entry.key === keyToFind) {
          return entry;
        }
      }
    }

    return null;
  }

  async getLogEntry(epoch: number, offset: number): Promise<LogEntry | null> {
    const redisKey = `${this.entity}:${epoch}:${offset}`;
    const data = await redis.hgetall(redisKey);

    if (!data || Object.keys(data).length === 0) {
      return null;
    }
    return {
      epoch,
      offset,
      key: data.key,
      value: data.value,
      committed: data.committed === "1",
    };
  }

  async commitLogEntry(epoch: number, offset: number) {
    const redisKey = `${this.entity}:${epoch}:${offset}`;
    await redis.hset(redisKey, "committed", "1");
  }

  async getAllLogs(): Promise<LogEntry[]> {
    const logs: LogEntry[] = [];

    const logOffsetKeys = await redis.keys(`${this.entity}:log_offsets:*`);

    const epochs = logOffsetKeys
      .map((key) => parseInt(key.split(":").at(-1) as string))
      .sort((a, b) => a - b);

    // For each epoch, get all offsets and retrieve log entries
    for (const epoch of epochs) {
      const logOffsetKey = `${this.entity}:log_offsets:${epoch}`;
      const offsets = await redis.lrange(logOffsetKey, 0, -1);
      const sortedOffsets = offsets.map(Number).sort((a, b) => a - b);

      for (const offset of sortedOffsets) {
        const entry = await this.getLogEntry(epoch, offset);
        if (entry) {
          logs.push(entry);
        }
      }
    }

    return logs;
  }

  async setNewStateFromLog(logs: LogEntry[]) {
    const existingKeys = await redis.keys(`${this.entity}:*`);
    if (existingKeys.length > 0) {
      await redis.del(...existingKeys);
    }

    for (const entry of logs) {
      console.log(
        `Saving log entry: epoch=${entry.epoch}, offset=${entry.offset}, key=${entry.key}, value=${entry.value}, committed=${entry.committed}`
      );
      await this.saveLogEntry(entry);
      if (entry.committed) {
        await this.commitLogEntry(entry.epoch, entry.offset);
      }
    }
    return logs;
  }
}
