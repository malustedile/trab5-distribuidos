import { LogEntry } from "../db/redis";
import { leaderState } from "./leader";

export function getLogState(_: any, callback: any) {
  getLogStateFromDatabase()
    .then((entries) => {
      callback(null, { entries });
    })
    .catch(() => undefined);
}

export async function getLogStateFromDatabase(): Promise<LogEntry[]> {
  return await leaderState.getLog();
}
