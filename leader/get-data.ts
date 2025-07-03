import { REDIS_KEYS, RedisManager } from "../database/redis";
import { leaderState } from "./leader-state";

export async function getData(call: any, callback: any) {
  const { key } = call.request;

  try {
    // Primeiro, buscar no log local (dados mais recentes, incluindo não commitados)
    const logEntry = leaderState.findLatestEntryForKey(key);

    if (logEntry) {
      console.log(
        `[Leader] Valor de "${key}" encontrado no log local: "${logEntry.value}" (epoch=${logEntry.epoch}, offset=${logEntry.offset})`
      );
      return callback(null, { key, value: logEntry.value });
    }

    // Se não encontrado no log, buscar no Redis (dados commitados)
    const dataKey = `${REDIS_KEYS.DATA}${key}`;
    const localData = await RedisManager.get(dataKey);

    if (localData) {
      const parsedData = JSON.parse(localData);
      console.log(
        `[Leader] Valor de "${key}" encontrado no Redis local: "${parsedData.value}"`
      );
      return callback(null, { key, value: parsedData.value });
    } else {
      console.log(`[Leader] Chave "${key}" não encontrada.`);
      callback(null, { key, value: "" });
    }
  } catch (error) {
    console.error("[Leader] Erro ao buscar dados:", error);
    callback(null, { key, value: "" });
  }
}
