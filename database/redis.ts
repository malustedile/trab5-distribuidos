import { createClient } from "redis";
// Configuração do Redis
const redisClient = createClient({
  url: "redis://localhost:6379",
});

// Inicializar conexão com Redis
redisClient.on("error", (err) => console.log("Redis Client Error", err));
redisClient.connect();

// Chaves Redis para persistência
export const REDIS_KEYS = {
  EPOCH: "leader:epoch",
  OFFSET: "leader:offset",
  LOG: "leader:log",
  DATA: "data:", // prefixo para dados do usuário
};

export class RedisManager {
  static async quit() {
    await redisClient.quit();
  }
  static async set(key: string, value: string): Promise<void> {
    await redisClient.set(key, value);
  }

  static async get(key: string): Promise<string | null> {
    return await redisClient.get(key);
  }

  static async delete(key: string): Promise<void> {
    await redisClient.del(key);
  }

  static async exists(key: string): Promise<boolean> {
    const result = await redisClient.exists(key);
    return result > 0;
  }
}

export default redisClient;
