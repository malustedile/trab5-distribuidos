// leader/leader.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { leaderState } from "./leader-state";
import { RedisManager } from "../database/redis";
import { getData } from "./get-data";
import { sendData } from "./send-data";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
export const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = "50051";

export const replicas = [
  { address: "localhost:50052" },
  { address: "localhost:50053" },
  { address: "localhost:50054" },
];

async function main() {
  // Inicializar estado do Redis primeiro
  await leaderState.initialize();

  const server = new grpc.Server();
  server.addService(proto.replication.ClientService.service, {
    SendData: sendData,
    GetData: getData,
  });
  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    () => {
      console.log(`Leader gRPC server running at ${PORT}`);
      console.log(`Connected to Redis at localhost:6379`);
    }
  );

  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log("\n[Leader] Recebido SIGINT, fazendo shutdown graceful...");
    await leaderState.persistState();
    await RedisManager.quit();
    server.forceShutdown();
    process.exit(0);
  });
}

main();
