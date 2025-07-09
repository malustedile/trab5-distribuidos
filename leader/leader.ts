// leader/leader.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { sendData } from "./send-data";
import { getData } from "./get-data";
import { DatabaseState } from "../db/database-state";
import { getLogState } from "./get-log-state";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = "50051";
export const leaderState = new DatabaseState("leader");

function main() {
  const server = new grpc.Server();
  server.addService(proto.replication.ClientService.service, {
    SendData: sendData,
    GetData: getData,
    GetLogState: getLogState,
  });
  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    () => {
      console.log(`Leader gRPC server running at ${PORT}`);
    }
  );
}

main();
