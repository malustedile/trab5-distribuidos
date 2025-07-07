// leader/leader.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { sendData } from "./send-data";
import { getData } from "./get-data";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = "50051";

function main() {
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
    }
  );
}

main();
