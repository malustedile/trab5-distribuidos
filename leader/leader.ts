// leader/leader.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { v4 as uuidv4 } from "uuid";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = "50051";

// Log e banco locais
let epoch = 1;
let offset = 0;
const log: any[] = [];
const database: Record<string, string> = {};

const replicas = [
  { address: "localhost:50052" },
  { address: "localhost:50053" },
  { address: "localhost:50054" },
];

function replicateToReplicas(entry: any): Promise<number> {
  let ackCount = 0;
  return new Promise((resolve) => {
    replicas.forEach((replica) => {
      const client = new proto.replication.ReplicaService(
        replica.address,
        grpc.credentials.createInsecure()
      );
      client.ReplicateLogEntry(entry, (err: any, res: any) => {
        if (!err && res.success) ackCount++;
        if (ackCount >= 2) resolve(ackCount); // maioria
      });
    });
  });
}

function commitToReplicas(entry: any) {
  replicas.forEach((replica) => {
    const client = new proto.replication.ReplicaService(
      replica.address,
      grpc.credentials.createInsecure()
    );
    client.CommitEntry(entry, () => {});
  });
}

function sendData(call: any, callback: any) {
  const { key, value } = call.request;
  const entry = { epoch, offset, key, value };

  log.push(entry);
  replicateToReplicas(entry).then((ackCount) => {
    if (ackCount >= 2) {
      commitToReplicas(entry);
      database[key] = value;
      offset++;
      callback(null, { success: true, message: "Committed" });
    } else {
      callback(null, { success: false, message: "Not enough replicas" });
    }
  });
}

function getData(call: any, callback: any) {
  const { key } = call.request;
  if (database[key]) {
    callback(null, { key, value: database[key] });
  } else {
    callback(null, { key, value: "" });
  }
}

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
      server.start();
    }
  );
}

main();
