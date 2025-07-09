import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { leaderState } from "./leader";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

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

export function sendData(call: any, callback: any) {
  const { key, value } = call.request;
  leaderState.appendEntry(key, value).then((entry) => {
    replicateToReplicas(entry).then((ackCount) => {
      if (ackCount >= 2) {
        commitToReplicas(entry);
        leaderState.markCommitted(entry.epoch, entry.offset);
        callback(null, { success: true, message: "Committed" });
      } else {
        callback(null, { success: false, message: "Not enough replicas" });
      }
    });
  });
}
