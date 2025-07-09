// replica/replica.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { LogEntry } from "../db/redis";
import { DatabaseState } from "../db/database-state";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = process.argv[2] || "50052";
const dbName = `replica:${PORT}`;
const database = new DatabaseState(dbName);
console.log(
  `database: ${dbName} epoch: ${database.epoch}, offset: ${database.offset}`
);

const leader = new proto.replication.ClientService(
  "localhost:50051",
  grpc.credentials.createInsecure()
);
function isConsistent(log: LogEntry[], newEntry: LogEntry): boolean {
  if (newEntry.epoch != database.epoch) return false;
  const last = log[log.length - 1];
  if (!last) return false;
  return newEntry.epoch === last.epoch && newEntry.offset === last.offset + 1;
}

function syncLogs() {
  return new Promise<void>((resolve) => {
    leader.GetLogState({}, (_: any, response: any) => {
      const entries = response.entries as LogEntry[];
      database.setLog(entries).then(() => {
        console.log(`Log sincronizado com ${entries.length} entradas.`);
        resolve();
      });
    });
  });
}

async function appendEntry(
  entry: LogEntry,
  callback: (...args: any[]) => void
) {
  console.log(
    `[${PORT}] Recebendo entrada: epoch=${entry.epoch} offset=${entry.offset}, epoca_atual=${database.epoch}, offset_atual=${database.offset}`
  );
  const log = await database.getLog();
  if (!isConsistent(log, entry)) {
    await syncLogs();
  }
  await database.appendEntry(entry.key, entry.value);
  database.epoch = entry.epoch;
  database.offset = entry.offset;
  callback(null, { success: true, message: "ACK" });
  console.log(
    `[${PORT}] Entrada replicada: epoch=${entry.epoch} offset=${entry.offset} - key=${entry.key} value=${entry.value}`
  );
}

function replicateLogEntry(call: any, callback: any) {
  const entry = call.request as LogEntry;

  appendEntry(entry, callback);
}

function commitEntry(call: any, callback: any) {
  const { epoch, offset } = call.request;
  database
    .appendEntry(epoch, offset)
    .then(() => {
      console.log(`[${PORT}] Commit aplicado: epoch=${epoch} offset=${offset}`);
      callback(null, { success: true, message: "Commit aplicado" });
    })
    .catch(() => {
      callback(null, { success: false, message: "Commit não aplicado" });
    });
}

function main() {
  const server = new grpc.Server();
  server.addService(proto.replication.ReplicaService.service, {
    ReplicateLogEntry: replicateLogEntry,
    CommitEntry: commitEntry,
  });
  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    () => {
      console.log(`Réplica escutando na porta ${PORT}`);
      // server.start();
    }
  );
}

main();
