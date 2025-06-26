// leader/leader.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { v4 as uuidv4 } from "uuid";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = "50051";

// o (0,15) Recebe solicitações de gravação de dados do cliente e é
// responsável pela replicação desses dados. Ele salva os dados em
// seu log local, contendo época (versão do líder) e offset (número
// sequencial que representa a posição da entrada no log dentro de
// uma determinada época, indicando a ordem dos registros);

// o (0,15) Envia a nova entrada para as réplicas (modelo Push) e
// aguarda as confirmações (acks);

// o (0,15) Após receber a confirmação da maioria das réplicas, envia
// uma ordem de commit para que as réplicas efetivem a gravação
// no banco de dados final;

// o (0,15) O líder só marca uma entrada como committed após
// receber confirmação da maioria (quórum). Quando isso acontecer,
// ele deve confirmar a gravação ao cliente;

// o (0,15) Persiste todos os dados (intermediários e finais) com época
// e offset;

// o (0,15) Responde consultas do cliente.

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
  console.log("chegou");
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
