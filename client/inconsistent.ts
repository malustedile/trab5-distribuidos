// client/client.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import readline from "readline";
import path from "path";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const replica = new proto.replication.ReplicaService(
  "localhost:50052",
  grpc.credentials.createInsecure()
);

// o (0,15) Envia dados para o líder gravar;
// o (0,15) Consulta dados do líder.

replica.ReplicateLogEntry(
  {
    epoch: 95,
    offset: 157,
    key: 34,
    value: 54,
    committed: false,
  },
  (err: any, res: any) => {
    if (err) console.error("Erro:", err);
    else console.log("Resposta:", res.message);
  }
);
