// replica/replica.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = process.argv[2] || "50052";

// o (0,15) Recebe entradas de log do líder e armazena de forma
// persistente essas entradas localmente como dados
// intermediários (uncommited), que não podem ser
// considerados finais e nem serem lidos até a ordem de commit
// do líder. Deve-se enviar uma confirmação (ack) ao líder;

// o (0,35) Verificar se a nova entrada é consistente com o log local.
// Quando uma réplica recebe uma entrada de log do líder, ela espera
// que essa entrada seja a continuação exata do seu próprio log local,
// ou seja, que a época e o offset estejam alinhados com o que ela
// já tem.
// § (0,15) Em caso de consistência, deve-se aceitar a nova
// entrada corretamente;
// § (0,2) Em caso de inconsistência, a réplica deve truncar o log
// local, ou seja, apagar as entradas a partir do offset (índice)
// conflitante para remover dados inconsistentes ou que não
// foram confirmados pelo líder atual. Dessa forma, a réplica
// descarta as entradas divergentes e retorna a um estado
// consistente em relação ao líder. Em seguida, informa seu
// estado atual de log ao líder, permitindo que este envie
// novamente as entradas corretas, a partir do ponto de
// sincronização, para reconstruir o log na forma correta.

// o (0,15) Ao receber a ordem de commit do líder, efetiva a gravação
// no banco de dados final, tornando os dados visíveis e confiáveis
// para leitura.

// o (0,15) Persiste todos os dados (intermediários e finais) com época
// e offset.

interface LogEntry {
  epoch: number;
  offset: number;
  key: string;
  value: string;
  committed: boolean;
}

let log: LogEntry[] = [];
const database: Record<string, string> = {};

function isConsistent(newEntry: LogEntry): boolean {
  if (log.length === 0) return newEntry.offset === 0;
  const last = log[log.length - 1];
  return newEntry.epoch === last.epoch && newEntry.offset === last.offset + 1;
}

function replicateLogEntry(call: any, callback: any) {
  const entry = call.request as LogEntry;
  if (!isConsistent(entry)) {
    // truncar log
    log = log.filter(
      (e) =>
        e.epoch < entry.epoch ||
        (e.epoch === entry.epoch && e.offset < entry.offset)
    );
    console.log(`[${PORT}] Inconsistência detectada. Log truncado.`);
    return callback(null, {
      success: false,
      message: "Log inconsistente. Truncado.",
    });
  }
  log.push({ ...entry, committed: false });
  console.log(`[${PORT}] Entrada adicionada (uncommitted):`, entry);
  callback(null, { success: true, message: "ACK" });
}

function commitEntry(call: any, callback: any) {
  const { epoch, offset } = call.request;
  const idx = log.findIndex((e) => e.epoch === epoch && e.offset === offset);
  if (idx !== -1) {
    log[idx].committed = true;
    database[log[idx].key] = log[idx].value;
    console.log(`[${PORT}] Committed:`, log[idx]);
  }
  callback(null, { success: true, message: "Commit aplicado" });
}

function getDataByKey(call: any, callback: any) {
  const { key } = call.request;
  const value = database[key];

  if (value === undefined) {
    console.log(`[${PORT}] Consulta: chave "${key}" não encontrada.`);
    return callback(null, { key, value: "" }); // Ou você pode lançar erro se preferir
  }

  console.log(`[${PORT}] Consulta: chave "${key}" retornou valor "${value}".`);
  callback(null, { key, value });
}

function main() {
  const server = new grpc.Server();
  server.addService(proto.replication.ReplicaService.service, {
    ReplicateLogEntry: replicateLogEntry,
    CommitEntry: commitEntry,
    GetDataByKey: getDataByKey,
  });
  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    () => {
      console.log(`Réplica escutando na porta ${PORT}`);
      server.start();
    }
  );
}

main();
