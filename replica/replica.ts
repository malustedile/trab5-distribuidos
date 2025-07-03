// replica/replica.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { createClient } from "redis";
import path from "path";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = process.argv[2] || "50052";

// Configuração do Redis para dados committed
const redisClient = createClient({
  url: "redis://localhost:6379",
});

// Inicializar conexão com Redis
redisClient.on("error", (err) =>
  console.log(`[${PORT}] Redis Client Error`, err)
);
redisClient.connect();

// Chaves Redis para dados committed
const REDIS_KEYS = {
  DATA: `replica:${PORT}:committed:`, // prefixo para dados committed
};

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

// Função para persistir dados committed no Redis
async function persistCommittedData(
  key: string,
  value: string,
  epoch: number,
  offset: number
) {
  try {
    const dataKey = `${REDIS_KEYS.DATA}${key}`;
    const dataValue = JSON.stringify({
      value,
      epoch,
      offset,
      timestamp: Date.now(),
    });
    await redisClient.set(dataKey, dataValue);
    console.log(
      `[${PORT}] Dados committed persistidos no Redis - Chave: ${key}, Valor: ${value}`
    );
  } catch (error) {
    console.error(
      `[${PORT}] Erro ao persistir dados committed no Redis:`,
      error
    );
  }
}

// Função para buscar dados committed do Redis
async function getCommittedDataFromRedis(key: string): Promise<string> {
  try {
    const dataKey = `${REDIS_KEYS.DATA}${key}`;
    const data = await redisClient.get(dataKey);

    if (data) {
      const parsedData = JSON.parse(data);
      return parsedData.value;
    }
    return "";
  } catch (error) {
    console.error(`[${PORT}] Erro ao buscar dados committed no Redis:`, error);
    return "";
  }
}

function isConsistent(newEntry: LogEntry): boolean {
  console.log(
    `[${PORT}] Verificando consistência para entrada: epoch=${newEntry.epoch}, offset=${newEntry.offset}`
  );

  if (log.length === 0) {
    const isConsistent = newEntry.offset === 0;
    console.log(
      `[${PORT}] Log vazio. Esperando offset 0, recebido ${newEntry.offset}. Consistente: ${isConsistent}`
    );
    return isConsistent;
  }

  const last = log[log.length - 1];

  // Verificar se é uma nova época (maior que a atual)
  if (newEntry.epoch > last.epoch) {
    // Nova época deve sempre começar com offset 0
    const isConsistent = newEntry.offset === 0;
    console.log(
      `[${PORT}] Nova época detectada: ${last.epoch} -> ${newEntry.epoch}`
    );
    console.log(
      `[${PORT}] Esperado offset 0 para nova época, recebido ${newEntry.offset}. Consistente: ${isConsistent}`
    );
    return isConsistent;
  }

  // Verificar se é época anterior (não deveria acontecer)
  if (newEntry.epoch < last.epoch) {
    console.log(
      `[${PORT}] Época anterior detectada: ${newEntry.epoch} < ${last.epoch}. Inconsistente.`
    );
    return false;
  }

  // Mesma época - verificar continuidade do offset
  const expectedOffset = last.offset + 1;
  const isConsistent = newEntry.offset === expectedOffset;

  console.log(`[${PORT}] Mesma época: ${newEntry.epoch}`);
  console.log(
    `[${PORT}] Última entrada: epoch=${last.epoch}, offset=${last.offset}`
  );
  console.log(
    `[${PORT}] Esperado: epoch=${last.epoch}, offset=${expectedOffset}`
  );
  console.log(
    `[${PORT}] Recebido: epoch=${newEntry.epoch}, offset=${newEntry.offset}`
  );
  console.log(`[${PORT}] Consistente: ${isConsistent}`);

  return isConsistent;
}

function replicateLogEntry(call: any, callback: any) {
  const entry = call.request as LogEntry;
  console.log(
    `[${PORT}] Recebida entrada: epoch=${entry.epoch}, offset=${entry.offset}, key=${entry.key}`
  );
  console.log(`[${PORT}] Estado atual do log: ${log.length} entradas`);

  if (log.length > 0) {
    const last = log[log.length - 1];
    console.log(
      `[${PORT}] Última entrada: epoch=${last.epoch}, offset=${last.offset}`
    );

    // Se recebemos uma nova época, limpar o log anterior
    if (entry.epoch > last.epoch) {
      console.log(
        `[${PORT}] Nova época detectada (${last.epoch} -> ${entry.epoch}). Limpando log anterior.`
      );
      log = [];
    }
  }

  if (!isConsistent(entry)) {
    // truncar log
    const oldLogLength = log.length;
    log = log.filter(
      (e) =>
        e.epoch < entry.epoch ||
        (e.epoch === entry.epoch && e.offset < entry.offset)
    );
    console.log(
      `[${PORT}] Inconsistência detectada. Log truncado de ${oldLogLength} para ${log.length} entradas.`
    );

    // Retornar estado atual para o líder saber onde sincronizar
    const currentState =
      log.length > 0
        ? {
            lastEpoch: log[log.length - 1].epoch,
            lastOffset: log[log.length - 1].offset,
          }
        : { lastEpoch: entry.epoch, lastOffset: -1 }; // Use a época da entrada recebida

    console.log(
      `[${PORT}] Retornando estado atual: epoch=${currentState.lastEpoch}, offset=${currentState.lastOffset}`
    );

    return callback(null, {
      success: false,
      message: "Log inconsistente. Truncado.",
      currentState: currentState,
    });
  }

  log.push({ ...entry, committed: false });
  console.log(`[${PORT}] Entrada adicionada (uncommitted):`, entry);
  callback(null, { success: true, message: "ACK" });
}

function commitEntry(call: any, callback: any) {
  const { epoch, offset } = call.request;
  console.log(
    `[${PORT}] Recebido comando de commit: epoch=${epoch}, offset=${offset}`
  );

  const idx = log.findIndex((e) => e.epoch === epoch && e.offset === offset);
  if (idx !== -1) {
    log[idx].committed = true;
    console.log(`[${PORT}] Committed:`, log[idx]);

    // Persistir dados committed no Redis
    persistCommittedData(
      log[idx].key,
      log[idx].value,
      log[idx].epoch,
      log[idx].offset
    );
  } else {
    console.log(
      `[${PORT}] ERRO: Entrada não encontrada para commit - epoch=${epoch}, offset=${offset}`
    );
    console.log(
      `[${PORT}] Log atual:`,
      log.map(
        (e) =>
          `epoch=${e.epoch}, offset=${e.offset}, key=${e.key}, committed=${e.committed}`
      )
    );
  }
  callback(null, { success: true, message: "Commit aplicado" });
}

function syncLog(call: any, callback: any) {
  const { entries, startOffset } = call.request;

  console.log(
    `[${PORT}] Iniciando sincronização a partir do offset ${startOffset}`
  );
  console.log(
    `[${PORT}] Log atual antes da sincronização: ${log.length} entradas`
  );
  console.log(
    `[${PORT}] Recebendo ${entries.length} entradas para sincronização`
  );

  // Limpar log a partir do startOffset
  const oldLog = [...log];
  log = log.filter((e) => e.offset < startOffset);
  console.log(
    `[${PORT}] Log filtrado: removidas ${oldLog.length - log.length} entradas`
  );

  // Adicionar as novas entradas
  entries.forEach((entry: LogEntry, index: number) => {
    console.log(
      `[${PORT}] Adicionando entrada ${index + 1}/${entries.length}: epoch=${
        entry.epoch
      }, offset=${entry.offset}, key=${entry.key}`
    );
    log.push({ ...entry, committed: true });
  });

  console.log(
    `[${PORT}] Log sincronizado. Estado final: ${log.length} entradas`
  );

  // Mostrar estado final do log
  log.forEach((entry, index) => {
    console.log(
      `[${PORT}] Log[${index}]: epoch=${entry.epoch}, offset=${entry.offset}, key=${entry.key}, committed=${entry.committed}`
    );
  });

  callback(null, { success: true, message: "Log sincronizado" });
}

function getDataByKey(call: any, callback: any) {
  const { key } = call.request;

  // Primeiro, buscar no Redis (dados committed)
  getCommittedDataFromRedis(key)
    .then((redisValue) => {
      if (redisValue) {
        console.log(
          `[${PORT}] Valor encontrado no Redis para chave "${key}": "${redisValue}"`
        );
        return callback(null, { key, value: redisValue });
      }

      // Se não encontrou no Redis, buscar no log em memória
      const committedEntry = log.find(
        (entry) => entry.key === key && entry.committed
      );

      if (committedEntry) {
        console.log(
          `[${PORT}] Valor encontrado no log para chave "${key}": "${committedEntry.value}"`
        );
        callback(null, { key, value: committedEntry.value });
      } else {
        console.log(`[${PORT}] Chave "${key}" não encontrada ou não committed`);
        callback(null, { key, value: "" });
      }
    })
    .catch((error) => {
      console.error(`[${PORT}] Erro ao buscar no Redis:`, error);

      // Fallback para log em memória
      const committedEntry = log.find(
        (entry) => entry.key === key && entry.committed
      );

      if (committedEntry) {
        console.log(
          `[${PORT}] Valor encontrado no log (fallback) para chave "${key}": "${committedEntry.value}"`
        );
        callback(null, { key, value: committedEntry.value });
      } else {
        console.log(`[${PORT}] Chave "${key}" não encontrada ou não committed`);
        callback(null, { key, value: "" });
      }
    });
}

function main() {
  console.log(`[${PORT}] Iniciando réplica na porta ${PORT}...`);
  console.log(
    `[${PORT}] ⚠️  NOTA: Primera replicação pode falhar devido a inconsistências iniciais`
  );
  console.log(
    `[${PORT}] ✅ Sistema irá sincronizar automaticamente e tentar novamente`
  );

  const server = new grpc.Server();
  server.addService(proto.replication.ReplicaService.service, {
    ReplicateLogEntry: replicateLogEntry,
    CommitEntry: commitEntry,
    SyncLog: syncLog,
    GetDataByKey: getDataByKey,
  });
  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    () => {
      console.log(`✅ Réplica escutando na porta ${PORT}`);
      console.log(`📡 Connected to Redis at localhost:6379`);
      console.log(`🔄 Pronta para sincronização com líder`);
      server.start();
    }
  );

  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log(`\n[${PORT}] Recebido SIGINT, fazendo shutdown graceful...`);
    await redisClient.quit();
    server.forceShutdown();
    process.exit(0);
  });
}

main();
