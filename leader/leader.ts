// leader/leader.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { createClient } from "redis";
import path from "path";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const PORT = "50051";

// Configuração do Redis
const redisClient = createClient({
  url: "redis://localhost:6379",
});

// Inicializar conexão com Redis
redisClient.on("error", (err) => console.log("Redis Client Error", err));
redisClient.connect();

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

// Chaves Redis para persistência
const REDIS_KEYS = {
  EPOCH: "leader:epoch",
  OFFSET: "leader:offset",
  LOG: "leader:log",
  DATA: "data:", // prefixo para dados do usuário
};

// Função para inicializar estado do líder do Redis
async function initializeLeaderState() {
  try {
    const savedEpoch = await redisClient.get(REDIS_KEYS.EPOCH);
    const savedOffset = await redisClient.get(REDIS_KEYS.OFFSET);
    const savedLog = await redisClient.get(REDIS_KEYS.LOG);
    console.log({ savedEpoch, savedOffset, savedLog });

    // Incrementar época a cada inicialização do líder
    if (savedEpoch) {
      epoch = parseInt(savedEpoch) + 1;
      console.log(
        `[Leader] Nova época iniciada: ${epoch} (anterior: ${savedEpoch})`
      );
    } else {
      epoch = 1;
      console.log(`[Leader] Primeira inicialização - Época: ${epoch}`);
    }

    // Reset offset para nova época
    offset = 0;

    // Carregar log anterior mas não usar para nova época
    if (savedLog) {
      const parsedLog = JSON.parse(savedLog);
      log.push(...parsedLog);
      console.log(
        `[Leader] Log anterior carregado: ${parsedLog.length} entradas`
      );
    }

    // Persistir novo estado
    await persistState();

    console.log(
      `[Leader] Estado inicializado - Época: ${epoch}, Offset: ${offset}, Log entries: ${log.length}`
    );
  } catch (error) {
    console.error("[Leader] Erro ao inicializar estado do Redis:", error);
  }
}

// Função para persistir estado no Redis
async function persistState() {
  try {
    await redisClient.set(REDIS_KEYS.EPOCH, epoch.toString());
    await redisClient.set(REDIS_KEYS.OFFSET, offset.toString());
    await redisClient.set(REDIS_KEYS.LOG, JSON.stringify(log));
  } catch (error) {
    console.error("[Leader] Erro ao persistir estado no Redis:", error);
  }
}

// Função para persistir dados do usuário no Redis
async function persistData(
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
      `[Leader] Dados persistidos no Redis - Chave: ${key}, Valor: ${value}`
    );
  } catch (error) {
    console.error("[Leader] Erro ao persistir dados no Redis:", error);
  }
}

const replicas = [
  { address: "localhost:50052" },
  { address: "localhost:50053" },
  { address: "localhost:50054" },
];

function replicateToReplicas(entry: any): Promise<number> {
  console.log(
    `[Leader] Iniciando replicação para entrada: epoch=${entry.epoch}, offset=${entry.offset}, key=${entry.key}`
  );

  const promises: Promise<boolean>[] = [];

  replicas.forEach((replica) => {
    const promise = new Promise<boolean>((resolve) => {
      replicateToSingleReplica(replica, entry, resolve);
    });
    promises.push(promise);
  });

  return Promise.all(promises).then((results) => {
    const ackCount = results.filter((success) => success).length;
    console.log(
      `[Leader] Resultado final da replicação: ${ackCount}/${replicas.length} ACKs recebidos`
    );
    return ackCount;
  });
}

// Função auxiliar para replicar para uma única réplica com retry automático
function replicateToSingleReplica(
  replica: any,
  entry: any,
  resolve: (success: boolean) => void,
  attempt: number = 1,
  alreadySynced: boolean = false
) {
  const maxAttempts = 3;

  console.log(
    `[Leader] Tentativa ${attempt}/${maxAttempts} para réplica ${replica.address}`
  );

  const client = new proto.replication.ReplicaService(
    replica.address,
    grpc.credentials.createInsecure()
  );

  // Set proper deadline as Date object
  const deadline = new Date();
  deadline.setSeconds(deadline.getSeconds() + 5);

  client.ReplicateLogEntry(entry, { deadline }, (err: any, res: any) => {
    if (err) {
      console.log(
        `[Leader] ❌ Erro de conexão com ${replica.address} (tentativa ${attempt}): ${err.message}`
      );
      console.log(
        `[Leader] 🔍 Código do erro: ${err.code}, Detalhes: ${
          err.details || "N/A"
        }`
      );

      if (attempt < maxAttempts && (err.code === 14 || err.code === 4)) {
        // UNAVAILABLE or DEADLINE_EXCEEDED
        console.log(`[Leader] 🔁 Tentando novamente em 1 segundo...`);
        setTimeout(() => {
          replicateToSingleReplica(
            replica,
            entry,
            resolve,
            attempt + 1,
            alreadySynced
          );
        }, 1000);
      } else {
        console.log(
          `[Leader] ❌ Desistindo da réplica ${replica.address} após ${attempt} tentativas`
        );
        return resolve(false);
      }
      return;
    }

    if (res.success) {
      console.log(
        `[Leader] ✅ ACK recebido de ${replica.address} na tentativa ${attempt}`
      );
      return resolve(true);
    }

    if (!res.success && res.currentState) {
      console.log(
        `[Leader] 🔄 Réplica ${replica.address} inconsistente na tentativa ${attempt}. Sincronizando...`
      );

      // Se já tentamos sincronizar antes, não tentar novamente para evitar loop infinito
      if (alreadySynced) {
        console.log(
          `[Leader] ❌ Réplica ${replica.address} já foi sincronizada mas ainda está inconsistente. Desistindo.`
        );
        return resolve(false);
      }

      syncReplica(replica, res.currentState)
        .then(() => {
          console.log(
            `[Leader] ✅ Sincronização concluída para ${replica.address}`
          );

          // Verificar se a entrada atual já foi incluída na sincronização
          const entryAlreadySynced = res.currentState.lastOffset < entry.offset;

          if (entryAlreadySynced) {
            // A entrada atual foi incluída na sincronização, considerar como sucesso
            console.log(
              `[Leader] ✅ Entrada já sincronizada para ${replica.address}, considerando como ACK`
            );
            return resolve(true);
          } else {
            // A entrada não foi incluída na sincronização, tentar replicação novamente
            console.log(
              `[Leader] 🔁 Tentando replicação novamente após sincronização para ${replica.address}`
            );
            setTimeout(() => {
              replicateToSingleReplica(replica, entry, resolve, 1, true); // Reset attempt counter to 1, mark as already synced
            }, 100); // Pequeno delay para garantir que a sincronização foi processada
          }
        })
        .catch((syncErr) => {
          console.log(
            `[Leader] ❌ Erro na sincronização de ${replica.address}:`,
            syncErr.message
          );
          resolve(false);
        });
    } else {
      console.log(
        `[Leader] ❌ Falha na réplica ${replica.address}:`,
        res.message
      );
      resolve(false);
    }
  });
}

// Função para sincronizar uma réplica específica
async function syncReplica(replica: any, replicaState: any): Promise<void> {
  return new Promise((resolve, reject) => {
    const client = new proto.replication.ReplicaService(
      replica.address,
      grpc.credentials.createInsecure()
    );

    console.log(`[Leader] Sincronizando réplica ${replica.address}`);
    console.log(
      `[Leader] Estado da réplica: lastEpoch=${replicaState.lastEpoch}, lastOffset=${replicaState.lastOffset}`
    );
    console.log(`[Leader] Log do líder: ${log.length} entradas`);

    // Determinar quais entradas enviar
    const startOffset = replicaState.lastOffset + 1;
    const entriesToSync = log.filter((entry) => entry.offset >= startOffset);

    console.log(
      `[Leader] Entradas a sincronizar: ${entriesToSync.length} (a partir do offset ${startOffset})`
    );

    if (entriesToSync.length === 0) {
      console.log(`[Leader] Réplica ${replica.address} já está sincronizada`);
      return resolve();
    }

    console.log(`[Leader] Enviando entradas:`);
    entriesToSync.forEach((entry, index) => {
      console.log(
        `[Leader]   ${index + 1}/${entriesToSync.length}: epoch=${
          entry.epoch
        }, offset=${entry.offset}, key=${entry.key}`
      );
    });

    const syncRequest = {
      entries: entriesToSync,
      startOffset: startOffset,
    };

    // Set timeout for sync operation
    const deadline = new Date();
    deadline.setSeconds(deadline.getSeconds() + 10); // Longer timeout for sync

    client.SyncLog(syncRequest, { deadline }, (err: any, res: any) => {
      if (!err && res.success) {
        console.log(`[Leader] Sincronização concluída para ${replica.address}`);
        resolve();
      } else {
        console.log(
          `[Leader] Erro na sincronização de ${replica.address}:`,
          err ? err.message : res.message
        );
        reject(err || new Error(res.message));
      }
    });
  });
}

function commitToReplicas(entry: any) {
  console.log(
    `[Leader] Enviando comando de commit para as réplicas: epoch=${entry.epoch}, offset=${entry.offset}, key=${entry.key}`
  );

  replicas.forEach((replica) => {
    const client = new proto.replication.ReplicaService(
      replica.address,
      grpc.credentials.createInsecure()
    );
    client.CommitEntry(entry, (err: any, res: any) => {
      if (!err && res && res.success) {
        console.log(`[Leader] Commit confirmado por ${replica.address}`);
      } else {
        console.log(
          `[Leader] Erro no commit em ${replica.address}:`,
          err || (res && res.message)
        );
      }
    });
  });
}

function sendData(call: any, callback: any) {
  const { key, value } = call.request;

  console.log(
    `[Leader] 📝 Nova solicitação de dados: key="${key}", value="${value}"`
  );

  // Verificar se a chave já existe no log
  const existingEntryIndex = log.findIndex(
    (entry) => entry.key === key && !entry.superseded
  );

  if (existingEntryIndex !== -1) {
    // Marcar entrada existente como supersedida
    log[existingEntryIndex].superseded = true;
    console.log(
      `[Leader] � Chave "${key}" já existe no offset ${log[existingEntryIndex].offset}. Marcando como supersedida.`
    );
  }

  const entry = { epoch, offset, key, value, superseded: false };

  console.log(`[Leader] 🏷️  Criando entrada: epoch=${epoch}, offset=${offset}`);

  log.push(entry);

  // Persistir log no Redis
  persistState();

  console.log(`[Leader] 🔄 Iniciando processo de replicação...`);

  replicateToReplicas(entry)
    .then((ackCount) => {
      console.log(`[Leader] 📊 Processo de replicação finalizado:`);
      console.log(
        `[Leader]    - ACKs recebidos: ${ackCount}/${replicas.length}`
      );
      console.log(`[Leader]    - Quórum necessário: 2`);

      if (ackCount >= 2) {
        console.log(`[Leader] ✅ Quórum atingido! Prosseguindo com commit...`);

        commitToReplicas(entry);

        // Persistir dados finais no Redis após commit
        persistData(key, value, epoch, offset);

        // Incrementar offset APÓS commit bem-sucedido
        offset++;
        persistState(); // Atualizar offset no Redis

        console.log(`[Leader] 🎉 Dados commitados com sucesso!`);
        console.log(`[Leader] 📈 Próximo offset será: ${offset}`);

        callback(null, {
          success: true,
          message: `Committed successfully. Replicated to ${ackCount} replicas.`,
        });
      } else {
        // Se não conseguiu replicar, remover a entrada do log e restaurar a anterior se houver
        log.pop();
        if (existingEntryIndex !== -1) {
          log[existingEntryIndex].superseded = false;
          console.log(
            `[Leader] 🔙 Restaurando entrada anterior para chave "${key}"`
          );
        }
        console.log(`[Leader] ❌ Quórum não atingido!`);
        console.log(
          `[Leader] 🔙 Revertendo operação - entrada removida do log`
        );
        console.log(
          `[Leader] 💡 Dica: Verifique se pelo menos 2 réplicas estão online e funcionando`
        );

        callback(null, {
          success: false,
          message: `Failed to reach quorum. Only ${ackCount}/${replicas.length} replicas responded.`,
        });
      }
    })
    .catch((error) => {
      console.log(`[Leader] ❌ Erro durante replicação:`, error);
      log.pop(); // Remover entrada em caso de erro
      if (existingEntryIndex !== -1) {
        log[existingEntryIndex].superseded = false;
        console.log(
          `[Leader] 🔙 Restaurando entrada anterior para chave "${key}"`
        );
      }
      callback(null, {
        success: false,
        message: `Replication error: ${error.message}`,
      });
    });
}

async function getData(call: any, callback: any) {
  const { key } = call.request;

  try {
    // Primeiro, buscar no log local (dados mais recentes, incluindo não commitados)
    const logEntry = log
      .slice()
      .reverse() // Buscar do mais recente para o mais antigo
      .find((entry) => entry.key === key && !entry.superseded);

    if (logEntry) {
      console.log(
        `[Leader] Valor de "${key}" encontrado no log local: "${logEntry.value}" (epoch=${logEntry.epoch}, offset=${logEntry.offset})`
      );
      return callback(null, { key, value: logEntry.value });
    }

    // Se não encontrado no log, buscar no Redis (dados commitados)
    const dataKey = `${REDIS_KEYS.DATA}${key}`;
    const localData = await redisClient.get(dataKey);

    if (localData) {
      const parsedData = JSON.parse(localData);
      console.log(
        `[Leader] Valor de "${key}" encontrado no Redis local: "${parsedData.value}"`
      );
      return callback(null, { key, value: parsedData.value });
    } else {
      console.log(`[Leader] Chave "${key}" não encontrada.`);
      callback(null, { key, value: "" });
    }
  } catch (error) {
    console.error("[Leader] Erro ao buscar dados:", error);
    callback(null, { key, value: "" });
  }
}

async function main() {
  // Inicializar estado do Redis primeiro
  await initializeLeaderState();

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
      server.start();
    }
  );

  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log("\n[Leader] Recebido SIGINT, fazendo shutdown graceful...");
    await persistState();
    await redisClient.quit();
    server.forceShutdown();
    process.exit(0);
  });
}

main();
