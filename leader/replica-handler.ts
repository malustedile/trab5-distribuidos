import { proto, replicas } from "./leader";
import { leaderState } from "./leader-state";
import * as grpc from "@grpc/grpc-js";

export function replicateToReplicas(entry: any): Promise<number> {
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
    const log = leaderState.getLog();
    console.log(`[Leader] Log do líder: ${log.length} entradas`);

    // Determinar quais entradas enviar
    const startOffset = replicaState.lastOffset + 1;
    const entriesToSync = leaderState.filterEntriesFromOffset(startOffset);

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

export function commitToReplicas(entry: any) {
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
