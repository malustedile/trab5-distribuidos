import { replicas } from "./leader";
import { leaderState } from "./leader-state";
import { commitToReplicas, replicateToReplicas } from "./replica-handler";

export function sendData(call: any, callback: any) {
  const { key, value } = call.request;
  console.log(
    `[Leader] 📝 Nova solicitação de dados: key="${key}", value="${value}"`
  );

  // --- Verifica se já existe entrada para a chave ---
  const existingEntryIndex = leaderState.findExistingEntry(key);

  if (existingEntryIndex !== -1) {
    leaderState.markEntrySuperseded(existingEntryIndex);
    const existingEntry = leaderState.getLog()[existingEntryIndex];
    console.log(
      `[Leader] 🔁 Chave "${key}" já existe no offset ${existingEntry.offset}. Marcando como supersedida.`
    );
  }

  // --- Cria nova entrada no log ---
  const entry = leaderState.appendEntry(key, value);
  console.log(
    `[Leader] 🏷️  Criando entrada: epoch=${entry.epoch}, offset=${entry.offset}`
  );

  // --- Persiste log no Redis ---
  leaderState.persistState();
  console.log(`[Leader] 🔄 Iniciando processo de replicação...`);

  replicateToReplicas(entry)
    .then((ackCount) => {
      console.log(`[Leader] 📊 Replicação finalizada:`);
      console.log(`         - ACKs recebidos: ${ackCount}/${replicas.length}`);
      console.log(`         - Quórum necessário: 2`);

      if (ackCount >= 2) {
        // --- Commit com sucesso ---
        console.log(`[Leader] ✅ Quórum atingido! Realizando commit...`);

        commitToReplicas(entry);
        leaderState.persistData(key, value, entry.epoch, entry.offset);
        leaderState.incrementOffset();
        leaderState.persistState();

        console.log(`[Leader] 🎉 Dados commitados com sucesso!`);
        console.log(
          `[Leader] 📈 Próximo offset será: ${leaderState.getOffset()}`
        );

        return callback(null, {
          success: true,
          message: `Committed successfully. Replicated to ${ackCount} replicas.`,
        });
      }

      // --- Falha no quorum, rollback ---
      rollbackEntry(key, existingEntryIndex);
      console.log(`[Leader] ❌ Quórum não atingido!`);
      console.log(
        `[Leader] 💡 Dica: Verifique se pelo menos 2 réplicas estão online e funcionando`
      );

      callback(null, {
        success: false,
        message: `Failed to reach quorum. Only ${ackCount}/${replicas.length} replicas responded.`,
      });
    })
    .catch((error) => {
      // --- Erro durante replicação ---
      console.log(`[Leader] ❌ Erro durante replicação:`, error);
      rollbackEntry(key, existingEntryIndex);

      callback(null, {
        success: false,
        message: `Replication error: ${error.message}`,
      });
    });
}

// --- Função auxiliar para rollback ---
function rollbackEntry(key: string, previousIndex: number) {
  leaderState.removeLastEntry();

  if (previousIndex !== -1) {
    leaderState.restoreEntry(previousIndex);
    console.log(`[Leader] 🔙 Restaurando entrada anterior para chave "${key}"`);
  }

  console.log(`[Leader] 🔁 Operação revertida - entrada removida do log`);
}
