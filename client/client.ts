// client/client.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import readline from "readline";
import path from "path";

const PROTO_PATH = path.join(__dirname, "../proto/replication.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH);
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const client = new proto.replication.ClientService(
  "localhost:50051",
  grpc.credentials.createInsecure()
);

// o (0,15) Envia dados para o líder gravar;
// o (0,15) Consulta dados do líder.

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

function prompt(): void {
  rl.question(
    "\n1 - Enviar dado\n2 - Consultar dado\n0 - Sair\nEscolha: ",
    (answer) => {
      if (answer === "1") {
        rl.question("Chave: ", (key) => {
          rl.question("Valor: ", (value) => {
            client.SendData({ key, value }, (err: any, res: any) => {
              if (err) console.error("Erro:", err);
              else console.log("Resposta:", res.message);
              prompt();
            });
          });
        });
      } else if (answer === "2") {
        rl.question("Chave para consulta: ", (key) => {
          client.GetData({ key }, (err: any, res: any) => {
            if (err) console.error("Erro:", err);
            else console.log(`Valor: ${res.value || "(não encontrado)"}`);
            prompt();
          });
        });
      } else if (answer === "0") {
        rl.close();
      } else {
        prompt();
      }
    }
  );
}

prompt();
