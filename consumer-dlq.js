// Le as mensagens que foram parar na dead letter queue
// Uso: node consumer-dlq.js
const amqp = require("amqplib");
const { RABBIT_URL, FILA_DLQ, criarFilas, log } = require("./config");

async function main() {
  const conexao = await amqp.connect(RABBIT_URL);
  const canal = await conexao.createChannel();
  await criarFilas(canal);

  log("dlq", `aguardando mensagens na fila "${FILA_DLQ}"...`);

  canal.consume(FILA_DLQ, (msg) => {
    if (msg === null) return;

    const pedido = JSON.parse(msg.content.toString());
    // o RabbitMQ coloca o header x-death com o motivo
    const headers = msg.properties.headers || {};
    const morte = headers["x-death"];
    const motivo = morte ? morte[0].reason : "desconhecido";

    log("dlq", `pedido=${pedido.id} cliente=${pedido.cliente} caiu na DLQ (motivo=${motivo}) - precisa de analise manual`);
    canal.ack(msg);
  });

  process.on("SIGINT", async () => {
    await conexao.close();
    process.exit(0);
  });
}

main().catch((erro) => {
  console.error("Erro no consumer da DLQ:", erro.message);
  process.exit(1);
});
