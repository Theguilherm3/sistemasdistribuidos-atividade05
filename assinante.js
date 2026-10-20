// Desafio: assinante da exchange fanout.
// Cada servico tem a sua propria fila, entao cada um recebe todos os eventos.
// Uso: node assinante.js notificacoes
//      node assinante.js analytics
const amqp = require("amqplib");
const { RABBIT_URL, EXCHANGE_FANOUT, log } = require("./config");

const servico = process.argv[2];
if (!servico) {
  console.log("Uso: node assinante.js <notificacoes|analytics>");
  process.exit(1);
}

async function main() {
  const conexao = await amqp.connect(RABBIT_URL);
  const canal = await conexao.createChannel();

  await canal.assertExchange(EXCHANGE_FANOUT, "fanout", { durable: true });
  const nomeFila = `evento.${servico}`;
  await canal.assertQueue(nomeFila, { durable: true });
  await canal.bindQueue(nomeFila, EXCHANGE_FANOUT, "");

  log(servico, `fila "${nomeFila}" ligada na exchange "${EXCHANGE_FANOUT}", aguardando eventos...`);

  canal.consume(nomeFila, (msg) => {
    if (msg === null) return;
    const evento = JSON.parse(msg.content.toString());

    if (servico === "analytics") {
      log(servico, `pedido=${evento.id} registrado nas metricas (valor=${evento.valor})`);
    } else {
      log(servico, `pedido=${evento.id} enviando notificacao para ${evento.cliente}`);
    }
    canal.ack(msg);
  });

  process.on("SIGINT", async () => {
    await conexao.close();
    process.exit(0);
  });
}

main().catch((erro) => {
  console.error("Erro no assinante:", erro.message);
  process.exit(1);
});
