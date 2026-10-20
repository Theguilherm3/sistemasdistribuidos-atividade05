// Desafio: publica um evento "pedido criado" numa exchange fanout.
// Todo mundo que estiver ligado na exchange recebe uma copia.
// Uso: node publicar-evento.js [quantidade]
const amqp = require("amqplib");
const crypto = require("crypto");
const { RABBIT_URL, EXCHANGE_FANOUT, log } = require("./config");

async function main() {
  const quantidade = parseInt(process.argv[2] || "2");

  const conexao = await amqp.connect(RABBIT_URL);
  const canal = await conexao.createChannel();
  await canal.assertExchange(EXCHANGE_FANOUT, "fanout", { durable: true });

  for (let i = 0; i < quantidade; i++) {
    const evento = {
      tipo: "PedidoCriado",
      id: "PED-" + crypto.randomUUID().slice(0, 8),
      cliente: "Cliente " + (i + 1),
      valor: Number((Math.random() * 500 + 10).toFixed(2)),
    };

    // no fanout a routing key e ignorada, por isso ""
    canal.publish(EXCHANGE_FANOUT, "", Buffer.from(JSON.stringify(evento)), { persistent: true });
    log("publisher", `evento PedidoCriado pedido=${evento.id} publicado na exchange ${EXCHANGE_FANOUT}`);
  }

  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error("Erro ao publicar evento:", erro.message);
  process.exit(1);
});
