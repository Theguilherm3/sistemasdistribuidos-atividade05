// Producer: publica pedidos na fila "notificacoes"
// Uso: node producer.js [quantidade] [cliente]
const amqp = require("amqplib");
const crypto = require("crypto");
const { RABBIT_URL, FILA, criarFilas, log } = require("./config");

const clientes = ["Ana", "Bruno", "Carla", "Diego", "Elisa"];

async function main() {
  const quantidade = parseInt(process.argv[2] || "3");
  const clienteFixo = process.argv[3];

  const conexao = await amqp.connect(RABBIT_URL);
  const canal = await conexao.createChannel();
  await criarFilas(canal);

  for (let i = 0; i < quantidade; i++) {
    const pedido = {
      id: "PED-" + crypto.randomUUID().slice(0, 8),
      cliente: clienteFixo || clientes[Math.floor(Math.random() * clientes.length)],
      valor: Number((Math.random() * 500 + 10).toFixed(2)),
    };

    // persistent: true -> mensagem e salva em disco, sobrevive a restart do broker
    canal.sendToQueue(FILA, Buffer.from(JSON.stringify(pedido)), {
      persistent: true,
      contentType: "application/json",
      messageId: pedido.id,
    });

    log("producer", `pedido=${pedido.id} cliente=${pedido.cliente} valor=${pedido.valor} publicado`);
  }

  // o producer nao espera ninguem processar, so publica e sai
  log("producer", `${quantidade} mensagem(ns) enviada(s), encerrando`);
  await canal.close();
  await conexao.close();
}

main().catch((erro) => {
  console.error("Erro no producer:", erro.message);
  process.exit(1);
});
