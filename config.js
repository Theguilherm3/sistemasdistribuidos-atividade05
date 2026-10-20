// Configuracoes compartilhadas entre producer e consumers
const RABBIT_URL = process.env.RABBIT_URL || "amqp://guest:guest@localhost:5672";

const FILA = "notificacoes";
const FILA_DLQ = "notificacoes.dlq";
const EXCHANGE_FANOUT = "pedidos.eventos";

// Cria a fila principal ja apontando pra DLQ.
// Mensagens rejeitadas com requeue=false vao pra notificacoes.dlq
async function criarFilas(canal) {
  await canal.assertQueue(FILA_DLQ, { durable: true });
  await canal.assertQueue(FILA, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": "",
      "x-dead-letter-routing-key": FILA_DLQ,
    },
  });
}

function log(origem, mensagem) {
  const hora = new Date().toISOString();
  console.log(`${hora} [${origem}] ${mensagem}`);
}

module.exports = { RABBIT_URL, FILA, FILA_DLQ, EXCHANGE_FANOUT, criarFilas, log };
