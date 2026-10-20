// Consumer: le os pedidos da fila "notificacoes" e "processa"
// Uso: node consumer.js [nome] [clienteQueFalha]
//   nome            -> so pra identificar nos logs quando tiver mais de um consumer
//   clienteQueFalha -> se passado, pedidos desse cliente dao erro de proposito
const amqp = require("amqplib");
const { RABBIT_URL, FILA, criarFilas, log } = require("./config");

const nome = process.argv[2] || "consumer";
const clienteQueFalha = process.argv[3];
const TEMPO_PROCESSAMENTO = 1000; // ms, pra simular um trabalho demorado

function esperar(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processar(pedido) {
  await esperar(TEMPO_PROCESSAMENTO);
  if (clienteQueFalha && pedido.cliente === clienteQueFalha) {
    throw new Error(`falha simulada para o cliente ${pedido.cliente}`);
  }
}

async function main() {
  const conexao = await amqp.connect(RABBIT_URL);
  const canal = await conexao.createChannel();
  await criarFilas(canal);

  // pega so 1 mensagem por vez, assim o RabbitMQ divide melhor entre consumers
  canal.prefetch(1);

  log(nome, `aguardando mensagens na fila "${FILA}"...`);
  if (clienteQueFalha) {
    log(nome, `modo falha ativado para o cliente "${clienteQueFalha}"`);
  }

  canal.consume(FILA, async (msg) => {
    if (msg === null) return;

    const pedido = JSON.parse(msg.content.toString());
    const reentrega = msg.fields.redelivered;

    log(nome, `pedido=${pedido.id} recebido (cliente=${pedido.cliente}, valor=${pedido.valor}, redelivered=${reentrega})`);

    try {
      await processar(pedido);
      canal.ack(msg);
      log(nome, `pedido=${pedido.id} processado com sucesso -> ack`);
    } catch (erro) {
      if (!reentrega) {
        // primeira falha: devolve pra fila pra tentar de novo
        log(nome, `pedido=${pedido.id} ERRO: ${erro.message} -> nack com requeue (vai ser reentregue)`);
        canal.nack(msg, false, true);
      } else {
        // ja foi reentregue e falhou de novo: manda pra DLQ
        log(nome, `pedido=${pedido.id} ERRO de novo: ${erro.message} -> nack sem requeue (vai pra DLQ)`);
        canal.nack(msg, false, false);
      }
    }
  }, { noAck: false });

  // Ctrl+C fecha a conexao direitinho
  process.on("SIGINT", async () => {
    log(nome, "encerrando consumer");
    await conexao.close();
    process.exit(0);
  });
}

main().catch((erro) => {
  console.error("Erro no consumer:", erro.message);
  process.exit(1);
});
