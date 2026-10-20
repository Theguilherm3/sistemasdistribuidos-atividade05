# Aula 05 - Mensageria, Filas e Publish-Subscribe

Mini laboratório: pedido assíncrono com RabbitMQ, feito em Node.js usando a biblioteca `amqplib`.

## Arquivos

| Arquivo | O que faz |
|---|---|
| `docker-compose.yml` | Sobe o RabbitMQ com o painel de gerenciamento |
| `config.js` | URL do broker, nomes das filas e a função que cria a fila `notificacoes` (já ligada na DLQ) |
| `producer.js` | Publica pedidos em JSON (`id`, `cliente`, `valor`) na fila `notificacoes` |
| `consumer.js` | Lê os pedidos, simula o processamento e dá `ack`. Pode falhar de propósito |
| `consumer-dlq.js` | Lê a fila de dead letter `notificacoes.dlq` |
| `publicar-evento.js` | (Desafio) Publica eventos numa exchange fanout |
| `assinante.js` | (Desafio) Assinante da exchange fanout (Notificações ou Analytics) |

## Preparação

```bash
docker compose up -d     # sobe o RabbitMQ
npm install              # instala o amqplib
```

Painel web: http://localhost:15672 (usuário `guest`, senha `guest`).

## Passo a passo

### 1 e 2. Subir o RabbitMQ e criar a fila `notificacoes`

O RabbitMQ sobe pelo `docker compose up -d`. A fila não precisa ser criada na mão: o producer e o
consumer chamam `criarFilas()` (em `config.js`), que usa `assertQueue`. Se a fila não existe, ela é criada;
se já existe, nada muda. A fila é `durable`, ou seja, continua existindo se o RabbitMQ reiniciar.

### 3. Producer

```bash
node producer.js 3
```

Publica 3 pedidos e encerra. As mensagens são `persistent` e cada uma leva o `id` do pedido.

### 4. Consumer

```bash
node consumer.js consumer-1
```

Fica escutando a fila. Cada pedido demora 1 segundo pra "processar" e depois recebe `ack`.
Todo log tem `pedido=<id>`, assim dá pra seguir o mesmo pedido nos logs do producer e do consumer.

### 5. Parar o consumer e publicar com ele desligado

Pare o consumer (Ctrl+C) e rode:

```bash
node producer.js 5
docker exec rabbitmq-aula05 rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

A fila `notificacoes` deve mostrar 5 mensagens em `messages_ready`. Elas ficam guardadas no broker
mesmo sem ninguém consumindo (também dá pra ver isso no painel, em *Queues*).

### 6. Religar o consumer

```bash
node consumer.js consumer-1
```

O consumer processa as 5 mensagens que estavam esperando, uma atrás da outra.

### 7. Falha proposital e redelivery

O segundo argumento do consumer é um nome de cliente que vai dar erro:

```bash
node consumer.js consumer-1 Ana
node producer.js 1 Ana      # em outro terminal
```

O que acontece:
1. O consumer recebe o pedido (`redelivered=false`), dá erro e faz `nack` com `requeue=true`.
2. O RabbitMQ devolve a mensagem pra fila e entrega de novo, agora com `redelivered=true`.
3. Falhou de novo, então o consumer faz `nack` com `requeue=false`.

Sem a DLQ isso viraria um loop infinito (a mensagem voltaria pra fila pra sempre) ou a mensagem seria
descartada. Por isso o passo 8.

### 8. Dead Letter Queue

A fila `notificacoes` foi criada com os argumentos:

```js
"x-dead-letter-exchange": "",
"x-dead-letter-routing-key": "notificacoes.dlq"
```

Então quando o consumer rejeita sem requeue, o RabbitMQ manda a mensagem pra `notificacoes.dlq`.
Para ver:

```bash
node consumer-dlq.js
```

> Obs: se a fila `notificacoes` já tiver sido criada antes **sem** esses argumentos, o RabbitMQ
> reclama com `PRECONDITION_FAILED`. Nesse caso é só apagar a fila no painel (ou rodar
> `docker exec rabbitmq-aula05 rabbitmqctl delete_queue notificacoes`) e rodar de novo.

## Desafio adicional

### Dois consumidores concorrentes

```bash
node consumer.js consumer-1    # terminal 1
node consumer.js consumer-2    # terminal 2
node producer.js 10            # terminal 3
```

Os dois consumers leem da **mesma fila**, então cada mensagem vai pra só um deles. Com `prefetch(1)`
o RabbitMQ só manda uma nova mensagem quando o consumer termina a anterior, e o trabalho fica dividido
mais ou menos meio a meio (round-robin). Cada pedido aparece no log de só um dos consumers.

### Exchange fanout (Notificações e Analytics)

```bash
node assinante.js notificacoes   # terminal 1
node assinante.js analytics      # terminal 2
node publicar-evento.js 3        # terminal 3
```

Aqui cada assinante tem sua própria fila (`evento.notificacoes` e `evento.analytics`) ligada na exchange
`pedidos.eventos`. O fanout copia a mensagem pra todas as filas ligadas, então **os dois** recebem
todos os eventos. É a diferença entre fila (divide trabalho) e pub/sub (espalha o evento).

## Evidências de execução

> Colar aqui os logs / prints de cada passo.

- Passo 3/4 (producer e consumer funcionando):
- Passo 5 (`list_queues` com mensagens pendentes):
- Passo 6 (consumer processando o acumulado):
- Passo 7 (log com `redelivered=false` e depois `redelivered=true`):
- Passo 8 (mensagem chegando na DLQ):
- Desafio 1 (mensagens divididas entre consumer-1 e consumer-2):
- Desafio 2 (mesmo evento nos logs de notificacoes e analytics):

### Problemas encontrados

> Anotar aqui o que deu errado durante a execução e como foi resolvido.

## Conclusão

Com a fila no meio, o producer e o consumer não precisam estar ligados ao mesmo tempo: o producer
publica e já termina, e as mensagens ficam guardadas no RabbitMQ até alguém consumir. Isso diminui o
acoplamento temporal entre os componentes. Por outro lado, apareceram problemas novos: como o `ack` é
manual, uma mensagem que falha volta pra fila e pode ser processada mais de uma vez, então o consumer
precisa ser idempotente. A DLQ ajudou a tirar da fila as mensagens que nunca vão dar certo, sem travar
o resto. E o `id` do pedido nos logs foi essencial pra entender o caminho de cada mensagem, ainda mais
com dois consumers rodando juntos.

## Questões de revisão

**1. Quando uma fila é mais adequada que uma chamada HTTP direta?**

Quando quem chama não precisa da resposta na hora. Por exemplo, mandar e-mail ou notificação depois de
um pedido. Com HTTP, se o outro serviço estiver fora ou lento, quem chamou fica esperando ou dá erro.
Com fila, a mensagem fica guardada e é processada quando o consumer estiver disponível. Também é bom
pra absorver picos (a fila segura as mensagens) e pra distribuir trabalho entre vários workers. Já se
o cliente precisa do resultado imediatamente (ex.: consultar saldo), HTTP faz mais sentido.

**2. Qual a diferença entre Queue e Publish/Subscribe?**

Na queue, cada mensagem é entregue pra **um** consumer só. Se tem vários consumers, eles dividem o
trabalho. No pub/sub, a mensagem (evento) é entregue pra **todos** os assinantes, cada um com sua
cópia. No laboratório: `consumer-1` e `consumer-2` dividem os pedidos da fila `notificacoes`, enquanto
Notificações e Analytics recebem os mesmos eventos pela exchange fanout.

**3. Por que acknowledgement pode levar a redelivery?**

Porque o RabbitMQ só apaga a mensagem quando recebe o `ack`. Se o consumer dá `nack` com requeue, ou
cai/perde a conexão antes de mandar o `ack`, o broker entende que a mensagem não foi processada e entrega
de novo. O problema é que o consumer pode ter feito o trabalho e caído logo antes do `ack`, aí a
mensagem é processada duas vezes.

**4. Qual a relação entre at-least-once e idempotência?**

At-least-once garante que a mensagem vai ser entregue pelo menos uma vez, mas pode ser mais de uma
(por causa da redelivery da questão 3). Então o consumer precisa ser idempotente: processar a mesma
mensagem duas vezes tem que dar o mesmo resultado que processar uma vez. Um jeito comum é guardar os
`id`s dos pedidos já processados e ignorar quando chegar repetido.

**5. Qual a finalidade de uma Dead Letter Queue?**

Separar as mensagens que não conseguem ser processadas (dados inválidos, erro que sempre se repete,
mensagem expirada). Sem a DLQ, essas mensagens ficariam voltando pra fila pra sempre, gastando recurso
e atrasando as outras, ou seriam perdidas. Na DLQ elas ficam guardadas pra alguém analisar, corrigir e
reprocessar depois.
# sistemasdistribuidos-atividade05
