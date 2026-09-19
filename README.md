# Go Webhook Processor

Serviço HTTP em Go para recebimento e processamento assíncrono de eventos via webhook.

A aplicação foi desenhada para um cenário comum de backend: receber eventos de sistemas externos, validar o payload, persistir o evento e sua intenção de publicação na mesma transação, responder rapidamente ao cliente e processar o trabalho em segundo plano usando workers concorrentes.

## Objetivo

Este serviço evita bloquear requisições HTTP enquanto tarefas potencialmente demoradas são executadas. O endpoint `POST /events` valida, deduplica e persiste atomicamente o evento com uma mensagem de Outbox. Um dispatcher publica essa mensagem na fila de forma assíncrona. O processamento ocorre de forma assíncrona por workers em goroutines.

Esse padrão é útil para:

- webhooks de pagamento
- integrações com ERPs e CRMs
- processamento de pedidos
- envio de notificações
- tarefas internas baseadas em eventos
- chamadas para APIs externas com maior latência

## Fluxo

```text
Cliente externo
  -> POST /events
    -> validação do JSON
      -> transação PostgreSQL: events + outbox
        -> resposta HTTP 202 Accepted
          -> dispatcher lê mensagens pendentes
            -> RabbitMQ / EventQueue
              -> workers processam com retry/backoff
                -> atualização do status para processed ou failed
```

## Endpoints

### GET /health

Retorna o status da aplicação e informações básicas do buffer local de entregas do RabbitMQ.

### GET /ready

Verifica se a aplicação consegue acessar o PostgreSQL. Retorna `200` quando está pronta para receber tráfego e `503` durante indisponibilidade do banco.

### GET /metrics

Retorna um snapshot dos principais contadores operacionais da aplicação.

```json
{
  "eventsQueued": 10,
  "eventsRejected": 2,
  "eventsDuplicated": 1,
  "eventsSkippedDuplicate": 2,
  "eventsProcessed": 8,
  "eventsFailedPermanent": 1,
  "eventRetries": 3,
  "queueLength": 1,
  "queueCapacity": 100
}
```

### GET /dead-letters

Retorna os eventos que falharam permanentemente após esgotar as tentativas de retry. Este endpoint consulta falhas permanentes persistidas no PostgreSQL, mesmo após a reinicialização da aplicação.

### POST /internal/dead-letters/{event-id}/replay

Reprocessa uma dead letter específica. Exige o header `Authorization: Bearer <DLQ_REPLAY_TOKEN>` e retorna `202 Accepted` quando o evento volta para `queued`.

### POST /events

Recebe um evento para processamento assíncrono.

```json
{
  "id": "evt-001",
  "type": "payment.created",
  "payload": {
    "amount": 100
  }
}
```

Possíveis respostas:

```text
202 Accepted            evento e mensagem de Outbox persistidos atomicamente
400 Bad Request         JSON inválido ou campos obrigatórios ausentes
409 Conflict            event.id duplicado ou já persistido
```

## Arquitetura

```text
main.go             bootstrap, servidor HTTP e encerramento gracioso
config.go           leitura e validação de configurações por ambiente
logger.go           configuração de logs estruturados com slog
app.go              estado da aplicação, dependências e registro das rotas
queue.go            contratos de publicação, consumo e entrega de eventos
models.go           contratos de entrada e saída usados pela API
metrics.go          contadores thread-safe e snapshot de métricas
event_store.go      persistência PostgreSQL, estados, idempotência e tabela Outbox
migrations/         histórico versionado do esquema PostgreSQL
migrations.go       execução controlada das migrações
rabbitmq_queue.go   implementação durável da EventQueue com RabbitMQ
deadletter.go       contrato e implementações da dead-letter persistente e de teste
handlers.go         handlers HTTP, validação, persistência e respostas JSON
outbox.go          dispatcher de mensagens pendentes para a EventQueue
worker.go           workers, retry e backoff do processamento assíncrono
*_test.go           testes automatizados
.env.example        exemplo de variáveis de ambiente
Dockerfile          build de imagem containerizada
.dockerignore       exclusões do contexto de build Docker
requests.http       chamadas HTTP para testar pela IDE
```

Veja também a documentação detalhada em [`docs/architecture.md`](docs/architecture.md).

## Configuração

```text
PORT=8080
QUEUE_SIZE=100
WORKER_COUNT=3
READ_HEADER_TIMEOUT_SECONDS=5
SHUTDOWN_TIMEOUT_SECONDS=10
LOG_FORMAT=json
MAX_RETRIES=3
RETRY_BACKOFF_SECONDS=1
DEAD_LETTER_CAPACITY=100
DATABASE_URL=postgres://webhook:webhook_dev@localhost:5432/webhook?sslmode=disable
RABBITMQ_URL=amqp://webhook:webhook_dev@localhost:5672/
RABBITMQ_QUEUE=webhook.events
RABBITMQ_RECONNECT_MS=1000
RABBITMQ_CONNECT_TIMEOUT_MS=5000
```

Descrição das variáveis:

```text
PORT                          porta HTTP usada pelo servidor
QUEUE_SIZE                    capacidade do buffer local entre RabbitMQ e workers
WORKER_COUNT                  quantidade de workers processando eventos em paralelo
READ_HEADER_TIMEOUT_SECONDS   timeout para leitura dos headers HTTP
SHUTDOWN_TIMEOUT_SECONDS      tempo máximo para encerramento gracioso do servidor HTTP
LOG_FORMAT                    formato dos logs: json ou text
MAX_RETRIES                   quantidade de novas tentativas após a primeira falha
RETRY_BACKOFF_SECONDS         base em segundos para o backoff entre tentativas
DEAD_LETTER_CAPACITY          quantidade máxima de eventos retornados na consulta da dead-letter queue
DATABASE_URL                  URL de conexão com o PostgreSQL usado para persistir eventos
DLQ_REPLAY_TOKEN              token Bearer exigido pelo endpoint interno de replay da DLQ
RABBITMQ_URL                  endereço AMQP do RabbitMQ
RABBITMQ_QUEUE                nome da fila durável no RabbitMQ
RABBITMQ_RECONNECT_MS         espera entre tentativas de reconexão do consumidor
RABBITMQ_CONNECT_TIMEOUT_MS   timeout para cada tentativa de conexão AMQP
OUTBOX_POLL_INTERVAL_MS        intervalo de consulta da Outbox em milissegundos
OUTBOX_BATCH_SIZE              quantidade máxima de mensagens lidas por ciclo da Outbox
OUTBOX_DISPATCH_LEASE_SECONDS  prazo, em segundos, da reserva da mensagem por um dispatcher
PROCESSING_LEASE_SECONDS       tempo da reserva de processamento antes de expirar
PROCESSING_REQUEUE_DELAY_MS    espera antes de reenfileirar uma entrega em processamento
```

## Persistência

A aplicação usa PostgreSQL para persistir o histórico operacional dos eventos recebidos.
O PostgreSQL oferece concorrência de leitura e escrita adequada para a aplicação, mantendo a consistência transacional da Outbox, dos eventos e da DLQ.

O esquema é controlado por migrações SQL versionadas em `migrations/`. A aplicação não altera o banco ao iniciar: execute o comando abaixo antes de iniciar uma nova versão do serviço.

````powershell
go run . migrate
```

O comando registra a versão aplicada na tabela `schema_migrations`. Novas alterações devem adicionar um novo par de arquivos `NNNNNN_descricao.up.sql` e `NNNNNN_descricao.down.sql`, sem modificar migrações já aplicadas.

No Docker Compose, o serviço `migrate` executa essa etapa automaticamente e a API só inicia quando ela termina com sucesso.

Cada evento aceito é salvo inicialmente como `queued`. Quando um worker adquire a reserva, o status passa para:

```text
processing
```

Depois, o worker atualiza o status para:

```text
processed
failed
```

Também são registrados `attempts`, `error`, `created_at` e `updated_at`.

Por segurança, esta versão não expõe um endpoint público para listar todos os eventos persistidos. 


## Idempotência

O endpoint `POST /events` usa o campo `id` como chave de idempotência.

Se o mesmo `event.id` for recebido novamente, a aplicação rejeita o evento com:

```text
409 Conflict
```

Isso evita processamento duplicado em cenários comuns de webhook, nos quais o sistema externo pode reenviar o mesmo evento por timeout, falha de rede ou política própria de retry.

A deduplicação é persistente: a chave primária `events.id` no PostgreSQL impede que o mesmo evento seja aceito novamente, inclusive após a reinicialização da aplicação.
### Idempotência no consumidor

Antes de executar um evento recebido do RabbitMQ, o worker tenta alterar atomicamente seu estado de `queued` para `processing`. Apenas o worker que modificar a linha ganha o direito de executar o processamento.

- `processed` ou `failed`: a redelivery é ignorada e recebe ACK.
- `processing` com lease válido: a entrega recebe NACK com requeue após uma pequena espera.
- `processing` com lease expirado: outro worker pode reservar e recuperar o processamento.
- evento inexistente no PostgreSQL: a entrega é rejeitada sem requeue.

Essa coordenação protege contra workers concorrentes e redeliveries comuns. A garantia continua sendo `at-least-once`: efeitos realizados em sistemas externos também devem usar `event.id` como chave idempotente.

Cada evento novo também gera uma linha na tabela `outbox`, dentro da mesma transação. O dispatcher consulta registros cujo `published_at` está vazio, publica-os e só então registra a data de publicação. Mensagens pendentes sobrevivem à reinicialização da aplicação.

Em múltiplas réplicas, o PostgreSQL reserva cada mensagem com `FOR UPDATE SKIP LOCKED`, um token aleatório e `OUTBOX_DISPATCH_LEASE_SECONDS`. Dessa forma, dispatchers concorrentes recebem lotes distintos; uma reserva abandonada pode ser recuperada após o prazo expirar.

A entrega é `at-least-once`: uma falha depois da publicação e antes da atualização de `published_at` pode causar republicação. Consumidores devem permanecer idempotentes.

## Concorrência

A aplicação depende do contrato `EventQueue`, implementado exclusivamente por `RabbitMQEventQueue`. O contrato combina interfaces menores: `EventPublisher`, usada pelo Outbox, e `EventConsumer`, usada pelos workers. Um channel interno funciona apenas como ponte entre as entregas AMQP e as goroutines; ele não é uma fila alternativa nem durável.

O handler HTTP não publica diretamente. O `OutboxDispatcher` usa `Publish` e mantém a mensagem pendente quando a publicação falha. `Events`, `Stats`, `StopConsuming` e `Close` completam o ciclo de consumo, observabilidade e encerramento.

A quantidade de workers e a capacidade do buffer local são configuradas por `WORKER_COUNT` e `QUEUE_SIZE`. Com RabbitMQ, as estatísticas HTTP representam esse buffer local; a quantidade total de mensagens no broker deve ser acompanhada pelo painel de gerenciamento.

O consumidor RabbitMQ usa ACK manual. Se o processo cair antes do ACK, a entrega permanece não confirmada e o broker pode reenviá-la. Mensagens inválidas são rejeitadas sem requeue; eventos processados ou enviados para o fluxo de falha permanente são confirmados pelo worker.

Publisher e consumer refazem suas conexões automaticamente. A aplicação pode iniciar com o broker indisponível; enquanto ele estiver fora, eventos novos permanecem pendentes no Outbox e são publicados depois da reconexão.

## Retry com backoff

Quando o processamento de um evento falha, o worker tenta processá-lo novamente antes de desistir definitivamente.

Com os valores padrão, um evento pode ter até 4 tentativas no total:

```text
1 tentativa inicial + 3 retries
```

O backoff cresce de forma linear por tentativa:

```text
1ª falha -> aguarda 1 segundo
2ª falha -> aguarda 2 segundos
3ª falha -> aguarda 3 segundos
```

Se todas as tentativas falharem, o evento é marcado como `failed`, registrado nos logs, contabilizado nas métricas e persistido na dead-letter queue do PostgreSQL.

## Encerramento gracioso

A aplicação escuta sinais de interrupção do sistema, como `Ctrl+C` no terminal ou `SIGTERM` em ambientes de orquestração.

Ao receber o sinal, o serviço:

- para de aceitar novas requisições HTTP
- aguarda o servidor HTTP encerrar dentro do timeout configurado
- interrompe o dispatcher do Outbox
- interrompe o consumo da fila de eventos
- espera os workers terminarem os eventos já retirados da fila
- registra `shutdown complete` ao final do processo

## Execução local

````powershell
go run .
```

A aplicação sobe por padrão em:

```text
http://localhost:8080
```

## Docker

O Compose inicia PostgreSQL, RabbitMQ, o serviço pontual `migrate` e, após a migração concluir, a aplicação. Copie `.env.example` para `.env` e altere as credenciais de desenvolvimento antes de compartilhar o ambiente.

````powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Serviços locais:

```text
API HTTP             http://localhost:8080
RabbitMQ AMQP        localhost:5672
RabbitMQ Management  http://localhost:15672
```

O RabbitMQ é a fila obrigatória da aplicação. O adaptador declara uma fila durável, publica mensagens persistentes com confirmação do broker e usa ACK manual após o processamento.

Build isolado da imagem:

````powershell
docker build -t go-webhook-processor:local .
```

Executar o container conectado ao PostgreSQL:

````powershell
docker run --rm `
  -p 8080:8080 `
  -e PORT=8080 `
  -e LOG_FORMAT=json `
  -v go-webhook-data:/data `
  --name go-webhook-processor `
  go-webhook-processor:local
```

O container conecta ao PostgreSQL do Compose pela variável `DATABASE_URL`.

A imagem possui um `HEALTHCHECK` baseado no endpoint `/ready`. Para consultar o estado do container:

```powershell
docker inspect --format='{{.State.Health.Status}}' go-webhook-processor
```

## Testes

Os testes de persistência usam um banco PostgreSQL exclusivo. Crie-o uma vez no ambiente local:

````powershell
docker compose exec postgres createdb -U webhook webhook_test
```

Depois configure a conexão e execute os testes:

````powershell
$env:TEST_DATABASE_URL = "postgres://webhook:webhook_dev@localhost:5432/webhook_test?sslmode=disable"
go test ./...
```

A pipeline cria esse banco automaticamente antes da execução dos testes.

## Build

````powershell
go build .
```

## Reprocessamento seguro da Dead Letter Queue

Eventos marcados como `failed` podem ser reprocessados manualmente pelo comando interno:

Para execução local, com PostgreSQL disponível em `localhost:5432`:

````powershell
go run . replay-dead-letter <event-id>
```

Quando a aplicação estiver rodando pelo Docker Compose, use o endpoint HTTP autenticado ou execute o comando com `DATABASE_URL` apontando para `localhost:5432`.

````powershell
$env:DATABASE_URL = "postgres://webhook:webhook_dev@localhost:5432/webhook?sslmode=disable"
go run . replay-dead-letter <event-id>
```

O fluxo recomendado é usar o endpoint HTTP autenticado. O comando CLI permanece disponível para operações internas:

````powershell
docker compose run --rm --no-deps webhook-processor replay-dead-letter <event-id>
```

O comando executa uma transação no PostgreSQL e somente aceita um evento que esteja em `failed`. Nessa transação, o evento volta para `queued`, suas tentativas são zeradas, a mensagem correspondente da Outbox é reaberta e a falha recebe `replayed_at`. O registro original da DLQ não é apagado, preservando o histórico operacional.

Depois que o serviço for iniciado novamente, o dispatcher publica a mensagem reaberta no RabbitMQ. O comando não inicia o servidor HTTP nem precisa abrir uma conexão com o broker.

`replayed_at` é apenas uma marca de auditoria. Se o evento falhar novamente, a marca é limpa e a falha mais recente passa a representar o estado atual do evento.
## Reset do ambiente Docker

### Reset completo: PostgreSQL e RabbitMQ

Os comandos abaixo são destrutivos. Eles removem os volumes do PostgreSQL e do RabbitMQ, os containers e recriam a aplicação do zero:

````powershell
docker compose down --volumes --remove-orphans
docker compose up -d --build
```

Para acompanhar a inicialização:

````powershell
docker compose logs -f webhook-processor
```

### Reset apenas do PostgreSQL

Use esta opção quando quiser recriar apenas o banco PostgreSQL e preservar o RabbitMQ:

````powershell
docker compose stop webhook-processor postgres
docker compose rm -f postgres
$volume = docker volume ls -q -f name=postgres-data
if ($volume) { docker volume rm $volume }
docker compose up -d --build
```

O comando remove somente o volume do PostgreSQL e preserva o volume do RabbitMQ.
## Testes pela IDE

O arquivo [`requests.http`](requests.http) contém chamadas prontas para uso com a extensão REST Client do VS Code.

## Observabilidade atual

A aplicação oferece:

- endpoint `/health`
- endpoint `/ready` usado pelo healthcheck do container
- endpoint `/metrics`
- endpoint `/dead-letters`
- logs estruturados com `slog`
- status persistido em PostgreSQL


