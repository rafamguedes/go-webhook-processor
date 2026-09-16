# Go Webhook Processor

Serviço HTTP em Go para recebimento e processamento assíncrono de eventos via webhook.

A aplicação foi desenhada para um cenário comum de backend: receber eventos de sistemas externos, validar o payload, persistir o evento, responder rapidamente ao cliente e processar o trabalho em segundo plano usando uma fila interna com workers concorrentes.

## Objetivo

Este serviço evita bloquear requisições HTTP enquanto tarefas potencialmente demoradas são executadas. O endpoint `POST /events` valida, deduplica, persiste e enfileira o evento. O processamento ocorre de forma assíncrona por workers em goroutines.

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
      -> persistência em SQLite como queued e deduplicação por event.id
          -> envio para a fila interna
            -> resposta HTTP 202 Accepted
              -> workers processam eventos em background com retry/backoff
                -> atualização do status para processed ou failed
```

## Endpoints

### GET /health

Retorna o status da aplicação e informações básicas da fila interna.

### GET /metrics

Retorna um snapshot dos principais contadores operacionais da aplicação.

```json
{
  "eventsQueued": 10,
  "eventsRejected": 2,
  "eventsDuplicated": 1,
  "eventsProcessed": 8,
  "eventsFailedPermanent": 1,
  "eventRetries": 3,
  "queueLength": 1,
  "queueCapacity": 100
}
```

### GET /dead-letters

Retorna os eventos que falharam permanentemente após esgotar as tentativas de retry. Este endpoint mostra apenas falhas permanentes mantidas em memória.

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
202 Accepted            evento validado, persistido e enfileirado
400 Bad Request         JSON inválido ou campos obrigatórios ausentes
409 Conflict            event.id duplicado ou já persistido
503 Service Unavailable fila interna cheia
```

## Arquitetura

```text
main.go             bootstrap, servidor HTTP e encerramento gracioso
config.go           leitura e validação de configurações por ambiente
logger.go           configuração de logs estruturados com slog
app.go              estado da aplicação, dependências e registro das rotas
queue.go            contrato EventQueue e implementação em memória
models.go           contratos de entrada e saída usados pela API
metrics.go          contadores thread-safe e snapshot de métricas
event_store.go      persistência SQLite, estados e idempotência por event.id
deadletter.go       armazenamento em memória dos eventos com falha permanente
handlers.go         handlers HTTP, validação, métricas e respostas JSON
worker.go           recuperação, workers, retry e backoff do processamento assíncrono
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
DATABASE_PATH=./events.db
```

Descrição das variáveis:

```text
PORT                          porta HTTP usada pelo servidor
QUEUE_SIZE                    quantidade máxima de eventos aguardando na fila interna
WORKER_COUNT                  quantidade de workers processando eventos em paralelo
READ_HEADER_TIMEOUT_SECONDS   timeout para leitura dos headers HTTP
SHUTDOWN_TIMEOUT_SECONDS      tempo máximo para encerramento gracioso do servidor HTTP
LOG_FORMAT                    formato dos logs: json ou text
MAX_RETRIES                   quantidade de novas tentativas após a primeira falha
RETRY_BACKOFF_SECONDS         base em segundos para o backoff entre tentativas
DEAD_LETTER_CAPACITY          quantidade máxima de eventos mantidos na dead-letter queue
DATABASE_PATH                 caminho do arquivo SQLite usado para persistir eventos
```

## Persistência

A aplicação usa SQLite para persistir o histórico operacional dos eventos recebidos.

Cada evento aceito é salvo inicialmente com status:

```text
queued
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

A deduplicação é persistente: a chave primária `events.id` no SQLite impede que o mesmo evento seja aceito novamente, inclusive após a reinicialização da aplicação.

Na inicialização, a aplicação consulta os eventos com status `queued` e os recoloca na fila interna antes de disponibilizar o servidor HTTP. Eventos já marcados como `processed` ou `failed` não são recuperados.

## Concorrência

A aplicação depende do contrato `EventQueue`, não diretamente de um channel. Esse contrato combina interfaces menores: `EventPublisher`, usada para publicar, e `EventConsumer`, usada pelos workers para consumir. A implementação atual, `MemoryEventQueue`, encapsula um `chan Event`.

`TryPublish` é usado pelo endpoint HTTP e retorna imediatamente quando não há espaço, permitindo responder `503 Service Unavailable`. `Publish` aguarda espaço ou cancelamento do contexto e é usado na recuperação para não descartar eventos persistidos. `Events`, `Stats` e `Close` completam o ciclo de consumo, observabilidade e encerramento.

A quantidade de workers e a capacidade da fila em memória são configuradas por `WORKER_COUNT` e `QUEUE_SIZE`.

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

Se todas as tentativas falharem, o evento é marcado como `failed`, registrado nos logs, contabilizado nas métricas e adicionado à dead-letter queue em memória.

## Encerramento gracioso

A aplicação escuta sinais de interrupção do sistema, como `Ctrl+C` no terminal ou `SIGTERM` em ambientes de orquestração.

Ao receber o sinal, o serviço:

- para de aceitar novas requisições HTTP
- aguarda o servidor HTTP encerrar dentro do timeout configurado
- fecha a fila interna de eventos
- espera os workers terminarem os eventos já retirados da fila
- registra `shutdown complete` ao final do processo

## Execução local

```powershell
go run .
```

A aplicação sobe por padrão em:

```text
http://localhost:8080
```

## Docker

Build da imagem:

```powershell
docker build -t go-webhook-processor:local .
```

Executar o container com volume para persistir o SQLite:

```powershell
docker run --rm `
  -p 8080:8080 `
  -e PORT=8080 `
  -e LOG_FORMAT=json `
  -v go-webhook-data:/data `
  --name go-webhook-processor `
  go-webhook-processor:local
```

No container, o banco usa por padrão:

```text
/data/events.db
```

## Testes

```powershell
go test ./...
```

## Build

```powershell
go build .
```

## Testes pela IDE

O arquivo [`requests.http`](requests.http) contém chamadas prontas para uso com a extensão REST Client do VS Code.

## Observabilidade atual

A aplicação oferece:

- endpoint `/health`
- endpoint `/metrics`
- endpoint `/dead-letters`
- logs estruturados com `slog`
- status persistido em SQLite


