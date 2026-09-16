# Arquitetura de Alto Nível

Este documento descreve o fluxo atual do Go Webhook Processor.

## Fluxo principal

```mermaid
flowchart LR
    client[Cliente externo / sistema parceiro]
    api[Servidor HTTP Go]
    handler[POST /events]
    validation[Validação do JSON]
    queue[Fila interna<br/>chan Event]
    workers[Workers concorrentes<br/>goroutines]
    processor[Processamento do evento]
    health[GET /health]
    metrics[GET /metrics]
    deadletters[GET /dead-letters]

    client -->|POST /events| api
    api --> handler
    handler --> validation
    validation -->|evento inválido| badRequest[400 Bad Request]
    validation -->|evento válido| store[(SQLite)]
    store -->|event.id existente| conflict[409 Conflict]
    store -->|novo evento: status queued| queue
    queue -->|evento enfileirado| accepted[202 Accepted]
    queue --> workers
    workers --> processor

    client -->|GET /health| api
    api --> health
    health --> healthResponse[status, queueLength, queueCapacity]

    client -->|GET /metrics| api
    api --> metrics
    metrics --> metricsResponse[eventsQueued, eventsProcessed, retries, failures]

    client -->|GET /dead-letters| api
    api --> deadletters
    deadletters --> deadLetterResponse[evento, erro, tentativas, data da falha]
```

## Fluxo de inicialização e recuperação

```mermaid
flowchart TD
    start[Aplicação inicia]
    database[Abre o SQLite]
    workers[Inicia os workers]
    pending[Consulta eventos com status queued]
    restore[Recoloca eventos na fila interna]
    server[Disponibiliza o servidor HTTP]

    start --> database
    database --> workers
    workers --> pending
    pending --> restore
    restore --> server
```
## Fluxo de configuração

```mermaid
flowchart TD
    env[Variáveis de ambiente]
    defaults[Valores padrão]
    config[LoadConfig]
    app[NewApp]
    server[Servidor HTTP]
    queue[Fila interna]
    workers[Workers]

    env --> config
    defaults --> config
    config --> app
    config --> server
    config --> workers
    app --> queue
```

## Fluxo de encerramento gracioso

```mermaid
sequenceDiagram
    participant OS as Sistema operacional
    participant Main as main.go
    participant HTTP as Servidor HTTP
    participant Queue as Fila interna
    participant Workers as Workers

    OS->>Main: SIGINT / SIGTERM
    Main->>HTTP: Shutdown com timeout
    HTTP-->>Main: Para de aceitar novas requisições
    Main->>Queue: close(eventQueue)
    Queue-->>Workers: Não há novos eventos
    Workers-->>Workers: Finalizam eventos em andamento
    Workers-->>Main: WaitGroup concluído
    Main-->>OS: Processo encerrado com segurança
```

## Explicação rápida

1. Um sistema externo envia `POST /events`.
2. O handler valida o JSON e os campos obrigatórios.
3. Se o evento for válido, a aplicação tenta persistir o `event.id` no SQLite.
4. A chave primária do banco rejeita IDs já existentes; eventos duplicados recebem `409 Conflict` e não entram na fila.
5. Eventos novos são persistidos no SQLite com status `queued`.
6. Depois de persistidos, entram na fila interna `chan Event`.
7. A API responde `202 Accepted` rapidamente.
8. Os workers, rodando em goroutines, consomem a fila e processam os eventos em paralelo.
9. Se o processamento falhar, o worker aplica retry com backoff antes de registrar falha permanente.
10. Eventos com falha permanente entram na dead-letter queue em memória para investigação.
11. A aplicação atualiza métricas em memória para eventos enfileirados, rejeitados, processados, retentados e com falha permanente.
12. A aplicação registra logs estruturados com campos como `event_id`, `event_type` e `worker_id`.
13. O endpoint `GET /health` mostra o estado básico da aplicação e da fila.
14. Na inicialização, eventos que permaneceram como `queued` são recuperados do SQLite antes da abertura do servidor HTTP.
15. Quando a aplicação recebe `Ctrl+C` ou `SIGTERM`, ela executa shutdown gracioso.

## Componentes atuais

```text
Cliente externo -> HTTP server -> handler -> validação -> SQLite/idempotência -> fila interna -> workers -> retry/backoff -> processamento
```

A fila ainda é em memória. Em uma evolução futura, ela pode ser substituída ou complementada por uma fila externa, como RabbitMQ, Kafka, SQS ou Redis Streams.
