# Arquitetura de Alto Nível

Este documento descreve o fluxo atual do Go Webhook Processor com Transactional Outbox.

## Fluxo principal

```mermaid
flowchart LR
    client[Cliente externo]
    handler[POST /events]
    transaction[Transação SQLite]
    events[(events)]
    outbox[(outbox)]
    accepted[202 Accepted]
    dispatcher[OutboxDispatcher]
    queue[EventQueue<br/>Memory ou RabbitMQ]
    workers[Workers concorrentes]
    processor[Processamento]

    client --> handler
    handler -->|valida e deduplica| transaction
    transaction --> events
    transaction --> outbox
    transaction --> accepted
    outbox -->|mensagens sem published_at| dispatcher
    dispatcher -->|Publish + confirmação| queue
    dispatcher -->|marca published_at| outbox
    queue --> workers
    workers --> processor
    processor -->|processed ou failed| events
```

## Garantia transacional

O registro em `events` e a mensagem em `outbox` são inseridos na mesma transação SQLite. Ou ambos são confirmados, ou nenhum deles é persistido. Assim, uma falha entre o banco e o RabbitMQ não perde a intenção de publicação.

O dispatcher executa continuamente:

1. Busca um lote de mensagens com `published_at IS NULL`.
2. Publica cada evento pela interface `EventPublisher`.
3. Aguarda a confirmação do provider.
4. Marca a mensagem como publicada.
5. Em caso de falha, incrementa `attempts`, registra `last_error` e tenta novamente em outro ciclo.

## Semântica de entrega

A garantia é `at-least-once`. Se o processo cair depois de o broker confirmar a publicação e antes de o SQLite gravar `published_at`, a mensagem continuará pendente e poderá ser publicada novamente. Por isso, consumidores precisam tratar `event.id` de forma idempotente.

A implementação atual possui um dispatcher por instância. Para executar várias réplicas simultâneas, uma evolução deverá adicionar reivindicação de mensagens ou locking apropriado ao banco usado em produção.

## Inicialização

```mermaid
flowchart TD
    start[Aplicação inicia]
    migrate[Abre e migra o SQLite]
    queue[Conecta à EventQueue]
    workers[Inicia workers]
    dispatcher[Inicia OutboxDispatcher]
    pending[Busca mensagens pendentes]
    server[Disponibiliza HTTP]

    start --> migrate --> queue --> workers --> dispatcher
    dispatcher --> pending
    dispatcher --> server
```

A migração cria a tabela `outbox` e também gera mensagens para eventos antigos que ainda estejam com status `queued`.

## Encerramento gracioso

```mermaid
sequenceDiagram
    participant OS as Sistema operacional
    participant HTTP as Servidor HTTP
    participant Outbox as OutboxDispatcher
    participant Queue as EventQueue
    participant Workers as Workers

    OS->>HTTP: SIGINT ou SIGTERM
    HTTP-->>OS: Para de aceitar requisições
    OS->>Outbox: Cancel
    Outbox-->>OS: Dispatcher finalizado
    OS->>Queue: StopConsuming
    Queue-->>Workers: Não há novas entregas
    Workers-->>OS: Processamento em andamento concluído
    OS->>Queue: Close
```

## Componentes

```text
Cliente -> HTTP -> SQLite (events + outbox) -> dispatcher -> EventQueue -> workers -> processamento
```

- `handlers.go`: valida e persiste a transação.
- `event_store.go`: mantém eventos e mensagens Outbox.
- `outbox.go`: publica mensagens pendentes.
- `queue.go`: define contratos independentes do provider.
- `rabbitmq_queue.go`: implementa publicação confirmada e consumo com ACK manual.

## Reconexão RabbitMQ

O publisher cria uma conexão sob demanda e a invalida quando uma publicação ou confirmação falha. A tentativa seguinte cria uma nova sessão. O consumer mantém um loop próprio: quando a conexão fecha inesperadamente, aguarda `RABBITMQ_RECONNECT_MS` e conecta novamente. A inicialização da aplicação não exige que o broker já esteja disponível; o Outbox mantém as mensagens pendentes durante a indisponibilidade.
- `worker.go`: processa eventos, aplica retry e atualiza o status.
