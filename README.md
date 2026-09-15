# Go Webhook Processor

Servico HTTP em Go para recebimento e processamento assincrono de eventos via webhook.

A aplicacao foi desenhada para um cenario comum de backend: receber eventos de sistemas externos, validar o payload, responder rapidamente ao cliente e processar o trabalho em segundo plano usando uma fila interna com workers concorrentes.

## Objetivo

Este servico resolve o problema de nao bloquear requisicoes HTTP enquanto uma tarefa potencialmente demorada e executada. O endpoint `POST /events` apenas valida e enfileira o evento. O processamento ocorre de forma assincrona por workers em goroutines.

Esse padrao e util para:

- webhooks de pagamento
- integracoes com ERPs e CRMs
- processamento de pedidos
- envio de notificacoes
- tarefas internas baseadas em eventos
- chamadas para APIs externas com maior latencia

## Fluxo

```text
Cliente externo
  -> POST /events
    -> validacao do JSON
      -> envio para fila interna
        -> resposta HTTP 202 Accepted
          -> workers processam eventos em background
```

## Endpoints

### GET /health

Retorna o status da aplicacao e informacoes basicas da fila interna.

Exemplo de resposta:

```json
{
  "status": "ok",
  "time": "2026-09-14T22:30:00-03:00",
  "date": "2026-09-14",
  "queueLength": 0,
  "queueCapacity": 100
}
```

### POST /events

Recebe um evento para processamento assincrono.

Payload esperado:

```json
{
  "id": "evt-001",
  "type": "payment.created",
  "payload": {
    "amount": 100
  }
}
```

Resposta de sucesso:

```json
{
  "accepted": true,
  "eventId": "evt-001"
}
```

Possiveis respostas:

```text
202 Accepted            evento validado e enfileirado
400 Bad Request         JSON invalido ou campos obrigatorios ausentes
503 Service Unavailable fila interna cheia
```

## Arquitetura

```text
main.go       bootstrap da aplicacao e configuracao do servidor HTTP
app.go        estado da aplicacao, fila interna e registro das rotas
models.go     contratos de entrada e saida usados pela API
handlers.go   handlers HTTP, validacao e respostas JSON
worker.go     workers responsaveis pelo processamento assincrono
main_test.go  testes automatizados dos handlers
```

## Concorrencia

A aplicacao usa uma fila interna baseada em `chan Event`:

```go
eventQueue chan Event
```

Os workers sao iniciados como goroutines:

```go
go worker(workerID, eventQueue)
```

Com a configuracao atual, ate 3 eventos podem ser processados em paralelo:

```go
const workerCount = 3
```

A fila possui capacidade para 100 eventos aguardando processamento:

```go
const queueSize = 100
```

## Requisitos

- Go 1.27+

## Execucao local

```powershell
go run .
```

Se o Go ainda nao estiver no PATH da sessao atual:

```powershell
& "C:\Program Files\Go\bin\go.exe" run .
```

A aplicacao sobe em:

```text
http://localhost:8080
```

## Testes

```powershell
go test ./...
```

Se o Go ainda nao estiver no PATH da sessao atual:

```powershell
& "C:\Program Files\Go\bin\go.exe" test ./...
```

## Build

```powershell
go build .
```

O comando gera um binario executavel do servico.

## Testes manuais

Health check:

```powershell
Invoke-RestMethod http://localhost:8080/health
```

Enviar evento:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/events `
  -ContentType "application/json" `
  -Body '{"id":"evt-001","type":"payment.created","payload":{"amount":100}}'
```

Enviar multiplos eventos ajuda a observar os workers processando em paralelo pelos logs da aplicacao.

## Observabilidade atual

A aplicacao registra logs no console para os principais eventos operacionais:

```text
event queued
worker processing event
worker finished event
```

O endpoint `/health` tambem expoe o tamanho atual da fila por meio dos campos `queueLength` e `queueCapacity`.

## Limitacoes atuais

Esta versao ainda usa fila em memoria. Isso significa que eventos pendentes sao perdidos se o processo for encerrado antes do processamento.

Antes de uso real em producao, os proximos passos recomendados sao:

- persistir eventos em banco ou fila externa
- adicionar shutdown gracioso
- adicionar logs estruturados
- adicionar metricas
- adicionar retry com backoff
- adicionar configuracao por variaveis de ambiente
- adicionar Dockerfile

