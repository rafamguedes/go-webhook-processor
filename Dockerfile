FROM golang:1.27.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /app/go-webhook-processor .

FROM alpine:3.22

RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
RUN chown -R app:app /app

COPY --from=build /app/go-webhook-processor /app/go-webhook-processor
COPY --from=build /src/migrations /app/migrations

USER app
EXPOSE 8080

ENTRYPOINT ["/app/go-webhook-processor"]
