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

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 CMD wget -q -O - http://127.0.0.1:8080/ready || exit 1

ENTRYPOINT ["/app/go-webhook-processor"]
