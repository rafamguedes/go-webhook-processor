FROM golang:1.27.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /app/go-webhook-processor .

FROM alpine:3.22

RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
RUN mkdir -p /data && chown -R app:app /data /app

COPY --from=build /app/go-webhook-processor /app/go-webhook-processor

USER app
EXPOSE 8080
ENV DATABASE_PATH=/data/events.db

ENTRYPOINT ["/app/go-webhook-processor"]
