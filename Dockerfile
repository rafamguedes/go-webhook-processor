FROM golang:1.27.1-alpine AS build

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /app/go-webhook-processor .

FROM scratch

COPY --from=build /app/go-webhook-processor /go-webhook-processor

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/go-webhook-processor"]
