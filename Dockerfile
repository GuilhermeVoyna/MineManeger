# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copiar arquivos de dependências
COPY go.mod go.sum ./
RUN go mod download

# Copiar código fonte
COPY . .

# Compilar para Linux
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main ./cmd/main.go

# Construir imagem do fake server (opcional - será construída automaticamente se não existir)
# RUN docker build -f Dockerfile.fakeserver -t mine-manager-fakeserver:latest . || true

# Runtime stage
FROM alpine:latest

# Instalar Docker CLI para gerenciar containers
RUN apk --no-cache add ca-certificates tzdata docker-cli

WORKDIR /root/

# Copiar binário compilado
COPY --from=builder /app/main .

# Copiar Dockerfile.fakeserver para permitir construir a imagem do fake server dentro do container
COPY --from=builder /app/Dockerfile.fakeserver /root/Dockerfile.fakeserver
COPY --from=builder /app/go.mod /root/go.mod
COPY --from=builder /app/go.sum /root/go.sum
COPY --from=builder /app/internal /root/internal
COPY --from=builder /app/cmd /root/cmd

CMD ["./main"]

