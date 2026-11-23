# ビルドステージ（1.23-alpine から変更）
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

# 実行ステージ
FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/server ./server
COPY --from=builder /app/html ./html

EXPOSE 8080

CMD ["./server"]