# Transport service

Передаёт запросы агенту, отправляет сегменты через Kafka, собирает их и доставляет результаты application. Поддерживает имитацию потерь сегментов.

Требуется Go 1.23 или новее и доступный Kafka-брокер. Запустите Kafka, application и agent, затем из корня проекта:

```sh
go run ./cmd/transport
```

| Переменная | По умолчанию |
| --- | --- |
| `PORT` | `8082` |
| `KAFKA_BROKER` | `localhost:9092` |
| `KAFKA_TOPIC` | `segments` |
| `KAFKA_GROUP` | `prodname-transport` |
| `AGENT_URL` | `http://localhost:8083` |
| `APPLICATION_URL` | `http://localhost:8081` |
| `LOSS_RATE` | `0.04` |

`LOSS_RATE` — вероятность потери от 0 до 1. Сервис ожидает готовности Kafka перед запуском HTTP-сервера.

`cmd` собирает зависимости; `internal/domain` содержит модели; `internal/usecase` отвечает за сборку и проверку сегментов; `internal/adapter` содержит HTTP API, HTTP-клиент и Kafka; `internal/config` читает окружение. Проверка: `go test ./...`.
