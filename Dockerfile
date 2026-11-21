FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY common ./common
COPY transport ./transport
RUN CGO_ENABLED=0 go build -o /out/service ./transport
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/service /app/service
CMD ["/app/service"]
