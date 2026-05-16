# syntax=docker/dockerfile:1

# ----- Build stage -----
FROM golang:1.26.3-alpine3.23 AS build
# RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" -trimpath -o /pomodoro

# ----- Runtime stage -----
FROM alpine:3.23
RUN apk add --no-cache ca-certificates
COPY --from=build /pomodoro /pomodoro
RUN mkdir -p /etc/pomodoro
ENTRYPOINT ["/pomodoro"]
CMD ["-config", "/etc/pomodoro/config.yml"]