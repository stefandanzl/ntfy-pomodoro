# syntax=docker/dockerfile:1

# ----- Build stage -----
FROM golang:1.22-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" -trimpath -o /pomodoro

# ----- Runtime stage -----
FROM scratch
# TLS roots for the HTTPS calls to ntfy
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /pomodoro /pomodoro
ENTRYPOINT ["/pomodoro"]
CMD ["-config", "/etc/pomodoro/config.yml"]