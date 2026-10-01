# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25
ARG BUF_VERSION=1.64.0

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS buf
ARG BUF_VERSION
RUN apk add --no-cache curl
RUN curl -sSL \
      "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
RUN apk add --no-cache git

WORKDIR /src
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
COPY go.mod go.sum ./
RUN go mod download
COPY buf.gen.yaml ./
RUN buf generate --include-imports --template buf.gen.yaml -o /src
COPY . .
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN go build -o /out/k8s-runner ./cmd/k8s-runner

FROM alpine:3.19
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
WORKDIR /app
COPY --from=build /out/k8s-runner /app/k8s-runner
USER appuser
ENTRYPOINT ["/app/k8s-runner"]
