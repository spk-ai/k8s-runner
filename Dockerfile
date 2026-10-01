# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
ARG BUF_VERSION=1.64.0
# Release sha256.txt entries for buf-Linux-x86_64 and buf-Linux-aarch64.
ARG BUF_SHA256_X86_64=ce39d3b9b8cad23f75b8f6645605055fbe9760f81571c1d10a7928602bce601c
ARG BUF_SHA256_AARCH64=d2cec7e4dff0a93ccac2382c97798339be18248b164ec00aefcea1205a2a9cea

# Images are pinned to the index digests of their tags (golang:1.25-alpine
# was Go 1.25.14 on 2026-10-02). Update each tag and digest together.

FROM --platform=$BUILDPLATFORM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS buf
ARG BUF_VERSION BUF_SHA256_X86_64 BUF_SHA256_AARCH64
RUN apk add --no-cache curl
RUN set -eu; \
    arch="$(uname -m)"; \
    case "${arch}" in \
      x86_64) sha256="${BUF_SHA256_X86_64}" ;; \
      aarch64) sha256="${BUF_SHA256_AARCH64}" ;; \
      *) echo "no pinned buf checksum for ${arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL \
      "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-Linux-${arch}" \
      -o /usr/local/bin/buf; \
    echo "${sha256}  /usr/local/bin/buf" | sha256sum -c -; \
    chmod +x /usr/local/bin/buf

FROM --platform=$BUILDPLATFORM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS build
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
RUN go build -o /out/k8s-runner ./cmd/k8s-runner && \
    go build -o /out/workload-proxy ./cmd/workload-proxy

FROM alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
WORKDIR /app
COPY --from=build /out/k8s-runner /app/k8s-runner
COPY --from=build /out/workload-proxy /app/workload-proxy
USER appuser
ENTRYPOINT ["/app/k8s-runner"]
