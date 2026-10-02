# syntax=docker/dockerfile:1.8
# Disposable E2E only; same source release as agyn-platform 0.73.0's hook.
FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS build
ADD --checksum=sha256:0af3e2c801310ddebaff05ebb7962152b763a4b1960801c2bc4dedc1a5adcf47 https://codeload.github.com/minio/mc/tar.gz/ec185ff65d64f1e57a10a1870c5149f3a2e31d52 /tmp/mc.tgz
RUN mkdir /src && tar -xzf /tmp/mc.tgz --strip-components=1 -C /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -o /out/mc .
FROM alpine:3.21@sha256:ce64758a109eb420d874a118f87920e625e12d3634e03b4a5573fd9f6e5d3507
COPY --from=build /out/mc /usr/bin/mc
COPY --from=build /src/LICENSE /licenses/minio-mc-LICENSE
LABEL org.opencontainers.image.source="https://github.com/minio/mc" \
      org.opencontainers.image.revision="ec185ff65d64f1e57a10a1870c5149f3a2e31d52"
ENTRYPOINT ["/usr/bin/mc"]
