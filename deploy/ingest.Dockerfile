# syntax=docker/dockerfile:1
FROM golang:1.23-alpine AS build
WORKDIR /src

# The workspace spans several modules, so the whole tree is needed to resolve
# the `use` directives. Copy go.work first for layer caching.
COPY go.work go.work.sum* ./
COPY gen/ gen/
COPY pkg/ pkg/
COPY services/ services/
COPY tools/ tools/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ingest ./services/ingest/cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingest /ingest
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/ingest"]
