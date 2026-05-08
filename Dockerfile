# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
ARG VERSION=dev
COPY go.mod ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.Version=${VERSION}" \
    -o /out/mesh0-metrics-agent .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mesh0-metrics-agent /mesh0-metrics-agent
EXPOSE 8125/udp
USER nonroot:nonroot
ENTRYPOINT ["/mesh0-metrics-agent"]
