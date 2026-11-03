#!/bin/sh
# Everything CI runs (.github/workflows/ci.yml), also runnable locally with
# Docker: the generated-file checks, the unit suite on Linux FPC 3.2.2, and
# the integration suite against the docker/ broker (Kafka 4.4.0, KRaft, one
# node), which this script starts and waits for, the interoperability test
# with the Java client and the samples (tools/test_interop_docker.sh).
# Delphi Community Edition can't build headless, so the Delphi side is
# validated in the IDE (see CLAUDE.md, "Tests").
#
# FPC image: built here from Debian bookworm's fpc package (3.2.2, OpenSSL 3)
# and tagged pascalkafka-fpc322, unless FPC_IMAGE names an existing one.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ -z "${FPC_IMAGE:-}" ]; then
  FPC_IMAGE=pascalkafka-fpc322
  docker build -q -t "$FPC_IMAGE" - <<'DOCKERFILE' >/dev/null
FROM debian:bookworm
RUN apt-get update && apt-get install -y --no-install-recommends fpc libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
DOCKERFILE
fi

echo "== broker (docker/docker-compose.yml)"
docker compose -f docker/docker-compose.yml up -d --wait

FPC_IMAGE="$FPC_IMAGE" KAFKA_IT=require sh tools/test_fpc_docker.sh
FPC_IMAGE="$FPC_IMAGE" sh tools/test_interop_docker.sh
