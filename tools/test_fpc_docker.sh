#!/bin/sh
# Builds and runs the suites on Linux FPC 3.2.2 inside a Docker container,
# with heaptrc. Acceptance criterion: 0 errors, 0 failures, 0 unfreed blocks.
#
# The repository is mounted read-only and copied inside the container, so
# nothing is written to the working tree. No Lazarus needed: the FPCUnit
# runners only pull in the LCL GUI on Windows.
#
# The integration suite runs in a second container that shares the network
# namespace of the docker/ broker (--network container:pascal-kafka-faa-broker):
# there, localhost:9092/9093 are the broker's own listeners, which is what it
# advertises and what its certificate (CN=localhost) names. KAFKA_IT works as
# in tools/test_fpc.sh (auto, require, skip).
#
# FPC_IMAGE: any image with FPC 3.2.2 and libssl on the PATH (default:
# fpc322-bookworm, Debian 12 with OpenSSL 3; tools/ci-test.sh builds one).
# heaptrc on Linux only writes its summary when given a log file
# (HEAPTRC=log=...), hence the log below.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${FPC_IMAGE:-fpc322-bookworm}"
MOUNT="$ROOT"
command -v cygpath >/dev/null 2>&1 && MOUNT="$(cygpath -w "$ROOT")"
cd "$ROOT"
sh tools/check_generated.sh
[ -f external/pascal-common-faa/src/PascalCommon.Version.pas ] \
  || { echo "external/pascal-common-faa is empty: git submodule update --init"; exit 1; }

# $1 = suite folder (Unit or Integration), $2 = runner name
SUITE_SCRIPT='
  set -e
  mkdir -p /t/u /t/external/pascal-common-faa/tests
  cp -r /src/src /src/tests /t/
  cp -r /src/external/pascal-common-faa/src /t/external/pascal-common-faa/
  cp -r /src/external/pascal-common-faa/tests/pki /t/external/pascal-common-faa/tests/
  C=/t/external/pascal-common-faa/src
  cd /t/tests/$SUITE/fpc
  fpc -v0 -Mdelphi -Fu/t/src -Fi/t/src -Fu$C -Fi$C -Fu.. -Fu/t/tests/Unit -FU/t/u -gh -gl -o/t/runner $RUNNER.lpr > /t/build.log 2>&1 \
    || { grep -iE "error|fatal" /t/build.log | head -30; exit 1; }
  cd /t
  HEAPTRC="log=/t/heap.txt" ./runner --all --format=plain > /t/run.log 2>&1 || true
  echo "== $RUNNER (Linux)"
  grep -E "^Number of|notice" /t/run.log
  grep "unfreed" /t/heap.txt
  grep -A4 "Message:" /t/run.log | head -40 || true
  grep -qE "^Number of errors: +0$" /t/run.log && grep -qE "^Number of failures: +0$" /t/run.log \
    && grep -qE "^0 unfreed memory blocks" /t/heap.txt'

MSYS_NO_PATHCONV=1 docker run --rm -v "$MOUNT:/src:ro" -e SUITE=Unit -e RUNNER=KafkaUnitTestsFpc \
  "$IMAGE" sh -c "$SUITE_SCRIPT"

case "${KAFKA_IT:-auto}" in
  skip) echo "== integration: skipped (KAFKA_IT=skip)"; exit 0 ;;
esac
if docker inspect -f '{{.State.Running}}' pascal-kafka-faa-broker 2>/dev/null | grep -q true; then
  MSYS_NO_PATHCONV=1 docker run --rm -v "$MOUNT:/src:ro" -e SUITE=Integration \
    -e RUNNER=KafkaIntegrationTestsFpc --network container:pascal-kafka-faa-broker \
    "$IMAGE" sh -c "$SUITE_SCRIPT"
elif [ "${KAFKA_IT:-auto}" = require ]; then
  echo "== integration: the broker is not running (docker compose -f docker/docker-compose.yml up -d)"
  exit 1
else
  echo "== integration: SKIPPED, the broker is not running (docker compose -f docker/docker-compose.yml up -d)"
fi
