#!/bin/sh
# Builds and runs the suites on FPC (Windows, lazbuild), after checking that
# every generated file is up to date. Acceptance criterion: 0 errors,
# 0 failures and "0 unfreed memory blocks" (heaptrc) in each suite.
#
#   sh tools/test_fpc.sh          unit suite, then the integration suite if
#                                 the docker/ broker is running
#   KAFKA_IT=require sh ...       fail instead of skipping the integration suite
#   KAFKA_IT=skip sh ...          unit suite only
#
# The Delphi side has no command-line equivalent: Delphi Community Edition
# doesn't compile outside the IDE (dcc32 prints "This version of the product
# does not support command line compiling." and exits with code 0). Run the
# two .dproj of Kafka.groupproj from the IDE.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LAZBUILD="${LAZBUILD:-lazbuild}"
command -v "$LAZBUILD" >/dev/null 2>&1 || LAZBUILD=/c/lazarus4.0/lazbuild.exe
cd "$ROOT"
sh tools/check_generated.sh

run_suite() { # $1 = folder, $2 = project name
  (
    cd "$1"
    if ! "$LAZBUILD" -B "$2.lpi" > build.log 2>&1; then
      grep -E "Error|Fatal" build.log | grep -v "generics\." | head -30
      exit 1
    fi
    "./$2.exe" --all --format=plain > run.log 2>&1 || true
    echo "== $2"
    grep -E "^Number of|unfreed|notice" run.log
    grep -A4 "Message:" run.log | head -40 || true
    grep -qE "^Number of errors: +0$" run.log && grep -qE "^Number of failures: +0$" run.log \
      && grep -qE "^0 unfreed memory blocks" run.log
  )
}

run_suite tests/Unit/fpc KafkaUnitTestsFpc

case "${KAFKA_IT:-auto}" in
  skip) echo "== integration: skipped (KAFKA_IT=skip)"; exit 0 ;;
esac
if docker inspect -f '{{.State.Running}}' pascal-kafka-faa-broker 2>/dev/null | grep -q true; then
  run_suite tests/Integration/fpc KafkaIntegrationTestsFpc
  # The interoperability test with the Java client (tools/interop.py).
  if ! "$LAZBUILD" -B tests/Interop/fpc/KafkaInteropFpc.lpi > tests/Interop/fpc/build.log 2>&1; then
    grep -E "Error|Fatal" tests/Interop/fpc/build.log | head -30
    exit 1
  fi
  echo "== interop with the Java client"
  RC=0
  MSYS_NO_PATHCONV=1 "${PYTHON:-python}" -I tools/interop.py \
    --pascal tests/Interop/fpc/KafkaInteropFpc.exe > tests/Interop/fpc/run.log 2>&1 || RC=$?
  grep -vE "memory blocks|heap|Heap dump" tests/Interop/fpc/run.log
  [ "$RC" -eq 0 ]
  # The K1 sample, against the SASL_SSL listener (its defaults).
  if ! "$LAZBUILD" -B samples/ProduceConsume/KafkaSample.lpi > samples/ProduceConsume/build.log 2>&1; then
    grep -E "Error|Fatal" samples/ProduceConsume/build.log | head -30
    exit 1
  fi
  echo "== sample (samples/ProduceConsume, SASL_SSL + SCRAM)"
  samples/ProduceConsume/KafkaSample.exe > samples/ProduceConsume/run.log 2>&1 || true
  grep -E "^(localhost|done)|Error|failed|unfreed" samples/ProduceConsume/run.log
  grep -q "^done" samples/ProduceConsume/run.log && grep -qE "^0 unfreed memory blocks" samples/ProduceConsume/run.log
elif [ "${KAFKA_IT:-auto}" = require ]; then
  echo "== integration: the broker is not running (docker compose -f docker/docker-compose.yml up -d)"
  exit 1
else
  echo "== integration: SKIPPED, the broker is not running (docker compose -f docker/docker-compose.yml up -d)"
fi
