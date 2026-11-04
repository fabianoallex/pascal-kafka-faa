#!/bin/sh
# Runs what the Delphi IDE built, from the shell: Delphi Community Edition
# doesn't compile from the command line (dcc32 prints "This version of the
# product does not support command line compiling." and exits with code 0),
# but its executables run anywhere. Build Kafka.groupproj in the IDE first
# (Win32 and Win64, Debug), then:
#
#   sh tools/test_delphi.sh            both platforms
#   PLATFORMS=Win64 sh tools/test_delphi.sh
#   KAFKA_IT=skip|require              as in tools/test_fpc.sh
#
# Per platform: the unit suite; with the docker/ broker running, the
# integration suite, the interoperability test with the Java client and both
# samples with their defaults. With kafka-cloud.env at the root (git-ignored,
# CLAUDE.md "Tests"), its variables are loaded first, so the integration
# suite runs Kafka.CloudIntegrationTests against Confluent Cloud, and the K1
# sample runs there too (--system-ca); the bootstrap host is masked in what
# is printed.
#
# Acceptance criterion: every test passed and 0 leaks. A leak makes FastMM
# show a message box that blocks the program (ReportMemoryLeaksOnShutdown):
# each program runs under a timeout, so a leak shows as a timeout (rc 124).
# Logs go next to each executable (Win32/Debug, Win64/Debug: git-ignored).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-python}"
CLOUD=0
if [ -f kafka-cloud.env ]; then
  set -a; . ./kafka-cloud.env; set +a
  [ -n "$KAFKA_CLOUD_BOOTSTRAP" ] && CLOUD=1
fi
BROKER=0
if docker inspect -f '{{.State.Running}}' pascal-kafka-faa-broker 2>/dev/null | grep -q true; then
  BROKER=1
fi
case "${KAFKA_IT:-auto}" in
  skip) BROKER=0; CLOUD=0 ;;
  require) [ "$BROKER" -eq 1 ] || {
    echo "== integration: the broker is not running (docker compose -f docker/docker-compose.yml up -d)"
    exit 1; } ;;
esac
FAILED=0

mask() { # the bootstrap host and the API key never reach the output
  if [ -n "$KAFKA_CLOUD_API_KEY" ]; then
    sed -E -e 's/pkc-[a-z0-9.-]+/<bootstrap>/g' -e "s/$KAFKA_CLOUD_API_KEY/<key>/g"
  else
    sed -E 's/pkc-[a-z0-9.-]+/<bootstrap>/g'
  fi
}

need() { # $1 = executable
  [ -f "$1" ] || { echo "$1 not found: build Kafka.groupproj in the Delphi IDE (Win32 and Win64, Debug)"; exit 1; }
}

run_suite() { # $1 = folder, $2 = executable, $3 = label
  need "$1/$2"
  RC=0
  ( cd "$1" && timeout 1200 "./$2" > run.log 2>&1 ) || RC=$?
  echo "== $3"
  grep -E "Tests Found|Tests Passed|Tests Failed|Tests Errored|notice|\[cloud\]" "$1/run.log" | mask
  if [ "$RC" -ne 0 ] || ! grep -qE "Tests Failed +: 0" "$1/run.log" \
      || ! grep -qE "Tests Errored +: 0" "$1/run.log"; then
    echo "   FAILED (rc=$RC; 124 = timed out, a leak's message box?)"
    grep -A3 -E "^ *Failed|Exception" "$1/run.log" | mask | head -30 || true
    FAILED=1
  fi
}

run_program() { # $1 = label, then the command; the program must print "done"
  LABEL="$1"; shift
  need "$1"
  LOG="$(dirname "$1")/run.log"
  RC=0
  timeout 600 "$@" > "$LOG" 2>&1 || RC=$?
  echo "== $LABEL"
  if [ "$RC" -ne 0 ] || ! grep -q "^done" "$LOG"; then
    echo "   FAILED (rc=$RC; 124 = timed out, a leak's message box?)"
    grep -vE "^(sent|received) " "$LOG" | mask | head -20
    FAILED=1
  else
    echo "   $(grep -c '^received' "$LOG") records received, done"
  fi
}

for P in ${PLATFORMS:-Win32 Win64}; do
  run_suite tests/Unit/$P/Debug Kafka.UnitTests.exe "Kafka.UnitTests $P"
  if [ "$BROKER" -eq 1 ]; then
    run_suite tests/Integration/$P/Debug Kafka.IntegrationSuite.exe "Kafka.IntegrationSuite $P"
    need tests/Interop/$P/Debug/KafkaInterop.exe
    echo "== interop with the Java client $P"
    RC=0
    MSYS_NO_PATHCONV=1 timeout 1200 "$PYTHON" -I tools/interop.py \
      --pascal tests/Interop/$P/Debug/KafkaInterop.exe > tests/Interop/$P/Debug/run.log 2>&1 || RC=$?
    grep -E "^interop:|FAIL" tests/Interop/$P/Debug/run.log || true
    [ "$RC" -eq 0 ] || { echo "   FAILED (rc=$RC)"; FAILED=1; }
    run_program "sample KafkaSample $P (SASL_SSL + SCRAM)" \
      samples/ProduceConsume/$P/Debug/KafkaSample.exe
    run_program "sample KafkaGroupSample $P (SASL_SSL + SCRAM)" \
      samples/ConsumerGroup/$P/Debug/KafkaGroupSample.exe
  else
    echo "== integration $P: SKIPPED, the broker is not running"
  fi
  if [ "$CLOUD" -eq 1 ]; then
    run_program "sample KafkaSample $P (Confluent Cloud)" \
      samples/ProduceConsume/$P/Debug/KafkaSample.exe --bootstrap "$KAFKA_CLOUD_BOOTSTRAP" \
      --sasl plain --user "$KAFKA_CLOUD_API_KEY" --password-env KAFKA_CLOUD_API_SECRET \
      --system-ca --topic "${KAFKA_CLOUD_TOPIC:-pascal-kafka-faa-it}"
  fi
done
[ "$FAILED" -eq 0 ] && echo "== Delphi: all green" || { echo "== Delphi: FAILED"; exit 1; }
