#!/bin/sh
# The interoperability test with the Java client (tools/interop.py), then the
# K1 sample (samples/ProduceConsume), with the library's side built by FPC
# 3.2.2 on Linux, in a container that shares the broker's network namespace
# (as tools/test_fpc_docker.sh's integration suite). The container stays up while tools/interop.py runs the program in
# it through docker exec, and goes at the end.
#
# heaptrc on Linux writes its summary only to a log file: each run of the
# program goes through /t/run.sh, which prints the "unfreed" line of its own
# log after the program's output, where tools/interop.py looks for it.
#
# FPC_IMAGE as in tools/test_fpc_docker.sh; PYTHON (default python).
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${FPC_IMAGE:-fpc322-bookworm}"
NAME=pascal-kafka-faa-interop
MOUNT="$ROOT"
command -v cygpath >/dev/null 2>&1 && MOUNT="$(cygpath -w "$ROOT")"
cd "$ROOT"

docker rm -f "$NAME" >/dev/null 2>&1 || true
MSYS_NO_PATHCONV=1 docker run -d --name "$NAME" -v "$MOUNT:/src:ro" \
  --network container:pascal-kafka-faa-broker "$IMAGE" sleep infinity >/dev/null
trap 'docker rm -f "$NAME" >/dev/null 2>&1' EXIT

BUILD='
  set -e
  mkdir -p /t/u /t/external/pascal-common-faa
  cp -r /src/src /src/tests /t/
  cp -r /src/external/pascal-common-faa/src /t/external/pascal-common-faa/
  cp /src/tests/data/interop-records.tsv /t/
  C=/t/external/pascal-common-faa/src
  cd /t/tests/Interop/fpc
  fpc -v0 -Mdelphi -Fu/t/src -Fi/t/src -Fu$C -Fi$C -Fu.. -FU/t/u -gh -gl -o/t/interop KafkaInteropFpc.lpr > /t/build.log 2>&1 \
    || { grep -iE "error|fatal" /t/build.log | head -30; exit 1; }
  printf "#!/bin/sh\nHEAPTRC=\"log=/t/heap-\$\$.txt\" /t/interop \"\$@\"\nrc=\$?\ngrep unfreed /t/heap-\$\$.txt\nexit \$rc\n" > /t/run.sh
  chmod +x /t/run.sh'
MSYS_NO_PATHCONV=1 docker exec "$NAME" sh -c "$BUILD"

echo "== interop with the Java client (Linux, FPC)"
MSYS_NO_PATHCONV=1 "${PYTHON:-python}" -I tools/interop.py \
  --pascal "docker exec $NAME /t/run.sh" --fixture-path /t/interop-records.tsv

# The K1 sample, with its defaults (the SASL_SSL listener, SCRAM), on Linux.
SAMPLE='
  set -e
  mkdir -p /t/s /t/samples /t/external/pascal-common-faa/tests
  cp -r /src/samples/ProduceConsume /t/samples/
  cp -r /src/external/pascal-common-faa/tests/pki /t/external/pascal-common-faa/tests/
  C=/t/external/pascal-common-faa/src
  cd /t/samples/ProduceConsume
  fpc -v0 -Mdelphi -Fu/t/src -Fi/t/src -Fu$C -Fi$C -FU/t/s -gh -gl -oKafkaSample KafkaSample.dpr > /t/sample-build.log 2>&1 \
    || { grep -iE "error|fatal" /t/sample-build.log | head -30; exit 1; }
  HEAPTRC="log=/t/sample-heap.txt" ./KafkaSample > /t/sample.log 2>&1 || true
  grep -E "^(localhost|done)|Error|failed" /t/sample.log
  grep "unfreed" /t/sample-heap.txt
  grep -q "^done" /t/sample.log && grep -qE "^0 unfreed memory blocks" /t/sample-heap.txt'
echo "== sample (samples/ProduceConsume, Linux)"
MSYS_NO_PATHCONV=1 docker exec "$NAME" sh -c "$SAMPLE"
