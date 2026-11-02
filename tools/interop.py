#!/usr/bin/env python3
"""Interoperability test with the Java client 4.4.0, in both directions.

The records are tests/data/interop-records.tsv (keys with accents and CJK, a
200-byte key, null keys, headers with a null value and a repeated name, an
empty value), which is also the Java console producer's input format:
"headers TAB key TAB value". The Java tools run inside the broker's container
(docker exec); the library's side is tests/Interop (KafkaInterop.dpr, or
fpc/KafkaInteropFpc.lpr), given here as a command (--pascal).

  Java -> library   kafka-console-producer.sh sends the fixture with gzip to
                    a topic of 6 partitions; the program reads every
                    partition and checks each record against its line, and
                    that Java's partitioner and this library's murmur2 put
                    each keyed record in the same partition.
  library -> Java   the program sends the fixture with gzip and fixed
                    timestamps and prints where each record went;
                    kafka-console-consumer.sh reads the topic and this
                    script checks every field it prints (timestamp and its
                    type, partition, offset, headers, key, value) against
                    the fixture and the program's report; kafka-dump-log.sh
                    reads the broker's log files and every batch must be
                    gzip with a valid CRC, as Java's own reader sees it.

Two new topics per run (interop-java-<n>, interop-pascal-<n>), deleted when
the run passes and kept for a look when it fails.

    python -I tools/interop.py --pascal tests/Interop/fpc/KafkaInteropFpc.exe
    python -I tools/interop.py --pascal "docker exec interop-runner /t/interop" \\
        --fixture-path /t/interop-records.tsv          (tools/test_interop_docker.sh)

Leaks: heaptrc's summary, when the program prints one (FPC), must say 0
unfreed blocks. Exit code 0 when both directions pass.
"""

import argparse
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "tests" / "data" / "interop-records.tsv"
BASE_TIMESTAMP = 1767225600000  # KAFKA_INTEROP_BASE_TIMESTAMP in Kafka.Interop
PARTITIONS = 6
BIN = "/opt/kafka/bin/"
LOG_DIR = "/tmp/kraft-combined-logs"  # KAFKA_LOG_DIRS in docker/docker-compose.yml


class Failure(Exception):
    pass


def run(cmd, stdin=None, check=True):
    p = subprocess.run(cmd, input=stdin, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = p.stdout.decode("utf-8", "replace")
    if check and p.returncode != 0:
        raise Failure(f"{' '.join(cmd)} exited with {p.returncode}:\n{out}")
    return out


def run_pascal(cmd):
    """The program's output; a Failure when heaptrc (FPC) reports a leak."""
    out = run(cmd, check=False)
    if "unfreed memory blocks" in out and not re.search(r"^0 unfreed memory blocks", out, re.M):
        raise Failure(f"{cmd[-4]}: memory leaked:\n{out}")
    return out


def broker(container, *args, stdin=None):
    return run(["docker", "exec", "-i", container] + list(args), stdin=stdin)


def fixture_records():
    rows = []
    for line in FIXTURE.read_text(encoding="utf-8").split("\n"):
        if line:
            headers, key, value = line.split("\t")
            rows.append((headers, key, value))
    return rows


def java_to_pascal(args, pascal, topic):
    broker(args.container, BIN + "kafka-console-producer.sh", "--bootstrap-server", args.bootstrap,
           "--topic", topic, "--compression-codec", "gzip",
           "--reader-property", "parse.key=true", "--reader-property", "parse.headers=true",
           "--reader-property", "null.marker=NULL", stdin=FIXTURE.read_bytes())
    out = run_pascal(pascal + ["consume", args.bootstrap, topic, args.fixture_path])
    print(out.strip())
    if not any(l.startswith("OK java->pascal") for l in out.splitlines()):
        raise Failure("java -> pascal failed")


def pascal_to_java(args, pascal, topic, rows):
    out = run_pascal(pascal + ["produce", args.bootstrap, topic, args.fixture_path])
    report = {}
    for line in out.splitlines():
        if line.startswith("R "):
            _, n, p, o = line.split()
            report[int(n)] = (int(p), int(o))
    if not any(l.startswith("OK pascal produced") for l in out.splitlines()):
        raise Failure("pascal produce failed:\n" + out)
    if len(report) != len(rows):
        raise Failure(f"the program reported {len(report)} records of {len(rows)}")

    out = broker(args.container, BIN + "kafka-console-consumer.sh", "--bootstrap-server",
                 args.bootstrap, "--topic", topic, "--from-beginning",
                 "--max-messages", str(len(rows)), "--timeout-ms", "30000",
                 "--formatter-property", "print.timestamp=true",
                 "--formatter-property", "print.partition=true",
                 "--formatter-property", "print.offset=true",
                 "--formatter-property", "print.headers=true",
                 "--formatter-property", "print.key=true",
                 "--formatter-property", "key.separator=\t",
                 "--formatter-property", "null.literal=NULL")
    seen = set()
    for line in out.split("\n"):
        if not line.startswith(("CreateTime:", "LogAppendTime:", "NO_TIMESTAMP")):
            continue
        ts, part, off, headers, key, value = line.split("\t", 5)
        n = int(headers.split(",")[0].split(":")[1])
        want_headers, want_key, want_value = rows[n]
        got = (ts, part, off, headers, key, value)
        want = (f"CreateTime:{BASE_TIMESTAMP + 1000 * n}", f"Partition:{report[n][0]}",
                f"Offset:{report[n][1]}", want_headers, want_key, want_value)
        if got != want:
            raise Failure(f"record n={n}: Java read {got!r}, expected {want!r}")
        if n in seen:
            raise Failure(f"record n={n} twice")
        seen.add(n)
    if len(seen) != len(rows):
        raise Failure(f"Java read {len(seen)} records of {len(rows)}:\n{out}")
    print(f"OK pascal->java: {len(seen)} records read by kafka-console-consumer.sh as sent "
          "(timestamp, partition, offset, headers, key, value)")

    batches = records = 0
    for p in range(PARTITIONS):
        files = broker(args.container, "sh", "-c", f"ls {LOG_DIR}/{topic}-{p}/*.log").split()
        dump = broker(args.container, BIN + "kafka-dump-log.sh", "--files", ",".join(files))
        for line in dump.splitlines():
            if not line.startswith("baseOffset:"):
                continue
            batches += 1
            records += int(line.split(" count: ")[1].split()[0])
            if "compresscodec: gzip" not in line or "isvalid: true" not in line:
                raise Failure(f"partition {p}: {line}")
    if records != len(rows):
        raise Failure(f"kafka-dump-log.sh found {records} records of {len(rows)}")
    print(f"OK pascal->java: kafka-dump-log.sh reads {batches} batches, all gzip, every CRC valid")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--pascal", required=True, help="the KafkaInterop program, as a command")
    ap.add_argument("--fixture-path", default=str(FIXTURE),
                    help="the fixture's path as the program sees it")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--container", default="pascal-kafka-faa-broker")
    args = ap.parse_args()
    pascal = shlex.split(args.pascal, posix=True)
    if Path(pascal[0]).is_file():
        # Windows' CreateProcess doesn't find a relative path with slashes.
        pascal[0] = str(Path(pascal[0]).resolve())
    rows = fixture_records()
    stamp = f"{int(time.time() * 1000)}"
    topics = [f"interop-java-{stamp}", f"interop-pascal-{stamp}"]
    try:
        for t in topics:
            broker(args.container, BIN + "kafka-topics.sh", "--bootstrap-server", args.bootstrap,
                   "--create", "--topic", t, "--partitions", str(PARTITIONS),
                   # The produced records carry 2026-01-01 timestamps: with
                   # the default retention (7 days) the broker deletes their
                   # segment at once (measured).
                   "--config", "retention.ms=-1")
        java_to_pascal(args, pascal, topics[0])
        pascal_to_java(args, pascal, topics[1], rows)
    except Failure as e:
        print(f"FAIL {e}\n(topics kept: {', '.join(topics)})")
        return 1
    for t in topics:
        broker(args.container, BIN + "kafka-topics.sh", "--bootstrap-server", args.bootstrap,
               "--delete", "--topic", t)
    print("interop: both directions OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
