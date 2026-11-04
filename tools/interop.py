#!/usr/bin/env python3
"""Interoperability test with the Java client 4.4.0, in both directions.

The records are tests/data/interop-records.tsv (keys with accents and CJK, a
200-byte key, null keys, headers with a null value and a repeated name, an
empty value), which is also the Java console producer's input format:
"headers TAB key TAB value". The Java tools run inside the broker's container
(docker exec); the library's side is tests/Interop (KafkaInterop.dpr, or
fpc/KafkaInteropFpc.lpr), given here as a command (--pascal).

  Java -> library   kafka-console-producer.sh sends the fixture to a topic
                    of 6 partitions; the program reads every partition and
                    checks each record against its line, and that Java's
                    partitioner and this library's murmur2 put each keyed
                    record in the same partition; kafka-dump-log.sh says
                    every batch has the codec asked for.
  library -> Java   the program sends the fixture with fixed timestamps and
                    prints where each record went;
                    kafka-console-consumer.sh reads the topic and this
                    script checks every field it prints (timestamp and its
                    type, partition, offset, headers, key, value) against
                    the fixture and the program's report; kafka-dump-log.sh
                    reads the broker's log files and every batch must be
                    of the codec sent, with a valid CRC, as Java's own reader
                    sees it.

  Both directions run once per codec: gzip, snappy (Java's xerial framing)
  and lz4 (the KIP-57 frame). zstd runs Java -> library only: the library
  reads zstd and doesn't write it (docs/design.md, open question 2).

  one group         the Java console consumer (group.protocol=consumer, the
                    KIP-848 protocol) joins a group on a topic of 6
                    partitions and reads the records 0-59; the program
                    joins the same group, is given 3 partitions (the Java
                    member revoked them, committing first), produces the
                    records 60-119, reads its partitions to their end and
                    leaves (committing); then 120-149 are produced and the
                    Java member, holding all 6 again, reads them. Every
                    record is read once: none lost, none by both, the
                    program's only from its partitions, and nothing of its
                    partitions read again by Java after it left.

  the trace         the program sends records with the tracer on: the Java
                    console consumer (print.headers) must see each record's
                    traceparent header, in the program's trace and naming
                    one of its send spans. Then the Java console producer
                    sends records with a traceparent header of this script's,
                    and the program's process span for each is a child of
                    the span that header names, in its trace.

Ten new topics per run (interop-java-<codec>-<n> for each codec,
interop-pascal-<codec>-<n> for those the library writes, interop-group-<n>,
interop-trace-pascal-<n> and interop-trace-java-<n>), deleted when the run
passes and kept for a look when it fails.

    python -I tools/interop.py --pascal tests/Interop/fpc/KafkaInteropFpc.exe
    python -I tools/interop.py --pascal "docker exec interop-runner /t/interop" \\
        --fixture-path /t/interop-records.tsv          (tools/test_interop_docker.sh)

Leaks: heaptrc's summary, when the program prints one (FPC), must say 0
unfreed blocks. Exit code 0 when both directions pass.
"""

import argparse
import re
import secrets
import shlex
import subprocess
import sys
import threading
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


CODECS = ("gzip", "snappy", "lz4")
# Read by the library, not written: Java -> library only.
READ_ONLY_CODECS = ("zstd",)


def check_log(args, topic, codec, count):
    """kafka-dump-log.sh over the topic's segments: count records, every batch of codec."""
    batches = records = 0
    for p in range(PARTITIONS):
        files = broker(args.container, "sh", "-c", f"ls {LOG_DIR}/{topic}-{p}/*.log").split()
        dump = broker(args.container, BIN + "kafka-dump-log.sh", "--files", ",".join(files))
        for line in dump.splitlines():
            if not line.startswith("baseOffset:"):
                continue
            batches += 1
            records += int(line.split(" count: ")[1].split()[0])
            if f"compresscodec: {codec} " not in line + " " or "isvalid: true" not in line:
                raise Failure(f"partition {p}: {line}")
    if records != count:
        raise Failure(f"kafka-dump-log.sh found {records} records of {count}")
    return batches


def java_to_pascal(args, pascal, topic, codec, rows):
    broker(args.container, BIN + "kafka-console-producer.sh", "--bootstrap-server", args.bootstrap,
           "--topic", topic, "--compression-codec", codec,
           "--reader-property", "parse.key=true", "--reader-property", "parse.headers=true",
           "--reader-property", "null.marker=NULL", stdin=FIXTURE.read_bytes())
    batches = check_log(args, topic, codec, len(rows))
    out = run_pascal(pascal + ["consume", args.bootstrap, topic, args.fixture_path])
    print(f"[{codec}] " + out.strip())
    if not any(l.startswith("OK java->pascal") for l in out.splitlines()):
        raise Failure(f"java -> pascal failed ({codec})")
    print(f"[{codec}] OK java->pascal: the Java producer wrote {batches} batches, all {codec}")


def pascal_to_java(args, pascal, topic, codec, rows):
    out = run_pascal(pascal + ["produce", args.bootstrap, topic, args.fixture_path, codec])
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
    print(f"[{codec}] OK pascal->java: {len(seen)} records read by kafka-console-consumer.sh "
          "as sent (timestamp, partition, offset, headers, key, value)")
    batches = check_log(args, topic, codec, len(rows))
    print(f"[{codec}] OK pascal->java: kafka-dump-log.sh reads {batches} batches, all {codec}, "
          "every CRC valid")


def produce_group_records(args, topic, first, last):
    """Records first..last-1 through the Java console producer: key n, value "g n"."""
    lines = "".join(f"{n}\tg {n}\n" for n in range(first, last))
    broker(args.container, BIN + "kafka-console-producer.sh", "--bootstrap-server", args.bootstrap,
           "--topic", topic, "--reader-property", "parse.key=true",
           "--reader-property", "key.separator=\t", stdin=lines.encode("utf-8"))


class JavaGroupConsumer:
    """kafka-console-consumer.sh in a KIP-848 group, its output read as it comes."""

    def __init__(self, args, topic, group):
        self.lines = []
        # An RLock underneath: records() takes it again inside wait_for.
        self.changed = threading.Condition()
        self.ended = False
        self.proc = subprocess.Popen(
            ["docker", "exec", "-i", args.container, BIN + "kafka-console-consumer.sh",
             "--bootstrap-server", args.bootstrap, "--topic", topic, "--group", group,
             "--from-beginning", "--consumer-property", "group.protocol=consumer",
             "--formatter-property", "print.partition=true",
             # It ends on its own once nothing came for this long.
             "--timeout-ms", "15000"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for raw in self.proc.stdout:
            with self.changed:
                self.lines.append(raw.decode("utf-8", "replace").rstrip("\r\n"))
                self.changed.notify_all()
        with self.changed:
            self.ended = True
            self.changed.notify_all()

    def records(self):
        """(n, partition) for each record printed so far ("Partition:p TAB g n")."""
        out = []
        with self.changed:
            for line in self.lines:
                m = re.match(r"^Partition:(\d+)\tg (\d+)$", line)
                if m:
                    out.append((int(m.group(2)), int(m.group(1))))
        return out

    def wait_for(self, predicate, timeout_s, what):
        deadline = time.monotonic() + timeout_s
        with self.changed:
            while not predicate(self.records()):
                left = deadline - time.monotonic()
                if self.ended or left <= 0:
                    tail = "\n".join(self.lines[-30:])
                    raise Failure(f"the Java consumer: {what}; its output ended with:\n{tail}")
                # Woken by each line the reader takes, or the end of the output.
                self.changed.wait(timeout=left)

    def finish(self, timeout_s=90):
        try:
            self.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise Failure("the Java consumer did not end")
        self.reader.join(timeout=10)


def group_interop(args, pascal, topic):
    group = topic
    produce_group_records(args, topic, 0, 60)
    java = JavaGroupConsumer(args, topic, group)
    try:
        java.wait_for(lambda r: len({n for n, _ in r}) >= 60, 90,
                      "did not read the first 60 records")
        out = run_pascal(pascal + ["group", args.bootstrap, topic, group, "60", "120",
                                   str(PARTITIONS // 2)])
        if not any(l.startswith("OK pascal group") for l in out.splitlines()):
            raise Failure("pascal group failed:\n" + out)
        mine = None
        pascal_read = []
        for line in out.splitlines():
            if line.startswith("A "):
                mine = {int(p) for p in line[2:].split(",")}
            elif line.startswith("G "):
                _, n, p = line.split()
                pascal_read.append((int(n), int(p)))
        if not mine or len(mine) != PARTITIONS // 2:
            raise Failure(f"the program reported partitions {mine}")
        produce_group_records(args, topic, 120, 150)
        java.wait_for(lambda r: len({n for n, _ in r} | {n for n, _ in pascal_read}) >= 150, 90,
                      "the records were not all read by the two members")
        java.finish()
    finally:
        if java.proc.poll() is None:
            java.proc.kill()
    java_read = java.records()
    java_ns = [n for n, _ in java_read]
    pascal_ns = [n for n, _ in pascal_read]
    if len(set(java_ns)) != len(java_ns):
        raise Failure("the Java consumer read a record twice")
    if len(set(pascal_ns)) != len(pascal_ns):
        raise Failure("the program read a record twice")
    both = set(java_ns) & set(pascal_ns)
    if both:
        raise Failure(f"records read by both members: {sorted(both)[:20]}")
    missing = set(range(150)) - set(java_ns) - set(pascal_ns)
    if missing:
        raise Failure(f"records nobody read: {sorted(missing)[:20]}")
    if any(p not in mine for _, p in pascal_read):
        raise Failure("the program read a partition it wasn't given")
    if not any(60 <= n < 120 for n in pascal_ns):
        raise Failure("the program read none of the records it produced")
    if not any(p in mine for n, p in java_read if n >= 120):
        raise Failure("the Java member read nothing of the program's partitions after it left")
    print(f"OK one group: the Java consumer (KIP-848) and the program shared {PARTITIONS} "
          f"partitions (the program had {sorted(mine)}); {len(java_ns)} records read by Java, "
          f"{len(pascal_ns)} by the program, none lost, none twice")


TRACE_COUNT = 5
TRACEPARENT = re.compile(r"^00-([0-9a-f]{32})-([0-9a-f]{16})-01$")


def trace_interop(args, pascal, pascal_topic, java_topic):
    """The traceparent header both ways (the module's docstring, "the trace")."""
    out = run_pascal(pascal + ["trace-produce", args.bootstrap, pascal_topic, str(TRACE_COUNT)])
    if not any(l.startswith("OK pascal trace-produce") for l in out.splitlines()):
        raise Failure("pascal trace-produce failed:\n" + out)
    trace = None
    spans = set()
    for line in out.splitlines():
        if line.startswith("TRACE "):
            trace = line.split()[1]
        elif line.startswith("S "):
            spans.add(line.split()[1])
    if not trace or len(spans) != TRACE_COUNT:
        raise Failure("the program reported no trace or the wrong spans:\n" + out)
    out = broker(args.container, BIN + "kafka-console-consumer.sh", "--bootstrap-server",
                 args.bootstrap, "--topic", pascal_topic, "--from-beginning",
                 "--max-messages", str(TRACE_COUNT), "--timeout-ms", "30000",
                 "--formatter-property", "print.headers=true")
    seen = 0
    for line in out.split("\n"):
        if "\tt " not in line:
            continue
        headers, value = line.split("\t", 1)
        parents = [h.split(":", 1)[1] for h in headers.split(",") if h.startswith("traceparent:")]
        if len(parents) != 1:
            raise Failure(f"{value}: Java saw the headers {headers!r}")
        m = TRACEPARENT.match(parents[0])
        if not m or m.group(1) != trace or m.group(2) not in spans:
            raise Failure(f"{value}: traceparent {parents[0]!r}, not a send span of trace {trace}")
        seen += 1
    if seen != TRACE_COUNT:
        raise Failure(f"Java read {seen} records of {TRACE_COUNT}:\n{out}")
    print(f"OK trace pascal->java: kafka-console-consumer.sh saw the traceparent of a send span "
          f"of the program's trace on each of {seen} records")

    sent = {}
    lines = ""
    for n in range(TRACE_COUNT):
        trace_id, span_id = secrets.token_hex(16), secrets.token_hex(8)
        sent[f"j_{n}"] = (trace_id, span_id)
        lines += f"traceparent:00-{trace_id}-{span_id}-01\tj {n}\n"
    broker(args.container, BIN + "kafka-console-producer.sh", "--bootstrap-server", args.bootstrap,
           "--topic", java_topic, "--reader-property", "parse.headers=true",
           stdin=lines.encode("utf-8"))
    out = run_pascal(pascal + ["trace-consume", args.bootstrap, java_topic, str(TRACE_COUNT)])
    if not any(l.startswith("OK pascal trace-consume") for l in out.splitlines()):
        raise Failure("pascal trace-consume failed:\n" + out)
    got = {}
    for line in out.splitlines():
        if line.startswith("P "):
            _, value, trace_id, parent = line.split()
            got[value] = (trace_id, parent)
    if got != sent:
        raise Failure(f"the program's process spans {got!r}, the Java producer sent {sent!r}")
    print(f"OK trace java->pascal: the program's process span for each of {len(got)} records "
          "is a child of the span its Java-written traceparent names")


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
    topics = ([f"interop-{side}-{codec}-{stamp}" for codec in CODECS for side in ("java", "pascal")]
              + [f"interop-java-{codec}-{stamp}" for codec in READ_ONLY_CODECS]
              + [f"interop-group-{stamp}"]
              + [f"interop-trace-pascal-{stamp}", f"interop-trace-java-{stamp}"])
    try:
        for t in topics:
            broker(args.container, BIN + "kafka-topics.sh", "--bootstrap-server", args.bootstrap,
                   "--create", "--topic", t, "--partitions", str(PARTITIONS),
                   # The produced records carry 2026-01-01 timestamps: with
                   # the default retention (7 days) the broker deletes their
                   # segment at once (measured).
                   "--config", "retention.ms=-1")
        for i, codec in enumerate(CODECS):
            java_to_pascal(args, pascal, topics[2 * i], codec, rows)
            pascal_to_java(args, pascal, topics[2 * i + 1], codec, rows)
        for i, codec in enumerate(READ_ONLY_CODECS):
            java_to_pascal(args, pascal, topics[2 * len(CODECS) + i], codec, rows)
        group_interop(args, pascal, topics[-3])
        trace_interop(args, pascal, topics[-2], topics[-1])
    except Failure as e:
        print(f"FAIL {e}\n(topics kept: {', '.join(topics)})")
        return 1
    for t in topics:
        broker(args.container, BIN + "kafka-topics.sh", "--bootstrap-server", args.bootstrap,
               "--delete", "--topic", t)
    print(f"interop: both directions with {', '.join(CODECS)}, Java -> library with "
          f"{', '.join(READ_ONLY_CODECS)}, the shared group and the trace context OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
