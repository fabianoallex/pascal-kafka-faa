#!/usr/bin/env python3
"""Records Kafka frames between a client and a broker, for the codec tests.

A plain TCP proxy: it listens on --listen, forwards every connection to
--target, and writes each frame (the 4-byte size prefix included) to a JSON
Lines file, one object per frame:

    {"conn": 1, "dir": "request", "hex": "..."}
    {"conn": 1, "dir": "response", "hex": "..."}

Kafka frames are length-prefixed and a connection's responses come back in
the order of its requests, so cutting the byte stream at the size prefixes is
enough; nothing is parsed here. tools/captures_to_data.py turns the records
into tests/data/captures-<kafka version>.txt.

Typical use (docs/codec-captures.md has the full recipe): run this on the
host, then point a Java tool in the broker's container at it:

    python -I tools/capture_proxy.py --listen 19092 --target localhost:9092 --out .ci/cap.jsonl
    docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-broker-api-versions.sh \\
        --bootstrap-server host.docker.internal:19092

The Java client then reads Metadata, which advertises localhost:9092, and
opens its next connections to the broker directly: only the bootstrap
connection is recorded, and that one holds ApiVersions and Metadata.
"""

import argparse
import json
import socket
import struct
import sys
import threading

_lock = threading.Lock()
_conn_seq = 0


def _record(out, conn, direction, frame):
    with _lock:
        out.write(json.dumps({"conn": conn, "dir": direction, "hex": frame.hex()}) + "\n")
        out.flush()


def _pump(src, dst, out, conn, direction):
    buf = b""
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
            buf += data
            while len(buf) >= 4:
                (size,) = struct.unpack(">i", buf[:4])
                if len(buf) < 4 + size:
                    break
                _record(out, conn, direction, buf[: 4 + size])
                buf = buf[4 + size:]
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def main():
    global _conn_seq
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--listen", type=int, required=True, help="local port")
    ap.add_argument("--target", required=True, help="broker host:port")
    ap.add_argument("--out", required=True, help="JSON Lines file (appended)")
    args = ap.parse_args()
    host, port = args.target.rsplit(":", 1)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", args.listen))
    srv.listen(16)
    print(f"listening on {args.listen}, forwarding to {args.target}", file=sys.stderr)
    with open(args.out, "a", encoding="ascii") as out:
        while True:
            client, _ = srv.accept()
            upstream = socket.create_connection((host, int(port)))
            with _lock:
                _conn_seq += 1
                conn = _conn_seq
            threading.Thread(target=_pump, args=(client, upstream, out, conn, "request"), daemon=True).start()
            threading.Thread(target=_pump, args=(upstream, client, out, conn, "response"), daemon=True).start()


if __name__ == "__main__":
    main()
