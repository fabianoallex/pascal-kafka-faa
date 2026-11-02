#!/usr/bin/env python3
"""Records a broker's real answers to every version of the generated APIs.

The Java client always uses the newest versions (tools/capture_proxy.py
records those). This script covers the rest: it builds ApiVersions v0..v5 and
Metadata v0..v13 requests BY HAND, from the protocol guide, sends each to the
broker on its own connection, and records request and response.

Hand-built on purpose, independent of the Pascal encoder: the tests encode the
same logical request with the generated codec and compare the bytes, so a
mistake shared by the generator and this script is the only one that could
pass, and the broker accepting the request (the script checks error code 0)
rules most of those out.

Output: JSON Lines appended to --out, one object per frame, in the format of
tests/data/captures-<tag>.jsonl (docs/codec-captures.md):

    {"name": "metadata-v4", "source": "hand-built", "dir": "request",
     "api_key": 3, "api_version": 4, "hex": "..."}

"hex" is the frame without its 4-byte size prefix: header and body.

    python -I tools/capture_versions.py --broker localhost:9092 --topic captured-topic --out FILE
"""

import argparse
import json
import socket
import struct

CLIENT_ID = "pascal-kafka-faa-capture"


def uvarint(n):
    out = b""
    while n & ~0x7F:
        out += bytes([(n & 0x7F) | 0x80])
        n >>= 7
    return out + bytes([n])


def string(s, compact):
    b = s.encode("utf-8")
    return (uvarint(len(b) + 1) if compact else struct.pack(">h", len(b))) + b


def null_string(compact):
    return uvarint(0) if compact else struct.pack(">h", -1)


def array_len(n, compact):
    return uvarint(n + 1) if compact else struct.pack(">i", n)


def header(api_key, version, corr, flexible):
    h = struct.pack(">hhi", api_key, version, corr) + string(CLIENT_ID, False)
    if flexible:
        h += uvarint(0)  # no tagged fields
    return h


def api_versions_body(v):
    if v < 3:
        return b""
    body = string("pascal-kafka-faa-capture", True) + string("1.0", True)
    if v >= 5:
        body += null_string(True) + struct.pack(">i", -1)  # ClusterId null, NodeId -1
    return body + uvarint(0)


def metadata_body(v, topic):
    flex = v >= 9
    body = array_len(1, flex)
    if v >= 10:
        body += bytes(16)  # TopicId: zero
    body += string(topic, flex)
    if flex:
        body += uvarint(0)  # the topic's tagged fields
    if v >= 4:
        body += b"\x01"  # AllowAutoTopicCreation
    if 8 <= v <= 10:
        body += b"\x00"  # IncludeClusterAuthorizedOperations
    if v >= 8:
        body += b"\x00"  # IncludeTopicAuthorizedOperations
    if flex:
        body += uvarint(0)
    return body


def exchange(host, port, content):
    with socket.create_connection((host, port), timeout=10) as s:
        s.sendall(struct.pack(">i", len(content)) + content)
        size = struct.unpack(">i", recv_exact(s, 4))[0]
        return recv_exact(s, size)


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed by the broker")
        buf += chunk
    return buf


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--topic", default="captured-topic")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    host, port = args.broker.rsplit(":", 1)
    port = int(port)

    jobs = [("api-versions", 18, v, 3, api_versions_body(v)) for v in range(0, 6)]
    jobs += [("metadata", 3, v, 9, metadata_body(v, args.topic)) for v in range(0, 14)]
    with open(args.out, "a", encoding="ascii") as out:
        for name, key, v, first_flex, body in jobs:
            corr = 1000 + v
            req = header(key, v, corr, v >= first_flex) + body
            rsp = exchange(host, port, req)
            (rcorr,) = struct.unpack(">i", rsp[:4])
            if rcorr != corr:
                raise SystemExit(f"{name}-v{v}: correlation {rcorr}, expected {corr}")
            # The body's first error code: ApiVersions starts with it; Metadata
            # has one per topic, checked by the tests.
            if key == 18:
                (err,) = struct.unpack(">h", rsp[4:6])
                if err != 0:
                    raise SystemExit(f"{name}-v{v}: error code {err}")
            for direction, data in (("request", req), ("response", rsp)):
                out.write(json.dumps({"name": f"{name}-v{v}", "source": "hand-built",
                                      "dir": direction, "api_key": key, "api_version": v,
                                      "hex": data.hex()}) + "\n")
            print(f"{name}-v{v}: {len(req)} -> {len(rsp)} bytes")


if __name__ == "__main__":
    main()
