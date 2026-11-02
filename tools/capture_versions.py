#!/usr/bin/env python3
"""Records a broker's real answers to every version of the generated APIs.

The Java client always uses the newest versions (tools/capture_proxy.py
records those). This script covers the rest: it builds every version of
ApiVersions (v0-v5), Metadata (v0-v13), Produce (v3-v13), ListOffsets
(v1-v11) and Fetch (v4-v18) BY HAND, from the protocol guide and the JSON
definitions, sends each to the broker on its own connection, and records
request and response.

Hand-built on purpose, independent of the Pascal encoder: the tests encode the
same logical request with the generated codec and compare the bytes, so a
mistake shared by the generator and this script is the only one that could
pass, and the broker accepting the request rules most of those out. The
script checks the error code of every answer (the first partition's for
Produce, ListOffsets and Fetch) and stops on one that isn't 0.

The record batches Produce sends are built here too (record batch v2, with
its own CRC-32C), one per version, uncompressed except v13's, which is gzip
from Python's gzip module (mtime 0): the library's gzip reader is checked
against an encoder it doesn't share code with. Every batch goes to partition
0 of --records-topic, so the Fetch answers that follow hold all of them;
one more Fetch (fetch-v18-cut) asks for fewer bytes than the partition holds,
to record a batch cut short by the broker.

Produce v13 and Fetch v13 and later name the topic by id: the script reads
it from a Metadata v12 answer it parses by hand.

SASL (--apis sasl) needs several requests on one connection, so it has
exchanges of its own, on the SASL_PLAINTEXT listener (--sasl-broker) and
the one whose sessions last 2 s (--reauth-broker): SaslHandshake v0 and v1
(PLAIN), SaslAuthenticate v0, v1 and v2 with PLAIN (alice / alice-secret,
docker/docker-compose.yml), v1 and v2 again where the broker gives a session
lifetime, a mechanism the broker doesn't enable (GSSAPI: error 33 and the
list) and a wrong password (error 58 and the broker's message). The
expected error code of each is checked. SCRAM's exchanges come from the
Java client instead (docs/codec-captures.md): its proof needs the nonce,
which a test can only replay from a recorded exchange.

Output: JSON Lines appended to --out, one object per frame, in the format of
tests/data/captures-<tag>.jsonl (docs/codec-captures.md):

    {"name": "metadata-v4", "source": "hand-built", "dir": "request",
     "api_key": 3, "api_version": 4, "hex": "..."}

"hex" is the frame without its 4-byte size prefix: header and body.

    python -I tools/capture_versions.py --broker localhost:9092 \\
        --apis produce,list-offsets,fetch --out FILE
    python -I tools/capture_versions.py --apis sasl --out FILE
"""

import argparse
import gzip
import json
import socket
import struct
import time

CLIENT_ID = "pascal-kafka-faa-capture"
ALL_APIS = ["api-versions", "metadata", "produce", "list-offsets", "fetch", "sasl"]
SASL_HANDSHAKE, SASL_AUTHENTICATE = 17, 36


# ---------------------------------------------------------------- primitives

def uvarint(n):
    out = b""
    while n & ~0x7F:
        out += bytes([(n & 0x7F) | 0x80])
        n >>= 7
    return out + bytes([n])


def varint(n):
    """Zigzag, as in record batches (32 and 64 bits alike for our values)."""
    return uvarint((n << 1) ^ (n >> 63))


def string(s, compact):
    b = s.encode("utf-8")
    return (uvarint(len(b) + 1) if compact else struct.pack(">h", len(b))) + b


def null_string(compact):
    return uvarint(0) if compact else struct.pack(">h", -1)


def nbytes(b, compact):
    return (uvarint(len(b) + 1) if compact else struct.pack(">i", len(b))) + b


def array_len(n, compact):
    return uvarint(n + 1) if compact else struct.pack(">i", n)


def tags(flex):
    return uvarint(0) if flex else b""


def header(api_key, version, corr, flexible):
    h = struct.pack(">hhi", api_key, version, corr) + string(CLIENT_ID, False)
    if flexible:
        h += uvarint(0)  # no tagged fields
    return h


class Reader:
    def __init__(self, data):
        self.d, self.p = data, 0

    def take(self, n):
        out = self.d[self.p:self.p + n]
        if len(out) != n:
            raise ValueError("answer cut short")
        self.p += n
        return out

    def i8(self):
        return struct.unpack(">b", self.take(1))[0]

    def i16(self):
        return struct.unpack(">h", self.take(2))[0]

    def i32(self):
        return struct.unpack(">i", self.take(4))[0]

    def i64(self):
        return struct.unpack(">q", self.take(8))[0]

    def uvarint(self):
        shift = n = 0
        while True:
            b = self.take(1)[0]
            n |= (b & 0x7F) << shift
            if not b & 0x80:
                return n
            shift += 7

    def array_len(self, compact):
        return self.uvarint() - 1 if compact else self.i32()

    def string(self, compact):
        n = self.uvarint() - 1 if compact else self.i16()
        return None if n < 0 else self.take(n).decode("utf-8")

    def skip_tags(self):
        for _ in range(self.uvarint()):
            self.uvarint()
            self.take(self.uvarint())


# ---------------------------------------------------------------- CRC-32C, record batch v2

def _crc32c_table():
    t = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        t.append(c)
    return t


_CRC = _crc32c_table()


def crc32c(data):
    c = 0xFFFFFFFF
    for b in data:
        c = _CRC[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF


def record(offset_delta, ts_delta, key, value, headers):
    body = b"\x00" + varint(ts_delta) + varint(offset_delta)
    body += varint(-1) if key is None else varint(len(key)) + key
    body += varint(-1) if value is None else varint(len(value)) + value
    body += varint(len(headers))
    for hk, hv in headers:
        body += varint(len(hk)) + hk
        body += varint(-1) if hv is None else varint(len(hv)) + hv
    return varint(len(body)) + body


def record_batch(records, base_ts, codec=0):
    """records: (key, value, headers, timestamp); codec 0 none, 1 gzip."""
    payload = b"".join(record(i, ts - base_ts, k, v, h) for i, (k, v, h, ts) in enumerate(records))
    if codec == 1:
        payload = gzip.compress(payload, mtime=0)
    max_ts = max(r[3] for r in records)
    after_crc = struct.pack(">hiqqqhii", codec, len(records) - 1, base_ts, max_ts,
                            -1, -1, -1, len(records)) + payload
    crc = crc32c(after_crc)
    # partitionLeaderEpoch, magic, crc, then the rest
    tail = struct.pack(">ibI", -1, 2, crc) + after_crc
    return struct.pack(">qi", 0, len(tail)) + tail


# ---------------------------------------------------------------- requests

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


def produce_body(v, topic, topic_id, batch):
    flex = v >= 9
    body = null_string(flex)                       # TransactionalId (v3+)
    body += struct.pack(">hi", -1, 30000)          # Acks all, TimeoutMs
    body += array_len(1, flex)
    body += topic_id if v >= 13 else string(topic, flex)
    body += array_len(1, flex)
    body += struct.pack(">i", 0) + nbytes(batch, flex) + tags(flex)  # Index, Records
    body += tags(flex)                             # the topic's
    return body + tags(flex)


def list_offsets_body(v, topic, timestamp):
    flex = v >= 6
    body = struct.pack(">i", -1)                   # ReplicaId
    if v >= 2:
        body += b"\x00"                            # IsolationLevel
    body += array_len(1, flex) + string(topic, flex)
    body += array_len(1, flex) + struct.pack(">i", 0)  # PartitionIndex
    if v >= 4:
        body += struct.pack(">i", -1)              # CurrentLeaderEpoch
    body += struct.pack(">q", timestamp) + tags(flex)
    body += tags(flex)
    if v >= 10:
        body += struct.pack(">i", 30000)           # TimeoutMs
    return body + tags(flex)


def fetch_body(v, topic, topic_id, partition_max_bytes):
    flex = v >= 12
    body = b""
    if v <= 14:
        body += struct.pack(">i", -1)              # ReplicaId
    body += struct.pack(">ii", 100, 1)             # MaxWaitMs, MinBytes
    body += struct.pack(">i", 0x7FFFFFFF)          # MaxBytes (v3+)
    body += b"\x00"                                # IsolationLevel (v4+)
    if v >= 7:
        body += struct.pack(">ii", 0, -1)          # SessionId, SessionEpoch
    body += array_len(1, flex)
    body += topic_id if v >= 13 else string(topic, flex)
    body += array_len(1, flex) + struct.pack(">i", 0)  # Partition
    if v >= 9:
        body += struct.pack(">i", -1)              # CurrentLeaderEpoch
    body += struct.pack(">q", 0)                   # FetchOffset
    if v >= 12:
        body += struct.pack(">i", -1)              # LastFetchedEpoch
    if v >= 5:
        body += struct.pack(">q", -1)              # LogStartOffset
    body += struct.pack(">i", partition_max_bytes) + tags(flex)
    body += tags(flex)                             # the topic's
    if v >= 7:
        body += array_len(0, flex)                 # ForgottenTopicsData
    if v >= 11:
        body += string("", flex)                   # RackId
    return body + tags(flex)


# ---------------------------------------------------------------- answers

def body_reader(rsp, flex):
    r = Reader(rsp)
    r.i32()  # correlation id
    if flex:
        r.skip_tags()
    return r


def produce_error(rsp, v):
    flex = v >= 9
    r = body_reader(rsp, flex)
    r.array_len(flex)
    r.take(16) if v >= 13 else r.string(flex)
    r.array_len(flex)
    r.i32()
    return r.i16()


def list_offsets_error(rsp, v):
    flex = v >= 6
    r = body_reader(rsp, flex)
    if v >= 2:
        r.i32()  # ThrottleTimeMs
    r.array_len(flex)
    r.string(flex)
    r.array_len(flex)
    r.i32()
    return r.i16()


def fetch_error(rsp, v):
    flex = v >= 12
    r = body_reader(rsp, flex)
    r.i32()  # ThrottleTimeMs
    if v >= 7:
        top = r.i16()
        if top:
            return top
        r.i32()  # SessionId
    r.array_len(flex)
    r.take(16) if v >= 13 else r.string(flex)
    r.array_len(flex)
    r.i32()
    return r.i16()


def topic_id_and_leader(host, port, topic):
    """Metadata v12 by hand: the topic's id, once partition 0 has a leader."""
    for _ in range(60):
        rsp = exchange(host, port, header(3, 12, 999, True) + metadata_body(12, topic))
        r = body_reader(rsp, True)
        r.i32()  # ThrottleTimeMs
        for _ in range(r.array_len(True)):  # brokers
            r.i32(), r.string(True), r.i32(), r.string(True)
            r.skip_tags()
        r.string(True), r.i32()  # ClusterId, ControllerId
        r.array_len(True)
        err, name, tid = r.i16(), r.string(True), r.take(16)
        r.take(1)  # IsInternal
        leader = -1
        for _ in range(r.array_len(True)):
            r.i16()
            index, lead = r.i32(), r.i32()
            r.i32()  # LeaderEpoch
            for _ in range(3):  # ReplicaNodes, IsrNodes, OfflineReplicas
                for _ in range(r.array_len(True)):
                    r.i32()
            r.skip_tags()
            if index == 0:
                leader = lead
        if err == 0 and leader >= 0:
            return tid
        time.sleep(0.5)  # a new topic needs a moment to elect a leader
    raise SystemExit(f"topic {topic}: no leader for partition 0")


# ---------------------------------------------------------------- SASL

def sasl_handshake_body(mechanism):
    return string(mechanism, False)  # v0 and v1, never flexible


def sasl_authenticate_body(v, auth):
    flex = v >= 2
    return nbytes(auth, flex) + tags(flex)


def plain_token(user, password):
    return b"\0" + user.encode() + b"\0" + password.encode()


def sasl_jobs(sasl, reauth):
    """Connections, each a list of (name or None, key, version, body, expected
    error); a name of None is sent but not recorded (the handshake again)."""
    good = plain_token("alice", "alice-secret")
    hs = (None, SASL_HANDSHAKE, 1, sasl_handshake_body("PLAIN"), 0)
    return [
        (sasl, [("sasl-handshake-v0", SASL_HANDSHAKE, 0, sasl_handshake_body("PLAIN"), 0)]),
        (sasl, [("sasl-handshake-v1", SASL_HANDSHAKE, 1, sasl_handshake_body("PLAIN"), 0),
                ("sasl-authenticate-v0", SASL_AUTHENTICATE, 0, sasl_authenticate_body(0, good), 0)]),
        (sasl, [hs, ("sasl-authenticate-v1", SASL_AUTHENTICATE, 1, sasl_authenticate_body(1, good), 0)]),
        (sasl, [hs, ("sasl-authenticate-v2", SASL_AUTHENTICATE, 2, sasl_authenticate_body(2, good), 0)]),
        (reauth, [hs, ("sasl-authenticate-v1-lifetime", SASL_AUTHENTICATE, 1,
                       sasl_authenticate_body(1, good), 0)]),
        (reauth, [hs, ("sasl-authenticate-v2-lifetime", SASL_AUTHENTICATE, 2,
                       sasl_authenticate_body(2, good), 0)]),
        (sasl, [("sasl-handshake-v1-unsupported", SASL_HANDSHAKE, 1,
                 sasl_handshake_body("GSSAPI"), 33)]),
        (sasl, [hs, ("sasl-authenticate-v2-failed", SASL_AUTHENTICATE, 2,
                     sasl_authenticate_body(2, plain_token("alice", "wrong")), 58)]),
    ]


def run_sasl(jobs, out):
    corr = 2000
    for (host, port), steps in jobs:
        with socket.create_connection((host, port), timeout=10) as s:
            for name, key, v, body, expected in steps:
                corr += 1
                flex = key == SASL_AUTHENTICATE and v >= 2
                req = header(key, v, corr, flex) + body
                s.sendall(struct.pack(">i", len(req)) + req)
                rsp = recv_exact(s, struct.unpack(">i", recv_exact(s, 4))[0])
                (rcorr,) = struct.unpack(">i", rsp[:4])
                if rcorr != corr:
                    raise SystemExit(f"{name}: correlation {rcorr}, expected {corr}")
                # Response header v1 (a tagged-field count) for a flexible body.
                (err,) = struct.unpack(">h", rsp[5:7] if flex else rsp[4:6])
                if err != expected:
                    raise SystemExit(f"{name or 'handshake'}: error code {err}, expected {expected}")
                if name is None:
                    continue
                for direction, data in (("request", req), ("response", rsp)):
                    out.write(json.dumps({"name": name, "source": "hand-built",
                                          "dir": direction, "api_key": key, "api_version": v,
                                          "hex": data.hex()}) + "\n")
                print(f"{name}: {len(req)} -> {len(rsp)} bytes")


# ---------------------------------------------------------------- the exchange

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
    ap.add_argument("--topic", default="captured-topic", help="the Metadata requests' topic")
    ap.add_argument("--records-topic", default="captured-records",
                    help="the topic Produce writes to and Fetch reads from")
    ap.add_argument("--apis", default=",".join(a for a in ALL_APIS if a != "sasl"))
    ap.add_argument("--sasl-broker", default="localhost:9096",
                    help="a SASL_PLAINTEXT listener with PLAIN (--apis sasl)")
    ap.add_argument("--reauth-broker", default="localhost:9098",
                    help="the same with connections.max.reauth.ms set (--apis sasl)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    host, port = args.broker.rsplit(":", 1)
    port = int(port)
    apis = args.apis.split(",")
    unknown = set(apis) - set(ALL_APIS)
    if unknown:
        raise SystemExit(f"unknown --apis: {', '.join(sorted(unknown))}")

    # (name, api key, version, first flexible version, body or a function
    # giving it, error check or None)
    jobs = []
    if "api-versions" in apis:
        jobs += [(f"api-versions-v{v}", 18, v, 3, api_versions_body(v),
                  lambda rsp, v: struct.unpack(">h", rsp[4:6])[0]) for v in range(0, 6)]
    if "metadata" in apis:
        # Metadata has one error per topic, checked by the tests.
        jobs += [(f"metadata-v{v}", 3, v, 9, metadata_body(v, args.topic), None)
                 for v in range(0, 14)]
    topic_id = None
    if {"produce", "fetch"} & set(apis):
        topic_id = topic_id_and_leader(host, port, args.records_topic)
    t0 = 1767225600000  # 2026-01-01T00:00:00Z: fixed, so a capture is reproducible
    if "produce" in apis:
        for v in range(3, 14):
            recs = [(f"key-{v}".encode(), f"value of produce v{v}".encode(),
                     [(b"trace", f"t{v}".encode()), (b"empty", None)], t0 + 1000 * v),
                    (None, b"no key", [], t0 + 1000 * v + 5)]
            batch = record_batch(recs, t0 + 1000 * v, codec=1 if v == 13 else 0)
            jobs.append((f"produce-v{v}", 0, v, 9,
                         produce_body(v, args.records_topic, topic_id, batch), produce_error))
    if "list-offsets" in apis:
        for v in range(1, 12):
            jobs.append((f"list-offsets-v{v}", 2, v, 6,
                         list_offsets_body(v, args.records_topic, -1), list_offsets_error))
    if "fetch" in apis:
        for v in range(4, 19):
            jobs.append((f"fetch-v{v}", 1, v, 12,
                         fetch_body(v, args.records_topic, topic_id, 1 << 20), fetch_error))
        # Fewer bytes than the partition holds: the broker cuts the last batch.
        jobs.append(("fetch-v18-cut", 1, 18, 12,
                     fetch_body(18, args.records_topic, topic_id, 300), fetch_error))

    with open(args.out, "a", encoding="ascii", newline="\n") as out:
        if "sasl" in apis:
            def addr(a):
                h, p = a.rsplit(":", 1)
                return h, int(p)
            run_sasl(sasl_jobs(addr(args.sasl_broker), addr(args.reauth_broker)), out)
        for name, key, v, first_flex, body, check in jobs:
            # 1000 + version, as when the first captures were made; each
            # request has a connection of its own, so APIs may share one.
            corr = 1000 + v + (100 if name.endswith("-cut") else 0)
            req = header(key, v, corr, v >= first_flex) + body
            rsp = exchange(host, port, req)
            (rcorr,) = struct.unpack(">i", rsp[:4])
            if rcorr != corr:
                raise SystemExit(f"{name}: correlation {rcorr}, expected {corr}")
            if check is not None:
                err = check(rsp, v)
                if err != 0:
                    raise SystemExit(f"{name}: error code {err}")
            for direction, data in (("request", req), ("response", rsp)):
                out.write(json.dumps({"name": name, "source": "hand-built",
                                      "dir": direction, "api_key": key, "api_version": v,
                                      "hex": data.hex()}) + "\n")
            print(f"{name}: {len(req)} -> {len(rsp)} bytes")


if __name__ == "__main__":
    main()
