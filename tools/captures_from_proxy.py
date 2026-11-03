#!/usr/bin/env python3
"""Turns tools/capture_proxy.py's records into test captures.

Pairs each response of one recorded connection with its request (by
correlation id), keeps the APIs the library generates (or those --apis
names), and appends them to
--out in the format of tests/data/captures-<tag>.jsonl:

    {"name": "java-producer-metadata-v13", "source": "java console-producer 4.4.0",
     "dir": "request", "api_key": 3, "api_version": 13, "hex": "..."}

"hex" drops the 4-byte size prefix, as capture_versions.py does.

A repeated API keeps its first exchange (the Java client asks Metadata and
Fetch again and again), except SaslAuthenticate: SCRAM sends it twice in the
same version, and both steps are kept, named "-step1", "-step2". And
ConsumerGroupHeartbeat (KIP-848), sent every few seconds by a member: kept
are the first ("-first", which may be an error: a coordinator still loading
answers NOT_COORDINATOR), the first answered without error
("-joined", where the member gets its id, epoch and assignment) and the
last ("-last", which leaves when the consumer closed cleanly).

    python -I tools/captures_from_proxy.py --in .ci/cap.jsonl --conn 3 \\
        --prefix java-producer --source "java console-producer 4.4.0" --out FILE
"""

import argparse
import json
import struct

API_NAMES = {18: "api-versions", 3: "metadata", 0: "produce", 1: "fetch", 2: "list-offsets",
             17: "sasl-handshake", 36: "sasl-authenticate", 10: "find-coordinator",
             8: "offset-commit", 9: "offset-fetch", 68: "consumer-group-heartbeat",
             69: "consumer-group-describe"}
SASL_AUTHENTICATE = 36
CONSUMER_GROUP_HEARTBEAT = 68


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--conn", type=int, required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--apis", default=",".join(API_NAMES.values()),
                    help="the APIs to keep, by name (default: every one generated)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    keep = {k for k, n in API_NAMES.items() if n in args.apis.split(",")}

    requests = {}
    pairs = []
    for line in open(args.inp, encoding="ascii"):
        r = json.loads(line)
        if r["conn"] != args.conn:
            continue
        content = bytes.fromhex(r["hex"])[4:]
        if r["dir"] == "request":
            key, ver, corr = struct.unpack(">hhi", content[:8])
            requests[corr] = (key, ver, content)
        else:
            (corr,) = struct.unpack(">i", content[:4])
            key, ver, req = requests.pop(corr)
            if key in keep:
                pairs.append((key, ver, req, content))

    beats = [i for i, p in enumerate(pairs) if p[0] == CONSUMER_GROUP_HEARTBEAT]
    beat_names = {}
    if beats:
        beat_names[beats[-1]] = "-last"
        # The error code follows the correlation id, the header's tagged
        # fields (an empty count) and ThrottleTimeMs.
        ok = [i for i in beats if struct.unpack(">h", pairs[i][3][9:11])[0] == 0]
        if ok:
            beat_names[ok[0]] = "-joined"
        beat_names[beats[0]] = "-first"
    seen = set()
    steps = 0
    with open(args.out, "a", encoding="ascii", newline="\n") as out:
        for index, (key, ver, req, rsp) in enumerate(pairs):
            name = f"{args.prefix}-{API_NAMES[key]}-v{ver}"
            if key == CONSUMER_GROUP_HEARTBEAT:
                if index not in beat_names:
                    continue
                name += beat_names[index]
            if key == SASL_AUTHENTICATE:
                steps += 1
                name += f"-step{steps}"
            if name in seen:
                continue  # the Java client repeats Metadata and Fetch; the first is kept
            seen.add(name)
            for direction, data in (("request", req), ("response", rsp)):
                out.write(json.dumps({"name": name, "source": args.source, "dir": direction,
                                      "api_key": key, "api_version": ver,
                                      "hex": data.hex()}) + "\n")
            print(f"{name}: {len(req)} -> {len(rsp)} bytes")


if __name__ == "__main__":
    main()
