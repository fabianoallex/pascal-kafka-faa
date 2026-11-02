#!/usr/bin/env python3
"""Turns tools/capture_proxy.py's records into test captures.

Pairs each response of one recorded connection with its request (by
correlation id), keeps the APIs the library generates, and appends them to
--out in the format of tests/data/captures-<tag>.jsonl:

    {"name": "java-producer-metadata-v13", "source": "java console-producer 4.4.0",
     "dir": "request", "api_key": 3, "api_version": 13, "hex": "..."}

"hex" drops the 4-byte size prefix, as capture_versions.py does.

    python -I tools/captures_from_proxy.py --in .ci/cap.jsonl --conn 3 \\
        --prefix java-producer --source "java console-producer 4.4.0" --out FILE
"""

import argparse
import json
import struct

API_NAMES = {18: "api-versions", 3: "metadata"}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--conn", type=int, required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--source", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

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
            if key in API_NAMES:
                pairs.append((key, ver, req, content))

    seen = set()
    with open(args.out, "a", encoding="ascii") as out:
        for key, ver, req, rsp in pairs:
            name = f"{args.prefix}-{API_NAMES[key]}-v{ver}"
            if name in seen:
                continue  # the Java client repeats Metadata; one is enough
            seen.add(name)
            for direction, data in (("request", req), ("response", rsp)):
                out.write(json.dumps({"name": name, "source": args.source, "dir": direction,
                                      "api_key": key, "api_version": ver,
                                      "hex": data.hex()}) + "\n")
            print(f"{name}: {len(req)} -> {len(rsp)} bytes")


if __name__ == "__main__":
    main()
