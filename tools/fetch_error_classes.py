"""Writes tools/kafka-messages/<tag>/error-classes.tsv: for each error code of
Errors.java, its Java exception class and that class's superclasses, read from
Apache Kafka's sources at the same tag.

Why: whether a client retries after an error, and whether it refreshes
metadata first, is decided in Java by the exception's type
(RetriableException, InvalidMetadataException). gen_messages.py turns this
table into KafkaErrorIsRetriable and KafkaErrorIsInvalidMetadata, so the
build needs no network and the lists can't drift from Java's by hand.

Run it once per Kafka tag (it downloads ~140 small files from GitHub):

    python tools/fetch_error_classes.py
"""
import re
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gen_messages  # noqa: E402

TAG = gen_messages.KAFKA_TAG
RAW = f"https://raw.githubusercontent.com/apache/kafka/{TAG}/clients/src/main/java/"
PACKAGES = {
    "org.apache.kafka.common.errors": "org/apache/kafka/common/errors/",
    "org.apache.kafka.common": "org/apache/kafka/common/",
}
# The roots: the chain stops at the first of these.
ROOTS = {"ApiException", "KafkaException", "RuntimeException", "Exception"}

ENTRY_RE = re.compile(r"^\s{4}([A-Z_]+)\((-?\d+),", re.M)
NEW_RE = re.compile(r"(\w+)::new")
EXTENDS_RE = re.compile(r"\bclass\s+\w+(?:<[^>]*>)?\s+extends\s+([\w.]+)")
IMPORT_RE = re.compile(r"^import\s+([\w.]+)\.(\w+);", re.M)

_cache = {}


def fetch(path):
    if path not in _cache:
        with urllib.request.urlopen(RAW + path, timeout=30) as r:
            _cache[path] = r.read().decode("utf-8")
    return _cache[path]


def class_path(name, imports, package_dir):
    if name in imports:
        pkg = imports[name]
        if pkg not in PACKAGES:
            raise SystemExit(f"{name}: package {pkg} not handled")
        return PACKAGES[pkg] + name + ".java"
    return package_dir + name + ".java"


def chain(name, path):
    result = [name]
    while True:
        text = fetch(path)
        m = EXTENDS_RE.search(text)
        if not m:
            raise SystemExit(f"{path}: no 'extends'")
        parent = m.group(1).split(".")[-1]
        result.append(parent)
        if parent in ROOTS:
            return result
        imports = {c: p for p, c in IMPORT_RE.findall(text)}
        path = class_path(parent, imports, path.rsplit("/", 1)[0] + "/")


def main():
    errors_java = (gen_messages.DEFS / "Errors.java").read_text(encoding="utf-8")
    imports = {c: p for p, c in IMPORT_RE.findall(errors_java)}
    body = errors_java[errors_java.index("public enum Errors {"):]
    body = body[:body.index(";\n\n")]
    matches = list(ENTRY_RE.finditer(body))
    rows = []
    for i, m in enumerate(matches):
        chunk = body[m.end(): matches[i + 1].start() if i + 1 < len(matches) else len(body)]
        name, code = m.group(1), int(m.group(2))
        n = NEW_RE.search(chunk)
        if not n:
            rows.append((code, name, "-"))  # NONE
            continue
        cls = n.group(1)
        rows.append((code, name, ">".join(chain(cls, class_path(cls, imports, PACKAGES["org.apache.kafka.common.errors"])))))
        print(f"{code:4} {name}: {rows[-1][2]}")
    out = gen_messages.DEFS / "error-classes.tsv"
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"# Apache Kafka {TAG}: error code, name, exception class > superclasses.\n")
        f.write("# Written by tools/fetch_error_classes.py; read by tools/gen_messages.py.\n")
        for code, name, ch in rows:
            f.write(f"{code}\t{name}\t{ch}\n")
    print("wrote", out, len(rows), "codes")


if __name__ == "__main__":
    main()
