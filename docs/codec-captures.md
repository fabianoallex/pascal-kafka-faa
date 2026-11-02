# Codec captures

The generated codecs are tested against bytes recorded from a real broker and from the Java
client (`docs/design.md`, section 6). This page says how those bytes were made, so they can be
made again for a new Kafka tag or a new API.

## Files

- `tests/data/captures-<tag>.jsonl`: one JSON object per frame, `name`, `source`, `dir`
  (`request` or `response`), `api_key`, `api_version` and `hex`, the frame without its 4-byte
  size prefix (header and body). This is the committed source of truth.
- `tests/Unit/Kafka.TestCaptures.pas`: the same, compiled into the tests, written by
  `python tools/gen_test_captures.py` (`--check` in `tools/check_generated.sh`). Compiled in so
  that the tests find their data from any output folder with no path to configure.

## How `captures-4.4.0.jsonl` was made (2026-11-02)

Broker: `docker compose -f docker/docker-compose.yml up -d` (`apache/kafka:4.4.0`), then a topic
of 3 partitions:

```sh
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic captured-topic --partitions 3
```

(Git Bash on Windows: `export MSYS_NO_PATHCONV=1` first, or the container paths get rewritten.)

**The Java client, newest versions.** `tools/capture_proxy.py` forwards a port to the broker and
records every frame; Java tools inside the broker's container connect through it:

```sh
python -I tools/capture_proxy.py --listen 19092 --target localhost:9092 --out .ci/cap.jsonl &
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server host.docker.internal:19092                       # connection 1
echo k:v | docker exec -i pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server host.docker.internal:19092 --topic captured-topic  # connection 3
python -I tools/captures_from_proxy.py --in .ci/cap.jsonl --conn 1 --prefix java-admin \
  --source "java kafka-broker-api-versions.sh 4.4.0 (admin client)" --out tests/data/captures-4.4.0.jsonl
python -I tools/captures_from_proxy.py --in .ci/cap.jsonl --conn 3 --prefix java-producer \
  --source "java kafka-console-producer.sh 4.4.0" --out tests/data/captures-4.4.0.jsonl
```

Only the bootstrap connection goes through the proxy: Metadata advertises `localhost:9092`, and
the Java client opens its next connections there directly. That connection holds ApiVersions
and Metadata, which is what the library generates today. `kafka-topics.sh --describe` was not
used: in Kafka 4 it describes topics with DescribeTopicPartitions (API 75), not Metadata.

**Every other version.** `tools/capture_versions.py` builds ApiVersions v0-v5 and Metadata v0-v13
requests by hand, from the protocol guide (not with the library's encoder), sends each on its
own connection and records request and response:

```sh
python -I tools/capture_versions.py --broker localhost:9092 --topic captured-topic \
  --out tests/data/captures-4.4.0.jsonl
python tools/gen_test_captures.py
```

The broker accepted all 20 requests (ApiVersions answered error 0; every Metadata answer
describes the topic with no error, which `Kafka.MessagesTests` checks).

## What the tests do with them

`Kafka.MessagesTests`: every capture decodes and encodes back to the same bytes (header and
body); the hand-built requests and the Java producer's Metadata request are rebuilt with the
generated records and must encode to the captured bytes; the decoded answers are checked field by
field (APIs and versions offered, features in tagged fields, the topic's id, partitions, leader).

The topic id in the captures (`gIIkYqBfQzW_O9gEpXKFuQ`) belongs to the broker the captures came
from; a new capture has a new id, and the tests that name it change with it.
