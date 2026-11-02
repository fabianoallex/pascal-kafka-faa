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

## Records: Produce, Fetch, ListOffsets (2026-11-02, same file)

**By hand, every version.** The same script, for the three APIs, against a topic of its own
(`captured-records`, auto-created; the script waits for partition 0's leader):

```sh
python -I tools/capture_versions.py --broker localhost:9092 \
  --apis produce,list-offsets,fetch --out tests/data/captures-4.4.0.jsonl
```

Produce v3-v13, one request each, each with a record batch of two records that the script
builds itself (its own CRC-32C; fixed timestamps from 2026-01-01T00:00:00Z, so a new capture has
the same requests): uncompressed up to v12, gzip from Python's `gzip` module (`mtime=0`) in v13.
Then ListOffsets v1-v11 (the end of partition 0: 22) and Fetch v4-v18 from offset 0, each answer
holding the eleven batches, and `fetch-v18-cut`, a Fetch with a 300-byte partition limit, which
the broker answers with two whole batches and 58 bytes of the third. Produce v13 and Fetch v13+
name the topic by id; the script reads it from a Metadata v12 answer it parses itself. It checks
every answer's error code and stops on one that isn't 0: all 38 were 0.

**The Java client, Produce and Fetch.** The proxy alone records only the bootstrap connection
(above). `docker/docker-compose.capture.yml` adds a listener, `CAPTURE` on 9095, advertised as
`host.docker.internal:19092`, where the proxy listens: a Java tool bootstrapped through the
proxy finds the proxy in Metadata too, and every connection it opens is recorded.

```sh
docker compose -f docker/docker-compose.yml -f docker/docker-compose.capture.yml up -d --wait
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic captured-java --partitions 1
python -I tools/capture_proxy.py --listen 19092 --target localhost:9095 --out .ci/cap2.jsonl &
printf 'trace:00-abc-01,lang:pt\tk1\tolá mundo\ntrace:00-def-01\tk2\tsecond value\nempty:NULL\tNULL\tnull key, null header value\n' |
  docker exec -i pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server host.docker.internal:19092 --topic captured-java --compression-codec gzip \
    --reader-property parse.key=true --reader-property parse.headers=true \
    --reader-property null.marker=NULL --producer-property linger.ms=200
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server host.docker.internal:19092 --topic captured-java \
  --partition 0 --offset earliest --max-messages 3
python -I tools/captures_from_proxy.py --in .ci/cap2.jsonl --conn 2 --apis produce \
  --prefix java-producer-gzip --source "java kafka-console-producer.sh 4.4.0, --compression-codec gzip" \
  --out tests/data/captures-4.4.0.jsonl
python -I tools/captures_from_proxy.py --in .ci/cap2.jsonl --conn 4 --apis list-offsets,fetch \
  --prefix java-consumer --source "java kafka-console-consumer.sh 4.4.0, --partition 0 --offset earliest" \
  --out tests/data/captures-4.4.0.jsonl
python tools/gen_test_captures.py
docker compose -f docker/docker-compose.yml up -d --wait   # back to the tests' broker
```

Connection numbers are the proxy's, in the order the tool opened them (the producer's
bootstrap was 1 and its Produce went on 2; the consumer's were 3 and 4); look them up in the
proxy's file before running `captures_from_proxy.py`. `--compression-codec` matters: the console
producer sets `compression.type` from it, over `--producer-property` (the first try recorded an
uncompressed batch). A topic of one partition, so the consumer reads everything from partition 0.

## SASL: SaslHandshake and SaslAuthenticate (2026-11-02, same file)

The broker of `docker/docker-compose.yml` with its SASL listeners (9096 SASL_PLAINTEXT, 9098
the same with 2-second sessions). Users: PLAIN alice / alice-secret, SCRAM-SHA-256 bob /
bob-secret (8192 iterations).

**By hand, every version.** SASL needs several requests on one connection, so the script has
exchanges of its own for it:

```sh
python -I tools/capture_versions.py --apis sasl --out tests/data/captures-4.4.0.jsonl
```

SaslHandshake v0 and v1 (PLAIN; v0 alone on its connection, since v0's tokens would go
unframed), SaslAuthenticate v0, v1 and v2 with alice's PLAIN token, v1 and v2 again on 9098
(`-lifetime`: the answer carries SessionLifetimeMs 2000), `sasl-handshake-v1-unsupported`
(GSSAPI: error 33 and the enabled list) and `sasl-authenticate-v2-failed` (a wrong password:
error 58 and the broker's message). The script checks each expected error code; all 9 matched.

**The Java client, PLAIN and SCRAM.** SASL runs on every connection, the bootstrap one included,
so the proxy alone records it, with no capture listener:

```sh
python -I tools/capture_proxy.py --listen 19093 --target localhost:9096 --out .ci/cap-sasl.jsonl &
docker exec pascal-kafka-faa-broker sh -c '
  printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-secret\";\n" > /tmp/plain.properties
  printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bob\" password=\"bob-secret\";\n" > /tmp/scram.properties
  for c in plain scram; do
    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server host.docker.internal:19093 \
      --command-config /tmp/$c.properties
  done'
python -I tools/captures_from_proxy.py --in .ci/cap-sasl.jsonl --conn 1 \
  --apis sasl-handshake,sasl-authenticate --prefix java-plain \
  --source "java kafka-broker-api-versions.sh 4.4.0, SASL PLAIN" --out tests/data/captures-4.4.0.jsonl
python -I tools/captures_from_proxy.py --in .ci/cap-sasl.jsonl --conn 2 \
  --apis sasl-handshake,sasl-authenticate --prefix java-scram \
  --source "java kafka-broker-api-versions.sh 4.4.0, SASL SCRAM-SHA-256" --out tests/data/captures-4.4.0.jsonl
python tools/gen_test_captures.py
```

SaslAuthenticate is kept every time it repeats, numbered (`-step1`, `-step2`): SCRAM sends it
twice at the same version. The Java client sent ApiVersions v0, SaslHandshake v1,
SaslAuthenticate v2 (once for PLAIN, twice for SCRAM), then ApiVersions v5 and Metadata.

## Partitions: the Java producer's choice for each key (2026-11-02)

`tests/data/partitions-java-4.4.0.tsv`: 70 keys and the partition the Java console producer
4.4.0 sent each to, on topics of 1, 3, 12 and 100 partitions. The keys: `key-0` to `key-59`,
Kafka's six murmur2 vectors, `São Paulo`, `ÿ€漢字 ok`, 300 `x`, `order:123/item:7` (one per line
of `.ci/keys.txt`, as `key|key`).

```sh
for n in 1 3 12 100; do
  docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic partitions-$n --partitions $n
  docker exec -i pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic partitions-$n \
    --reader-property parse.key=true --reader-property key.separator='|' < .ci/keys.txt
  docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic partitions-$n --from-beginning --max-messages 70 \
    --property print.partition=true --property print.key=true --property print.value=false \
    > .ci/part-$n.txt
done
```

The four outputs became the table (key, then the partition of 1, 3, 12, 100). Before it was
kept, an independent Python port of murmur2 (Kafka's `Utils.murmur2`, which also passes Kafka's
own six vectors) gave the same partition for all 280 cases: the console producer sent the keys
as UTF-8, and the table records Java's rule, `toPositive(murmur2(key)) % partitions`. Then
`python tools/gen_test_captures.py` writes `Kafka.TestPartitions` (each key as the hex of its
UTF-8 bytes).

## What the tests do with them

`Kafka.MessagesTests`: every capture, of the seven APIs, decodes and encodes back to the same
bytes (header and body); the hand-built ApiVersions and Metadata requests and the Java producer's
Metadata request are rebuilt with the generated records and must encode to the captured bytes;
the decoded answers are checked field by field (APIs and versions offered, features in tagged
fields, the topic's id, partitions, leader).

`Kafka.ProduceFetchTests`: the hand-built Produce, ListOffsets and Fetch requests rebuilt with
the generated records, and Produce's batches with `TKafkaRecordBatch`, encode to the captured
bytes (the uncompressed batches byte for byte; v13's gzip batch by its records); each Produce
answer at the next offset; every Fetch holds the eleven batches; the cut Fetch keeps two; the
Java gzip batch fetched back is the one sent. `Kafka.RecordBatchTests` reads the broker's
batches back to the same bytes. `Kafka.PartitionerTests` checks every key of the partitions
table on the four partition counts. `Kafka.SaslTests`: the hand-built SASL requests rebuilt with
the generated records encode to the captured bytes, the answers decode (mechanisms, lifetimes,
errors); the PLAIN token is Java's; and with the nonce read from Java's SCRAM client-first, the
library's client writes Java's client-first and client-final, and the broker's server-final
verifies.

The topic id in the captures (`gIIkYqBfQzW_O9gEpXKFuQ`) belongs to the broker the captures came
from; a new capture has a new id, and the tests that name it change with it.
