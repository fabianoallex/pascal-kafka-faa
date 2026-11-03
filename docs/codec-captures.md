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

## Consumer groups: FindCoordinator, OffsetCommit, OffsetFetch, ConsumerGroupHeartbeat, ConsumerGroupDescribe (2026-11-02, same file)

**By hand, every version**, against the PLAINTEXT listener:

```sh
python -I tools/capture_versions.py --apis group --out tests/data/captures-4.4.0.jsonl
```

FindCoordinator v0-v6 for group `captured-group` (v4+ in the list form); OffsetCommit v2-v10,
a simple commit (no member, generation -1) of offset 100 + version on partition 0 of
`captured-records`, metadata `commit v<n>` (v10 names the topic by id); OffsetFetch v1-v10 of
it (each reads the last commit, 110); and for ConsumerGroupHeartbeat v0 and v1 a member
(`capture-member-v<n>`) joining group `captured-cgh-v<n>` subscribed to `captured-records`, the
group described with ConsumerGroupDescribe of the same version while the member is in it, and
the member leaving (epoch -1). 32 exchanges, every error code 0. A broker that just started
answers COORDINATOR_LOAD_IN_PROGRESS or COORDINATOR_NOT_AVAILABLE for a while: the script waits
those out (30 s) and records only the answer without error.

**The Java client in a KIP-848 group**, through the capture listener (the coordinator is
reached by its advertised address, so every connection must go through the proxy):

```sh
docker compose -f docker/docker-compose.yml -f docker/docker-compose.capture.yml up -d --wait
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic captured-group-java --partitions 2
printf 'k1\tone\nk2\ttwo\nk3\tthree\n' | docker exec -i pascal-kafka-faa-broker \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic captured-group-java --reader-property parse.key=true
python -I tools/capture_proxy.py --listen 19092 --target localhost:9095 --out .ci/cap-group.jsonl &
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server host.docker.internal:19092 --topic captured-group-java \
  --group captured-java-group --consumer-property group.protocol=consumer \
  --consumer-property auto.offset.reset=earliest --max-messages 3
python -I tools/captures_from_proxy.py --in .ci/cap-group.jsonl --conn 1 --apis find-coordinator \
  --prefix java-consumer-group \
  --source "java kafka-console-consumer.sh 4.4.0, --group, group.protocol=consumer" \
  --out tests/data/captures-4.4.0.jsonl
python -I tools/captures_from_proxy.py --in .ci/cap-group.jsonl --conn 3 \
  --apis offset-commit,offset-fetch,consumer-group-heartbeat --prefix java-consumer-group \
  --source "java kafka-console-consumer.sh 4.4.0, --group, group.protocol=consumer" \
  --out tests/data/captures-4.4.0.jsonl
python tools/gen_test_captures.py
docker compose -f docker/docker-compose.yml up -d --wait   # back to the tests' broker
```

Connection 3 was the coordinator's: OffsetFetch v10, ConsumerGroupHeartbeat v1 (thirteen of
them) and OffsetCommit v10 when the consumer closed. Of the heartbeats `captures_from_proxy.py`
keeps the first, the first answered without error and the last (`-first`, `-joined`, `-last`).
The broker had just been restarted with the capture listener: the consumer's FindCoordinator
was answered COORDINATOR_NOT_AVAILABLE (node -1) and its first ten heartbeats NOT_COORDINATOR,
until the coordinator had loaded. Those answers are kept on purpose: they are what a client
meets after a broker restart. The member id is the consumer's own (KIP-1082), and the broker
answers with it.

## The idempotent producer: InitProducerId and idempotent Produce (2026-11-03, same file)

**By hand**, against the PLAINTEXT listener:

```sh
python -I tools/capture_versions.py --apis producer-id --out tests/data/captures-4.4.0.jsonl
```

InitProducerId v0-v5, each on its own connection, as the Java producer asks (a null
transactional id, `TransactionTimeoutMs` 2147483647); then v5 again with the id and epoch the
first v5 got (`init-producer-id-v5-again`). v6 isn't recorded: 4.4.0 marks it unstable
(`latestVersionUnstable`), so the generator stops at v5. Then idempotent Produce v13 batches on
partition 0 of `captured-idempotent` (auto-created, new), each two records, with the id of the
second answer, on one connection; the answers, checked by the script:

| Exchange | Batch | Answer |
|---|---|---|
| `-first` | epoch 0, sequence 0 | offset 0 |
| `-retry` | the same bytes again | no error, offset 0 (not written twice) |
| `-next` | sequence 2 | offset 2 |
| `-gap` | sequence 10 (4 expected) | OUT_OF_ORDER_SEQUENCE_NUMBER (45) |
| `-new-epoch` | epoch 1, sequence 0 | offset 4 |
| `-old-epoch` | epoch 0, sequence 4 | INVALID_PRODUCER_EPOCH (47) |
| `-unknown-producer` | an id never given out, sequence 6 | offset 6 |
| `-evicted-retry` | `-new-epoch`'s batch again, after six more (not recorded) | OUT_OF_ORDER_SEQUENCE_NUMBER (45) |

What that says about a 4.4.0 broker: InitProducerId without a transactional id always gives a
new id with epoch 0, even when asked with an id and epoch; a retry among the last five batches
gets the first one's offset back; the epoch can be raised by the client alone, starting again
at sequence 0; a producer the broker doesn't know is taken at any sequence; and a retry older
than the five batches remembered is out of order, not DUPLICATE_SEQUENCE_NUMBER, which no answer
here carried. A topic that already holds batches gives other offsets: delete
`captured-idempotent` before capturing again.

**The Java console producer**, through the capture listener (its Produce goes to the advertised
address):

```sh
docker compose -f docker/docker-compose.yml -f docker/docker-compose.capture.yml up -d --wait
docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic captured-idempotent-java --partitions 1
python -I tools/capture_proxy.py --listen 19092 --target localhost:9095 --out .ci/cap-idem.jsonl &
printf 'k1\tone\nk2\ttwo\nk3\tthree\n' | docker exec -i pascal-kafka-faa-broker \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server host.docker.internal:19092 \
  --topic captured-idempotent-java --reader-property parse.key=true --producer-property linger.ms=200
python -I tools/captures_from_proxy.py --in .ci/cap-idem.jsonl --conn 1 --apis init-producer-id \
  --prefix java-idempotent --source "java kafka-console-producer.sh 4.4.0 (idempotent by default)" \
  --out .ci/try-1.jsonl      # renamed to java-idempotent-init-producer-id-v5-loading
python -I tools/captures_from_proxy.py --in .ci/cap-idem.jsonl --conn 2 --apis init-producer-id,produce \
  --prefix java-idempotent --source "java kafka-console-producer.sh 4.4.0 (idempotent by default)" \
  --out tests/data/captures-4.4.0.jsonl
python tools/gen_test_captures.py
docker compose -f docker/docker-compose.yml up -d --wait
```

The broker had just been restarted: the producer's first InitProducerId (connection 1, the
bootstrap) was answered COORDINATOR_LOAD_IN_PROGRESS with no id, and the producer asked again on
connection 2, its partition leader's, and got id 0, then sent its batch there (epoch 0,
sequence 0, acks all). Both answers are kept; the first one's name was given the `-loading`
suffix by hand, since both are v5 from the same tool. The Java request body is byte for byte the
hand-built v5.

## snappy and lz4: the Java producer's batches (2026-11-03, same file)

Through the capture listener, as the gzip batch above. Four records: the gzip capture's three
and a fourth, `size:long` / `k4` / `the long value: ` and `abcdefghij` 8000 times (80 016
bytes), so that Java writes more than one xerial chunk (32 KiB) and more than one lz4 block
(64 KiB). `batch.size=262144` keeps the four in one batch. One topic per codec, one partition.

```sh
docker compose -f docker/docker-compose.yml -f docker/docker-compose.capture.yml up -d --wait
python -c "
long = 'the long value: ' + 'abcdefghij' * 8000
with open('.ci/records.txt', 'w', encoding='utf-8', newline='\n') as f:
    f.write('trace:00-abc-01,lang:pt\tk1\tolá mundo\n')
    f.write('trace:00-def-01\tk2\tsecond value\n')
    f.write('empty:NULL\tNULL\tnull key, null header value\n')
    f.write('size:long\tk4\t' + long + '\n')
"
for c in snappy lz4; do
  docker exec pascal-kafka-faa-broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic captured-java-$c --partitions 1
  python -I tools/capture_proxy.py --listen 19092 --target localhost:9095 --out .ci/cap-$c.jsonl &
  docker exec -i pascal-kafka-faa-broker /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server host.docker.internal:19092 --topic captured-java-$c --compression-codec $c \
    --reader-property parse.key=true --reader-property parse.headers=true \
    --reader-property null.marker=NULL --producer-property linger.ms=200 \
    --producer-property batch.size=262144 < .ci/records.txt
  kill %1
  python -I tools/captures_from_proxy.py --in .ci/cap-$c.jsonl --conn 2 --apis produce \
    --prefix java-producer-$c --source "java kafka-console-producer.sh 4.4.0, --compression-codec $c" \
    --out tests/data/captures-4.4.0.jsonl
done
python tools/gen_test_captures.py
docker compose -f docker/docker-compose.yml up -d --wait
```

(The script that ran was a file in `.ci/`, gotcha 17: a heredoc would break the backslashes
above.) Connection 2 is the leader's, where the Produce went, in both. `kafka-dump-log.sh` on
the two topics: one batch of 4 records each, `compresscodec: snappy` (4 028 bytes) and
`compresscodec: lz4` (577 bytes), both `isvalid: true`. The snappy payload is xerial's header
and three chunks; the lz4 one a frame with FLG 60, BD 40, header checksum 82 and two blocks, no
checksums.

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

`Kafka.MessagesTests`: every capture, of the thirteen APIs, decodes and encodes back to the same
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

`Kafka.GroupMessagesTests`: the hand-built group requests rebuilt with the generated records
encode to the captured bytes; the answers decode to what was asked (the coordinator, every commit
accepted, every OffsetFetch version reading 110, the member's id, epoch, interval and
assignment, the group Stable with the uniform assignor, epoch -1 on leaving); Java's heartbeats
give ConsumerGroupHeartbeatResponse.Assignment null and present, the generator's first nullable
struct.

`Kafka.ProduceFetchTests` also decodes the Java snappy and lz4 batches to the four records
sent, checks their framing (xerial's header and 32 KiB chunks; the lz4 frame's descriptor and
blocks) and compresses them again with this library; `Kafka.ConsumerTests` polls them from the
fake broker as Java wrote them (`AppendRawBatch`).

`Kafka.ProducerIdMessagesTests`: the hand-built InitProducerId requests and idempotent Produce
requests (their batches with `TKafkaRecordBatch`) rebuilt from the generated records encode to
the captured bytes; the answers are the table above; Java's request is the hand-built v5, its
first answer COORDINATOR_LOAD_IN_PROGRESS, its batch carries the id of the second.

The topic ids in the captures belong to the broker they came from (the records' captures and
the group ones came from two brokers: the container is recreated between sessions); a new capture
has new ids. The tests read an id from a capture (`CapturedTopicId` in
`Kafka.GroupMessagesTests`) or name the one they need (`gIIkYqBfQzW_O9gEpXKFuQ`, the records'
topic, in `Kafka.MessagesTests`).
