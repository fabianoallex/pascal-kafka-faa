# pascal-kafka-faa — design

Status: **K0 done; K1 started** (its foundation, 2026-11-02: "K1 progress" below). From the
feasibility review of 2026-11-01. The user's decisions the same day: the transport moves to
pascal-common-faa (1), no embedded broker for now (2), and the recommendations of this document
accepted (3 and 4, and the transport's T1–T8).

Everything in this repository is in **English**: code, comments, runtime messages, tests,
documentation and commit messages.

This document keeps the **why** and the order of the work. Once the library exists, `CLAUDE.md`
keeps the **what** (short rules) and `README.md` the **how to use**, as in pascal-redis-faa.

---

## 1. Why a native library

A Kafka client for Delphi **and** FPC with no external dependency, under the family's rules:
`{$MODE DELPHI}`, one `src/`, mirrored DUnitX + FPCUnit tests, 0 leaks on both sides, native
TLS (SChannel) or OpenSSL.

What exists today (searched 2026-11-01):

- librdkafka wrappers for Delphi (norgepaul/DelphiKafkaClient and forks; HeZiHang/Delphi-Kafka,
  untouched since 2018). The first calls itself a proof of concept never used in production.
- One commercial native client (eSeGeCe), Delphi only.
- Nothing maintained for FPC.

**Why not wrap librdkafka:** it is complete, but it brings a DLL/.so to ship to every machine
(the family has no mandatory dependency), callbacks called from C threads into Pascal code (on
FPC the thread has to be known to the RTL), and no path to Android. It stays plan B if the
native protocol turns out too expensive; it isn't the direction.

## 2. What Kafka 4 makes simpler

- **KIP-896** (Kafka 4.0) removed the API versions older than Kafka 2.1; a 4.x broker answers
  `UNSUPPORTED_VERSION` to them. For a client this means **only record batch v2** (magic 2, no
  v0/v1 message formats) and only the recent versions of each API.
- **KIP-848** (GA in 4.0, on by default on the server since 4.0) moves partition assignment to
  the broker. The client sends `ConsumerGroupHeartbeat` and receives its partitions: no
  JoinGroup/SyncGroup, no client-side assignor, no eager rebalance. The most complex part of a
  classic Kafka client is gone.
- **KRaft** (no ZooKeeper): a one-node container is enough for tests.

## 3. Decisions

### Decision 1 — the transport moves to pascal-common-faa (taken)

The TCP + TLS layer exists today in three copies (amqp, redis — a copy of amqp's — and pipes,
written separately). Kafka would be the fourth. Under pascal-common-faa's rule ("what at least
two libraries use") it belongs there.

Its design is `../pascal-common-faa/docs/transport-design.md` (decisions T1–T8, accepted on
2026-11-01). Comparing the copies showed the right base is **pipes'**, not amqp's: only it has
a private CA, client certificates (mTLS) and hostname verification, which Kafka needs. The TLS
backend is chosen at run time, with no define. The libraries move one at a time (pipes, redis,
then amqp), and Kafka is written on it from the start. **Order:** the extraction is phase K0
below; Kafka doesn't start before it.

Gotchas the transport carries, already measured in the siblings (`rtl-gotchas.md` in the
`dual-compiler-delphi-lazarus` skill, "Sockets / timeout"): Delphi's socket timeouts applied
only after connecting, `TCP_NODELAY` (~40 ms per round trip on Linux without it, invisible on
Windows loopback), `MSG_NOSIGNAL`/SIGPIPE, OpenSSL 3's default provider on Debian 12.

### Decision 2 — no embedded broker (taken)

pascal-amqp-faa's embedded broker is feasible because AMQP 0-9-1 is small on the server side.
A Kafka broker (segmented log, KRaft controller, group coordinator, replication) is a project
as large as the client or larger. Tests use `apache/kafka` in Docker (KRaft, one node).
Reconsider after the client ships.

### Decision 3 — generate the codec from Kafka's JSON message definitions (accepted)

Apache Kafka publishes every request and response as JSON
(`clients/src/main/resources/common/message/*.json`): fields, types, the versions each field
exists in, the flexible versions (compact strings and arrays, tagged fields, KIP-482) and
defaults. A Python generator (`tools/gen_messages.py`, like pascal-common-faa's
`gen_tzdata.py`) writes Pascal units with one record per message and the `Encode`/`Decode` of
each supported version.

Why generate: about 20 APIs with several versions each, and hand-written codecs are where byte
errors come in that only show against a broker. The generator also states exactly which
versions the code supports, and `ApiVersions` negotiation uses that table. The JSON definitions
are committed at a fixed Kafka tag (noted), so the build doesn't need the network.

Hand-written, because the JSON doesn't describe them: record batch v2 (header, CRC32C, varints,
record headers, compression), the primitives (big-endian, varint/zigzag, `UNSIGNED_VARINT`,
`UUID`) and the request envelope (size, `api_key`, `api_version`, `correlation_id`,
`client_id`).

### Decision 4 — Kafka 4.x first, with the seam ready for 3.x (accepted)

Adding 3.x later **splits** the work rather than adding to it, provided the consumer's public
API is designed now without depending on KIP-848:

- **The data path works on 3.x for free.** Produce, Fetch, Metadata and ListOffsets in their
  post-2.1 versions exist on 3.x brokers too; the client takes the highest version both sides
  know (the `ApiVersions` table) and the same code serves both. For the producer and for a
  consumer with manually assigned partitions, 3.x costs nothing.
- **Only the consumer group differs.** 3.x has no `ConsumerGroupHeartbeat`: it's the classic
  protocol (`FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`) with an
  assignor on the group leader (cooperative-sticky, or range to start). That is **new, separate
  code**, not a rewrite of what KIP-848 uses.

The seam: an internal group-membership interface with two implementations.

```
IKafkaGroupMembership      Join, Poll(deadline), Commit, Leave; reports assign/revoke
  TKafkaConsumerProtocol   KIP-848 (phase K2)
  TKafkaClassicProtocol    JoinGroup/SyncGroup + assignor (later, if needed)
```

What the consumer's public API guarantees **from the start**, so the classic protocol fits
later without breaking users:

1. **Incremental** assignment events (`OnPartitionsAssigned`, `OnPartitionsRevoked`,
   `OnPartitionsLost`), never "here is the whole new list". KIP-848 is incremental already, and
   so is the classic protocol with cooperative-sticky; an eager API would break on the change.
2. A `GroupProtocol` option (`consumer` | `classic`), as in the Java client, present with one
   accepted value. Asking for `classic` before it exists raises an error that says so.
3. No option that only makes sense in one of the two exposed as required:
   `session.timeout`/`heartbeat.interval` belong to the server under KIP-848 and to the client
   under the classic protocol.

What waiting doesn't save: the classic assignor is the largest piece (cooperative-sticky is
delicate) and costs the same now or later. Estimated extra, from the siblings' sizes: 1 500 to
2 500 lines plus the rebalance test suite.

**Checked 2026-11-02** (vendor docs, not tested): Confluent Cloud has KIP-848 GA; Amazon MSK
has it on clusters of 4.0 or later, opt-in on the client. Nothing found for Redpanda (its docs
don't mention it) or Azure Event Hubs (documented for clients from 1.0 on, no transactions).
A first user on either of those two would move the classic protocol up to K2; it can be
checked against Redpanda in a container before then. The chosen brokers are in open
question 1.

## 4. Architecture

Prefix `Kafka.`, include `src/kafka.inc`, platform define `KAFKA_WINDOWS`. The shape follows
redis and amqp, but Kafka has one difference that changes the design, as redis's design
explained against amqp's:

**A Kafka client is "smart".** It doesn't talk to *a* server but to the cluster: it discovers
the brokers through `Metadata`, keeps one connection per broker, sends each produce or fetch to
the partition's leader, and refreshes metadata on `NOT_LEADER_OR_FOLLOWER`,
`UNKNOWN_TOPIC_OR_PARTITION` and similar. One connection carries several requests in flight,
matched by `correlation_id` (like AMQP's channels, unlike Redis's strict order).

```
Kafka.Protocol.*        primitives, envelope, record batch v2, CRC32C (hand-written)
Kafka.Messages.*        generated requests/responses (decision 3)
Kafka.Compression       none, gzip; snappy/lz4/zstd in later phases
Kafka.Sasl              PLAIN, SCRAM-SHA-256/512, OAUTHBEARER (token supplied)
Kafka.Connection        one broker: common's endpoint, reader thread, correlation, ApiVersions, SASL
Kafka.Cluster           bootstrap, metadata, leaders, connections per broker, refresh
Kafka.Producer          per-partition accumulator, linger, sender thread, acks, retries
Kafka.Partitioner       murmur2 compatible with Java; sticky without a key
Kafka.Consumer          manual assign, Poll, offsets; groups through IKafkaGroupMembership
Kafka.Group.Consumer    KIP-848
Kafka.Admin             CreateTopics, DeleteTopics, DescribeCluster (what the tests need)
Kafka.Tracing           PascalCommon.Tracing spans; traceparent in record headers
```

### Threads

- **Connection:** one reader thread per broker, handing each response to whoever waits for that
  `correlation_id` (common's `TPcMonitor`); writers write under a lock. The socket has no read
  deadline while idle (the reader waits indefinitely); request timeouts live in the correlation
  table.
- **Producer:** `Send` only queues into the accumulator and returns. A sender thread groups
  batches per broker and sends them. Each record's result (offset or error) goes to an
  `of object` callback or an `IKafkaDeliveryFuture` one can wait on. No closures on FPC (the
  skill's table).
- **Consumer:** `Poll(timeout)` runs on the caller's thread, as in Java. The group heartbeat
  runs on its own thread, so slow processing between two `Poll`s doesn't drop the member.
  Assign/revoke events are delivered **inside `Poll`**, on the user's thread, never on the
  heartbeat thread (the same care as redis's pub/sub).
- Nothing queued on the shared `PcPool` (as redis and amqp). Shutdown: `Close` stops the threads
  in the right order (consumer: Leave, then the connections; producer: flush with a deadline,
  then the connections). The `using-faa-libraries` skill lists getting this wrong as the cause
  of shutdown hangs.

### Dual-compiler points already known

From the `dual-compiler-delphi-lazarus` skill and the siblings; each goes into the library's
`CLAUDE.md`:

- **32-bit arithmetic with intentional overflow** (murmur2, CRC32C, zigzag): `{$Q-}{$R-}` in the
  unit (Delphi's Debug configuration turns both on) and intermediates in `Cardinal`/`UInt64`.
  `shr` on a signed value is not Java's `>>>`: murmur2 uses `Cardinal`. Proven by a test with
  keys and partitions produced by the Java client (section 6).
- **Payloads are `TBytes`, never `string`.** Key, value and headers are bytes. Conversion only at
  the edges, with `PcStringToUtf8Bytes`/`PcTryUtf8BytesToString`, never `TEncoding.UTF8`, which
  raises on invalid bytes on Delphi.
- **Big-endian integers and `Int64`:** our own functions, no `Swap`/`SwapEndian` (names and types
  differ between the RTLs).
- **gzip:** `System.ZLib` (Delphi) and `paszlib`/`zstream` (FPC), as `Pipes.Compression`. Kafka
  uses **gzip** (RFC 1952 header), not raw zlib: check on both compilers, both directions,
  against a record compressed by the Java client.
- **Source:** UTF-8 with BOM, LF, everything in English.
- **Linux early:** `tools/test_fpc_docker.sh` from phase K1, not at the end.

## 5. Features: what and when

| Item | Phase | Note |
|---|---|---|
| ApiVersions, Metadata, Produce, Fetch, ListOffsets | K1 | highest common version negotiated |
| acks 0/1/all, retries, linger, batch per partition | K1 | |
| murmur2 partitioner (Java) + sticky without a key | K1 | key compatibility is a requirement |
| Consumer with manually assigned partitions | K1 | `Assign`, `Seek`, `Poll` |
| Record headers | K1 | tracing needs them |
| TLS (SChannel/OpenSSL, from common), private CA, mTLS | K1 | |
| SASL PLAIN, SCRAM-SHA-256 | K1 | PLAIN covers Confluent Cloud; SCRAM uses `PcHmacSha256`/`PcPbkdf2HmacSha256` |
| Compression none + gzip | K1 | |
| Minimal admin (create and delete topics) | K1/K2 | for the integration tests |
| KIP-848 consumer group, OffsetCommit/OffsetFetch | K2 | decision 4's seam |
| Auto-commit and manual commit (sync and async) | K2 | |
| Idempotent producer (InitProducerId, sequences) | K3 | Java's default since 3.0 |
| snappy and lz4 (pure Pascal) | K3 | a consumer must read whatever any producer sent |
| zstd | K3 | large decoder: opt-in libzstd, loaded dynamically like OpenSSL |
| SCRAM-SHA-512 | K3 | needs SHA-512 in common (a new item there) |
| OAUTHBEARER (token supplied by the application) | K3 | no OIDC flow |
| Spans and metrics | K3 | traceparent in headers, OpenTelemetry `messaging.*` conventions |
| Transactions (exactly-once) | K4 | |
| Outbox (pascal-db-faa) | K4 | the same pattern as `amqp/outbox` |
| Classic group protocol (3.x brokers) | when a user needs it | decision 4 |
| GSSAPI/Kerberos | out | no plan |
| Embedded broker | out for now | decision 2 |

## 6. Tests

- **Mirrored unit tests from the first commit:** the skill's `check_test_kit.py --install Kafka`
  (the `gen_fpc_mirror.py` generator and the DUnitXCompat adapter), identical bodies on both
  sides, `tools/gen_fpc_mirror.py --check` in CI.
- **Codec against real bytes:** requests and responses captured from a broker and the Java
  client, for each supported API and version, kept as a data file (like the time zone goldens).
  Round trip `Decode(Encode(x)) = x` for every generated version.
- **CRC32C, murmur2, varint:** published vectors (RFC 3720 for CRC32C) and a key → partition
  table from Java's `DefaultPartitioner` for 1, 3, 12 and 100 partitions.
- **Integration:** `docker/` with `apache/kafka` KRaft one node; another compose with TLS and
  SASL (PLAIN and SCRAM). Failure scenarios as in redis: broker restarted during a produce,
  leader change (needs 3 nodes, a separate compose), socket timeout, a member leaving the
  group, a rebalance with two consumers.
- **Interoperability:** produce with the library and consume with `kcat`/confluent-kafka-python,
  and the reverse, comparing key, partition, headers, timestamp and compression. This is what
  proves murmur2 and gzip match the rest of the world.
- **Each phase's bar:** FPC Windows x64 + Linux (Docker) + Delphi 12 Win32/Win64 green, 0 leaks
  (heaptrc and FastMM).

## 7. Phases

| Phase | Content | Done when |
|---|---|---|
| **K0** | the transport in pascal-common-faa (decision 1; its migration order) | common releases the transport with pipes and redis running on it |
| **K1** | protocol, generator, connection, cluster, producer, manual consumer, TLS, SASL PLAIN/SCRAM-256, gzip | a sample produces and consumes against Kafka 4.x over TLS + SASL, and against Confluent Cloud (SASL PLAIN over TLS); interoperability tests green |
| **K2** | KIP-848 consumer group, commits, incremental event API | two consumers share a topic and rebalance without loss, and with no duplicates beyond at-least-once |
| **K3** | idempotence, snappy/lz4/zstd, SCRAM-512, OAUTHBEARER, tracing and metrics | consumes what Java produces with each codec; idempotent producer with no duplicate after a retry |
| **K4** | transactions, outbox | on demand |

Estimated size from the siblings (`src` of redis ~11 600 lines, amqp ~24 000): K1 + K2 between
12 000 and 15 000 lines, generated code not counted.

## K1 progress

### The foundation (2026-11-02)

The session's scope, as the user set it: the repository, the protocol primitives, the generator
with ApiVersions and Metadata only, the connection, and a milestone against a real broker.

**Done:**

- Repository: dual-compiler skeleton (`src/kafka.inc` with `KAFKA_WINDOWS`, `Kafka.groupproj`
  and `Kafka.lpg`, `packages/pascal_kafka_faa.lpk`, DUnitX + FPCUnit runners), the skill's test
  kit (`check_test_kit.py --install Kafka`), `CLAUDE.md`, `CHANGELOG.md`. pascal-common-faa as a
  submodule at `v1.27.1`; `Kafka.Types` and the `.lpk` require 1.27.1. Published at
  https://github.com/fabianoallex/pascal-kafka-faa (MIT), CI on FPC Linux.
- `Kafka.Protocol.*` by hand (decision 3): primitives, CRC32C, the envelope.
- `tools/gen_messages.py` from Kafka 4.4.0's JSON: ApiVersions v0-v5, Metadata v0-v13, the API
  table, and `Kafka.Errors` from `Errors.java`.
- `Kafka.Connection` over `PcTcpConnect`/`PcTlsWrap`, as section 4 "Threads" planned: reader
  thread, a correlation table with any number of requests in flight, writers under a lock,
  request timeouts in the table rather than on the socket. `Kafka.Transport` is the T6
  boundary.
- **Milestone reached** against `apache/kafka:4.4.0` (KRaft, one node, `docker/`): connect,
  negotiate ApiVersions (v5; Metadata v13) and read the Metadata of a topic, in plaintext and
  over TLS with the common's test CA, on SChannel and OpenSSL (Windows) and OpenSSL 3 (Linux).
- Codec tested against real bytes: 48 frames (`docs/codec-captures.md`), from the Java client
  4.4.0 and from hand-built requests of every version, all accepted by the broker. Each decodes
  and encodes back to the same bytes.

**Measured:** FPC 3.2.2 Windows x64: 91 unit and 8 integration tests, 0 leaks. FPC 3.2.2 Linux
(Debian 12, OpenSSL 3, Docker): the same, 0 leaks; the unit suite also 15 times in three
containers at once with `--cpus=1`. Delphi 12 Win32 and Win64 (built in the IDE; CE doesn't
build from the command line): 91 unit and 8 integration tests, 0 leaks, SChannel and OpenSSL.

**Decided while writing, not in the plan before:**

- **The connection reads the endpoint directly**, not through a `TPcEndpointStream` subclass as
  amqp did. Frames are length-prefixed and read whole, and `TStream.ReadBuffer`'s behaviour on a
  short read is an RTL detail; `KafkaReadExactly` loops on its own. The boundary is still one
  place (`Kafka.Transport`).
- **`EKafkaTls` descends from `EKafkaConnection`** (amqp and redis keep TLS apart, for catch
  blocks older than their TLS; Kafka has none).
- **A request timeout doesn't close the connection** (Java's client does). It raises
  `EKafkaTimeout`, forgets the correlation id and discards a late answer. Whether to drop the
  connection is the cluster layer's call, which knows what else shares it.
- **A response above `MaxResponseBytes` (100 MiB) breaks the connection** instead of being
  allocated. A plaintext client on a TLS port lands here (the broker's alert reads as 352 MB),
  and the message says the bytes look like TLS.
- **Generated records** carry `<Field>IsNull` for nullable fields and `UnknownTaggedFields`
  everywhere; a non-default value that a version can't carry raises
  `EKafkaUnsupportedVersion`, as Java's `UnsupportedVersionException`. The generator refuses
  what it doesn't support yet (`records`, nullable or tagged structs).

### Records: Produce, Fetch, ListOffsets, record batch v2, gzip (2026-11-02)

**Done:**

- The generator takes the `records` type (raw nullable bytes, null by default, as Java's) and
  tagged struct fields (written when they differ from a fresh `Init`, Java's `equals` against
  `new X()`: every record now has `IsDefault`). Generated: Produce v3-v13, Fetch v4-v18,
  ListOffsets v1-v11. Nullable structs are still refused; no API so far has one.
- `Kafka.Protocol.RecordBatch` by hand: record batch v2 with its CRC32C, records with their
  deltas kept (decode then encode gives the same bytes) and the absolute offset and timestamp
  filled in (LogAppendTime as Java: the batch's MaxTimestamp), headers, null against empty key
  and value. Reading a Fetch's records skips a last batch the broker cut short at the byte
  limit, as Java's iterator does. Magic 0 and 1 are refused (Kafka 4 dropped them, KIP-724).
- `Kafka.Compression`: none and gzip. RFC 1952 framing written and read here, over raw deflate
  from `System.ZLib` (Delphi) and paszlib's `ZStream` (FPC), so the gzip detail is the same
  code on both; trailer checked (CRC-32 and size), output bounded while it comes out (a zip
  bomb fails without allocating). snappy, lz4 and zstd raise `EKafkaCompression` until K3.
- `TKafkaConnection.Produce`, `Fetch` and `ListOffsets`. Produce with acks=0 goes out through
  `SendOneWay` (a correlation id, no entry in the table: the broker never answers it). Fetch
  waits `RequestTimeoutMs + MaxWaitMs`, as Java's client.
- Captures: 130 frames (from 48). By hand, every version of the three new requests, with
  Python's own record batches (uncompressed, and gzip from Python's `gzip` for Produce v13), all
  accepted by the broker, and a Fetch the broker cut inside a batch. From Java, through a
  listener that advertises the capture proxy (`docker/docker-compose.capture.yml`), so Produce
  and Fetch were recorded too and not only the bootstrap connection: the console producer's
  gzip batch (idempotent, keys, headers, a null key, a null header value, `olá`) and the console
  consumer's ListOffsets and Fetch of it.

**Checked against the captures:** the generated requests encode to the hand-built bytes, and
the batches `TKafkaRecordBatch` writes are byte for byte Python's; every Fetch answer decodes
to the eleven batches produced; the Java batch the consumer fetched is the one the producer
sent, the same bytes except the partition leader epoch, which the broker sets and the CRC
doesn't cover. Integration: a batch produced by the library and fetched back, in plaintext
and gzip over TLS; acks=0 found by a long-polling Fetch (no `Sleep`).

**Measured:** FPC 3.2.2 Windows x64: 121 unit and 11 integration tests, 0 leaks. Linux (Docker):
the same, 0 leaks, and both suites 5 times in three containers at once with `--cpus=1` (30
runs). Delphi 12 Win32 and Win64: 121 unit and 11 integration tests, 0 leaks.

**Decided while writing:**

- **`EKafkaCompression`** under `EKafka`: a codec the library doesn't have, or data that
  doesn't decompress. Not under `EKafkaProtocol`: the bytes followed the protocol.
- **One gzip member.** Java, librdkafka and the Go clients write one; a second member fails the
  trailer check with a message that names the possibility, rather than being read.
- **A header key that isn't UTF-8 raises `EKafkaProtocol`**, as every string in this codec.
  Java decodes such a key with replacement characters; a consumer that needs to read past such a
  record will need an option (not before someone asks).

### The cluster and the partitioner (2026-11-02)

**Done:**

- `Kafka.Partitioner`: murmur2 and `toPositive(murmur2(key)) % n`, Java's
  `BuiltInPartitioner.partitionForKey`. Checked against Kafka's six murmur2 vectors, edge
  lengths and high bytes, and a table the Java console producer made: 70 keys (accents, CJK, a
  300-byte key) on 1, 3, 12 and 100 partitions, all 280 the same. A record without a key isn't
  hashed: the sticky choice belongs to the producer.
- `Kafka.Cluster`: the bootstrap (the first address that answers; any failure moves to the
  next), an immutable metadata snapshot swapped whole on each refresh, only the tracked topics
  asked for, refreshed when older than `MetadataMaxAgeMs` or after `Invalidate`, one refresh at
  a time, the known brokers asked before the bootstrap addresses. `Topic` and `Leader` wait up
  to `MaxBlockMs` on an event that `Close` sets. One connection per broker, opened on demand,
  handed out as an interface (`IKafkaBrokerConnection`) so that replacing a broken one or
  closing the cluster never frees an object a caller is using.
  `KafkaErrorInvalidatesMetadata`: the codes whose Java exception is an
  `InvalidMetadataException`, checked class by class in the 4.4.0 sources. *Corrected in the
  producer step:* that hand check missed four codes (13 NETWORK_EXCEPTION, 80, 83, 84); the
  list is now generated (`KafkaErrorIsInvalidMetadata`, below).

**Measured:** FPC 3.2.2 Windows x64: 141 unit and 15 integration tests, 0 leaks. Linux (Docker):
the same, and both suites 5 times in three containers at once with `--cpus=1`. Delphi 12
Win32 and Win64: 141 and 15, 0 leaks.

**Decided while writing:**

- **The bootstrap connection is closed** after its Metadata, and brokers are reached by the
  addresses they advertise (Java keeps bootstrap nodes as separate, negative-id nodes; the
  effect is the same, one connection more for a moment).
- **No reconnect backoff in the cluster** for now: the producer's and consumer's retry backoff
  will space the attempts. Connections are opened one at a time (a lock), which serializes
  connecting to two different brokers; measure before changing it.
- **Brokers that leave the metadata keep their connection** until it breaks or the cluster
  closes. A cluster that shrinks is rare and the cost is a socket.

### The producer (2026-11-02)

**Done:**

- `Kafka.Producer`: `Send` queues and returns a future (`IKafkaDeliveryFuture`), with an
  optional `of object` callback and a tag; batches per partition (`BatchSize`, `LingerMs`); a
  sender thread that groups ready batches by leader into Produce requests (`MaxRequestSize`,
  `MaxInFlightPerBroker`); acks all, 1 and 0; retries of retriable errors after
  `RetryBackoffMs`, with a metadata refresh first when the error calls for one, bounded by
  `Retries` and `DeliveryTimeoutMs`; a request with no answer abandoned after the request timeout
  and sent again; batches that wait too long expire (Java's "Expiring N record(s)");
  `BufferMemory` and `MaxBlockMs` bound what `Send` queues and how long it may block; `Flush`;
  `Close` with a timeout; gzip. Java's parameter names and defaults
  (`KafkaProducerParams`).
- `TKafkaStickyPartitioner` (`Kafka.Partitioner`): records without a key go to one partition
  until `BatchSize` bytes went there, then to another chosen at random among those with a
  leader (KIP-480 and KIP-794).
- `Kafka.Connection`: `Send` can fire a signal (`IKafkaSignal`) when the future is done, so the
  sender waits on one event for every request in flight.
- `KafkaErrorIsRetriable` and `KafkaErrorIsInvalidMetadata`, generated in `Kafka.Errors` from
  `tools/kafka-messages/4.4.0/error-classes.tsv`: each code's Java exception and its
  superclasses, read from the 4.4.0 sources by `tools/fetch_error_classes.py`. The cluster uses
  the generated one in place of its hand-written list.

**Measured:** FPC 3.2.2 Windows x64: 167 unit and 18 integration tests, 0 leaks. Linux (Docker):
the same, and both suites 5 times in three containers at once with `--cpus=1`. Delphi 12
Win32 and Win64: 167 and 18, 0 leaks. Two mutations (no muting; a retried batch put at the tail) each
make the order test fail. Throughput, 20 000 records of 100 bytes, acks=all, heaptrc on:
0.79 s on Linux with the broker on the same network namespace; 3 s on Windows through Docker
Desktop's port forwarding, where a lone record also waits for the 15.6 ms timer tick (a 5 ms
linger rounds up).

**Decided while writing:**

- **One batch in flight per partition** (Java does the same without idempotence, by muting the
  partition): a retry never reorders a partition's records. Requests to one broker still go
  several at a time (`MaxInFlightPerBroker`, for different partitions). The cost is one round trip
  per batch per partition; the idempotent producer (K3) can lift it, as Java does.
- **At-least-once**: a request that timed out may have been written, and its retry writes the
  records again. The connection stays open after a request timeout (Java closes it); with one
  batch in flight per partition and the broker answering a connection in order, that can
  duplicate but not reorder.
- **Send fails the future, not the call**, as Java (metadata timeout, buffer full, a record too
  large, a partition the topic doesn't have); the callback then runs on the caller's thread.
  Send raises only for the caller's mistakes (no topic, a negative partition, not open,
  closed). A record too large gets `EKafkaBrokerError` with MESSAGE_TOO_LARGE, the code the
  broker would answer.
- **Callbacks run on the sender thread**, then the future completes: when `Flush` returns
  True, every callback has run. `Flush` and `Close` raise when called from a callback.
- **Futures share one monitor** (a hub the futures keep alive), not an event each: a kernel
  object per record would not scale on Windows.
- **Not done yet:** splitting a batch the broker refuses as too large (Java does), the adaptive
  partitioner (weights by queue size; Java's default), reconnect backoff beyond
  `RetryBackoffMs`, connecting to a broker without blocking the sender (the sender opens
  connections itself, one at a time), metrics.

**Next in K1** (section 5): the manual consumer (`Assign`, `Seek`, `Poll`); SASL PLAIN and
SCRAM-SHA-256; the interoperability tests (consume with the Java client what this producer
sent, and the reverse); the opt-in Confluent Cloud test. Open question 1 is answered: Apache
4.4.0 in CI, Confluent Cloud as the first managed broker.

## 8. Open questions

1. **First real user:** which broker (Apache 4.x? Confluent Cloud? MSK? Redpanda?). It decides
   whether the classic group protocol moves up to K2. *Answered 2026-11-02:* **Apache Kafka
   4.4.0** (`docker/`) stays the reference, run in CI on every commit; **Confluent Cloud** is
   the first managed broker. It has KIP-848 and uses SASL PLAIN over TLS, so it needs nothing
   beyond K1's plan, and it is the first broker this project didn't set up. Its test is manual
   and opt-in (credentials from the environment, never in the repository), not part of CI.
   The classic protocol stays after K2. MSK (SCRAM-SHA-512 or IAM), Redpanda (KIP-848
   unknown) and Event Hubs (classic protocol only, as far as its docs say) wait for a user who
   needs them.
2. **zstd:** opt-in libzstd (proposed) or a pure-Pascal decoder (~2 000 to 3 000 lines).
3. **Version of the JSON definitions** for the generator: the latest stable 4.x on the day K1
   starts. *Answered 2026-11-02: 4.4.0* (the user chose it; 4.3.2 was the other candidate). The
   broker in `docker/` runs the same tag.
4. **Android (Delphi):** out of scope unless asked for; the common transport keeps it in pipes
   (its T4).

## Sources

- KIP-896: https://cwiki.apache.org/confluence/display/KAFKA/KIP-896:+Remove+old+client+protocol+API+versions+in+Kafka+4.0
- Confluent, client minimums: https://docs.confluent.io/platform/current/clients/deprecate-how-to.html
- Kafka 4.0, consumer rebalance protocol: https://kafka.apache.org/40/operations/consumer-rebalance-protocol/
- Kafka 4.2, the same (on by default on the server): https://kafka.apache.org/42/operations/consumer-rebalance-protocol/
- Existing Delphi wrappers: https://relatedrepos.com/gh/norgepaul/DelphiKafkaClient, https://github.com/maker2k/DelphiKafkaClient
- Commercial native client: https://esegece.com/products/websockets/protocols/kafka/
