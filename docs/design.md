# pascal-kafka-faa — design

Status: **K0 done; K1 done but for its Confluent Cloud run; K2 started** (2026-11-02: "K1
progress" and "K2 progress" below). From the feasibility review of 2026-11-01. The user's decisions the same day: the transport moves to
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
| SASL PLAIN, SCRAM-SHA-256 | K1 | done; PLAIN covers Confluent Cloud; SCRAM uses `PcHmacSha256`/`PcPbkdf2HmacSha256`; KIP-368 by a new connection |
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

### The manual consumer (2026-11-02)

**Done:**

- `Kafka.Consumer`: `Assign`, `Seek`, `SeekToBeginning`, `SeekToEnd`, `Position`, `Poll`,
  `Wakeup`, `BeginningOffsets`, `EndOffsets`. Positions from `AutoOffsetReset` (latest,
  earliest, none) through ListOffsets, and again after OFFSET_OUT_OF_RANGE. `Poll` returns what
  is fetched (up to `MaxPollRecords`) and sends the next fetches before returning; one Fetch
  at a time per broker, waited on through the connection's signal, so `Wakeup` from another
  thread ends a `Poll` at once (`EKafkaWakeup`). Control batches skipped, the records of a
  compressed batch before the position dropped, the position moved past a batch's end
  (compaction gaps). Retriable errors retried (after a metadata refresh when the error calls
  for one: the partition isn't fetched again until the refresh); others raise from `Poll`.
  Java's parameter names and defaults (`KafkaConsumerParams`).
- `EKafkaWakeup`; `KAFKA_WAIT_INFINITE` moved to `Kafka.Types`.
- The fake broker answers Fetch (with a long poll: a Fetch with nothing to return waits for
  records or its MaxWaitMs, on a thread of its own) and ListOffsets, keeps batches as written
  (control ones too), and has a log start.

**Measured:** FPC 3.2.2 Windows x64: 183 unit and 21 integration tests, 0 leaks. Linux (Docker):
the same, and both suites 5 times in three containers at once with `--cpus=1`. Delphi 12
Win32 and Win64: 183 and 21, 0 leaks. Two mutations (the records before the position kept; control
batches returned) make three tests fail.

**Decided while writing:**

- **Poll runs on the caller's thread**, as Java's: no background fetcher. The next fetches are
  sent before `Poll` returns, so they travel while the caller works.
- **Full fetches, no fetch session** (KIP-227, `SessionEpoch` -1): simpler, and correct with
  every broker; a session saves bytes when many partitions are idle. Partitions are listed in a
  rotating order, so a partition whose batch exceeds `MaxPartitionFetchBytes` comes first in a
  later Fetch (a broker always returns the first batch whole).
- **read_uncommitted only** (Java's default); read_committed waits for transactions (K4).
- **Not thread-safe, as Java's**: a second thread inside the consumer gets `EKafka` at once;
  `Wakeup` is the one method any thread may call.
- **`KafkaConsumerParams` turns off auto topic creation** for the consumer's metadata (Java
  creates topics only for a group's subscription, never for `assign`).
- **Not done yet:** pause/resume, `OffsetsForTimes`, the preferred read replica (KIP-392),
  leader epochs in Fetch (fencing, KIP-320), metrics.

### SASL PLAIN and SCRAM-SHA-256 (2026-11-02)

**Done:**

- Generated: `Kafka.Messages.SaslHandshake` (v0-v1) and `Kafka.Messages.SaslAuthenticate`
  (v0-v2), from the same 4.4.0 definitions.
- `Kafka.Sasl`: the client side of PLAIN (RFC 4616) and SCRAM-SHA-256 (RFC 5802, RFC 7677),
  one object per exchange (`InitialResponse`, then `Evaluate` until `IsComplete`, Java's
  `SaslClient` shape). SCRAM uses pascal-common-faa's `PcSha256`, `PcHmacSha256`,
  `PcPbkdf2HmacSha256`, `PcRandomBytes` (the nonce) and `PcSameBytes` (the server signature,
  checked before the exchange counts as complete). Standard Base64 with padding is done over the
  common's Base64url by translating the two characters that differ and the padding.
- `Kafka.Connection`: `SaslMechanism`, `SaslUsername`, `SaslPassword` in
  `TKafkaConnectionParams`. `Open` runs SaslHandshake v1 and SaslAuthenticate right after
  ApiVersions, in plaintext and over TLS. The cluster, producer and consumer take them from
  their connection template with no change of their own.
- `EKafkaAuthentication` (`Kafka.Types`), with the broker's error code. The cluster's refresh
  raises it at once when a broker refused the credentials, instead of `EKafkaConnection` after
  `MaxBlockMs`.
- KIP-368, the cheap way: the connection keeps the lifetime from SaslAuthenticate v1+
  (`SessionLifetimeMs`) and a point between 85% and 95% of it, chosen at random as Java's
  (`ReauthenticationDue`); `Kafka.Cluster` stops handing out a connection past that point and
  opens a new one. Requests already sent on the old one finish there.
- The fake broker speaks SASL: PLAIN, and SCRAM-SHA-256 written as a server (the proof checked
  by recovering the client key and hashing it), with right and wrong credentials, a wrong server
  signature, a mechanism it doesn't enable, a listener without SASL, and session lifetimes.
- `docker/`: SASL_PLAINTEXT on 9096, SASL_SSL on 9097, and 9098 with
  `connections.max.reauth.ms` = 2000. PLAIN's user comes from the JAAS line; SCRAM's is created
  by `kafka-storage.sh format --add-scram`, run by `docker/kafka-start.sh` before the image's
  own format (which takes no extra arguments).
- Captures: 28 more frames (158). From the Java client 4.4.0 through the proxy, PLAIN and SCRAM
  (both SCRAM steps; `captures_from_proxy.py` now keeps repeated SaslAuthenticate steps). By
  hand (`capture_versions.py --apis sasl`): SaslHandshake v0 and v1, SaslAuthenticate v0-v2,
  v1 and v2 with a session lifetime, an unsupported mechanism and a failed authentication.

**Checked against the captures and the broker:** RFC 7677's example byte for byte; given the
nonce the Java client chose, this client writes Java's client-first and client-final (the same
proof) and the broker's signature in that exchange verifies. The broker answers a SASL client
on its PLAINTEXT listener with ILLEGAL_SASL_STATE, hangs up on a client without SASL at its
first request after ApiVersions, and closes a connection whose session expired at its next
request (the integration test sees that, then the replacement working).

**Measured:** FPC 3.2.2 Windows x64: 215 unit and 31 integration tests, 0 leaks. Linux
(Docker): the same, 0 leaks, and both suites 5 times in three containers at once with
`--cpus=1` (30 runs). Delphi 12 Win32 and Win64: 215 and 31, 0 leaks.

**Decided while writing:**

- **Re-authentication is a new connection**, not a SASL exchange on the old socket (Java's
  way). Doing it in place means pausing every writer of the connection between SaslHandshake
  and the end of SaslAuthenticate while the reader keeps delivering earlier answers; a new
  connection costs a TCP (and TLS) handshake per session and no new locking. Sessions are
  minutes or hours in practice. Someone holding a `TKafkaConnection` directly past its session
  gets `EKafkaConnection` when the broker hangs up.
- **`EKafkaAuthentication` is not an `EKafkaConnection`**, so the producer's and consumer's
  retry of connection failures doesn't spin on bad credentials (Java's
  `AuthenticationException` isn't retriable either).
- **ApiVersions v5 before SASL.** Java's client sends ApiVersions v0 before the handshake and
  the newest version after authenticating (seen in the captures); one ApiVersions at the newest
  version first is accepted by the broker, and the negotiated table is then known before
  SaslHandshake needs it.
- **No SASLprep**, as Java: names and passwords go as their UTF-8 bytes, which is what Kafka's
  credential tools store. SCRAM user names are escaped (`=` and `,`).
- **SCRAM iterations between 4096 and 16384**: RFC 7677's minimum (Java's check) and the most
  Kafka's tools accept for a credential. Java has no upper bound; this one keeps a hostile
  broker from asking for billions of PBKDF2 rounds.
- **SaslHandshake v0 isn't spoken** (its tokens went unframed on the socket); every broker since
  1.0 has v1.
- **Standard Base64 stays in `Kafka.Sasl`** over the common's Base64url. A second library that
  needs it would move it to pascal-common-faa.

**Fixed on the way:** `Kafka.Producer`'s `Flush` could return True while a batch whose answer
had just arrived was still being completed: between leaving the in-flight list and its futures
completing, the batch was in no list `Flush` reads. Seen once as a failure of
`BufferFull_SendWaitsMaxBlockThenFails`, in one of four runs. Such
batches now stay in the transit list until every future is done, also when they expire or the
producer aborts them; the producer tests then passed 25 times in a row.

### Interoperability, the Confluent Cloud test, the sample (2026-11-02)

**Done:**

- **Interoperability with the Java client 4.4.0**, both directions (section 6), over the
  records of `tests/data/interop-records.tsv`: keys with accents, CJK and colons, a 200-byte
  key, null keys, a header with a null value, a header name twice, a non-ASCII header value, an
  empty value. The file is the Java console producer's own input format. `tools/interop.py`
  runs the Java tools in the broker's container and the library's side,
  `tests/Interop` (`KafkaInterop.dpr` and `fpc/KafkaInteropFpc.lpr` over one unit,
  `Kafka.Interop`), on two new topics of 6 partitions:
  - Java to library: the console producer sends the file with gzip; the program reads every
    partition and checks key, value, headers, CreateTime, the file's order within each
    partition, and that every keyed record is in the partition `KafkaPartitionForKey` gives
    (Java's partitioner and this murmur2 agree).
  - Library to Java: the program sends the file with gzip and fixed timestamps; the console
    consumer reads timestamp, partition, offset, headers, key and value exactly as sent and as
    the program's reports said; `kafka-dump-log.sh` reads the broker's segments, and every
    batch is gzip with a valid CRC.
  It runs in `tools/test_fpc.sh` (Windows), `tools/test_interop_docker.sh` (Linux, in a
  container on the broker's network) and CI. A program and a script, not a fixture: on Linux
  the suites run in a container with no Docker inside, and the Java side needs `docker exec`.
- **`Kafka.CloudIntegrationTests`**: Confluent Cloud, opt-in, credentials only from the
  environment (`KAFKA_CLOUD_BOOTSTRAP`, `KAFKA_CLOUD_API_KEY`, `KAFKA_CLOUD_API_SECRET`,
  `KAFKA_CLOUD_TOPIC`), SASL PLAIN over TLS with the system's trust store; without them it
  prints a notice and passes. Its code path ran against the local SASL_SSL listener
  (`KAFKA_CLOUD_CA_FILE`, alice): 30 records produced and polled back. **Not yet run against
  Confluent Cloud itself** (no account here).
- **The K1 sample**, `samples/ProduceConsume/KafkaSample.dpr`: one source for Delphi (`.dproj`)
  and Lazarus (`.lpi`), a producer and a consumer over TLS and SASL, options for bootstrap, CA
  (`""` for the system's store), mechanism, user, password (or the name of an environment
  variable holding it), topic and count. Its defaults use the SASL_SSL listener with SCRAM; the
  test scripts run it that way on Windows and Linux.

**Measured:** FPC 3.2.2 Windows x64: 215 unit and 32 integration tests, the interoperability
test and the sample, 0 leaks. Linux (Docker): the same, and both suites 5 times in three
containers at once with `--cpus=1` (30 runs). Delphi 12 Win32 and Win64: 215 and 32, 0 leaks;
the interoperability test and the sample with the Delphi programs too.

**Found on the way:**

- **Old timestamps meet retention.** The library's records carry 2026-01-01 timestamps (fixed,
  so a run is reproducible); with the broker's default retention (7 days) their segment was
  deleted as soon as the broker looked, and `kafka-dump-log.sh` found no file. The interop topics
  are created with `retention.ms=-1`.
- The consumer turns off topic auto-creation (decided with the consumer): the sample opens the
  producer first and asks it for the topic, which a broker that allows it then creates.

**K1's criterion** (section 7): "a sample produces and consumes against Kafka 4.x over TLS +
SASL" is met (both mechanisms; FPC on Windows and Linux, Delphi Win32 and Win64); "and against Confluent
Cloud (SASL PLAIN over TLS)" waits for a run with an account
(`KafkaSample --sasl plain --ca "" ...`, or the opt-in test); "interoperability tests green"
is met.

**Next:** run the sample and the opt-in test against Confluent Cloud; then K2 (the KIP-848
consumer group: "K2 progress", below). Open question 1 is answered: Apache 4.4.0 in CI, Confluent Cloud as the first
managed broker.

## K2 progress

### The plan (2026-11-02)

K2's criterion (section 7): two consumers share a topic and rebalance without loss, and with no
duplicates beyond at-least-once. The order, each step a commit with the usual bar:

1. **The foundation:** the codecs and their captures (below).
2. **The fake broker serving several connections at once.** A consumer in a group talks to the
   partition leaders and to the group coordinator, which in a one-broker test are the same node;
   today's fake serves one connection at a time, and such a test would hang (CLAUDE.md, "Tests").
3. **The coordinator** (`Kafka.Group.Coordinator`): FindCoordinator (the v4+ list form, v0-v3
   for an older broker), the coordinator's connection through the cluster, and rediscovery with
   backoff on NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE and COORDINATOR_LOAD_IN_PROGRESS, which
   is what a broker that just started answers for a while (the captures recorded it).
4. **The membership** (decision 4's seam: `IKafkaGroupMembership`, implemented by
   `TKafkaConsumerProtocol` in `Kafka.Group.Consumer`): a member id the client chooses (KIP-1082,
   a random UUID), a heartbeat thread at the interval the coordinator gives, member epochs, the
   fields Java sends only when they changed (subscription, rebalance timeout), and
   reconciliation: the assignment comes by topic id and is named through the metadata;
   partitions taken away are revoked (their offsets committed first, `OnPartitionsRevoked`)
   before the next heartbeat acknowledges the new assignment; FENCED_MEMBER_EPOCH and
   UNKNOWN_MEMBER_ID lose the partitions (`OnPartitionsLost`) and rejoin with epoch 0; `Close`
   leaves (epoch -1). Assign, revoke and lose events are queued for `Poll`, delivered on the
   caller's thread (section 4, "Threads").
5. **The consumer's side:** `Subscribe`, `GroupId`, the starting positions from OffsetFetch
   (v9+, with the member id and epoch), `Commit` synchronous and asynchronous (OffsetCommit v9+
   with the member epoch; STALE_MEMBER_EPOCH), auto-commit (on its interval inside `Poll`, before
   a revoke, at `Close`), and `GroupProtocol` with one accepted value (decision 4: `classic`
   raises an error that says it doesn't exist yet).
6. **Tests:** unit against the fake as coordinator (fencing, a lost coordinator, a revoke that
   commits first); integration with two consumers on the real broker, watched with
   ConsumerGroupDescribe; the Java consumer and this one in the same group.

### The foundation: the group APIs' codecs and captures (2026-11-02)

**Done:**

- Generated: `Kafka.Messages.FindCoordinator` (v0-v6), `Kafka.Messages.ConsumerGroupHeartbeat`
  (v0-v1), `Kafka.Messages.OffsetCommit` (v2-v10), `Kafka.Messages.OffsetFetch` (v1-v10),
  `Kafka.Messages.ConsumerGroupDescribe` (v0-v1), from the 4.4.0 definitions.
- The generator takes nullable struct fields, which it refused until an API needed one
  (`ConsumerGroupHeartbeatResponse.Assignment`): an `<Name>IsNull` flag, and Java's wire form, an
  int8 marker (-1 null, 1 before the struct) in the versions where the field is nullable. A
  tagged nullable struct (Java marks those with a varint) is still refused. The units generated
  before came out the same.
- Captures: 38 more exchanges (234 frames). By hand, every version of the five APIs, all
  accepted (`capture_versions.py --apis group`); from the Java client 4.4.0 in a KIP-848 group
  (`group.protocol=consumer`), its FindCoordinator, OffsetFetch, OffsetCommit and three of its
  heartbeats (`docs/codec-captures.md`).

**Seen in the captures:** a broker that just started answers FindCoordinator with
COORDINATOR_NOT_AVAILABLE and heartbeats with NOT_COORDINATOR until its coordinator has loaded
(the Java consumer got ten NOT_COORDINATOR in a row, then joined); the member id is the
client's own and the broker keeps it (KIP-1082); a lone member gets every partition at once in
its first answer, with the uniform assignor; the heartbeat interval is 5 s (the broker's
`group.consumer.heartbeat.interval.ms`).

**Fixed on the way (tests):** the integration suites told one run's records from another's by a
marker of a tick count and `Random`. Without `Randomize` that sequence is the same in every
process, and the monotonic clock is the same in every container: in the stress run two of three
runners made the same marker in the same millisecond and read each other's records (a record
seen twice, an offset off by ten; twice in 30 runs). Markers and new topic names now come from
`KafkaTestUniqueId` (`Kafka.TestHelpers`, the OS random source). The auto-created topic's name
had the same flaw: a later run could find the topic an earlier one had created.

**Measured:** FPC 3.2.2 Windows x64: 226 unit and 32 integration tests, 0 leaks. Linux
(Docker): the same, and both suites 5 times in three containers at once with `--cpus=1` (30
runs; 28 before the markers' fix). Delphi 12 Win32 and Win64: 226 and 32, 0 leaks.

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
