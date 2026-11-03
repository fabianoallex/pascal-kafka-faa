# pascal-kafka-faa — design

Status: **K0 done; K1 done but for its Confluent Cloud run; K2's six steps done, its criterion
met against Kafka 4.4.0; K3's steps 1 to 4 done (the idempotent producer, snappy and lz4,
zstd read)**
(2026-11-03:
"K1 progress",
"K2 progress" and "K3 progress" below). From the feasibility review of 2026-11-01. The user's decisions the same day: the transport moves to
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
Kafka.Compression       none, gzip, snappy, lz4 (Pascal); zstd read (Kafka.Compression.Zstd)
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
  heartbeat thread (the same care as redis's pub/sub). Asynchronous commits are sent by the
  heartbeat thread too; their callbacks run inside a later `Poll`, `Commit` or `Close`, on the
  user's thread, as Java's.
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
| Idempotent producer (InitProducerId, sequences) | K3 | done; the default, as Java's since 3.0 |
| snappy and lz4 (pure Pascal) | K3 | done, both directions; a consumer must read whatever any producer sent |
| zstd | K3 | read: a decoder in Pascal (open question 2, option C); writing waits for a user who asks |
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

### The fake broker's connections, the coordinator, the membership (2026-11-02)

Steps 2 to 4 of the plan above. The consumer's side (step 5) and the two-consumer integration
(step 6) are the next session's.

**Done:**

- **The fake broker serves any number of connections at once**: the accept loop hands each to a
  thread of its own, with its own socket, write lock and SASL state; the log, the counters, the
  held and parked requests stay the broker's, under its monitor, and a held or parked request
  remembers its connection. Every test that ran on the one-at-a-time fake passed unchanged.
- **`Kafka.Group`**: decision 4's seam, `IKafkaGroupMembership` (Subscribe, Poll with an event
  handler, Assignment, Commit, Committed, Leave, Close), its events (revoked, assigned, lost),
  `TKafkaCommittedOffset`, and `TKafkaWaitHub`: one monitor that is also the `IKafkaSignal` the
  group's requests are sent with, so a thread waits for an answer, a state change or Close on
  one object. `KafkaBackoffMs` is Java's exponential backoff (factor 2, jitter 0.2).
- **`Kafka.Group.Coordinator`**: FindCoordinator to any broker (`TKafkaCluster.AnyBroker`), the
  list form from v4 (KIP-699) and the single-key form below; the coordinator's connection is a
  separate one (`TKafkaCluster.Coordinator`), so a heartbeat never waits behind a Fetch the
  broker holds for its MaxWaitMs (a broker answers one connection's requests in order; Java
  keeps the coordinator as node `MAX_VALUE - id` for the same reason). Forgotten on
  NOT_COORDINATOR, COORDINATOR_NOT_AVAILABLE or a failed connection; lookups at least
  `RetryBackoffMs` apart, doubling per failure up to `RetryBackoffMaxMs` (1 s), one at a time.
- **`Kafka.Group.Consumer`** (`TKafkaConsumerProtocol`), after Java 4.4.0's
  `AbstractMembershipManager`, `ConsumerMembershipManager` and `ConsumerHeartbeatRequestManager`
  (read for this step): a member id made by the client (KIP-1082, a version 4 UUID, never one
  that starts with `-`); a heartbeat thread that joins at once, then heartbeats at the
  coordinator's interval, and at once again to acknowledge; Java's `HeartbeatState` (the
  subscription, rebalance timeout, server assignor and owned partitions sent when joining and
  when they changed; everything again after a failure); Java's local epoch on assignments, so a
  new target with the same partitions is still acknowledged; reconciliation by topic id, named
  through the metadata (the part that has names reconciled meanwhile); revoked partitions
  handed to the consumer, and only after it handled them the acknowledgement; FENCED_MEMBER_EPOCH
  and UNKNOWN_MEMBER_ID lose the partitions and join again with epoch 0 and the same id;
  NOT_COORDINATOR and COORDINATOR_NOT_AVAILABLE look the coordinator up again at once,
  COORDINATOR_LOAD_IN_PROGRESS backs off, TOPIC_AUTHORIZATION_FAILED is raised from Poll with
  the member kept, the rest is fatal (the partitions lost, the error raised from Poll once); a
  broker without ConsumerGroupHeartbeat is fatal with EKafkaUnsupportedVersion. `Leave`
  revokes, stops the thread and sends epoch -1 once to the known coordinator. `Commit` and
  `Committed` (OffsetCommit, OffsetFetch v8+ with `require_stable`) with the member's id and
  epoch, retried as Java's `CommitRequestManager` retries.
- **The fake broker as coordinator** (`Kafka.TestFakeGroup`): FindCoordinator (both forms,
  injected errors); heartbeats with Java's epoch rules (0 joins, -1 leaves, the current or the
  previous epoch, FENCED_MEMBER_EPOCH or UNKNOWN_MEMBER_ID otherwise); a round-robin assignor
  that gives a member its target minus what another member still holds or was given, so a
  partition moves only after its owner's heartbeat let it go; passive members for a
  deterministic rebalance; OffsetCommit and OffsetFetch with the member's epoch checked.
- `TKafkaTopicPartition` moved to `Kafka.Types` (the group units need it, and the consumer will
  use them).
- Tests: `Kafka.GroupTests`, 24 unit tests (the list above, each against the fake);
  `Kafka.GroupIntegrationTests`, 2 against Kafka 4.4.0: one member joins a new topic's group,
  commits, reads the commits back and leaves, and another membership of the group reads them
  without joining; two members split the topic's three partitions (the first revokes, then the
  second is given what it let go) and, when the second leaves, the first gets them all back.

**Checked against the broker:** the membership worked against 4.4.0 as written: the join, the
acknowledgement of the first assignment, the revoke-then-assign of a rebalance (the uniform
assignor split three partitions two and one), commits with the member epoch (v10, by topic id),
OffsetFetch v10 with the member's epoch, a simple consumer's read with no member, and the leave.
A rebalance takes up to two heartbeat intervals of the broker (5 s each): the first member hears
of it at its next heartbeat, the second gets the partitions at its own.

**Measured:** FPC 3.2.2 Windows x64: 250 unit and 34 integration tests, the
interoperability test and the sample, 0 leaks; the group fixture 15 times in a row. Linux
(Docker): the same, and both suites 5 times in three containers at once with `--cpus=1` (30
runs). Delphi 12 Win32 and Win64: 250 and 34, 0 leaks. Two mutations (acknowledging an assignment
before the consumer handled the revoke; every field in every heartbeat) each make one test fail
(`TwoMembers_PartitionsMoveOnlyAfterTheRevoke`, `Heartbeat_SendsOnlyWhatChanged`).

**Decided while writing:**

- **Revoked and Assigned are queued together** and handed out in that order in one Poll, where
  Java hands out the revoke, waits for it, then the assignment: the same order seen from the
  consumer, without a second round trip between threads. Assigned is always queued, even with no
  new partition (Java calls `onPartitionsAssigned` with an empty set too: the consumer applies
  the assignment there); Revoked and Lost only when they name a partition.
- **A handler that raises** ends that Poll with its exception, and the event still counts as
  handled: the reconciliation goes on and is acknowledged. Java leaves the member unacknowledged
  after a failed callback and runs it again in the next reconciliation; for a revoke the result
  is the same (the partitions are gone either way), for an assignment the partitions are kept
  instead of left unfetched.
- **The partitions a Lost event names are the consumer's at the moment it takes the event**, not
  when the member was fenced: a revoke being handled meanwhile isn't counted twice.
- **COORDINATOR_LOAD_IN_PROGRESS waits the backoff even while joining.** Java's
  `shouldHeartbeatNow` sends a joining member's heartbeat at once whatever the backoff, so
  against a loading coordinator it retries as fast as the round trip.
- **The subscription's metadata is asked for before joining** (Java asks for it when
  subscribing): without it the first assignment arrived before the topic ids could be named, and
  was reconciled as an empty one first, then again (seen in the first run of the unit tests).
- **One wait hub per membership**, shared with its coordinator: the heartbeat thread, the
  consumer's commits and the coordinator's lookup wait on one monitor, and Close stops them all
  at once. The heartbeat request is built under its lock and written outside it (the
  connection's reader needs the lock to signal answers).
- **The coordinator's lookups are spaced even when they work**, as Java's `RequestState`: a broker
  that just started answers FindCoordinator with itself and heartbeats with NOT_COORDINATOR,
  and without the spacing the two would alternate as fast as the network allows.
- **OffsetFetch v8 or later only** (the groups form): every broker with ConsumerGroupHeartbeat has
  v9. **Topic ids in commits (v10) only when the metadata has all of them**, v9 by name
  otherwise (Java's `canUseTopicIds`).
- **The membership doesn't own the cluster**: the consumer's cluster serves both, and the
  membership is closed first.

**Not done yet:** leaving and rejoining when Poll isn't called within the rebalance timeout
(Java's `max.poll.interval.ms` check: the member goes stale), static membership
(`group.instance.id`), regex subscriptions (v1's `SubscribedTopicRegex`), naming a topic id the
subscription doesn't hold (Java asks the metadata by id); the consumer's use of all this (the
next step).

**Found on the way:** a `{$MODE DELPHI}` written inside a `{ }` comment ended the comment at its
`}`, and FPC reported "INTERFACE expected" on the unit's header (CLAUDE.md, gotcha 18).

### The consumer's side, the poll timer, the Java consumer in the group (2026-11-03)

Steps 5 and 6 of the plan. Java 4.4.0's `AsyncKafkaConsumer`, `AbstractMembershipManager` and
`CommitRequestManager` were read again for the points below marked "Java".

**Done:**

- **`Kafka.Consumer` over `IKafkaGroupMembership`**: `GroupId`, `Subscribe` (it only records the
  topics; the first `Poll` joins, as Java 4.x), `Unsubscribe`, exclusive with `Assign` (Java's
  "mutually exclusive"; `Assign([])` is `Unsubscribe`, as Java). The membership's events are
  applied by the consumer inside `Poll` before the user's handlers see them: an assigned
  partition is added and starts from the group's committed offset (OffsetFetch with the
  member's epoch), then `AutoOffsetReset`; a revoked or lost one stops being fetched, and what
  was buffered for it is dropped (an answer in flight for it finds no state and is discarded).
  `OnPartitionsAssigned`, `OnPartitionsRevoked`, `OnPartitionsLost`, "of object", on the
  polling thread; `OnPartitionsLost` falls back to `OnPartitionsRevoked` when unset (Java's
  default `onPartitionsLost`). A partition assigned by hand with a `GroupId` also starts from
  the commit (Java's).
- **Commits**: `Commit`/`CommitOffsets` (synchronous, retried up to `DefaultApiTimeoutMs`, Java's
  default.api.timeout.ms) and `CommitAsync`/`CommitOffsetsAsync` (a `TKafkaCommitReport` to an
  "of object" callback and an `IKafkaCommitFuture`). A synchronous commit waits first for the
  asynchronous ones queued before it and runs their callbacks (Java's
  `awaitPendingAsyncCommitsAndExecuteCommitCallbacks`), so an older commit never lands after a
  newer one. The membership sends asynchronous commits from its heartbeat thread, in order, one
  at a time, once each: Java doesn't retry `commitAsync` either (a retriable error reaches the
  callback as `RetriableCommitFailedException`; here, `EKafkaBrokerError` with its code), and
  waits for an unknown coordinator first.
- **Auto-commit** (`EnableAutoCommit`, on in `KafkaConsumerParams` as Java's): the consumed
  positions asynchronously every `AutoCommitIntervalMs` at the start of `Poll`, not while the
  previous one is in flight (Java's `maybeAutoCommitAsync`); synchronously before a revoke,
  every consumed position, a failure not stopping the revoke (Java's
  `maybeAutoCommitSyncBeforeRebalance`); and at `Close`. A position is consumed once `Poll`
  returned the record before it, and commits happen at the start of a `Poll` or in `Close`,
  after the caller processed the previous one's records: at-least-once.
- **`Close(ATimeoutMs)`** (30 s by default, Java's): the auto-commit, the asynchronous commits
  waited for, the leave (the revoke handler), the membership, then the fetches and the cluster;
  each step's failure swallowed so the next still runs.
- **`GroupProtocol`**: `kgpConsumer`, the one accepted; `kgpClassic` raises at `Create` saying it
  doesn't exist yet (decision 4).
- **The poll timer** (`max.poll.interval.ms`, `MaxPollIntervalMs`, the rebalance timeout the
  member sends): each membership `Poll` restarts it; when it runs out, the heartbeat thread
  sends one heartbeat with epoch -1 and the member is stale (`kmsStale`), its partitions lost
  (Java's `transitionToSendingLeaveGroup(true)` then `transitionToStale`: `onPartitionsLost`,
  not revoked); the next `Poll` hands out the Lost event and joins again with epoch 0 and the same
  member id (Java's `maybeRejoinStaleMember`). The answer to that leave is ignored. A `Poll`
  that waits long still wakes every half interval to restart the timer.
- **Handlers may call the consumer back** on the polling thread (`Commit` in
  `OnPartitionsRevoked`, `Seek` in `OnPartitionsAssigned`, `Position`, `Committed`); `Poll`,
  `Close`, `Subscribe`, `Unsubscribe` and `Assign` raise there. Another thread is still refused.
- **`samples/ConsumerGroup`** (`KafkaGroupSample`): a second sample rather than a change to K1's,
  which shows `Assign`. It joins a group, prints its partitions, sends records, reads them with
  an asynchronous commit per `Poll` and a synchronous one at the end, and closes; a second run
  starts at the first one's commits. Run by `tools/test_fpc.sh` and
  `tools/test_interop_docker.sh` with its defaults (SASL_SSL, SCRAM).
- **Tests**: `Kafka.ConsumerGroupTests`, 10 unit tests against the fake (the list in its header,
  among them two consumers rebalancing with every record read once, the revoke's commit before
  the move, and the poll timer); `Kafka.ConsumerGroupIntegrationTests`, 2 against Kafka 4.4.0;
  and a third scenario in `tools/interop.py`, the Java consumer and this one in one group.

**K2's criterion** (section 7) **is met** against Kafka 4.4.0: two of these consumers share a
three-partition topic while records keep coming; the second's join moves partitions (the first
revokes, committing first), both read, the second closes and the first takes everything back;
every record produced is read, none twice. With the revoke's commit there is no duplicate to
allow; at-least-once would allow one after a failed commit. **The Java consumer 4.4.0**
(`group.protocol=consumer`) and this one shared a six-partition topic in one group: the Java
member read the first records alone, revoked three partitions when this one joined, this one
read its three to the end and left, and the Java member took them back from its commits; 150
records, none lost, none read by both.

**Measured:** FPC 3.2.2 Windows x64: 260 unit and 36 integration tests, the
interoperability test (three scenarios) and both samples, 0 leaks; the new fixture 8 times in a
row. Two mutations (no auto-commit before a revoke; a stale member without its Lost event) each
make one test fail. Linux (Docker): the same, the interoperability test and both samples too,
and both suites 5 times in three containers at once with `--cpus=1` (30 runs). Delphi 12 Win32
and Win64: 260 and 36, 0 leaks; the interoperability test (three scenarios) and both samples
with the Delphi programs too.

**Decided while writing:**

- **Commits at the start of `Poll`**, positions as `Poll` returned them: the same moment as
  Java's, with no background fetcher to make positions move between two calls.
- **The auto-commit on its interval sends only positions that changed** since the last commit
  (Java sends them again anyway, which keeps offsets alive under `offsets.retention.minutes`; a
  KIP-848 group's offsets don't expire while it has members).
- **The asynchronous commits are waited for before the leave in `Close`** (Java waits after it):
  the member can still commit with its epoch.
- **Reading the committed offsets blocks the `Poll` that needs them** (up to the request
  timeout; retried after `RetryBackoffMs` on a timeout). Java reads them on its background
  thread. A `Wakeup` meanwhile takes effect when that read returns.
- **A commit's failure inside the revoke doesn't stop the revoke** (Java's), and `Close`
  swallows each step's failure; a failed auto-commit is retried at the next interval.

**Not done yet:** static membership (`group.instance.id`), regex subscriptions, pause/resume,
the classic protocol (decision 4), metrics.

## K3 progress

### The plan (2026-11-03)

K3's criterion (section 7): the consumer reads what the Java producer writes with each codec,
and the idempotent producer writes no duplicate after a retry. The order, each step a commit
with the usual bar (FPC Windows x64, Linux in Docker and its stress run, Delphi 12 Win32 and
Win64, 0 leaks; the interoperability test and the samples):

1. **The foundation: InitProducerId.** `Kafka.Messages.InitProducerId` generated from the 4.4.0
   definitions (the API into `APIS`, the unit into the `.lpk`, the package and both `.dpr`), its
   captures (`capture_versions.py`: every version by hand; from the Java console producer,
   which is idempotent by default, through the proxy). A small commit, as K2's step 1: the
   producer step then starts on a tested codec.
2. **The idempotent producer.** Java 4.4.0's `TransactionManager` (its idempotent half),
   `RecordAccumulator` and `Sender` read first; what they do decides each point below marked
   "Java", and the step records what was found.
   - **The producer id:** InitProducerId (no transactional id) to the least loaded broker before
     the first batch goes out, retried as Java retries it (COORDINATOR_LOAD_IN_PROGRESS and the
     like), its failure failing the batches waiting, as Java's.
   - **Sequences per partition** in the record batch (`ProducerId`, `ProducerEpoch`,
     `BaseSequence`, which `Kafka.Protocol.RecordBatch` already writes): fixed when a batch is
     first drained, never again for that epoch; a retried batch keeps its sequence; wrapping at
     `MaxInt` as Java's `DefaultRecordBatch.incrementSequence`.
   - **Up to 5 batches in flight per partition**, lifting K1's "one batch in flight per
     partition": still one batch per partition per request (Java's drain), several requests at
     once. A retried batch goes back into its partition's queue by sequence, and is sent again
     only when it is the first in flight (Java's `insertInSequenceOrder` and the drain's check
     against the first in-flight sequence), so a retry never overtakes the batch before it.
   - **The errors:** DUPLICATE_SEQUENCE_NUMBER is the batch already written (success; Java
     reports offset -1 then, to check); OUT_OF_ORDER_SEQUENCE_NUMBER retried while an earlier
     batch of the partition is unresolved, otherwise an unresolved sequence; UNKNOWN_PRODUCER_ID
     and INVALID_PRODUCER_EPOCH, and a batch expired or failed with its sequence possibly
     written, lead to an **epoch bump** (KIP-360: every partition's sequence back to 0 once
     its in-flight batches are resolved; a new id when the epoch would overflow). The broker
     accepts both ways of bumping (step 1's captures): a higher epoch the client chose, from
     sequence 0, and a new id from InitProducerId, which without a transactional id never bumps
     on its side. Which of the two Java uses, and which errors fail the batch and which retry
     it, is Java's table, read in step 2, not decided here.
   - **The parameter:** `Idempotence` in `TKafkaProducerParams`, three values as Java's
     behaviour has them: `kidAuto` (the default: on when `Acks` is all and `Retries` above 0,
     silently off otherwise, as Java 3.0+ does when `enable.idempotence` wasn't set), `kidOn`
     (those checked, Java's `ConfigException`), `kidOff` (K1's producer, one batch in flight per
     partition). A Pascal record can't tell "left at its default" from "set", which is why the
     third value. *Corrected in step 2:* Java refuses `MaxInFlightPerBroker` above 5 whenever
     idempotence is on, the default included, and only acks and retries turn it off silently;
     the checks raise in `Create`.
   - **The fake broker** answers InitProducerId (a new id each time, injected errors) and
     checks sequences as 4.4.0 did in step 1's captures: per producer id and partition, the last
     5 batches kept; a batch among them gets no error and its first offset; a gap, or a retry
     older than those 5, OUT_OF_ORDER_SEQUENCE_NUMBER; a lower epoch INVALID_PRODUCER_EPOCH; a
     higher one from sequence 0 accepted; an unknown producer accepted at any sequence. And,
     on request, UNKNOWN_PRODUCER_ID and DUPLICATE_SEQUENCE_NUMBER, which 4.4.0 didn't answer
     in the captures but Java handles.
   - **Tests.** Unit, against the fake: a Produce answer dropped after the broker wrote it, the
     batch sent again, the record in the log once (the criterion); 5 batches of one partition in
     flight and a failure of the second, the log in order; each error above; the epoch bump
     after an expiry; `kidAuto` off with acks=1. Integration, against Kafka 4.4.0: the same
     lost-answer case through an in-process proxy (`Kafka.TestProxy`, integration helper) that
     relays the bytes, rewrites the brokers in Metadata answers to its own address (gotcha 9:
     otherwise only the bootstrap goes through it; the generated codec does the rewrite), and
     holds back one Produce answer on request; the topic read back, each record once. And many
     batches in flight on one partition, read back in order. No `Sleep`: the proxy signals the
     answer it held.
3. **snappy and lz4, in pure Pascal**, both directions (a consumer must read whatever a producer
   sent; writing them costs little more). snappy in the xerial framing Java's `SnappyOutputStream`
   writes (and the raw block, which some clients send, read too: to check against librdkafka's
   reader); lz4 in the LZ4 frame format with the corrected header checksum (KIP-57). Tests
   against the reference vectors and against batches the Java console producer compresses with
   each codec (`--compression-codec`, gotcha 8), captured; the interoperability test gets a
   scenario per codec, both directions.
4. **zstd.** Open question 2 (libzstd loaded on demand, or a decoder in Pascal) is put to the
   user with a recommendation at this step, not decided before. Then the same tests as step 3.
   This step and step 3 meet the criterion's first half. *Decided 2026-11-03:* option C, a
   decoder in Pascal; writing zstd waits for a user who asks. *Done the same day* ("zstd,
   read", below).
5. **SCRAM-SHA-512.** SHA-512, HMAC-SHA-512 and PBKDF2-HMAC-SHA-512 go to pascal-common-faa
   first (a release there, with its own tests and vectors); the submodule, the `Kafka.Types`
   check and the `.lpk`'s `MinVersion` move to it together. `Kafka.Sasl` takes the hash as a
   parameter of its SCRAM class; the fake broker's SCRAM server too; a user `carol` with
   SCRAM-SHA-512 in `docker/`.
6. **OAUTHBEARER, with the token supplied** (no OIDC flow): RFC 7628's initial response, a token
   given by the application through an "of object" provider called at each authentication, so
   KIP-368's new connection gets a fresh token; the broker's error JSON reported in
   `EKafkaAuthentication`. Against Kafka's unsecured JWS validator on a new listener in
   `docker/` (test only, as Kafka's own docs use it), and the fake.
7. **Spans and metrics.** `PascalCommon.Tracing` spans for send and poll with OpenTelemetry's
   `messaging.*` conventions, `traceparent` written to and read from record headers as the
   siblings do (`PascalCommon.TraceContext`); `PascalCommon.Metrics` counters and gauges for
   the producer (records, bytes, retries, batches in flight, epoch bumps) and the consumer
   (records, lag, commits, rebalances). Last because it touches the producer and the consumer
   once their K3 shape is final.

**Why this order:** idempotence first, because it changes the sender the most and every later
step's producer tests then run on it. The codecs next: they are the other half of the
criterion, independent of the rest, and zstd waits for a decision. SCRAM-SHA-512 needs a
pascal-common-faa release, kept out of the way of the steps above. OAUTHBEARER after it: the
same unit, and the token's refresh reuses KIP-368's new connection. Spans and metrics last.

### The foundation: InitProducerId and the broker's answers to idempotent batches (2026-11-03)

Step 1 of the plan.

**Done:**

- Generated: `Kafka.Messages.InitProducerId` (v0-v5) from the 4.4.0 definitions. 4.4.0 marks
  the request's v6 unstable (`latestVersionUnstable`, KIP-939's two-phase commit) and only the
  request carries the mark; the generator, which already dropped such a version, now caps the
  response at the request's last stable version too, as Java's `ApiKeys` does, instead of
  refusing the pair. No other generated unit changed.
- Captures: 18 more exchanges (270 frames; `docs/codec-captures.md`). By hand
  (`capture_versions.py --apis producer-id`): InitProducerId v0-v5 and v5 again with the id it
  got, then idempotent Produce v13 batches that record what the broker answers a retry, a gap, a
  new epoch, the old one, an unknown producer and a retry older than the five batches it keeps.
  From the Java console producer through the capture listener: its InitProducerId and its batch.
- `Kafka.ProducerIdMessagesTests`, 7 unit tests: the requests rebuilt from the generated
  records (the batches with `TKafkaRecordBatch`) encode to the captured bytes, and the answers
  hold what the table in `docs/codec-captures.md` says.

**Seen in the captures** (step 2's fake copies it):

- InitProducerId without a transactional id gives **a new id with epoch 0** every time, also
  when asked with an id and epoch: the broker has no epoch of its own to bump for an idempotent
  producer.
- A batch sent again while it is among the last five of its producer and partition is answered
  **without error and with its first offset**: a retry after a lost answer isn't written twice,
  and the client can't tell it from the first write.
- A **higher epoch starting at sequence 0 is accepted** with no request first: a client can bump
  the epoch alone. The lower epoch is then INVALID_PRODUCER_EPOCH.
- A **producer id the broker doesn't know is accepted at any sequence** (6 here), not
  UNKNOWN_PRODUCER_ID.
- A gap, and a retry **older than the five batches kept**, are OUT_OF_ORDER_SEQUENCE_NUMBER;
  DUPLICATE_SEQUENCE_NUMBER came in no answer.
- The Java producer asks with `TransactionTimeoutMs` MaxInt and a null transactional id (its
  body is the hand-built v5, byte for byte). Against a broker that had just started, its first
  InitProducerId got COORDINATOR_LOAD_IN_PROGRESS and no id; it asked again on its other
  connection and got one.

**Measured:** FPC 3.2.2 Windows x64: 267 unit and 36 integration tests, the
interoperability test (three scenarios) and both samples, 0 leaks. Linux (Docker): the same,
and both suites 5 times in three containers at once with `--cpus=1` (30 runs). Delphi 12 Win32
and Win64: 267 and 36, 0 leaks; the interoperability test and both samples with the Delphi
programs too.

### The idempotent producer (2026-11-03)

Step 2 of the plan. Java 4.4.0's `TransactionManager`, `TxnPartitionEntry`, `RecordAccumulator`,
`Sender` and `ProducerConfig` were read first; what they do is marked "Java" below.

**Done:**

- **`Kafka.Producer` is idempotent by default** (`Idempotence`, `TKafkaIdempotence`): an
  InitProducerId (no transactional id, `TransactionTimeoutMs` MaxInt, as Java) to any broker
  before the first batch goes; the id, the epoch and the partition's next sequence written into
  each batch when it is first sent, and kept through its retries; up to five batches of a
  partition in flight (one per request), lifting K1's "one batch in flight per partition".
- **Java's rules**, the idempotent half of `TransactionManager.canRetry` and `handleFailedBatch`:
  - a retried batch goes back into its partition's queue in sequence order, and is sent again
    only when it is the first not done (Java's `insertInSequenceOrder` and
    `shouldStopDrainBatchesForPartition`), so retries go one at a time;
  - DUPLICATE_SEQUENCE_NUMBER is delivered, offset -1;
  - OUT_OF_ORDER_SEQUENCE_NUMBER is retried: without a new epoch when the batch isn't the one
    after the last acknowledged (an earlier one failed), with one when it is (a gap);
  - UNKNOWN_PRODUCER_ID is retried with a new epoch (unless the answer has no log start yet, or
    the batch was renumbered already);
  - any other failure of a batch whose sequence was used (INVALID_PRODUCER_EPOCH, a request out
    of time) bumps the epoch; a batch that expires while queued for a retry leaves its sequence
    *unresolved*: new batches of the partition wait until the ones before them are done, then
    either go on (the broker had it) or bump.
- **The bump is local, as Java's** (`bumpIdempotentProducerEpoch`): the epoch plus one, the
  partition's batches not done renumbered from 0 and encoded again; other partitions start
  from 0 on the new epoch once their old batches are done; a new id only when the epoch reaches
  32767. Step 1's captures had shown the broker takes a higher epoch from sequence 0.
- **InitProducerId errors:** retriable ones (COORDINATOR_LOAD_IN_PROGRESS, as the Java producer
  met) and a lost answer are asked again after `RetryBackoffMs`; any other error, a broker
  without the API or refused credentials fail the records waiting, and the request is made
  again after the backoff.
- **The parameter, as Java's `postProcessAndValidateIdempotenceConfigs`:** `kidAuto` is off,
  silently, when `Acks` isn't `kaAll` or `Retries` is 0; `kidOn` refuses those two;
  `MaxInFlightPerBroker` above 5 is refused whenever idempotence is on, `kidAuto` included
  (Java refuses it with the default too), so a K1 producer with more needs `kidOff`. The checks
  raise `EArgumentException` in `Create`, as the producer's other parameter checks (the plan
  said `EKafka` at `Open`).
- Diagnostics: `Idempotent`, `ProducerId`, `ProducerEpoch`, `InitProducerIdCount`,
  `EpochBumps`.
- **The fake broker** answers InitProducerId and checks sequences as 4.4.0 did in step 1's
  captures (its header); `LoseProduceAnswers` writes a batch and loses the answer;
  `ProduceErrorSkip` fails a later request; `StoredBatches` returns the batches as stored.
- **`Kafka.TestProxy`** (integration helper): relays a real broker's connections, rewrites the
  brokers in Metadata answers to itself (gotcha 9) with the generated codec, can lose Produce
  answers, and counts the Produce requests in flight on a connection.
- Tests: `Kafka.IdempotentProducerTests`, 16 unit tests (its header), and
  `Kafka.IdempotenceIntegrationTests`, 2 against Kafka 4.4.0 through the proxy.

**K3's criterion for idempotence is met** against Kafka 4.4.0: the proxy swallowed the answer
to a batch the broker had written; the producer sent the batch again after its request
timeout, with the same sequence; the broker answered with the first write's offset; a consumer
reading the partition found the record once. And 3 000 single-record batches of one partition
went with up to five requests in flight on the connection (the proxy counted them), all read
back once and in order.

**Measured:** FPC 3.2.2 Windows x64: 283 unit and 38 integration tests, the
interoperability test (three scenarios) and both samples, 0 leaks; the new unit fixture 40
times in a row. Linux (Docker): the same, and both suites 5 times in three containers at once
with `--cpus=1` (30 runs). Delphi 12 Win32 and Win64: 283 and 38, 0 leaks; the
interoperability test and both samples with the Delphi programs too.

Three mutations, each run against the idempotent fixture: a retried batch given a new
sequence fails `LostAnswer_SentAgain_WrittenOnce` (the record written twice); a bump that
keeps the epoch fails five tests. Retries allowed to overtake the first batch in flight fail
none: against one broker that answers a connection in order, the broker's own sequence check
keeps the log in order, and Java's rule saves requests (and bumps, with several brokers)
rather than records. Kept, as Java's.

**Decided while writing:**

- **InitProducerId is sent at once when the sender starts**, not with the first record (Java's
  `bumpIdempotentEpochAndResetIdIfNeeded` runs on every pass of its sender too). An idle
  producer costs one request.
- **A non-retriable InitProducerId error doesn't end the producer.** Java moves to a fatal
  state for most of them; here the records waiting fail with the error and the next request
  is made after the backoff (Java's behaviour for authorization errors). A producer that can
  never get an id fails each record, which is what a caller would see either way.
- **The connection stays open after a Produce request times out** (K1's decision, Java closes
  it): with idempotence the retry on the same connection reaches the broker after the
  original, and the broker answers it as a duplicate.
- **Not done yet:** splitting a batch the broker refuses as too large (with idempotence the
  split batches need sequences of their own, Java's `splitAndReenqueue`), and the transactional
  producer (K4).

**Found on the way:**

- The fake broker's `ReleaseHeldProduce` stopped holding before it answered the held
  requests, so a request the producer sent after the first answer was handled before the
  ones still held: answers out of order on one connection, which no broker gives. The
  producer saw a gap on its next batch and bumped the epoch, as Java would (one run in about
  twenty). Requests that come while the held ones are answered are now held and answered after
  them.
- The proxy hung its fixture's teardown: after `Close`, pascal-common-faa's listener `Accept`
  returns nil rather than raising, and the loop went on accepting nothing. It reads
  correlation ids of requests at bytes 4 to 7 (`KafkaPeekCorrelationId` reads a response's,
  bytes 0 to 3; using it for requests matched no answer).

### snappy and lz4 (2026-11-03)

Step 3 of the plan. Read first: Java 4.4.0's `SnappyCompression`, `Lz4Compression`,
`Lz4BlockInputStream` and `Lz4BlockOutputStream` (clients/.../common/compress, tag 4.4.0);
xerial snappy-java 1.1.10.7's `SnappyOutputStream`, `SnappyInputStream` and `SnappyCodec` (the
version 4.4.0 ships); librdkafka's `rdkafka_msgset_reader.c`, `rdkafka_msgset_writer.c` and
`rdkafka_lz4.c`. What they do is marked "Java" and "librdkafka" below.

**Done:**

- **snappy** in `Kafka.Compression`, in Pascal: the raw block (google/snappy's
  `format_description.txt`: a varint length, literals, copies with 1-, 2- and 4-byte offsets),
  and xerial's framing, which Java's producer writes (`SnappyOutputStream`: the 16-byte header,
  magic `82 'SNAPPY' 00`, version 1, compatible version 1, then each 32 KiB of input as a
  big-endian length and a raw block). Reading follows `SnappyInputStream`: 16 bytes or more
  that start with the magic are framed, anything else is one raw block; a header between chunks
  (concatenated streams) is read on, as xerial does.
- **lz4**: the LZ4 frame as Java's `Lz4BlockOutputStream` writes it (FLG 60: version 1,
  independent blocks; BD 40: 64 KiB; the header checksum over the descriptor only, KIP-57;
  blocks stored uncompressed when they don't shrink; no checksums, no content size), and
  `KafkaXxHash32`. Reading takes what `Lz4BlockInputStream` takes (blocks of 64 KiB to 4 MiB,
  block checksums, a content size, a content checksum) and refuses what it refuses (dependent
  blocks, a dictionary id, reserved bits, a version other than 1, a wrong header checksum).
- **The compressors** are the simple greedy ones (a hash table of 4-byte sequences; snappy's
  input cut into 64 KiB fragments, lz4's end-of-block rules). They give valid streams, not the
  reference's exact bytes; their size was within a few per cent of the reference libraries' on
  every shape tried (text, zeros, random, runs, mixed; snappy 31 323 against 31 183 bytes for
  160 KB of text, lz4 36 008 against 35 687).
- `KafkaCompress`, `KafkaDecompress` and `KafkaCompressionSupported` take both codecs, so the
  producer (`Compression := kcSnappy` or `kcLz4`), the consumer and the fake broker use them with
  no change of their own. zstd still raises `EKafkaCompression` (step 4).
- **Captures** (`docs/codec-captures.md`): the Java console producer's snappy and lz4 batches,
  the gzip capture's three records and a fourth with an 80 016-byte value, so Java wrote three
  xerial chunks and two lz4 blocks (274 frames).
- **The fake broker**: `AppendRawBatch` stores a batch's bytes as given (Java's), so the
  consumer reads Java's compression from the fake.
- **Tests**: `Kafka.CompressionTests`, 20 (its header): xxHash32's published vectors; raw
  snappy blocks and lz4 frames written by the reference libraries (`tools/codec_vectors.py`:
  cramjam's snappy, python-lz4, with every option Java's reader takes), elements built by hand,
  the framing Java writes, round trips, what is refused (each with the reason in the message),
  the limit, and corrupt input; in `Kafka.ProduceFetchTests` the Java batches, decoded to the
  records sent and framed as Java frames them; the producer and the consumer against the fake
  (`SnappyAndLz4_LargeAndSmallRecordsArrive`, `Poll_ReadsJavasSnappyAndLz4Batches_AndThisLibrarys`);
  four integration tests (a batch of each codec produced and fetched back, plaintext and TLS,
  and polled by the consumer); `tools/interop.py` runs both directions with gzip, snappy and
  lz4, and checks the codec of every batch in the broker's log in both.

**Checked:** while writing, this library's compressed output and the reference libraries'
were decoded by the other side, for 35 inputs (sizes around the 60-byte literal tags, the 32 KiB
and 64 KiB block edges, up to 160 KB), every one the same both ways. Kafka 4.4.0 decompresses a
compressed batch to validate it before writing it, so every batch the integration tests and the
interop test produced was also read by Java's codecs; the Java console consumer then read the
records as sent. A fuzz of 64 000 corrupt inputs (bytes changed, cut, inserted) against a copy
of the unit built with range and overflow checks on: every one decoded or raised
`EKafkaCompression`, with no index outside the data (a lighter version of it is a unit test).
Five mutations each fail at least one test: lz4's header checksum over the magic (6 tests), no
xerial (7), no check of a snappy length larger than its block can hold (1), and a copy or match
reaching into the chunk or block before (1 each; the last two were found missing by the
mutations and added).

**Measured:** FPC 3.2.2 Windows x64: 307 unit and 42 integration tests, the interoperability
test (gzip, snappy and lz4 in both directions, and the group) and both samples, 0 leaks. Linux
(Docker): the same, and both suites 5 times in three containers at once with `--cpus=1` (30
runs). Delphi 12 Win32 and Win64: 307 and 42, 0 leaks; the interoperability test (Win32 and
Win64) and both samples with the Delphi programs too.

**Decided while writing:**

- **A raw snappy block is read, and must be.** librdkafka *writes* raw blocks (its writer calls
  `rd_kafka_snappy_compress_slice`, no framing), so every producer built on it (confluent-kafka
  for Python, Go, .NET) sends them; its reader, like xerial's, tells the two apart by the magic.
- **Stricter than Java where Java is silent:** 1 to 3 bytes after the last snappy chunk, a
  header between chunks with a bad version, and bytes after the lz4 end mark are refused (Java
  stops reading there and ignores them); the lz4 content checksum and content size are checked
  (Java skips both: "TODO" in its reader; the reference library checks them). No producer writes
  any of those; a batch that has them is more likely corrupt. gzip's one-member rule is the same
  choice.
- **Copies stay inside their chunk or block.** Each xerial chunk is a raw block of its own, and
  Kafka's lz4 blocks are independent: a copy that reaches before its block is refused, not read
  from the previous one.
- **The limit before the allocation:** a snappy block declares its length, which is checked
  against `AMaxSize` and against what the block's bytes could hold (no element gives more than 64
  bytes for 3: more than 22 times the bytes left is refused) before anything is allocated; lz4
  grows its output in steps, each checked first; an lz4 content size above the limit is refused
  from the header.
- **One unit**, as the plan said (`Kafka.Compression`, 1 300 lines with gzip): no new unit for
  the packages and project files to list.

**Found on the way:** python-lz4 writes a one-block frame with independent blocks whatever
`block_linked` says (LZ4F does that for a frame of one block), so the dependent-blocks case is
built by hand (CLAUDE.md, gotcha 21). Gotcha 17 struck once more: a backslash-n in a Python
heredoc became a line break.

### zstd, read (2026-11-03)

Step 4 of the plan, option C of open question 2: the decoder in Pascal, no encoder. Read first:
Java 4.4.0's `ZstdCompression` (clients/.../common/compress, tag 4.4.0); zstd-jni 1.5.7-17's
`ZstdInputStreamNoFinalizer`, `ZstdOutputStreamNoFinalizer` and their JNI side (the version
4.4.0 ships: its `gradle/dependencies.gradle`, and the jar the console producer loaded); libzstd
1.5.7's `zstd_decompress.c` and `zstd_decompress_block.c`, the libzstd inside that zstd-jni, for
the points RFC 8878 leaves open. What they do is marked "Java" and "libzstd" below.

**Done:**

- **`Kafka.Compression.Zstd`**, the decoder (RFC 8878): frames one after the other and
  skippable frames; raw, RLE and compressed blocks; literals raw, RLE or Huffman (1 or 4
  streams, the weights written directly or with FSE, the previous block's tree reused);
  sequences with predefined, RLE, FSE or repeated tables and the three repeat offsets; the
  content size and the content checksum checked (`KafkaXxHash64`, XXH64).
  `KafkaZstdDecompress`. `Kafka.Compression` calls it for `kcZstd`, so the consumer and the fake
  broker read zstd with no change of their own.
- **Read, not written.** `KafkaDecompressionSupported` (every codec) next to
  `KafkaCompressionSupported`, which now means writing and is False for zstd, as open question
  2 said it would need to; `KafkaCompress(kcZstd)` and a producer with `Compression := kcZstd`
  raise, saying zstd is read but not written.
- **What Java's decoder refuses, refused** (zstd-jni over `ZSTD_decompressStream`, default
  parameters): a window above 128 MiB (`ZSTD_WINDOWLOG_LIMIT_DEFAULT`, 27); a dictionary id
  (Java sets no dictionary: "dictionary wrong"); zstd 0.x's legacy frames (zstd-jni builds
  libzstd without `ZSTD_LEGACY_SUPPORT`). Like Java: every frame read, skippable frames
  skipped, the checksum checked. Java's producer writes level 3 with no checksum, through a
  16 KiB buffered stream.
- **Captures** (`docs/codec-captures.md`): the Java console producer's zstd batch, the snappy
  and lz4 records and a fifth of 96 KB of text (276 frames). Java's frame: no content size, no
  checksum, a window descriptor of 2 MiB, two compressed blocks (4 Huffman streams, FSE tables)
  and an empty raw block marked last, the stream's end.
- **`docker/kafka-start.sh` creates `pascal-kafka-faa-it-zstd`** (one partition,
  `compression.type=zstd`) once the broker answers, and the healthcheck waits for it: the
  broker compresses every batch given to that topic again with Java's zstd, which is how the
  integration suite gets zstd from a library that writes none. The start script now keeps the
  image's start in the background and passes SIGTERM on (`docker stop` takes 2 s, as before).
- **Tests**: `Kafka.ZstdTests`, 10 (its header): XXH64's vectors; 18 frames written by python
  zstandard over libzstd 1.5.7 (`tools/codec_vectors.py zstd`), chosen so that between them they
  use every part of the format a compressor writes (checked with a script that lists the parts a
  frame's headers say it uses); 11 frames built by hand for what no compressor wrote (RLE
  literals, a 3-byte sequence count, an 8-byte content size, a dictionary id of 0...), each
  decoded by libzstd to the same bytes; several frames with skippable ones between; what Java
  refuses; 48 corrupt frames and blocks, each refused with its reason in the message and each
  refused by libzstd too; the limit; corrupt input. `Kafka.ProduceFetchTests`: the Java batch's
  five records and its framing; `Kafka.ConsumerTests`: the consumer polls it from the fake;
  `Kafka.ProducerTests`: zstd refused at `Create`; integration: 2 tests on the zstd topic (a
  gzip batch fetched back as the zstd batch the broker wrote; lz4 records produced and polled
  over TLS); `tools/interop.py` runs Java to library with zstd too, `kafka-dump-log.sh` saying
  every batch is zstd.

**Checked:** while writing, 2 080 frames from libzstd (levels -7 to 22, with and without
checksum and content size; streamed as Java writes; flushed into a block per few KB, so that
blocks reuse the tables before them; two frames with a skippable one between; a 4 MiB window),
over 40 inputs from empty to 2.8 MB, each decoded to the bytes given; Java's own frame to the
same 176 306 bytes as libzstd. A fuzz of 400 000 corrupt inputs (100 frames, five kinds of
change) against a copy of the unit built with range and overflow checks on (XXH64 excepted,
which wraps on purpose): every one decoded or raised `EKafkaCompression`, nothing outside its
data (a lighter version of it is a unit test). Eight mutations each fail at least one test:
the second repeat offset not swapped (5 tests), no shift of the repeat offsets after a sequence
with no literals (7), the states updated in the wrong order (7), the last Huffman weight taken
from the wrong state (2), a match reaching into the frame before (1), 4 Huffman streams split
by rounding down (4), the repeat offsets reset per block (3), and the literal and match
lengths' extra bits read in the wrong order (1); that last one failed none at first, since no
vector had a sequence where both lengths take extra bits, and one was added. Speed: about 300
MB/s of output (FPC `-O2`, Windows x64, a 2.8 MB frame of text, random bytes and words).

**Measured:** FPC 3.2.2 Windows x64: 321 unit and 44 integration tests, the interoperability
test (gzip, snappy and lz4 in both directions, zstd from Java to the library, the group) and
both samples, 0 leaks. Linux (Docker): the same, the interoperability test and both samples
too, and both suites 5 times in three containers at once with `--cpus=1` (30 runs). Delphi 12
Win32 and Win64: 321 and 44, 0 leaks; the interoperability test (zstd from Java too) and both
samples with the Delphi programs.

**Decided while writing:**

- **A unit of its own**, where step 3 kept snappy and lz4 in `Kafka.Compression`: the decoder
  (1 270 lines) would nearly double that unit and shares nothing with the other codecs but the
  idea of a bounded output. It is listed in the packages and every project file.
- **Bytes after the last frame are refused.** Java refuses them too unless they are the start
  of a magic number, which it ignores (libzstd checks the prefix of a header it hasn't had
  whole, and the stream ends where Java's `frameFinished` says); no producer writes either.
  The same choice as snappy's and lz4's trailing bytes in step 3.
- **The 128 MiB window is refused although this decoder doesn't need it**: it keeps the whole
  output (one batch, bounded by `AMaxSize`) and uses the window only for the block maximum,
  the window or 128 KiB, as libzstd. Refusing it keeps this consumer from reading what a Java
  consumer can't.
- **A match's offset is checked against the frame's output, not the window**, as libzstd's
  single-pass decoder (librdkafka's): a frame can't reach into the one before it.
- **Huffman codes of up to 12 bits**, as libzstd's `HUF_TABLELOG_MAX` (the RFC says 11), and
  libzstd's own check that the symbols of weight 1 are an even number, at least two.
- **The limit before the allocation**: a frame that says its content size is refused from its
  header when that is above `AMaxSize`; one that doesn't grows its output block by block, each
  reserved against the limit first.

**Found on the way:** python zstandard's `decompressobj` stops at the end of the first frame
and keeps the rest as unused data: it "accepted" bytes after a frame, and would read only one of
several. What Java does was read in libzstd's source instead (CLAUDE.md, gotcha 23). Gotcha 17
three more times: a backslash-n and shell line continuations in heredoc edits.

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
   *Answered 2026-11-03: C* (the recommendation, which the user took). What Java 4.4.0 does
   (`ZstdCompression`): zstd-jni's streaming encoder at level 3 by default (no dictionary, no
   content size known up front) and its streaming decoder, which reads any frame the format
   allows. The options:
   - **A. libzstd, loaded on demand** (as pascal-common-faa loads OpenSSL): ~400 lines of
     bindings, both directions, the reference's speed and ratio. A DLL to ship on Windows (none
     comes with the system); on Linux `libzstd.so.1` is almost always there. Without it, zstd
     raises as today, on the consumer's side too: a consumer can't read a zstd topic out of
     the box, which is the case K3's criterion is about.
   - **B. In Pascal, both directions:** the decoder (RFC 8878: frames, raw, RLE and compressed
     blocks, Huffman literals, FSE sequences, repeat offsets, XXH64 checksum; ~1 500 to 2 000
     lines) and an encoder. A simple encoder (raw literals, predefined FSE tables) is ~600 to
     900 lines and compresses about as lz4 does, not as zstd's level 3; a real one costs as much
     as the decoder again.
   - **C. The decoder in Pascal, writing later:** B's decoder now; the producer refuses zstd
     until a user asks for it, then A or B's encoder. Every consumer reads zstd with no
     dependency (the criterion), and the cost is the decoder alone.

   K3's step 4 is C: the zstd decoder in Pascal; the producer keeps refusing zstd (so
   `KafkaCompressionSupported` will need to tell reading from writing). *Done 2026-11-03*
   (`Kafka.Compression.Zstd`; `KafkaDecompressionSupported` for reading). Writing, when a user
   asks: A or B's encoder, as above.
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
