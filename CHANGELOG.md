# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/). While the version is 0.x, a minor version
may change the API; each such change is listed here.

## [0.1.0] - 2026-11-04

The first release: phases K1, K2 and K3 of `docs/design.md`. A native Kafka client with no
librdkafka, developed against Apache Kafka 4.4.0 and run against Confluent Cloud. Needs
pascal-common-faa 1.28.0 or later (`Kafka.Types` and the `.lpk` refuse an older copy).

### Added

- **The protocol.** `Kafka.Protocol.Primitives` (big-endian integers, `FLOAT64`, `UUID`,
  unsigned and zigzag varints, classic and compact strings, bytes and arrays with their nulls,
  tagged fields), `Kafka.Protocol.Crc32c` (CRC32C, checked against RFC 3720's vectors),
  `Kafka.Protocol.Envelope` (request header v1/v2, response header v0/v1, the size-prefixed
  frame).
- **Codecs generated from Kafka 4.4.0's message definitions** (`tools/gen_messages.py`,
  decision 3): `Kafka.Messages.ApiVersions` (v0-v5), `Metadata` (v0-v13), `Produce` (v3-v13),
  `Fetch` (v4-v18), `ListOffsets` (v1-v11), `SaslHandshake` (v0-v1), `SaslAuthenticate`
  (v0-v2), `FindCoordinator` (v0-v6), `ConsumerGroupHeartbeat` (v0-v1), `OffsetCommit`
  (v2-v10), `OffsetFetch` (v1-v10), `ConsumerGroupDescribe` (v0-v1), `InitProducerId` (v0-v5);
  `Kafka.Messages.Apis` (the table negotiation uses) and `Kafka.Errors` (every error code, from
  `Errors.java`). `KafkaErrorIsRetriable` and `KafkaErrorIsInvalidMetadata` come from each
  code's Java exception class (`tools/fetch_error_classes.py`), not from a hand-written list.
- **Record batches.** `Kafka.Protocol.RecordBatch`: record batch v2 by hand, CRC32C checked,
  headers, null against empty key and value, absolute offsets and timestamps (LogAppendTime as
  Java), a Fetch's last batch cut by the byte limit skipped.
- **Compression**, in Pascal on both compilers (`Kafka.Compression`): gzip (RFC 1952 over
  `System.ZLib` and paszlib), snappy (xerial's framing, as Java writes it; raw snappy, as
  librdkafka writes it, read too), lz4 (the LZ4 frame as Java writes it, KIP-57's header
  checksum), both directions; zstd read (`Kafka.Compression.Zstd`, RFC 8878, refusing what Java
  4.4.0's decoder refuses: a dictionary, a window above 128 MiB, legacy frames), not written:
  `Compression := kcZstd` raises `EArgumentException` in `TKafkaProducer.Create`.
  `KafkaCompressionSupported` (writing), `KafkaDecompressionSupported` (reading); output bounded
  on every codec; `EKafkaCompression`.
- **The connection.** `Kafka.Connection`: one broker over TCP or TLS (pascal-common-faa:
  SChannel or OpenSSL chosen at run time, a private CA or the system's store, client
  certificates), a reader thread, several requests in flight matched by correlation id,
  request timeouts, ApiVersions negotiation with KIP-511's fallback, typed `Metadata`,
  `Produce`, `Fetch` and `ListOffsets`, `SendOneWay` (acks=0), `Request` with a deadline of its
  own. `IKafkaSignal` and `TKafkaEventSignal`. `Kafka.Transport`: no `EPcTransport*` leaves the
  library; `Kafka.Types` has the exception tree and the version.
- **SASL** (`Kafka.Sasl`): PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 (`TKafkaScramClient`, the
  server's signature checked), OAUTHBEARER with a token the application supplies
  (`SaslOAuthBearerTokenProvider`, called at each new connection; `SaslOAuthBearerExtensions`,
  `KafkaSaslExtension`; no OIDC flow). `TKafkaConnectionParams.SaslMechanism`, `SaslUsername`,
  `SaslPassword`, in plaintext or over TLS. `EKafkaAuthentication` (the broker's error code and
  `BrokerMessage`), never retried. KIP-368: the connection keeps the session lifetime and the
  cluster replaces a connection whose session is about to end.
- **The cluster** (`Kafka.Cluster`): bootstrap, metadata snapshots, the tracked topics
  refreshed when stale or invalidated, waits for a topic and a partition's leader
  (`MaxBlockMs`), one connection per broker, a group coordinator's connection of its own.
  `Kafka.Partitioner`: murmur2, the partition of a key as Java's producer chooses it, and
  `TKafkaStickyPartitioner` for records without a key (KIP-480, KIP-794). `KafkaBackoffMs`:
  Java's exponential backoff with jitter.
- **The producer** (`Kafka.Producer`): `Send` with a future and an optional callback, batches
  per partition (`BatchSize`, `LingerMs`), a sender thread, acks all/1/0, retries bounded by
  `Retries` and `DeliveryTimeoutMs`, `BufferMemory` and `MaxBlockMs`, `Flush`, `Close` with a
  timeout, every codec but zstd; Java's defaults (`KafkaProducerParams`). **Idempotent by
  default** (`Idempotence = kidAuto`, Java's default since 3.0): with `Acks = kaAll` and
  `Retries > 0` it takes a producer id (InitProducerId), numbers each partition's batches and
  keeps up to five in flight, with Java's handling of OUT_OF_ORDER_SEQUENCE_NUMBER,
  UNKNOWN_PRODUCER_ID, INVALID_PRODUCER_EPOCH and DUPLICATE_SEQUENCE_NUMBER and the local epoch
  bump. A broker that refuses InitProducerId (an ACL without IdempotentWrite) fails the records
  with that error: set `Idempotence := kidOff`. `MaxInFlightPerBroker` above 5 needs it off
  (Java's rule).
- **The consumer** (`Kafka.Consumer`): partitions assigned by hand (`Assign`, `Seek`,
  `SeekToBeginning`, `SeekToEnd`, `Position`, `Poll`, `Wakeup` and `EKafkaWakeup`,
  `BeginningOffsets`, `EndOffsets`, `AutoOffsetReset`) or by a **consumer group, KIP-848**
  (Kafka 4.0 or later): `GroupId`, `Subscribe` (the join with the first `Poll`, as Java 4.x),
  `Unsubscribe`, `OnPartitionsAssigned`, `OnPartitionsRevoked` and `OnPartitionsLost` (inside
  `Poll`, on its thread; they may call `Commit`, `Seek`, `Position`...), positions from the
  group's committed offsets, `Commit`, `CommitOffsets`, `CommitAsync` and `CommitOffsetsAsync`
  (a callback inside a later `Poll`, and an `IKafkaCommitFuture`), `Committed`, auto-commit
  (on by default, every 5 s, before a revoke and at `Close`), `MaxPollIntervalMs` (a member
  that stops polling leaves and joins again), `Close(ATimeoutMs)` commits and leaves. Java's
  defaults (`KafkaConsumerParams`). `Kafka.Group`, `Kafka.Group.Coordinator` and
  `Kafka.Group.Consumer` hold the membership (`IKafkaGroupMembership`, decision 4's seam; the
  classic protocol raises). `TKafkaTopicPartition` and its array are in `Kafka.Types`.
- **Tracing and metrics.** `Kafka.Tracing`: OpenTelemetry spans on pascal-common-faa's tracer,
  `messaging.*` conventions. `Send` opens a producer span `send <topic>` that ends with the
  record's outcome and writes its `traceparent` (and `tracestate`) into the record's headers
  unless the record has one; `Poll` is a span `poll <topic>`, exported when records come back;
  `TKafkaConsumer.StartProcessSpan` opens `process <topic>` as a child of the record's
  `traceparent`, `KafkaFinishSpan` ends it. With the tracer off nothing is created and no
  header added. `Kafka.Metrics`, on `PcMetrics`: `messaging.client.sent.messages`,
  `pascalkafka.producer.bytes`, `.retries`, `.batches.in_flight`, `.epoch_bumps`;
  `messaging.client.consumed.messages`, `pascalkafka.consumer.lag`, `.commits`, `.rebalances`.
- **Samples**, one source each for Delphi and Lazarus: `samples/ProduceConsume` (a producer and
  a consumer over TLS and SASL, every mechanism) and `samples/ConsumerGroup` (a consumer in a
  group with its handlers and commits); `--system-ca` trusts the system's store.
- **Tests**: 354 unit tests (codecs against 306 frames captured from a Kafka 4.4.0 broker and
  the Java client, compression against the reference libraries' vectors and the Java
  producer's batches, murmur2 against Kafka's vectors, SCRAM against the RFCs' vectors and
  Java's messages; an in-process fake broker, plain, TLS and SASL, that fails, holds and drops
  requests, coordinates groups and checks idempotent sequences as 4.4.0 does) and 54
  integration tests against `docker/docker-compose.yml` (Kafka 4.4.0, KRaft: plaintext, TLS,
  SASL_PLAINTEXT, SASL_SSL, short SASL sessions, OAUTHBEARER), including the opt-in
  `Kafka.CloudIntegrationTests` against Confluent Cloud (SASL PLAIN over TLS: records produced
  and polled back; a group consumer reading the `traceparent` back, with its spans and
  metrics). Interoperability with the Java client 4.4.0 (`tools/interop.py`): both directions
  with gzip, snappy and lz4, zstd from Java, the Java consumer and this one in one group, the
  `traceparent` both ways.
