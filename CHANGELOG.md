# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/). While the version is 0.x, a minor version
may change the API; each such change is listed here.

## [Unreleased]

Phase K1 (`docs/design.md`, "K1 progress"). Not released: 0.1.0 comes once K1's last criterion,
the sample against Confluent Cloud, has been run.

### Added

- `Kafka.Protocol.Primitives`: big-endian integers, `FLOAT64`, `UUID`, unsigned varint and
  zigzag varint/varlong, classic and compact strings, bytes and arrays with their nulls, raw
  tagged fields.
- `Kafka.Protocol.Crc32c`: CRC32C (Castagnoli), checked against RFC 3720's vectors.
- `Kafka.Protocol.Envelope`: request header v1/v2, response header v0/v1 (always v0 for
  ApiVersions), the size-prefixed frame.
- `tools/gen_messages.py` (decision 3): the codecs generated from Apache Kafka 4.4.0's message
  definitions. Generated: `Kafka.Messages.ApiVersions` (v0-v5), `Kafka.Messages.Metadata`
  (v0-v13), `Kafka.Messages.Apis` (the table negotiation uses) and `Kafka.Errors` (every error
  code, from `Errors.java`).
- `Kafka.Connection`: one broker over TCP or TLS (pascal-common-faa: SChannel or OpenSSL,
  private CA, client certificates), a reader thread, several requests in flight matched by
  correlation id, request timeouts, ApiVersions negotiation with the fallback of KIP-511, and a
  typed `Metadata` call.
- `Kafka.Transport`: the exception boundary (no `EPcTransport*` leaves the library).
- `Kafka.Types`: the exception tree, the version, and the check for pascal-common-faa 1.27.1 or
  later.
- `Kafka.Protocol.RecordBatch`: record batch v2 (magic 2) by hand, CRC32C checked, headers,
  null against empty key and value, absolute offsets and timestamps (LogAppendTime as Java), a
  Fetch's last batch cut by the byte limit skipped.
- `Kafka.Compression`: none and gzip (RFC 1952 over raw deflate from `System.ZLib` and
  paszlib, the same framing code on both compilers; trailer checked, output bounded).
  `EKafkaCompression` for the other codecs (until K3) and data that doesn't decompress.
- Generated: `Kafka.Messages.Produce` (v3-v13), `Kafka.Messages.Fetch` (v4-v18),
  `Kafka.Messages.ListOffsets` (v1-v11). The generator now reads the `records` type and tagged
  struct fields; every record has `IsDefault`.
- `TKafkaConnection.Produce`, `Fetch`, `ListOffsets`, `SendOneWay` (Produce with acks=0, which
  the broker doesn't answer) and `Request` with a deadline of its own.
- `Kafka.Partitioner`: murmur2 and the partition of a key, as Java's producer chooses it.
- `Kafka.Cluster`: bootstrap, metadata snapshots, the tracked topics refreshed when stale or
  invalidated, waits for a topic and for a partition's leader (`MaxBlockMs`), one connection
  per broker (`IKafkaBrokerConnection`), `NeedsRefresh`.
- `Kafka.Producer`: `Send` with a future and an optional callback, batches per partition
  (`BatchSize`, `LingerMs`), a sender thread, acks all/1/0, retries bounded by `Retries` and
  `DeliveryTimeoutMs`, one batch in flight per partition (order kept through retries),
  `BufferMemory` and `MaxBlockMs`, `Flush`, `Close` with a timeout, gzip; Java's defaults
  (`KafkaProducerParams`).
- `TKafkaStickyPartitioner`: the partition of records without a key (KIP-480, KIP-794).
- `Kafka.Consumer`: manually assigned partitions (`Assign`, `Seek`, `SeekToBeginning`,
  `SeekToEnd`, `Position`, `Poll`, `Wakeup`, `BeginningOffsets`, `EndOffsets`),
  `AutoOffsetReset`, Java's defaults (`KafkaConsumerParams`). `EKafkaWakeup`;
  `KAFKA_WAIT_INFINITE` in `Kafka.Types`.
- `IKafkaSignal` and `TKafkaEventSignal`: `TKafkaConnection.Send` can fire a signal when the
  future is done.
- `KafkaErrorIsRetriable`, `KafkaErrorIsInvalidMetadata` (generated from each code's Java
  exception class, `tools/fetch_error_classes.py`). They replace
  `KafkaErrorInvalidatesMetadata`, whose hand-written list missed NETWORK_EXCEPTION and three
  leader-election codes.
- SASL: `Kafka.Sasl` (PLAIN, SCRAM-SHA-256 over pascal-common-faa's SHA-256, HMAC and
  PBKDF2; the server's signature checked), generated `Kafka.Messages.SaslHandshake` (v0-v1) and
  `Kafka.Messages.SaslAuthenticate` (v0-v2). `TKafkaConnectionParams.SaslMechanism`,
  `SaslUsername`, `SaslPassword`: `Open` authenticates after ApiVersions, in plaintext or over
  TLS, so the cluster, the producer and the consumer get it from their connection template.
  `EKafkaAuthentication` (with the broker's error code), which the producer and the consumer
  don't retry and the cluster's bootstrap raises at once. KIP-368: the connection keeps the
  session lifetime (`SessionLifetimeMs`, `ReauthenticationDue`) and the cluster replaces a
  connection whose session is about to end, instead of re-authenticating on the same socket.
- `docker/`: SASL_PLAINTEXT (9096), SASL_SSL (9097) and a listener with 2-second sessions
  (9098); the SCRAM user is created when the storage is formatted (`docker/kafka-start.sh`).
- Tests: 215 unit tests (codecs and record batches against 158 frames captured from a Kafka
  4.4.0 broker and the Java client, gzip against Python's, murmur2 against Kafka's vectors and
  the Java producer's partitions, SCRAM against RFC 7677's vectors and, given the nonce the
  Java client chose, against Java's own messages byte for byte; fake brokers in process, plain,
  TLS and SASL (PLAIN and SCRAM with right and wrong credentials, a wrong server signature, an
  unsupported mechanism, a listener without SASL, session lifetimes); the producer against fake
  brokers that fail, hold or drop requests, the consumer against fake brokers that answer Fetch
  with a long poll) and 32 integration tests against `docker/docker-compose.yml` (plaintext and
  TLS with a private CA, every TLS backend; records produced and fetched back, gzip, acks=0; the
  cluster's bootstrap, leaders, an auto-created topic, a keyed record through its partition's
  leader; the producer's keyed, sticky and bulk records fetched back; the consumer polling what
  the producer sent, plain and TLS; both through SASL_PLAINTEXT and SASL_SSL with each
  mechanism, and across SASL sessions that expire; Confluent Cloud, opt-in).
- Interoperability with the Java client 4.4.0 (`tools/interop.py`, the library's side in
  `tests/Interop`): what the Java console producer sends is read by the library field by field,
  each keyed record in the partition both murmur2 implementations agree on; what the library
  sends is read by the Java console consumer as sent, and `kafka-dump-log.sh` finds every batch
  gzip with a valid CRC. Run by `tools/test_fpc.sh`, `tools/test_interop_docker.sh` and CI.
- `Kafka.CloudIntegrationTests`: the opt-in test against Confluent Cloud (SASL PLAIN over TLS,
  the system's trust store), credentials from `KAFKA_CLOUD_*` in the environment.
- `samples/ProduceConsume`: K1's sample, a producer and a consumer over TLS and SASL, one source
  for Delphi and Lazarus; its defaults use the SASL_SSL listener of `docker/`.

### Fixed

- `Kafka.Producer`: `Flush` could return True while a batch whose answer had just arrived was
  still being completed (its futures not done yet): between leaving the in-flight list and its
  futures completing, the batch was in no list `Flush` looks at. Found as a rare failure of
  `BufferFull_SendWaitsMaxBlockThenFails`. Such batches now stay in transit until every future
  is done (also when they expire or the producer aborts them).
