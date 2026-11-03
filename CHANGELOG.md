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
- Phase K2's foundation (consumer groups, KIP-848): generated `Kafka.Messages.FindCoordinator`
  (v0-v6), `Kafka.Messages.ConsumerGroupHeartbeat` (v0-v1), `Kafka.Messages.OffsetCommit`
  (v2-v10), `Kafka.Messages.OffsetFetch` (v1-v10) and `Kafka.Messages.ConsumerGroupDescribe`
  (v0-v1). `tools/gen_messages.py` generates nullable struct fields (Java's int8 marker). 38
  more captured exchanges (234 frames), by hand for every version and from the Java consumer in
  a KIP-848 group; `Kafka.GroupMessagesTests` (226 unit tests in all).
- Consumer groups, KIP-848 (phase K2, steps 2 to 4): `Kafka.Group` (the membership interface
  `IKafkaGroupMembership`, decision 4's seam; its events `kgeRevoked`, `kgeAssigned`, `kgeLost`;
  `TKafkaCommittedOffset`; the wait hub), `Kafka.Group.Coordinator` (FindCoordinator in the list
  form, v4+, and the single-key form for older brokers; the coordinator's own connection;
  lookups again on NOT_COORDINATOR and COORDINATOR_NOT_AVAILABLE, spaced by an exponential
  backoff with jitter) and `Kafka.Group.Consumer` (`TKafkaConsumerProtocol`: a client-chosen
  member id, KIP-1082; a heartbeat thread at the coordinator's interval, sending only the
  fields that changed; reconciliation by topic id, named through the metadata, revoked
  partitions handed to the consumer before the new assignment is acknowledged; fenced and
  unknown members lose their partitions and join again; `Leave` with epoch -1; `Commit` and
  `Committed` over OffsetCommit and OffsetFetch with Java's retries); the consumer uses it from
  the next entry on.
- `TKafkaCluster.Coordinator` (a connection of its own for a group coordinator, as Java keeps)
  and `TKafkaCluster.AnyBroker`.
- `KafkaBackoffMs`: Java's exponential backoff (factor 2, jitter 0.2), and
  `KAFKA_DEFAULT_RETRY_BACKOFF_MAX_MS` (`retry.backoff.max.ms`).
- Tests: the fake broker serves any number of connections at once (a thread each, with its own
  SASL state) and is a group coordinator (`Kafka.TestFakeGroup`: heartbeats with a round-robin
  assignor that moves a partition only after its owner let it go, fencing, OffsetCommit and
  OffsetFetch with the member's epoch checked, passive members, injected errors).
  `Kafka.GroupTests` (24 unit tests: 250 in all) and `Kafka.GroupIntegrationTests` (one member
  joins, commits, reads back and leaves; two members split a topic and one leaves; against
  Kafka 4.4.0: 34 integration tests in all).
- The consumer in a consumer group (phase K2, steps 5 and 6): `TKafkaConsumerParams.GroupId`,
  `GroupProtocol` (`kgpConsumer`; `kgpClassic` raises, decision 4), `EnableAutoCommit` (on in
  `KafkaConsumerParams`), `AutoCommitIntervalMs`, `MaxPollIntervalMs`, `DefaultApiTimeoutMs`,
  `RetryBackoffMaxMs`, `GroupRemoteAssignor`, `ClientRack`; `TKafkaConsumer.Subscribe` (the
  join with the first `Poll`, as Java 4.x), `Unsubscribe`, `Subscription`, exclusive with
  `Assign` (`Assign([])` unsubscribes); `OnPartitionsAssigned`, `OnPartitionsRevoked`,
  `OnPartitionsLost` (`TKafkaPartitionsEvent`, "of object", inside `Poll` on its thread; the
  consumer applies the assignment first: revoked and lost partitions stop being fetched and
  their buffered records are dropped); positions from the group's committed offsets, then
  `AutoOffsetReset`; `Commit`, `CommitOffsets` (synchronous, after the asynchronous commits
  queued before), `CommitAsync`, `CommitOffsetsAsync` (a `TKafkaCommitEvent` callback run inside
  a later `Poll`, `Commit` or `Close`, and an `IKafkaCommitFuture`), `Committed`; auto-commit on
  its interval inside `Poll` (when the positions changed), before a revoke and at `Close`;
  `Close(ATimeoutMs)` commits, leaves (the revoke handler) and closes the membership before the
  connections; the `Group` property (member id, epoch, state). Handlers may call the consumer
  back (`Commit`, `Seek`, `Position`...); `Poll`, `Close`, `Subscribe`, `Unsubscribe` and
  `Assign` raise there.
- `Kafka.Group`: `IKafkaGroupMembership.CommitAsync` and `PendingCommits`, `TKafkaCommitReport`,
  `TKafkaCommitEvent`, `IKafkaCommitFuture` (`TKafkaCommitFuture`); `kmsStale`.
  `TKafkaConsumerProtocol` sends asynchronous commits from its thread, in order, once each
  (Java's commitAsync), and leaves the group when `Poll` isn't called within the rebalance
  timeout (`max.poll.interval.ms`): epoch -1, stale, the partitions lost, joined again at the
  next `Poll` (Java's `transitionToStale`).
- `samples/ConsumerGroup`: K2's sample, a consumer in a group with its handlers, asynchronous
  and synchronous commits; a second run starts where the first committed.
- Tests: `Kafka.ConsumerGroupTests` (10 unit tests: 260 in all; two consumers rebalancing on
  the fake with every record read once, the revoke's commit before the move, a manual commit in
  the revoke handler, positions from the commits, auto-commit, asynchronous commits, Close, the
  poll timer) and `Kafka.ConsumerGroupIntegrationTests` (2: K2's criterion against Kafka 4.4.0,
  two consumers rebalancing with no record lost or read twice; a new consumer starting after a
  closed one's commit: 36 integration tests in all). `tools/interop.py` has a third scenario: the
  Java consumer 4.4.0 (`group.protocol=consumer`) and this one in the same group, sharing a
  topic's partitions, every record read once.
- Phase K3's foundation (the idempotent producer): generated `Kafka.Messages.InitProducerId`
  (v0-v5; 4.4.0 marks v6 unstable, and the generator now caps the response at the request's
  last stable version, as Java's `ApiKeys`). 18 more captured exchanges (270 frames): by hand,
  InitProducerId v0-v5 and idempotent Produce batches showing what the broker answers a retry, a
  gap, a new epoch, the old one, an unknown producer and an evicted retry; from the Java console
  producer, its InitProducerId (answered COORDINATOR_LOAD_IN_PROGRESS first) and its batch;
  `Kafka.ProducerIdMessagesTests` (267 unit tests in all).

### Changed

- `TKafkaTopicPartition`, `TKafkaTopicPartitionArray` and `KafkaTopicPartition` moved from
  `Kafka.Consumer` to `Kafka.Types` (the group units need them and the consumer will use the
  group units). A program that names them through `Kafka.Consumer` only needs `Kafka.Types` in
  its `uses`.
- `TKafkaConsumer.Close` takes a timeout (default 30 s, Java's), for the group's commit and
  leave; `Close` with no argument still compiles. `Poll` without `Assign` raises only when the
  consumer isn't subscribed either.
- A consumer with a `GroupId` starts an assigned partition from the group's committed offset
  (Java's behaviour with `assign` and a `group.id`); without one, nothing changes.
- `IKafkaGroupMembership.Poll` also restarts the member's poll timer.

### Fixed

- `Kafka.Producer`: `Flush` could return True while a batch whose answer had just arrived was
  still being completed (its futures not done yet): between leaving the in-flight list and its
  futures completing, the batch was in no list `Flush` looks at. Found as a rare failure of
  `BufferFull_SendWaitsMaxBlockThenFails`. Such batches now stay in transit until every future
  is done (also when they expire or the producer aborts them).
