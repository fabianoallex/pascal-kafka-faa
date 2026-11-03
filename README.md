# pascal-kafka-faa

A native Apache Kafka client for **Delphi** and **Free Pascal/Lazarus** from one source tree,
with no external dependency (no librdkafka). TLS through SChannel or OpenSSL, chosen at run
time.

**Status: early, not released.** Phases K1, K2 and the start of K3: the protocol, the codecs generated from
Kafka 4.4.0's message definitions, record batch v2 with gzip, snappy and lz4 (in Pascal, both
directions) and zstd (read, in Pascal; not written yet), murmur2 partitioning compatible
with Java, a connection per broker, SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER over plaintext or TLS, the
cluster layer (bootstrap, metadata, leaders), the producer (idempotent by default), and the consumer with partitions
assigned by hand or by a consumer group (KIP-848, Kafka 4.0 and later: subscribe, rebalance
events, synchronous, asynchronous and automatic commits), checked against the Java client in
both directions and in one group with it. The plan is [`docs/design.md`](docs/design.md).

## What works today

```pascal
uses
  Kafka.Types, Kafka.Producer, PascalCommon.Utf8;

var
  Params: TKafkaProducerParams;
  Producer: TKafkaProducer;
  Report: TKafkaDeliveryReport;
begin
  Params := KafkaProducerParams('localhost:9093');  // Java's defaults: acks=all, linger.ms=5...
  Params.Cluster.Connection.UseTls := True;
  Params.Cluster.Connection.TlsCaFile := 'ca.pem';  // a private CA; '' = the system store
  Producer := TKafkaProducer.Create(Params);
  try
    Producer.Open;                                  // the bootstrap
    // Queued at once; the partition is murmur2 of the key, as Java's.
    Report := Producer.Send(KafkaProducerRecord('orders',
      PcStringToUtf8Bytes('customer-42'), PcStringToUtf8Bytes('{"total": 10}'))).Get;
    Writeln('partition ', Report.Partition, ', offset ', Report.Offset);
    // Or a callback per record (TKafkaDeliveryEvent, on the sender thread),
    // then Producer.Flush.
  finally
    Producer.Free;                                  // Close: flushes, then stops
  end;
end;
```

With SASL (here SASL_SSL with SCRAM-SHA-256; `ksmPlain` is PLAIN, as Confluent Cloud uses):

```pascal
uses
  Kafka.Types, Kafka.Sasl, Kafka.Producer;
...
  Params := KafkaProducerParams('broker1:9094,broker2:9094');
  Params.Cluster.Connection.UseTls := True;         // SASL_SSL; without it, SASL_PLAINTEXT
  Params.Cluster.Connection.SaslMechanism := ksmScramSha256;
  Params.Cluster.Connection.SaslUsername := 'orders-service';
  Params.Cluster.Connection.SaslPassword := Secret;  // from the environment, not the source
```

The same three fields on `TKafkaConsumerParams.Cluster.Connection` and on
`TKafkaConnectionParams`. Wrong credentials, a mechanism the broker doesn't enable or a
listener without SASL raise `EKafkaAuthentication` (with the broker's error code and message;
never retried); a client without SASL on a SASL listener is hung up on by the broker
(`EKafkaConnection`). A broker with `connections.max.reauth.ms` (KIP-368) gets a new
connection before each session ends.

OAUTHBEARER takes a token the application got from its issuer (the library runs no OIDC flow),
through a method called at each new connection:

```pascal
function TMyTokens.Current: string;   // thread-safe: called from the library's threads
begin
  Result := FToken;                   // renewed by the application before it expires
end;
...
  Params.Cluster.Connection.SaslMechanism := ksmOAuthBearer;
  Params.Cluster.Connection.SaslOAuthBearerTokenProvider := Tokens.Current;
  SetLength(Params.Cluster.Connection.SaslOAuthBearerExtensions, 1);   // if the broker asks
  Params.Cluster.Connection.SaslOAuthBearerExtensions[0] := KafkaSaslExtension('logicalCluster', 'lkc-1');
```

The broker's session lasts until the token's `exp`; the connection that replaces one near its
end asks for the token again. A refused token raises `EKafkaAuthentication` with the broker's
JSON in `BrokerMessage` (`{"status":"invalid_token"}`).

The producer is **idempotent by default**, as Java's since 3.0: a retry after a lost answer
isn't written twice, and up to five batches of a partition are in flight with their order
kept. It needs `Acks = kaAll` (the default) and `Retries > 0`; with `Acks = kaLeader` or
`kaNone` it is off, as Java's. `Params.Idempotence := kidOff` gives the at-least-once producer,
one batch in flight per partition; `MaxInFlightPerBroker` above 5 needs it.

Reading partitions assigned by hand:

```pascal
uses
  Kafka.Types, Kafka.Consumer, PascalCommon.Utf8;

var
  Params: TKafkaConsumerParams;
  Consumer: TKafkaConsumer;
  Records: TKafkaConsumerRecordArray;
  I: Integer;
  Text: string;
begin
  Params := KafkaConsumerParams('localhost:9092');
  Params.AutoOffsetReset := korEarliest;            // Java's default is latest
  Consumer := TKafkaConsumer.Create(Params);
  try
    Consumer.Open;
    Consumer.Assign([KafkaTopicPartition('orders', 0), KafkaTopicPartition('orders', 1)]);
    while Running do                                // Consumer.Wakeup from another thread
    begin                                           // ends a Poll (EKafkaWakeup)
      Records := Consumer.Poll(1000);
      for I := 0 to High(Records) do
        if PcTryUtf8BytesToString(Records[I].Value, Text) then
          Writeln(Records[I].Partition, '/', Records[I].Offset, ': ', Text);
    end;
  finally
    Consumer.Free;
  end;
end;
```

In a consumer group (KIP-848: the broker assigns the partitions; Kafka 4.0 or later). The
handlers are methods ("of object") and run inside `Poll`, on its thread:

```pascal
uses
  Kafka.Types, Kafka.Group, Kafka.Consumer;

procedure TWorker.Revoked(ASender: TKafkaConsumer; const APartitions: TKafkaTopicPartitionArray);
begin
  // The partitions go to another member after this returns. With
  // EnableAutoCommit (the default) the positions are committed already.
end;

procedure TWorker.Run;
var
  Params: TKafkaConsumerParams;
  Consumer: TKafkaConsumer;
begin
  Params := KafkaConsumerParams('localhost:9092');
  Params.GroupId := 'billing';                      // Java's group.id
  Params.AutoOffsetReset := korEarliest;            // where a partition with no commit starts
  Consumer := TKafkaConsumer.Create(Params);
  try
    Consumer.OnPartitionsRevoked := Revoked;        // also OnPartitionsAssigned, OnPartitionsLost
    Consumer.Open;
    Consumer.Subscribe(['orders']);                 // the first Poll joins the group
    while Running do
      Process(Consumer.Poll(1000));                 // auto-commit inside Poll (every 5 s)
    // Or EnableAutoCommit := False and Consumer.Commit / Consumer.CommitAsync(OnCommitted).
  finally
    Consumer.Free;                                  // Close: commits, leaves the group
  end;
end;
```

One connection to one broker, for the protocol's requests:

```pascal
uses
  Kafka.Types, Kafka.Connection, Kafka.Messages.Metadata;

var
  Params: TKafkaConnectionParams;
  Conn: TKafkaConnection;
  Req: TKafkaMetadataRequest;
  Rsp: TKafkaMetadataResponse;
begin
  Params := KafkaConnectionParams('localhost', 9093);
  Params.UseTls := True;
  Params.TlsCaFile := 'ca.pem';          // a private CA; '' = the system store
  Conn := TKafkaConnection.Create(Params);
  try
    Conn.Open;                           // connects, negotiates ApiVersions, SASL if set
    Req.Init;
    SetLength(Req.Topics, 1);
    Req.Topics[0].Init;
    Req.Topics[0].Name := 'orders';
    Rsp := Conn.Metadata(Req);           // at the highest version both sides know
    Writeln(Length(Rsp.Topics[0].Partitions), ' partitions');
  finally
    Conn.Free;
  end;
end;
```

## Sample

[`samples/ProduceConsume`](samples/ProduceConsume/KafkaSample.dpr): a producer and a consumer
over TLS and SASL, one source for Delphi (`.dproj`) and Lazarus (`.lpi`). With no arguments it
uses the SASL_SSL listener of `docker/docker-compose.yml`; against Confluent Cloud:

```sh
KafkaSample --bootstrap pkc-xxxxx.region.provider.confluent.cloud:9092 --sasl plain \
  --user <API key> --password-env CC_SECRET --ca "" --topic <an existing topic>
```

[`samples/ConsumerGroup`](samples/ConsumerGroup/KafkaGroupSample.dpr): a consumer in a group,
with its handlers and asynchronous and synchronous commits; the same options and `--group`. A
second run starts where the first committed; two at once split the topic's partitions.

## Works with the Java client

`tools/interop.py` checks both directions against the Java client 4.4.0: records the Java
console producer sends (keys with accents and CJK, null keys, headers with null values, an
empty value) read by this library field by field, each keyed record in the partition both
murmur2 implementations agree on; and records this library sends read back by the Java console
consumer with the same timestamp, partition, offset, headers, key and value, every batch of the
codec sent with a valid CRC according to `kafka-dump-log.sh`. Both directions run with gzip,
snappy and lz4; zstd runs from Java to this library, which reads zstd and doesn't write it. It
also puts the Java console consumer
(`group.protocol=consumer`) and this library's consumer in one group: they split the topic's
partitions, hand them over through their commits, and read every record once.

## Requirements

- Delphi 12, or FPC 3.2.2 / Lazarus 4.
- [pascal-common-faa](https://github.com/fabianoallex/pascal-common-faa) **1.28.0 or later**,
  provided once by the application (one search path in Delphi; one registered
  `pascal_common_faa.lpk` in Lazarus). Lazarus: `packages/pascal_kafka_faa.lpk`.
- A Kafka broker of 2.1 or later; consumer groups need 4.0 or later (KIP-848; the classic
  protocol of older brokers isn't there yet). Developed and tested against Apache Kafka 4.4.0.
- SASL: PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 (`ksmScramSha512`, as Amazon MSK uses). User
  name and password are sent as their UTF-8 bytes, without SASLprep (as Java's client).
  OAUTHBEARER with a token the application supplies (no OIDC flow, no token endpoint).
  GSSAPI/Kerberos: no plan.
- TLS certificates (the common library's rule): CA files are PEM on both backends; a client
  certificate is a PEM pair on OpenSSL and a PFX on SChannel. A JKS keystore must be converted
  (`keytool -importkeystore ... -deststoretype PKCS12`, then `openssl pkcs12` for PEM).

## Tests

See [`CLAUDE.md`](CLAUDE.md), "Tests". In short: `sh tools/test_fpc.sh` (FPC, Windows: the
suites, the Java interoperability test and the samples), `sh tools/test_fpc_docker.sh` and
`sh tools/test_interop_docker.sh` (the same on Linux in Docker), `Kafka.groupproj` in the Delphi
IDE. Everything but the unit suite needs `docker compose -f docker/docker-compose.yml up -d`
(plaintext 9092, TLS 9093, SASL_PLAINTEXT 9096, SASL_SSL 9097, and 9098 with 2-second SASL
sessions).

The Confluent Cloud test (`Kafka.CloudIntegrationTests`) runs only with credentials in the
environment: `KAFKA_CLOUD_BOOTSTRAP`, `KAFKA_CLOUD_API_KEY`, `KAFKA_CLOUD_API_SECRET` and an
existing topic in `KAFKA_CLOUD_TOPIC`. Without them it prints a notice and passes.

## License

MIT.
