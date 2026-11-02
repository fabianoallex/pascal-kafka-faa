# pascal-kafka-faa

A native Apache Kafka client for **Delphi** and **Free Pascal/Lazarus** from one source tree,
with no external dependency (no librdkafka). TLS through SChannel or OpenSSL, chosen at run
time.

**Status: early, not released.** Phase K1 is under way: the protocol, the codecs generated from
Kafka 4.4.0's message definitions (ApiVersions, Metadata, Produce, Fetch, ListOffsets), record
batch v2 with gzip, murmur2 partitioning compatible with Java, a connection per broker and the
cluster layer (bootstrap, metadata, leaders). The producer and consumer APIs and SASL come next;
today records are produced and fetched with the protocol's own requests, sent to the leader the
cluster names. The plan is
[`docs/design.md`](docs/design.md).

## What works today

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
    Conn.Open;                           // connects and negotiates ApiVersions
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

## Requirements

- Delphi 12, or FPC 3.2.2 / Lazarus 4.
- [pascal-common-faa](https://github.com/fabianoallex/pascal-common-faa) **1.27.1 or later**,
  provided once by the application (one search path in Delphi; one registered
  `pascal_common_faa.lpk` in Lazarus). Lazarus: `packages/pascal_kafka_faa.lpk`.
- A Kafka broker of 2.1 or later; developed and tested against Apache Kafka 4.4.0.
- TLS certificates (the common library's rule): CA files are PEM on both backends; a client
  certificate is a PEM pair on OpenSSL and a PFX on SChannel. A JKS keystore must be converted
  (`keytool -importkeystore ... -deststoretype PKCS12`, then `openssl pkcs12` for PEM).

## Tests

See [`CLAUDE.md`](CLAUDE.md), "Tests". In short: `sh tools/test_fpc.sh` (FPC, Windows),
`sh tools/test_fpc_docker.sh` (Linux in Docker), `Kafka.groupproj` in the Delphi IDE; the
integration suite needs `docker compose -f docker/docker-compose.yml up -d`.

## License

MIT.
