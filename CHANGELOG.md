# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/). While the version is 0.x, a minor version
may change the API; each such change is listed here.

## [Unreleased]

The foundation of phase K1 (`docs/design.md`, "K1 progress"). Not released: 0.1.0 comes with
the rest of K1 (cluster, producer, consumer, SASL, gzip).

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
- Tests: 91 unit tests (codecs against 48 frames captured from a Kafka 4.4.0 broker and the Java
  client, a fake broker in process, plain and TLS) and 8 integration tests against
  `docker/docker-compose.yml` (plaintext and TLS with a private CA, every TLS backend).
