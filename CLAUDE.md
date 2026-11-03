# pascal-kafka-faa — Guide for AI agents

A native **Apache Kafka client** for Delphi **and** Lazarus/FPC (dual-compiler), with no external
dependency: no librdkafka. The protocol codecs are generated from Kafka's own message
definitions; TCP and TLS come from pascal-common-faa.

**Read [`docs/design.md`](docs/design.md) first**: the why, the decisions (1 to 4), the phases
(K0 to K4) and where the work stands ("K1 progress"). This file keeps the short rules.

For the general dual-compiler rules (project anatomy, `.inc`, mirrored tests, CI), use the
`dual-compiler-delphi-lazarus` skill; for using the `*-faa` libraries, `using-faa-libraries`.
Both live in [pascal-skills-faa](https://github.com/fabianoallex/pascal-skills-faa) (a Claude
Code plugin). If they aren't loaded, ask the user to install them:
`/plugin marketplace add fabianoallex/pascal-skills-faa`, then
`/plugin install pascal-skills-faa@pascal-skills-faa`.

---

## Language

Everything in this repository is in **English**: code, identifiers, comments, runtime messages,
test names and assertion messages, documentation and commit messages. Test *data* may contain
non-ASCII values on purpose (`'São'`).

---

## Dependencies and versions

- **pascal-common-faa** is a submodule in `external/pascal-common-faa`, at a tag (today
  `v1.28.0`). The library needs **1.28.0 or later** (SHA-512 for SCRAM-SHA-512): `Kafka.Types`
  refuses an older copy at compile time (`PASCALCOMMON_VERSION < 12800`), and
  `packages/pascal_kafka_faa.lpk` says `MinVersion 1.28`. Raise both together, and move the
  submodule to the same tag.
- The `.lpk` requires `pascal_common_faa` **by name only**: the application provides the one
  copy. Only the test `.lpi` files point into `external/` (`Prefer="True"`).
- This library's own version: `KAFKA_FAA_VERSION*` in `Kafka.Types`, the `.lpk` and
  `CHANGELOG.md`, bumped together.

---

## Code rules (every unit in `src/`)

- **`{$I kafka.inc}` right after `unit ...;`.** It turns on `{$MODE DELPHI}{$H+}` on FPC and
  defines `KAFKA_WINDOWS`. There is no TLS define: the backend is chosen at run time
  (`TKafkaConnectionParams.TlsBackend`).
- **Prefixes:** units `Kafka.*`, types `TKafka*`, interfaces `IKafka*`, free functions `Kafka*`,
  exceptions `EKafka*`, constants `KAFKA_*`.
- **`uses` without namespaces** (`SysUtils`, `Generics.Collections`). No anonymous methods, no
  `reference to`, no inline `var`, no `TEncoding.UTF8` (text from the wire goes through
  `PcTryUtf8BytesToString`/`PcStringToUtf8Bytes`). Payloads are `TBytes`, never `string`.
- **Intentional 32/64-bit wrap-around** (CRC32C, zigzag, murmur2): `{$OVERFLOWCHECKS OFF}`
  `{$RANGECHECKS OFF}` in the unit, intermediates in `Cardinal`/`UInt64`. Delphi's Debug
  configuration turns both checks on, so any other arithmetic that may wrap (a counter) must be
  written not to (see `NextCorrelationId` in `Kafka.Connection`).
- **Big-endian by hand**, byte by byte; no `Swap`/`SwapEndian`.
- **The exception boundary** (pascal-common-faa's T6): no `EPcTransport*` leaves the library.
  `Kafka.Transport` turns them into `EKafkaConnection`/`EKafkaTls`. The tree is in `Kafka.Types`.
- **No lazily created shared state**: tables and globals are filled in `initialization`.
- **Top-of-file comment in every unit, program and test**: one sentence on what it is, then why,
  the decisions and the Delphi × FPC points. Plain prose.
- **Source files:** UTF-8 with BOM for `.pas`/`.dpr`/`.lpr`/`.dproj`/`.groupproj`, none for
  `.inc`/`.lpi`/`.lpk`/`.lpg`; LF everywhere (`.gitattributes`).

---

## Generated code: never edit by hand

| Files | Generator | From |
|---|---|---|
| `src/Kafka.Messages.*.pas`, `src/Kafka.Errors.pas` | `python tools/gen_messages.py` | `tools/kafka-messages/<tag>/*.json`, `Errors.java` and `error-classes.tsv` |
| `tests/Unit/Kafka.TestCaptures.pas`, `tests/Unit/Kafka.TestPartitions.pas` | `python tools/gen_test_captures.py` | `tests/data/captures-<tag>.jsonl`, `tests/data/partitions-java-<tag>.tsv` ([`docs/codec-captures.md`](docs/codec-captures.md)) |
| `tests/*/fpc/*Tests.pas` | `python tools/gen_fpc_mirror.py` | the DUnitX masters in `tests/*/` |

`sh tools/check_generated.sh` fails when any of them is out of date (the test scripts run it
first).

- **A new API** = copy its `*Request.json`/`*Response.json` from the same Kafka tag into
  `tools/kafka-messages/<tag>/`, add its name to `APIS` in `gen_messages.py`, run it, add the new
  unit to the `.lpk`, the `.pas` package file and both `.dpr` (and `.dproj`). Then capture it
  (the doc above). A request marked `latestVersionUnstable` loses its last version, and its
  response with it (InitProducerId stops at v5 on 4.4.0).
- **Retriable or not, refresh or not** comes from each error's Java exception class
  (`KafkaErrorIsRetriable`, `KafkaErrorIsInvalidMetadata`), never from a hand-written list:
  `error-classes.tsv` is written by `python tools/fetch_error_classes.py` (the only tool that
  needs the network: it reads the classes from GitHub at the tag). Run it with each new tag.
- **The Kafka tag is 4.4.0** (`KAFKA_TAG` in `gen_messages.py`, decided 2026-11-02: the newest
  stable 4.x that day). The broker in `docker/` runs the same tag.
- `records` fields are raw bytes (nullable, null by default): the batches inside are
  `Kafka.Protocol.RecordBatch`'s, by hand. Tagged structs are written when not `IsDefault`.
  A nullable struct has an `<Name>IsNull` flag and Java's int8 marker (-1 null, 1 present).
  What the generator doesn't support yet (a nullable struct that is also tagged) it refuses
  with a message; add the support with the first API that needs it.

---

## Tests

- Masters: **DUnitX** files in `tests/Unit/*Tests.pas` and `tests/Integration/*Tests.pas`,
  written in **FPCUnit's assertion dialect** (`TAssert.AssertEquals/AssertTrue/...`), provided on
  Delphi by `tests/Unit/Kafka.DUnitXCompat.pas` (the skill's canonical copy:
  `check_test_kit.py` reports drift). Helpers whose names don't end in `Tests`
  (`Kafka.TestHelpers`, `Kafka.TestFakeBroker`, `Kafka.TestCaptures`, `Kafka.TestMessages`,
  `Kafka.TestPartitions`, `Kafka.TestFakeGroup`, and in `tests/Integration` `Kafka.TestProxy`)
  are shared, not mirrored.
- **A new test unit** ends with `initialization TDUnitX.RegisterTestFixture(...)` (the mirror
  turns it into `RegisterTest`) and goes into the `.dpr`, the `.dproj` and the FPC `.lpr`.
  Without the registration both runners compile it and run none of it, silently.
- **Unit suite**: no network beyond loopback. `Kafka.ConnectionTests` runs against an
  in-process fake broker (`Kafka.TestFakeBroker`), plain and TLS (pascal-common-faa's
  `tests/pki`); `Kafka.ClusterTests` and `Kafka.ProducerTests` against several, given the same
  `ClusterView` (`KafkaFakeClusterView`, `KafkaFakeUseView`); `Kafka.ConsumerTests` too. The
  fake also answers Produce (and can fail, hold or drop it), Fetch (with a long poll, on a
  thread of its own: `WaitParked` tells a test the Fetch is waiting) and ListOffsets, and keeps
  the batches per partition (`AppendBatch`, `AppendRawBatch` for another client's bytes as
  they are, `SetLogStart`), and speaks SASL (`SaslMechanisms`, one user; PLAIN, and
  SCRAM-SHA-256 and SCRAM-SHA-512 written as a server, apart from `Kafka.Sasl`): before a
  client authenticates it answers only ApiVersions and SASL, as a broker. It serves any number
  of connections at once (a thread each, with its own SASL state; `DropClient` aborts them
  all), and it is every group's coordinator (`OfferGroups`, on with `KafkaFakeUseView`;
  `Kafka.TestFakeGroup`): FindCoordinator (`FindCoordinatorMax` for an older broker, injected
  errors), heartbeats with a round-robin assignor that moves a partition only after its owner
  let it go, fencing (`FenceNext`, `RemoveMember`), passive members (`AddMember`), OffsetCommit
  and OffsetFetch with the member's epoch checked; `WaitMemberOwns`, `WaitMemberHeartbeats`
  and the like wait on its monitor. `Kafka.GroupTests` (the membership) and
  `Kafka.ConsumerGroupTests` (the consumer in a group, two of them on one fake) run against it.
  It answers InitProducerId and checks idempotent batches' sequences as Kafka 4.4.0 does
  (`LoseProduceAnswers` writes a batch and loses its answer; `StoredBatches`):
  `Kafka.IdempotentProducerTests`. `ReleaseHeldProduce` answers in arrival order, the ones
  that came meanwhile too (a broker answers a connection in order).
- **The codecs' vectors** (`Kafka.CompressionTests`; `Kafka.ZstdTests`, from
  `tools/codec_vectors.py zstd`): hex constants written by `tools/codec_vectors.py` with the
  reference libraries (cramjam, lz4, xxhash and zstandard for Python, in a venv of their own:
  the build never needs them). The Java producer's own snappy, lz4 and zstd batches are
  captures (`docs/codec-captures.md`). zstd is read, not written (`KafkaCompressionSupported`
  is writing, `KafkaDecompressionSupported` reading).
- **Integration suite**: the broker of `docker/docker-compose.yml` (Kafka 4.4.0, KRaft, one
  node; 9092 plaintext, 9093 TLS with the common's test PKI, 9096 SASL_PLAINTEXT, 9097
  SASL_SSL, 9098 SASL_PLAINTEXT with 2-second sessions; PLAIN alice / alice-secret,
  SCRAM-SHA-256 bob / bob-secret, SCRAM-SHA-512 carol / carol-secret):
  `docker compose -f docker/docker-compose.yml up -d`. A change to the compose
  file needs the container recreated (`up -d` does it; a change to `docker/kafka-start.sh`
  alone needs `up -d --force-recreate`). `kafka-start.sh` creates the topic
  `pascal-kafka-faa-it-zstd` (`compression.type=zstd`) and the healthcheck waits for it: the
  broker writes zstd there with Java's codec, which is where the integration suite gets zstd.
- **`Kafka.TestProxy`** (integration): relays a real broker's connections, rewrites
  Metadata's brokers to itself (gotcha 9), loses Produce answers on request, counts the Produce
  requests in flight; `Kafka.IdempotenceIntegrationTests` (K3's criterion) runs through it.
- **Interoperability with the Java client:** `tools/interop.py --pascal <program>` (the
  library's side is `tests/Interop`, a console program, not a fixture: the Java side runs by
  `docker exec` in the broker's container, which a Linux test container can't do). Its records
  are `tests/data/interop-records.tsv`, the Java console producer's input format. The topics
  it creates have `retention.ms=-1` (its records' fixed 2026-01-01 timestamps would otherwise
  be deleted at once by retention). Both directions run once per codec (gzip, snappy, lz4),
  and `kafka-dump-log.sh` checks every batch's codec. The last scenario puts the Java console
  consumer (`group.protocol=consumer`, in the background) and the program's `group` mode in
  one group (`interop-group-<n>`, 6 partitions) and checks every record is read once.
- **The samples** `samples/ProduceConsume` (K1: `Assign`) and `samples/ConsumerGroup` (K2: a
  group, `KafkaGroupSample`), one `.dpr` each for Delphi and Lazarus, run with their defaults
  against the SASL_SSL listener in the test scripts; each must print `done`.
- **Confluent Cloud** (`Kafka.CloudIntegrationTests`): opt-in, `KAFKA_CLOUD_BOOTSTRAP`,
  `KAFKA_CLOUD_API_KEY`, `KAFKA_CLOUD_API_SECRET`, `KAFKA_CLOUD_TOPIC` (an existing topic);
  without them a notice and a pass. Never put credentials in the repository.
  `KAFKA_CLOUD_CA_FILE` points it at a private CA, to try it on `localhost:9097` with alice.
- **FPC on Windows:** `sh tools/test_fpc.sh` (both suites, then the interoperability test and
  the samples; everything but the unit suite is skipped with a notice when the broker isn't
  running, `KAFKA_IT=require` makes that a failure). **Linux:** `sh tools/test_fpc_docker.sh`
  (FPC 3.2.2, Debian 12, OpenSSL 3; the integration container shares the broker's network
  namespace) and `sh tools/test_interop_docker.sh` (interoperability and samples). **CI:**
  `.github/workflows/ci.yml` only calls `sh tools/ci-test.sh`, which runs both.
- **Delphi:** open `Kafka.groupproj`, run `tests/Unit/Kafka.UnitTests.dproj` and
  `tests/Integration/Kafka.IntegrationSuite.dproj` on Win32 and Win64; build
  `tests/Interop/KafkaInterop.dproj`, `samples/ProduceConsume/KafkaSample.dproj` and
  `samples/ConsumerGroup/KafkaGroupSample.dproj` and run them
  (`python -I tools/interop.py --pascal tests/Interop/Win64/Debug/KafkaInterop.exe`; the
  samples with no arguments). A leak in those console programs shows FastMM's message box,
  which blocks the script until closed. Delphi Community Edition
  doesn't compile from the command line: `dcc32` prints "This version of the product does not
  support command line compiling." and **exits with code 0**. Don't read that as success.
- **Acceptance criterion for every commit:** FPC Windows x64 + Linux (Docker) + Delphi 12
  Win32/Win64, every test green and **0 leaks** (heaptrc, `ReportMemoryLeaksOnShutdown`).
- **No `Sleep` in tests**: waits have deadlines and are woken by events. The exceptions, both
  in the integration suite: waiting for the broker to elect a leader for a new topic, and the
  KIP-368 tests waiting for a 2-second SASL session to end (in a consumer's `Poll`, looped to
  a deadline). Anything with threads: also three Linux runners at once with `--cpus=1`
  (`.ci/stress.sh`, `RUNS=5`).
- Floating point always with an explicit delta.

---

## Gotchas found here

1. **A plaintext client on a TLS listener** reads the broker's TLS alert (`15 03 03 00 02 02
   50`) as a size prefix of 352 518 912 bytes. The connection refuses sizes above
   `MaxResponseBytes` and says the bytes look like TLS (`Kafka.Connection.ReaderLoop`).
2. **A plaintext server hanging up during a TLS handshake** gives `EKafkaConnection`, not
   `EKafkaTls`: the common keeps the raw stream's exception class through the handshake. A test
   that wants `EKafkaTls` needs a real TLS failure (a wrong CA, a wrong name).
3. **Kafka 4's `kafka-topics.sh --describe` doesn't send Metadata** (it uses
   DescribeTopicPartitions, API 75). To capture a Metadata answer for a topic, use the console
   producer.
4. **Git Bash rewrites container paths** in `docker exec ... /opt/kafka/bin/...`
   (`C:/Program Files/Git/opt/...`): `MSYS_NO_PATHCONV=1`.
5. **FPC 3.2.2 failed with "Compilation raised exception internally / I/O error: File not
   open"** (at `PascalCommon.Version`) when `-FU` pointed into the session's temp folder, an 8.3
   path (`C:\Users\FABIAN~1.ARN\...`). The same command with `-FU` inside the repository
   (`.ci/`, ignored) worked. The short path is the likely cause; it wasn't isolated further.
6. **Delphi: two `array of T` declarations are two types.** A field `FHeld: array of THeld`
   and a local `LHeld: array of THeld` are not assignment-compatible on Delphi (E2008
   "Incompatible types"); FPC accepts it. Declare a named type (`THeldArray`) and use it on both
   sides.
7. **Delphi: `WriteLn` inside a DUnitX test class** resolves to a one-parameter `WriteLn` that
   DUnitX brings into scope (E2034 "Too many actual parameters"). Write `System.WriteLn`.
8. **`kafka-console-producer.sh` ignores `--producer-property compression.type=gzip`**: its own
   `--compression-codec` (default none) overrides it. Use `--compression-codec gzip`. Also: with
   `parse.headers=true` a line must have a header (`k:v`); `--reader-property null.marker=NULL`
   gives a null key or header value.
9. **Only the bootstrap connection went through the capture proxy**: Metadata advertises
   `localhost:9092`, so Produce and Fetch went to the broker directly.
   `docker/docker-compose.capture.yml` adds a listener advertised as the proxy's address
   (`docs/codec-captures.md`).
10. **Python on Windows writes CRLF** in text mode: the capture scripts open their output with
    `newline="\n"`. Python edits sent through a Bash heredoc also had a `\n` inside a string
    turn into a real line break (twice); a script file written with the Write tool avoids it.
11. **The broker cuts a Fetch at the byte limit inside a batch** (measured: `fetch-v18-cut`, 2
    whole batches and 58 bytes of the third). `KafkaDecodeRecordBatches` returns the whole ones
    and the size of the tail.
12. **A list of error codes checked by hand against Java's exception classes missed four**
    (NETWORK_EXCEPTION, 80, 83, 84 extend `InvalidMetadataException` too). Such lists are
    generated now (`error-classes.tsv`, "Generated code" above).
13. **On Windows a short wait lasts a timer tick**: `TEvent.WaitFor(5)` returns after ~15.6 ms,
    so a lone record with the default `LingerMs` of 5 takes ~15 ms to go (measured). Through
    Docker Desktop's port forwarding the broker's round trip is also several times Linux's
    (20 000 records: 3 s against 0.79 s). Measure throughput on Linux.
14. **The Delphi IDE once missed that a `.dpr`/`.dproj` changed on disk** (two units added by a
    script while the group was open): it built its loaded copy, without them, and said nothing;
    the suite ran 167 tests instead of 183. A later build asked to reload. After adding units,
    compare the Delphi runner's "Tests Found" with FPC's count.
15. **The broker formatted a log directory nobody used.** With any `KAFKA_*` variable the
    image doesn't read its own `server.properties`, so `log.dirs` is Kafka's default,
    `/tmp/kafka-logs`, not the image's `/tmp/kraft-combined-logs`. A `kafka-storage.sh format
    --add-scram` run on the latter had no effect, and SCRAM failed with the right password.
    `KAFKA_LOG_DIRS` is set in the compose file (`docker/kafka-start.sh`).
16. **What a broker does with SASL mistakes:** a SASL client on a listener without SASL gets
    ILLEGAL_SASL_STATE in the SaslHandshake answer; a client without SASL on a SASL listener
    gets ApiVersions answered and is hung up on at its next request (`EKafkaConnection`, no
    error code); after a session expires (KIP-368) the broker hangs up at the next request.
    All three measured against 4.4.0 (`Kafka.SaslIntegrationTests`).
17. **Bash heredocs keep breaking edit scripts** (gotcha 10, three more times in the SASL
    session): "unexpected EOF while looking for matching `'`" with nothing written, and a
    backslash-n inside a Python string turned into a real line break in a Markdown file (and in
    this very item). Write the script with the Write tool, then run it.
18. **A compiler directive inside a brace comment ends it.** `{ ... (FPC's {$MODE DELPHI}) ... }`
    closes at the directive's `}`, and FPC then reports a syntax error far from the cause
    ("INTERFACE expected but ')' found" on a unit's header comment). Name directives in prose
    inside `{ }` comments, or use `//`.
19. **pascal-common-faa's `TPcListener.Accept` returns nil once the listener is closed**; it
    doesn't raise. An accept loop that only catches exceptions spins forever (the proxy hung a
    fixture's teardown). Stop on nil, as `Kafka.TestFakeBroker` does.
20. **`KafkaPeekCorrelationId` reads a response's correlation id** (bytes 0 to 3). A request's
    is at bytes 4 to 7, after the API key and version.
21. **python-lz4 writes a one-block frame with independent blocks** whatever `block_linked`
    says (LZ4F turns a frame of one block independent). A dependent-blocks frame for a test is
    built by hand: FLG 40 and the header checksum recomputed.
22. **librdkafka writes raw snappy**, with no xerial framing (`rd_kafka_snappy_compress_slice`);
    Java writes xerial's. A reader must take both, told apart by xerial's magic (16 bytes or
    more starting with `82 53 4E 41 50 50 59 00`).
23. **python zstandard's `decompressobj` stops at the end of the first frame** and keeps the
    rest as unused data: it reads one frame of several and "accepts" bytes after it. Java's
    stream (zstd-jni over `ZSTD_decompressStream`) reads every frame and refuses bytes after
    the last one unless they start a magic number. `ZstdDecompressor().decompress` reads one
    frame too. Check Java's behaviour in libzstd's source, not with python zstandard.
24. **Java's zstd frames have no content size**: its producer writes through a stream of
    unknown size, so the frame has a window descriptor (2 MiB at level 3), no
    Frame_Content_Size and no checksum, and ends with an empty raw block. librdkafka and Python
    write the content size. A decoder bounded by the content size alone would allocate
    nothing up front for Java's batches: the limit has to hold block by block too.
