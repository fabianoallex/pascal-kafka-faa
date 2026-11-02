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
  `v1.27.1`). The library needs **1.27.1 or later**: `Kafka.Types` refuses an older copy at
  compile time (`PASCALCOMMON_VERSION < 12701`), and `packages/pascal_kafka_faa.lpk` says
  `MinVersion 1.27.1`. Raise both together, and move the submodule to the same tag.
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
| `src/Kafka.Messages.*.pas`, `src/Kafka.Errors.pas` | `python tools/gen_messages.py` | `tools/kafka-messages/<tag>/*.json` and `Errors.java` |
| `tests/Unit/Kafka.TestCaptures.pas` | `python tools/gen_test_captures.py` | `tests/data/captures-<tag>.jsonl` ([`docs/codec-captures.md`](docs/codec-captures.md)) |
| `tests/*/fpc/*Tests.pas` | `python tools/gen_fpc_mirror.py` | the DUnitX masters in `tests/*/` |

`sh tools/check_generated.sh` fails when any of them is out of date (the test scripts run it
first).

- **A new API** = copy its `*Request.json`/`*Response.json` from the same Kafka tag into
  `tools/kafka-messages/<tag>/`, add its name to `APIS` in `gen_messages.py`, run it, add the new
  unit to the `.lpk`, the `.pas` package file and both `.dpr`. Then capture it (the doc above).
- **The Kafka tag is 4.4.0** (`KAFKA_TAG` in `gen_messages.py`, decided 2026-11-02: the newest
  stable 4.x that day). The broker in `docker/` runs the same tag.
- What the generator doesn't support yet (the `records` type, nullable structs, tagged structs)
  it refuses with a message; add the support with the first API that needs it.

---

## Tests

- Masters: **DUnitX** files in `tests/Unit/*Tests.pas` and `tests/Integration/*Tests.pas`,
  written in **FPCUnit's assertion dialect** (`TAssert.AssertEquals/AssertTrue/...`), provided on
  Delphi by `tests/Unit/Kafka.DUnitXCompat.pas` (the skill's canonical copy:
  `check_test_kit.py` reports drift). Helpers whose names don't end in `Tests`
  (`Kafka.TestHelpers`, `Kafka.TestFakeBroker`, `Kafka.TestCaptures`) are shared, not mirrored.
- **Unit suite**: no network beyond loopback. `Kafka.ConnectionTests` runs against an
  in-process fake broker (`Kafka.TestFakeBroker`), plain and TLS (pascal-common-faa's
  `tests/pki`).
- **Integration suite**: the broker of `docker/docker-compose.yml` (Kafka 4.4.0, KRaft, one
  node; 9092 plaintext, 9093 TLS with the common's test PKI):
  `docker compose -f docker/docker-compose.yml up -d`.
- **FPC on Windows:** `sh tools/test_fpc.sh` (both suites; the integration one is skipped with a
  notice when the broker isn't running, `KAFKA_IT=require` makes that a failure).
  **Linux:** `sh tools/test_fpc_docker.sh` (FPC 3.2.2, Debian 12, OpenSSL 3; the integration
  container shares the broker's network namespace). **CI:** `.github/workflows/ci.yml` only
  calls `sh tools/ci-test.sh`.
- **Delphi:** open `Kafka.groupproj`, run `tests/Unit/Kafka.UnitTests.dproj` and
  `tests/Integration/Kafka.IntegrationSuite.dproj` on Win32 and Win64. Delphi Community Edition
  doesn't compile from the command line: `dcc32` prints "This version of the product does not
  support command line compiling." and **exits with code 0**. Don't read that as success.
- **Acceptance criterion for every commit:** FPC Windows x64 + Linux (Docker) + Delphi 12
  Win32/Win64, every test green and **0 leaks** (heaptrc, `ReportMemoryLeaksOnShutdown`).
- **No `Sleep` in tests**: waits have deadlines and are woken by events. The one exception is
  the integration suite waiting for the broker to elect a leader for a new topic. Anything with
  threads: also three Linux runners at once with `--cpus=1`.
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
