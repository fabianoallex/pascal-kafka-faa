#!/usr/bin/env python3
"""Prints the reference vectors of the codec tests as Pascal constants.

The bytes come from the reference libraries' Python bindings, not from this
library: cramjam (Google's snappy), lz4 (python-lz4 over the LZ4 library),
xxhash, and zstandard (over libzstd). They aren't needed by the build or
the tests, only to make the vectors again, so they go in a virtual
environment of their own:

    python -m venv .ci/venv
    .ci/venv/Scripts/python -m pip install lz4 cramjam xxhash zstandard   (bin/ on Linux)
    .ci/venv/Scripts/python tools/codec_vectors.py
    .ci/venv/Scripts/python tools/codec_vectors.py zstd

The first prints tests/Unit/Kafka.CompressionTests.pas's constants (between
"LIMIT = ..." and its first function), the second
tests/Unit/Kafka.ZstdTests.pas's (between "LIMIT = ..." and its first
function). Text, Pseudorandom, Letters and LowBytes make the same bytes as the
functions of the same names in the test units. Made with lz4 4.4.5, cramjam
2.14.0 (2026-11-03); zstandard 0.25.0 over libzstd 1.5.7, the libzstd of
Java 4.4.0's zstd-jni 1.5.7-17 (2026-11-03).
"""

import sys

import cramjam
import lz4.block
import lz4.frame
import xxhash
import zstandard

M = 0xFFFFFFFF


def xorshift(n):
    """xorshift32 from 12345, each step."""
    x = 12345
    for _ in range(n):
        x ^= (x << 13) & M
        x ^= x >> 17
        x ^= (x << 5) & M
        yield x


def pseudorandom(n):
    """The low byte of each xorshift32 step (Pseudorandom in the tests)."""
    return bytes(x & 0xFF for x in xorshift(n))


def text(n):
    return b"".join(b"record %d of a batch, key=k%d; " % (i, i % 17) for i in range(n))


def const(name, data, comment):
    h = data.hex()
    lines = [h[i:i + 76] for i in range(0, len(h), 76)] or [""]
    print(f"  // {comment}")
    if len(lines) == 1:
        print(f"  {name} = '{lines[0]}';")
        return
    print(f"  {name} = '{lines[0]}' +")
    for line in lines[1:-1]:
        print(f"    '{line}' +")
    print(f"    '{lines[-1]}';")


def letters(n, k):
    """'a' plus each xorshift32 step mod k (Letters in the zstd tests)."""
    return bytes(97 + x % k for x in xorshift(n))


def low_bytes(n, k):
    """Each xorshift32 step mod k (LowBytes in the zstd tests)."""
    return bytes(x % k for x in xorshift(n))


def zstd_blocks(data, level, step):
    """A frame flushed every step bytes (FLUSH_BLOCK): one block each, so a
    block can repeat the previous one's tables."""
    co = zstandard.ZstdCompressor(level=level).compressobj()
    parts = []
    for i in range(0, len(data), step):
        parts.append(co.compress(data[i:i + step]))
        parts.append(co.flush(zstandard.COMPRESSOBJ_FLUSH_BLOCK))
    parts.append(co.flush())
    return b"".join(parts)


def zstd_as_java(data):
    """As Java's producer: a stream of unknown size, level 3, written in
    16 KiB pieces (ZstdCompression's BufferedOutputStream)."""
    co = zstandard.ZstdCompressor(level=3).compressobj()
    return b"".join(co.compress(data[i:i + 16384]) for i in range(0, len(data), 16384)) + co.flush()


def zstd_vectors():
    z = lambda data, level=3, **kw: zstandard.ZstdCompressor(level=level, **kw).compress(data)
    const("ZSTD_HELLO", z(b"hello kafka"),
          'zstd frame of "hello kafka", level 3 (python zstandard over libzstd 1.5.7, Java\'s)')
    const("ZSTD_EMPTY", z(b""), "zstd frame of nothing")
    const("ZSTD_TEXT", z(text(200), write_checksum=True),
          "Text(200), level 3, content checksum: Huffman literals, FSE tables")
    const("ZSTD_TEXT_AS_JAVA", zstd_as_java(text(200)),
          "Text(200) as Java writes it: a window descriptor, no content size")
    const("ZSTD_TEXT_19", z(text(200), 19, write_content_size=False),
          "Text(200), level 19: Huffman literals in 4 streams")
    const("ZSTD_LETTERS_ONE_STREAM", z(letters(50, 2), 19),
          "Letters(50, 2), level 19: one Huffman stream, FSE weights, no sequences")
    const("ZSTD_LOW_DIRECT", z(low_bytes(100, 5), 1),
          "LowBytes(100, 5), level 1: Huffman weights written directly, predefined tables")
    const("ZSTD_LETTERS_FSE", z(letters(200, 2), 9), "Letters(200, 2), level 9: three FSE tables")
    const("ZSTD_LETTERS_ML_RLE", z(letters(100, 4)), "Letters(100, 4), level 3: an RLE match length table")
    const("ZSTD_LETTERS_MANY", z(letters(1000, 2), 3, write_content_size=False),
          "Letters(1000, 2), level 3: a 2-byte number of sequences")
    const("ZSTD_LETTERS_TREELESS", zstd_blocks(letters(1000, 2), 19, 500),
          "Letters(1000, 2), level 19, a block per 500 bytes: the previous Huffman tree reused")
    const("ZSTD_TEXT_REPEAT", zstd_blocks(text(40), 19, 500),
          "Text(40), level 19, a block per 500 bytes: the previous blocks' tables repeated")
    const("ZSTD_TEXT_RLE", zstd_blocks(text(40), 9, 500),
          "Text(40), level 9, a block per 500 bytes: RLE tables")
    const("ZSTD_TEXT_ML_REPEAT", zstd_blocks(text(20), 9, 500),
          "Text(20), level 9, a block per 500 bytes: the match length table repeated")
    const("ZSTD_ZEROS", z(bytes(300000), 9),
          "300000 zeros, level 9: a 4-byte content size, RLE and compressed blocks")
    const("ZSTD_ZEROS_RLE", zstd_blocks(bytes(1000), 9, 500),
          "1000 zeros, level 9, a block per 500 bytes: RLE blocks")
    const("ZSTD_RANDOM", z(pseudorandom(300)), "Pseudorandom(300), level 3: a raw block")
    const("ZSTD_RANDOM_THEN_RUN", z(pseudorandom(20) + b"abcdefgh" * 10),
          'Pseudorandom(20), "abcdefgh" 10 times: literal and match lengths with extra bits')


def main():
    if sys.argv[1:] == ["zstd"]:
        zstd_vectors()
        return
    snappy = lambda data: bytes(cramjam.snappy.compress_raw(data))
    const("SNAPPY_HELLO", snappy(b"hello kafka"),
          'snappy raw block of "hello kafka" (cramjam, the reference library)')
    const("SNAPPY_TEXT", snappy(text(200)), "snappy raw block of Text(200)")
    const("SNAPPY_RANDOM", snappy(pseudorandom(300)),
          "snappy raw block of Pseudorandom(300): one literal, a 2-byte length")
    const("SNAPPY_RUN", snappy(b"ab" * 500), 'snappy raw block of "ab" 500 times: overlapping copies')
    frames = [
        ("LZ4_HELLO", b"hello kafka", dict(store_size=False),
         'lz4 frame of "hello kafka" (python-lz4 4.4.5, the reference library), no options'),
        ("LZ4_TEXT_ALL", text(200), dict(block_checksum=True, content_checksum=True, store_size=True),
         "lz4 frame of Text(200), block and content checksums, content size"),
        ("LZ4_TEXT_4MB", text(200), dict(block_size=lz4.frame.BLOCKSIZE_MAX4MB, store_size=False),
         "lz4 frame of Text(200), 4 MiB blocks"),
        ("LZ4_TEXT_HC", text(200), dict(compression_level=12, store_size=False),
         "lz4 frame of Text(200), LZ4HC level 12"),
        ("LZ4_ZEROS_BLOCKS", bytes(150000), dict(block_checksum=True, store_size=False),
         "lz4 frame of 150000 zeros: three 64 KiB blocks, each with its checksum"),
        ("LZ4_RANDOM", pseudorandom(300), dict(store_size=False),
         "lz4 frame of Pseudorandom(300): the block stored uncompressed"),
    ]
    for name, data, opts, comment in frames:
        # Independent blocks: Kafka refuses dependent ones. (A frame of one
        # block is written independent anyway, whatever block_linked says:
        # a dependent frame for the tests is made by hand.)
        const(name, lz4.frame.compress(data, block_linked=False, **opts), comment)
    const("LZ4_BLOCK_TEXT", lz4.block.compress(text(200), store_size=False),
          "one lz4 block of Text(200)")
    # Checked by hand in the tests: the header checksum of FLG 60, BD 40.
    assert (xxhash.xxh32(b"\x60\x40").intdigest() >> 8) & 0xFF == 0x82


if __name__ == "__main__":
    main()
