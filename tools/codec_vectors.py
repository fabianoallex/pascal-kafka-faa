#!/usr/bin/env python3
"""Prints the reference vectors of tests/Unit/Kafka.CompressionTests.pas as Pascal constants.

The bytes come from the reference libraries' Python bindings, not from this
library: cramjam (Google's snappy), lz4 (python-lz4 over the LZ4 library)
and xxhash. They aren't needed by the build or the tests, only to make the
vectors again, so they go in a virtual environment of their own:

    python -m venv .ci/venv
    .ci/venv/Scripts/python -m pip install lz4 cramjam xxhash     (bin/ on Linux)
    .ci/venv/Scripts/python tools/codec_vectors.py

The output replaces the constants between "LIMIT = ..." and the first
function of the test unit. Text and Pseudorandom make the same bytes as the
functions of the same names in the test unit. Made with lz4 4.4.5, cramjam
2.14.0 (2026-11-03).
"""

import cramjam
import lz4.block
import lz4.frame
import xxhash

M = 0xFFFFFFFF


def pseudorandom(n):
    """xorshift32 from 12345, the low byte of each step (Pseudorandom in the tests)."""
    x, out = 12345, bytearray()
    for _ in range(n):
        x ^= (x << 13) & M
        x ^= x >> 17
        x ^= (x << 5) & M
        out.append(x & 0xFF)
    return bytes(out)


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


def main():
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
