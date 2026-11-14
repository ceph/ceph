#!/usr/bin/env python3
"""
Byte-range GET integration test.

Golden object: 4 MiB = 1,048,576 little-endian uint32 values 0 .. 1_048_575.
Each absolute byte index p holds byte (p % 4) of uint32(p // 4).

Verification splits the response into three regions:
  1. First partial word (1-3 bytes) when offset is not 4-byte aligned
  2. Middle: full uint32 compare (4-byte aligned)
  3. Last partial word (1-3 bytes) when range end is not 4-byte aligned
"""

from __future__ import annotations

import argparse
import random
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

OBJECT_SIZE = 4 * 1024 * 1024
NUM_WORDS = OBJECT_SIZE // 4
ITERATIONS = 10

# Object and verifier both use little-endian uint32 (matches x86/arm LE hosts).
# "<I" = uint32 little-endian; byte k of word N is (N >> (8*k)) & 0xFF.
UINT32_LE = "<I"


def assert_little_endian_layout() -> None:
    """Sanity check: partial-word slicing must use LE, not native/BE."""
    # word 0x01020304 -> LE bytes [04, 03, 02, 01]
    le = struct.pack(UINT32_LE, 0x01020304)
    if le != b"\x04\x03\x02\x01":
        raise RuntimeError(f"unexpected LE layout: {le.hex()}")
    # byte index 1 of word 1 is the second LE byte of value 1 -> 0x00
    if struct.pack(UINT32_LE, 1)[1:2] != b"\x00":
        raise RuntimeError("word 1 LE byte[1] must be 0x00")
    # BE would give byte[1] == 0x00 for value 1 too, but word 256 differs:
    if struct.pack(UINT32_LE, 256)[1:2] != b"\x01":
        raise RuntimeError("word 256 LE byte[1] must be 0x01 (BE would be 0x00)")


def build_golden() -> bytes:
    return b"".join(struct.pack(UINT32_LE, i) for i in range(NUM_WORDS))


def get_range(endpoint: str, bucket: str, key: str, offset: int, read_size: int, out_path: Path) -> None:
    end = offset + read_size - 1
    cmd = [
        "aws",
        "--endpoint-url",
        endpoint,
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        key,
        "--range",
        f"bytes={offset}-{end}",
        str(out_path),
    ]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def verify_partial_word(
    got: bytes,
    got_start: int,
    object_offset: int,
    nbytes: int,
    label: str,
) -> None:
    if nbytes == 0:
        return
    word_idx = object_offset // 4
    byte_in_word = object_offset % 4
    # Slice LE-encoded word bytes; byte_in_word 0 = LSB, 3 = MSB.
    expected = struct.pack(UINT32_LE, word_idx)[byte_in_word : byte_in_word + nbytes]
    got_frag = got[got_start : got_start + nbytes]
    if got_frag != expected:
        raise AssertionError(
            f"{label}: object offset {object_offset} word={word_idx} "
            f"byte_in_word={byte_in_word} len={nbytes} "
            f"got={got_frag.hex()} expected={expected.hex()}"
        )


def verify_range_data(got: bytes, offset: int, read_size: int) -> None:
    if len(got) != read_size:
        raise AssertionError(f"length {len(got)} != expected read_size {read_size}")

    end = offset + read_size
    pos = 0

    # --- 1. First partial word (1-3 bytes) ---
    head_len = 0
    if offset % 4 != 0:
        head_len = min(4 - (offset % 4), read_size)
        verify_partial_word(got, pos, offset, head_len, "first partial word")
        pos += head_len

    remaining = read_size - head_len
    tail_len = remaining % 4
    middle_len = remaining - tail_len

    # --- 2. Middle: full uint32 compare ---
    if middle_len > 0:
        if middle_len % 4 != 0:
            raise AssertionError(f"internal error: middle_len {middle_len} not multiple of 4")
        middle_off = offset + head_len
        first_word = middle_off // 4
        nwords = middle_len // 4
        got_words = struct.unpack(f"<{nwords}I", got[pos : pos + middle_len])
        expected_words = tuple(first_word + i for i in range(nwords))
        if got_words != expected_words:
            for i, (g, e) in enumerate(zip(got_words, expected_words)):
                if g != e:
                    raise AssertionError(
                        f"middle uint32 mismatch at word index {first_word + i}: "
                        f"got {g} expected {e}"
                    )
            raise AssertionError("middle uint32 mismatch")
        pos += middle_len

    # --- 3. Last partial word (1-3 bytes) ---
    if tail_len > 0:
        tail_object_offset = end - tail_len
        verify_partial_word(got, pos, tail_object_offset, tail_len, "last partial word")
        pos += tail_len

    if pos != read_size:
        raise AssertionError(f"internal error: consumed {pos} bytes, expected {read_size}")


def run_iterations(
    endpoint: str, bucket: str, key: str, golden: bytes, seed: int | None
) -> None:
    rng = random.Random(seed)
    with tempfile.TemporaryDirectory(prefix="kv-byte-range-") as tmp:
        tmp_path = Path(tmp)
        for n in range(1, ITERATIONS + 1):
            offset = rng.randrange(0, OBJECT_SIZE)
            max_read = OBJECT_SIZE - offset
            read_size = rng.randrange(1, max_read + 1)
            start_word = offset // 4

            out_file = tmp_path / f"part-{n}.bin"
            get_range(endpoint, bucket, key, offset, read_size, out_file)
            got = out_file.read_bytes()

            verify_range_data(got, offset, read_size)

            head_len = min(4 - (offset % 4), read_size) if offset % 4 else 0
            remaining = read_size - head_len
            tail_len = remaining % 4
            mid_words = (remaining - tail_len) // 4

            print(
                f"  iter {n}/{ITERATIONS}: offset={offset} read_size={read_size} "
                f"start_word={start_word} head={head_len}B mid={mid_words}w tail={tail_len}B ok"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Byte-range GET random verification")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", default="seq-u32-4mb")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed (default: random)")
    args = parser.parse_args()

    assert_little_endian_layout()

    golden = build_golden()
    if len(golden) != OBJECT_SIZE:
        print(f"FAIL: golden size {len(golden)} != {OBJECT_SIZE}", file=sys.stderr)
        sys.exit(1)

    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    print(f"byte-range GET: {ITERATIONS} iterations, seed={seed}")
    run_iterations(args.endpoint, args.bucket, args.key, golden, seed)
    print(f"PASS: byte-range GET ({ITERATIONS} random ranges on 4MiB uint32 object)")


if __name__ == "__main__":
    main()
