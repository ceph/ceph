#!/bin/bash
#
# Ceph - scalable distributed file system
#
# Author: Gabriel BenHanokh <gbenhano@redhat.com>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#
#!/usr/bin/env bash
# List KV entries by prefix from FDB.
#
# Usage:
#   fdb_list.sh [OPTIONS] PREFIX [LIMIT]
#
# PREFIX shortcuts (case-insensitive):
#   S        All S-namespace entries (objects)
#   S:O      Current objects only
#   B        Bucket metadata
#   T        Tenant metadata
#   P        Pending operations
#   G        GC entries
#   D        Data entries (Tier 2)
#   L        Local counters
#
# Also accepts raw hex like 'S\x00\x01\x00\x00'.
# LIMIT defaults to 20.
#
# Options:
#   --keys-only    Show only keys, strip values
#   --values-only  Show only values, strip keys
#   --both         Show keys and decoded values (default)
#
# Examples:
#   fdb_list.sh S:O 50            # first 50 current objects (keys + values)
#   fdb_list.sh --keys-only D     # data (Tier 2) keys only
#   fdb_list.sh --values-only G   # GC values decoded
#   fdb_list.sh B 100             # first 100 bucket entries

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/kvrgw-common.sh"

DISPLAY_MODE="both"
while [[ $# -gt 0 && "$1" == --* ]]; do
  case "$1" in
    --keys-only) DISPLAY_MODE="keys"; shift ;;
    --values-only) DISPLAY_MODE="values"; shift ;;
    --both) DISPLAY_MODE="both"; shift ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") [--keys-only] PREFIX [LIMIT]" >&2
  echo "  PREFIX: S, S:O, S:C, S:C:D, B, T, P, G, L" >&2
  exit 2
fi

INPUT_PREFIX="$1"
LIMIT="${2:-20}"

# Header: S <shard_count=1 2B> <shard_id=0 2B> = \x00\x01\x00\x00
HEADER="S\\x00\\x01\\x00\\x00"

resolve_prefix() {
  local p="${1^^}"
  case "${p}" in
    S:O)    echo "${HEADER}\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00O" ;;
    S)      echo "S" ;;
    B)      echo "B" ;;
    T)      echo "T" ;;
    P)      echo "P" ;;
    G)      echo "G" ;;
    D)      echo "D" ;;
    L)      echo "L" ;;
    *)      echo "${1}" ;;
  esac
}

needs_filter() {
  local p="${1^^}"
  case "${p}" in
    S:O) return 0 ;;
    *) return 1 ;;
  esac
}

FILTER=""
if needs_filter "${INPUT_PREFIX}"; then
  FILTER="${INPUT_PREFIX^^}"
  PREFIX="S"
else
  PREFIX="$(resolve_prefix "${INPUT_PREFIX}")"
fi

END="${PREFIX}\\xff"

FDB_CLI="${FDB_CLI:-${FDB_ROOT}/usr/bin/fdbcli}"

TMPRAW="$(mktemp)"
trap 'rm -f "${TMPRAW}"' EXIT

SCAN_LIMIT="${LIMIT}"
if [[ -n "${FILTER}" ]]; then
  SCAN_LIMIT=100000
fi

"${FDB_CLI}" -C "${FDB_CLUSTER_FILE}" --exec "getrange ${PREFIX} ${END} ${SCAN_LIMIT}" > "${TMPRAW}"

parse_keys() {
  python3 - "${DISPLAY_MODE}" "${TMPRAW}" "${LIMIT}" "${FILTER}" <<'PY'
import sys, struct, re, time

display_mode = sys.argv[1]  # "keys", "values", "both"
input_file = sys.argv[2]
display_limit = int(sys.argv[3])
key_filter = sys.argv[4] if len(sys.argv) > 4 else ""

def decode_fdb_str(s):
    """Decode fdbcli escaped string to bytes.
    fdbcli escapes: non-printable and space as \\xHH, printable ASCII as-is.
    A literal backslash in data appears as printable char (0x5C), NOT as \\\\.
    We only interpret \\xHH when followed by exactly two valid hex digits."""
    out = bytearray()
    i = 0
    while i < len(s):
        if s[i] == '\\' and i + 3 < len(s) and s[i+1] == 'x':
            hi = s[i+2] if i+2 < len(s) else ''
            lo = s[i+3] if i+3 < len(s) else ''
            if hi in '0123456789abcdefABCDEF' and lo in '0123456789abcdefABCDEF':
                out.append(int(hi + lo, 16))
                i += 4
                continue
        out.append(ord(s[i]))
        i += 1
    return bytes(out)

def parse_key(raw: bytes) -> str:
    if len(raw) < 1:
        return raw.hex()
    ns = chr(raw[0])

    # T <tenant_name>
    if ns == "T" and len(raw) > 1:
        return f"T:{raw[1:].decode('utf-8', errors='replace')}"

    # B <tenant_id 4B> <bucket_name>
    if ns == "B" and len(raw) >= 6:
        tid = struct.unpack(">I", raw[1:5])[0]
        bname = raw[5:].decode("utf-8", errors="replace")
        return f"B:t{tid}:{bname}"

    # L <type 1B> <name>
    if ns == "L" and len(raw) >= 3:
        ltype = chr(raw[1])
        name = raw[2:].decode("utf-8", errors="replace")
        return f"L:{ltype}:{name}"

    # D namespace: [D 1B][shard_count 2B][shard_id 2B][bucket_id 8B][size_tier 1B][hash_prefix 1B][mtime 4B][ref_tag 12B] = 31B
    if ns == "D" and len(raw) == 31:
        sc = struct.unpack(">H", raw[1:3])[0]
        si = struct.unpack(">H", raw[3:5])[0]
        bid = struct.unpack(">Q", raw[5:13])[0]
        size_tier = raw[13]
        hash_prefix = raw[14]
        mtime = struct.unpack(">I", raw[15:19])[0]
        ref_tag = raw[19:31].hex()
        return f"D:b{bid}:st{size_tier}:hp{hash_prefix}:mt{mtime}:rt={ref_tag}"

    # S/P namespace: <ns 1B> <shard_count 2B> <shard_id 2B> <bucket_id 8B> <cat 1B> <body>
    if ns in ("S", "P") and len(raw) >= 14:
        sc = struct.unpack(">H", raw[1:3])[0]
        si = struct.unpack(">H", raw[3:5])[0]
        bid = struct.unpack(">Q", raw[5:13])[0]
        cat = chr(raw[13])
        body = raw[14:]

        if ns == "S" and cat == "O":
            obj_name = body.decode("utf-8", errors="replace")
            return f"S:O:b{bid}:{obj_name}"

        if ns == "P" and cat == "O" and len(body) >= 12:
            ref_tag = body[-12:].hex()
            obj_name = body[:-12].decode("utf-8", errors="replace")
            return f"P:O:b{bid}:{obj_name}:rt={ref_tag}"

        return f"{ns}:{cat}:b{bid}:{body.hex()}"

    # G namespace: <G 1B> <size_tier 1B> <shard_count 2B> <shard_id 2B> <bucket_id 8B> <cat 1B> <body>
    if ns == "G" and len(raw) >= 15:
        st = raw[1]
        sc = struct.unpack(">H", raw[2:4])[0]
        si = struct.unpack(">H", raw[4:6])[0]
        bid = struct.unpack(">Q", raw[6:14])[0]
        cat = chr(raw[14])
        body = raw[15:]

        if cat == "O" and len(body) == 12:
            ref_tag = body.hex()
            return f"G:O:b{bid}:st{st}:rt={ref_tag}"

        return f"G:{cat}:b{bid}:st{st}:{body.hex()}"

    return f"{ns}:{raw[1:].hex()}"

def matches_filter(raw: bytes, filt: str) -> bool:
    if not filt:
        return True
    if len(raw) < 14:
        return False
    ns = chr(raw[0])
    if ns != "S":
        return False
    cat = chr(raw[13])
    if filt == "S:O":
        return cat == "O"
    return True

def parse_value(key_raw: bytes, val_raw: bytes) -> str:
    """Decode binary KV value based on key namespace."""
    if len(key_raw) < 1:
        return val_raw.hex()
    ns = chr(key_raw[0])

    # O: value — ObjectValueHeader (52B) + content_type + inline_data
    if ns == "S" and len(key_raw) >= 14 and chr(key_raw[13]) == "O" and len(val_raw) >= 52:
        ref_tag = val_raw[0:12].hex()
        etag_part_count = struct.unpack(">H", val_raw[12:14])[0]
        annotations_count = struct.unpack(">H", val_raw[14:16])[0]
        etag = val_raw[16:32].hex()
        size = struct.unpack(">Q", val_raw[32:40])[0]
        mtime_sec = struct.unpack(">I", val_raw[40:44])[0]
        mtime_nsec = struct.unpack(">I", val_raw[44:48])[0]
        chunk_type = chr(val_raw[48])
        flags = val_raw[49]
        tags_count = val_raw[50]
        ct_len = val_raw[51]
        content_type = val_raw[52:52+ct_len].decode("utf-8", errors="replace") if ct_len > 0 else ""
        parts = f"chunk={chunk_type} size={size} etag={etag}"
        if etag_part_count > 0:
            parts += f"-{etag_part_count}"
        parts += f" mtime={mtime_sec}.{mtime_nsec:09d} ct={content_type} rt={ref_tag}"
        if tags_count > 0:
            parts += f" tags={tags_count}"
        if annotations_count > 0:
            parts += f" annot={annotations_count}"
        if flags:
            parts += f" flags=0x{flags:02x}"
        inline_len = len(val_raw) - 52 - ct_len
        if inline_len > 0:
            parts += f" inline={inline_len}B"
        return parts

    # G:O value — GcValueHeader (13B)
    if ns == "G" and len(val_raw) >= 13:
        chunk_type = chr(val_raw[0])
        obj_size = struct.unpack(">Q", val_raw[1:9])[0]
        mtime = struct.unpack(">I", val_raw[9:13])[0]
        return f"chunk={chunk_type} object_size={obj_size} mtime={mtime}"

    # P:O value — PoValueHeader (12B)
    if ns == "P" and len(val_raw) >= 12:
        est_size = struct.unpack(">Q", val_raw[0:8])[0]
        created = struct.unpack(">I", val_raw[8:12])[0]
        return f"estimated_size={est_size} created_at={created}"

    # B: value — BucketValueHeader (16B)
    if ns == "B" and len(val_raw) >= 16:
        bucket_id = struct.unpack(">Q", val_raw[0:8])[0]
        created = struct.unpack(">q", val_raw[8:16])[0]
        return f"bucket_id={bucket_id} created_at={created}"

    # T: value — tenant (12B: tenant_id 4B + created_at 8B)
    if ns == "T" and len(val_raw) >= 12:
        tenant_id = struct.unpack(">I", val_raw[0:4])[0]
        created = struct.unpack(">q", val_raw[4:12])[0]
        return f"tenant_id={tenant_id} created_at={created}"

    # L: value — counter (8B LE uint64)
    if ns == "L" and len(val_raw) == 8:
        counter = struct.unpack("<Q", val_raw)[0]
        return f"counter={counter}"

    # D: value — raw data (show size only)
    if ns == "D":
        return f"data[{len(val_raw)}B]"

    return val_raw.hex()

count = 0
for line in open(input_file):
    line = line.rstrip("\n")
    if "' is `" not in line and "Range limited" not in line:
        continue

    if "Range limited" in line:
        if not key_filter:
            print(line)
        continue

    m = re.match(r"^`(.+)' is `(.+)'$", line)
    if not m:
        continue

    key_raw = decode_fdb_str(m.group(1))
    val_raw_str = m.group(2)
    val_raw = decode_fdb_str(val_raw_str)

    if not matches_filter(key_raw, key_filter):
        continue

    canon = parse_key(key_raw)
    decoded_val = parse_value(key_raw, val_raw)
    count += 1

    if display_mode == "keys":
        print(canon)
    elif display_mode == "values":
        print(decoded_val)
    else:
        print(f"{canon}  =  {decoded_val}")

    if display_limit > 0 and count >= display_limit:
        break

if key_filter:
    print(f"\n{count} entries (filter: {key_filter})")
PY
}

parse_keys
