# Delimiter Scan

## What is delimiter scan

The S3 `ListObjects` API supports a `delimiter` parameter (typically `"/"`). When set, keys containing the delimiter character after the request `prefix` are **not** returned as individual objects. Instead, the portion of the key up to and including the first delimiter occurrence is collapsed into a single **CommonPrefix** entry. This simulates filesystem directory listing.

Clients that use delimiter by default: **s3cmd**, **s5cmd**, **aws s3 ls**.

Clients that do **not** use delimiter by default: `aws s3api list-objects-v2` (unless `--delimiter` is passed explicitly).

## Behavior examples

### Example 1 — mixed content

Bucket contents (sorted):

```
alpha
beta
dir1/file1.txt
dir1/file2.txt
dir2/file3.txt
zebra
```

**Request:** `prefix=""`, `delimiter="/"`

**Response:**
- Objects: `alpha`, `beta`, `zebra`
- CommonPrefixes: `dir1/`, `dir2/`

The three keys under `dir1/` and `dir2/` are not returned as objects — they are collapsed into two CommonPrefix entries. The three top-level keys (no `/` in them) are returned as objects.

### Example 2 — drill-down

**Request:** `prefix="dir1/"`, `delimiter="/"`

**Response:**
- Objects: `dir1/file1.txt`, `dir1/file2.txt`
- CommonPrefixes: (none)

After the prefix `dir1/`, the remaining parts (`file1.txt`, `file2.txt`) contain no `/`, so they are returned as objects.

### Example 3 — deep nesting

Bucket contents:

```
perf/0000/000000000001
perf/0000/000000000002
...
perf/0127/000040000000
```

**Request:** `prefix=""`, `delimiter="/"`

**Response:**
- Objects: (none)
- CommonPrefixes: `perf/`

All 40M keys collapse into a single CommonPrefix.

**Request:** `prefix="perf/"`, `delimiter="/"`

**Response:**
- Objects: (none)
- CommonPrefixes: `perf/0000/`, `perf/0001/`, ..., `perf/0127/`

**Request:** `prefix="perf/0000/"`, `delimiter="/"`

**Response:**
- Objects: `perf/0000/000000000001`, `perf/0000/000000000002`, ... (paginated, max 1000 per page)
- CommonPrefixes: (none)

## Problem with the naive implementation

The original `ListObjects` delimiter scan (in `service_impl.cpp`) works as follows:

1. Scan keys from FDB in batches of `max_keys * 10` (10,000 keys per batch)
2. For each key, check if it produces a CommonPrefix (contains delimiter after the prefix)
3. If the CommonPrefix is new, add it to the response and increment the budget
4. If the CommonPrefix was already seen, skip the key (`continue`)
5. Repeat until budget reaches `max_keys` or the scan exhausts the bucket

The problem: after discovering CommonPrefix `perf/` from the first key, every subsequent key under `perf/` is read from FDB, parsed, checked against the seen-set, and discarded. With 40M keys, this means:

- 40,000,000 / 10,000 = **4,000 FDB range_scan calls**
- Each call reads 10,000 keys, parses their values, and discards them
- Total time: **minutes** for a response that contains a single CommonPrefix

The response is trivial (one entry), but the scan is linear in the total number of keys.

## Fix — prefix skip via FDB seek

FDB supports range scans with an arbitrary start key. When a key produces CommonPrefix `X`, instead of continuing to scan the next key, we **jump** `scan_begin` to the first key lexicographically past all keys starting with `X`.

The jump target is computed by `prefix_range_end(X)`:

- `prefix_range_end("perf/")` → `"perf0"` (byte after `'/'` is `'0'`)
- `prefix_range_end("dir1/")` → `"dir10"`

FDB seeks directly to that position in its B-tree — O(log N), no scanning of intermediate keys.

### Before (naive): 4,000 FDB calls for Example 3

```
range_scan → perf/0000/000000000001    → CommonPrefix "perf/"  → continue
range_scan → perf/0000/000000000002    → "perf/" already seen  → continue
... 40 million keys later ...
range_scan → (empty)                   → done
```

### After (prefix skip): 2 FDB calls for Example 3

```
range_scan → perf/0000/000000000001    → CommonPrefix "perf/"  → skip to "perf0"
range_scan from "perf0" → (empty)      → done
```

### Mixed content (Example 1): 4 FDB calls

```
range_scan(limit=1001) → alpha, beta, dir1/file1.txt ...
  alpha → Object (budget=1)
  beta  → Object (budget=2)
  dir1/file1.txt → CommonPrefix "dir1/" (budget=3) → break, skip to "dir10"

range_scan from "dir10" → dir2/file3.txt ...
  dir2/file3.txt → CommonPrefix "dir2/" (budget=4) → break, skip to "dir20"

range_scan from "dir20" → zebra ...
  zebra → Object (budget=5)
  (rows < batch_limit) → done
```

## Code changes

File: `backend/src/service_impl.cpp`, function `ListObjects`

1. **Prefix skip:** When a key produces a CommonPrefix (new or already seen), break out of the inner for-loop and set `scan_begin` to `make_object_key(bucket_id, prefix_range_end(common))`. The outer while-loop then issues a new `range_scan` from that position.

2. **Remove 10x overshoot:** `batch_limit` changes from `max_keys * 10` (delimiter) to `max_keys + 1` for all paths. With prefix skip, overshooting is unnecessary — we never read keys that belong to an already-discovered CommonPrefix.

3. **Remove `std::set` allocation:** `common_prefixes_seen` (`std::set<std::string>`) is replaced by a simple `std::string last_common_prefix`. With prefix skip, we never re-encounter the same CommonPrefix — we skip past it entirely. If the current key produces a CommonPrefix equal to `last_common_prefix`, it means `scan_begin` landed inside the same prefix (shouldn't happen with correct skip), and we skip again. No dynamic allocation needed.

## Continuation token

No change to the continuation token format. When `truncated_on_common_prefix` is true, the token encodes `last_scanned` (the key that triggered the truncation). On the next request, the scan resumes from that key and the prefix skip continues from there.
