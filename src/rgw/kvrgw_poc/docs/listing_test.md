# List-Test Verification Plan

## Overview

Post-test verification command that validates every object written by a perf test is present and in correct order, while profiling ListObjects page latency.

## 1. Hash-Based Key Prefix

The runner script (`run_perf_from_def.sh`) generates a unique prefix per test run:

- Input: `<Perf-Test-XXX.md name>:<FDB-Config-XXX.md name>:<TIMESTAMP>`
- Hash: SHA-256 of the concatenated string
- Prefix: last 6 bytes as 12 hex chars (e.g. `a3f1bc09de7f`)
- Passed to perf driver via `KVRGW_KEY_PREFIX` env var (default: `perf`)
- Object keys become: `<prefix>/<thread_id>/<instance_id>_<seq>`
  - e.g. `a3f1bc09de7f/0000/02_000000000042`

## 2. Metadata File

Created in the results directory: `<results_dir>/test_metadata.txt`

### Written by runner (before test):

```
prefix=a3f1bc09de7f
instances=3
threads=128
buckets=16
tiers=8192
burst=5
```

### Written by each perf driver instance (separate file per instance):

Each instance writes to `<results_dir>/test_metadata_<instance_id>.txt`:

```
global_max_seq=5000
```

The metadata file path is passed via `KVRGW_METADATA_FILE` env var. The runner merges all per-instance files after all instances finish into `<results_dir>/test_metadata.txt`.

## 3. KeyIterator Class

Zero-allocation key iterator in `perf_driver.cpp`:

```cpp
class KeyIterator {
public:
  KeyIterator(std::string_view base_prefix, int thread_count,
              int instance_count, const uint64_t* max_seq_per_instance);

  bool next();                    // advance to next key; false when done
  std::string_view view() const;  // current key (return by value)
  void reset();                   // back to first key
  uint64_t total_keys() const;    // total expected keys

private:
  char buf_[80];
  int len_;
  int thread_pos_, instance_pos_, seq_pos_;
  int thread_id_, instance_id_;
  uint64_t seq_;
  int thread_count_, instance_count_;
  const uint64_t* max_seq_;       // per-instance max_seq array

  void write_thread();            // write thread_id_ digits at thread_pos_
  void write_instance();          // write instance_id_ digits at instance_pos_
  void write_seq();               // write seq_ digits at seq_pos_
};
```

### Iteration order (matches FDB lexicographic sort):

```
<prefix>/0000/00_000000000000   thread=0, instance=0, seq=0
<prefix>/0000/00_000000000001   thread=0, instance=0, seq=1
...
<prefix>/0000/00_<max_seq_0-1>  thread=0, instance=0, seq=max_seq[0]-1
<prefix>/0000/01_000000000000   thread=0, instance=1, seq=0
...
<prefix>/0000/01_<max_seq_1-1>  thread=0, instance=1, seq=max_seq[1]-1
...
<prefix>/0001/00_000000000000   thread=1, instance=0, seq=0
...
```

### Key format in buffer:

```
a3f1bc09de7f/0000/00_000000000000
^prefix      ^thrd ^in ^seq
```

- `next()`: increment seq digits in place. If `seq >= max_seq[instance_id]`, reset seq to 0, increment instance_id. If `instance_id >= instance_count`, reset instance_id to 0, increment thread_id. If `thread_id >= thread_count`, return false (done).
- `view()`: returns `std::string_view(buf_, len_)` — no allocation.
- Only the changing digits are modified per call.

## 4. list-test Command

New perf driver command: `list-test [--blind] [--max-pages N]`

### Parameters (from metadata file):

- prefix, threads, instances, per-instance max_seq, buckets

### Algorithm:

```
for each bucket in all_buckets:
  create KeyIterator(prefix, threads, instances, max_seq_array)
  token = ""
  page = 0
  while true:
    t0 = now()
    ListObjectsRequest req(tenant, bucket, max_keys=1000, token, prefix)
    service.ListObjects(nullptr, &req, &resp)
    t1 = now()
    record_page_latency(t1 - t0)

    if not --blind:
      for each key in resp.objects():
        expected = iter.view()
        if key != expected:
          FAIL: mismatch at page P, index I: got=X expected=Y
          abort
        iter.next()

    if not resp.is_truncated(): break
    token = resp.next_continuation_token()
    page++

  if not --blind:
    if iter has remaining keys:
      FAIL: missing keys — iterator not exhausted
```

### Output:

```
=== LIST-TEST perf-bucket-0 ===
  pages=41000 keys=41000000 elapsed=82.3s
  Page latency: min=1.2ms max=8.5ms avg=2.0ms p99=4.1ms
  Verification: PASS (all keys matched)
```

### Modes:

- **Default**: validate keys + time pages. Fail-fast on mismatch.
- **`--blind`**: time pages only, skip key validation. Pure listing throughput benchmark.
- **`--max-pages N`**: stop after N pages (N × 1000 keys). For quick profiling without full scan.

## 5. Changes Summary

| File | Change |
|------|--------|
| `run_perf_from_def.sh` | Generate hash prefix, write metadata file, pass `KVRGW_KEY_PREFIX` and `KVRGW_METADATA_FILE` env vars |
| `perf_driver.cpp` | Read `KVRGW_KEY_PREFIX` (replace hardcoded "perf"), read `KVRGW_METADATA_FILE`, add KeyIterator class, add `list-test` command, write `global_max_seq_<id>` to metadata |
| `perf_testing.md` | Document prefix scheme, metadata file, KeyIterator, list-test command |
