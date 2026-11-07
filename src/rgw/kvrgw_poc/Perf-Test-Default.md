# Perf Test Configuration

Generated from: Perf-Test-Default.txt
Generated at: 2026-09-05T09:02:06Z

## Test Parameters

| Parameter | Value |
|-----------|-------|
| instances | 3 |
| mode | perf |
| clean | yes |
| workload | put |
| concurrency | 128 |
| buckets | 16 |
| burst | 5 |
| tiers | 8192 |
| duration | 120 |
| version_state | none |
| max_inline | 256 |
| max_kv_store | 4096 |

## Batch Configuration

| Parameter | Value |
|-----------|-------|
| batch_size | 10 |
| batch_timeout_us | 1000 |
| batch_threads | 8 |

## Simulated Disk Latency

| Parameter | Value |
|-----------|-------|
| sim_disk_write_us | 0 |
| sim_disk_read_us | 0 |

## Stats Collection

| Parameter | Value |
|-----------|-------|
| sample_interval | 2s |
| fdb_stats | full |
| kvrgw_stats | full |

### FDB Stats (full)

- Transaction rates: txn/sec, reads/sec, writes/sec, conflict/sec
- Per-process CPU utilization (SS, log, proxy)
- Per-process memory usage (RSS, FDB internal)
- Disk latency (read/write per SS)
- Log queue size / durability lag
- Log growth (current size, delta KB/MB, delta %)
- Storage queue size
- Data distribution (bytes stored per SS, key-range balance)

### KVRGW Stats (full)

**Latency (per op type):**
PutObject, GetObject, HeadObject, DeleteObject, DeleteMulti, DeleteBucket,
CreateBucket, ListBuckets, ListObjects, ListObjVersions, DeleteObjVersion,
CopyObject, PutObjTagging, GetObjTagging, DelObjTagging, BucketExists,
PutBucketPolicy, GetBucketPolicy, DelBucketPolicy, PutBucketVer, GetBucketVer

Each with: count, avg_total_us, avg_fdb_us, fdb_pct, avg_get_us, avg_put_us,
avg_del_us, avg_commit_us, avg_scan_us, avg_disk_us

**Batch stats:**
batch_commits, entries_batched, conflict_pushbacks,
avg/min/max batch_size, avg queue_size, avg/min/max wait_us

**Transaction health:**
txn_retries, txn_hard_failures, txn_max_retries_exceeded

**Error stats:**
Per-error-code counters (all KvrgwErrorCode values, non-zero only)

**Host stats:**
Per-process CPU/IO (pidstat), disk utilization (iostat), process memory

## Perf Driver Commands

Per instance:
```
set-batch size=10 timeout=1000 threads=8
create-buckets buckets=16 mode=none
put c=128 tiers=8192 duration=120 burst=5
```
