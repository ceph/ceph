# Per-storage-class quota — integration guide

This document is for the developer landing the observability-cache PR
(#66109) and wiring it into per-storage-class quota enforcement.

## What this PR shipped

1. **Data model**: `RGWQuotaInfo` was extended to v4 with
   `storage_class_quotas` (a `std::map<key, RGWStorageClassQuota>`) and
   `enforcement_mode`.  The key format is
   `rgw_sc_quota_key(placement, storage_class)` -> `"<pl>::<sc>"`.

2. **Enforcement engine**:
   `rgw::quota::rgw_check_storage_class_quota()` is called from every
   write-path op in `rgw_op.cc` (PutObj pre+post, PostObj pre+post,
   CopyObj, CompleteMultipart, BulkUpload).

3. **Admin tool**: `radosgw-admin quota set --bucket=B
   --placement-target=P --storage-class=C --max-size=N` writes into the
   new map; `radosgw-admin quota enable` / `quota disable` work the same
   way when both flags are provided.

4. **Unit tests**: encode/decode roundtrip + checker semantics in
   `src/test/rgw/test_rgw_sc_quota.cc`.

## What is intentionally pluggable

The checker reads usage stats through a tiny abstract interface
(`rgw::quota::ScUsageStatsProvider`).  Until a provider is installed,
the checker always returns 0 (fail-open) — which makes this PR
behaviour-neutral on any cluster that does not also have the obs-cache
PR.

## Integration steps for the obs-cache PR

1. **Implement the provider.**  Make your obs-cache singleton subclass
   `rgw::quota::ScUsageStatsProvider`:

   ```cpp
   class RGWObsCache final : public rgw::quota::ScUsageStatsProvider {
     std::optional<rgw::quota::ScUsageStats> lookup(
         const rgw_bucket& bucket,
         const std::string& sc_key) const override {
       // hash-map lookup into your pre-aggregated table
       ...
     }
   };
   ```

   `lookup()` must be thread-safe and non-blocking.

2. **Register the provider during RGW startup**, after the background
   refresh thread has at least primed itself once (or accept that
   first-window writes will fail-open):

   ```cpp
   rgw::quota::set_sc_stats_provider(&g_obs_cache);
   ```

3. **Unregister during shutdown** — call
   `rgw::quota::set_sc_stats_provider(nullptr)` BEFORE the obs-cache
   object is destroyed.  The pointer is stored in a
   `std::atomic<>`; in-flight requests already past the load will
   complete safely because the pointer install/uninstall is on the
   slow control path, not the request path.

4. **Populate keys from Pedro's bucket-index stats (PR #66501).**
   The obs cache's background refresh should iterate
   `rgw_bucket_dir_header::stats_per_storage_class` (or whatever
   Pedro's PR named the field), summing across shards, and produce a
   table keyed by `rgw_sc_quota_key(placement, sc_name)`.  Use the
   exact same key-builder so this PR's lookups match.

## Failure modes (intentional)

| Condition                                | Behaviour | Why                       |
| ---                                      | ---       | ---                       |
| No provider registered                   | Allow     | Obs-cache not enabled     |
| Provider returns `nullopt`               | Allow     | Cold cache, new bucket    |
| `enforcement_mode == LEGACY`             | Skip      | Backward compat default   |
| `storage_class_quotas` empty             | Skip      | No SC quota configured    |
| SC quota present but for a different key | Allow     | Doesn't apply to this op  |
| SC quota present, size or count exceeded | -EDQUOT   | The whole point           |

Every reach-the-cache failure path falls through to ALLOW the write —
this is the safety property that lets us merge this PR independently
of the obs-cache PR without any risk to existing deployments.

## Multi-RGW consistency

Each RGW process has its own ScUsageStatsProvider (its own obs cache),
so two RGWs may see slightly different cached usage values within one
refresh interval.  This is documented in the design and is the same
tradeoff every other distributed object store makes for quota
enforcement.  No cross-instance coordination is added by this PR.
