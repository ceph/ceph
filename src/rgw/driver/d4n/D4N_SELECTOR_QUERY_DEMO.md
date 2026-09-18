# D4N libfdb Cookbook

This guide shows D4N-shaped FoundationDB operations using the current libfdb
feature stack: Content keys, query algebra, traversal helpers, transaction
watches, and transaction-versioned timestamps.

## Mini-TOC

- [Common Key Helpers](#common-key-helpers)
- [Build A Block Directory Key](#build-a-block-directory-key)
- [Read All Blocks For One Object](#read-all-blocks-for-one-object)
- [Stream Blocks In A Transaction](#stream-blocks-in-a-transaction)
- [List Objects With Prefix And Optional Marker](#list-objects-with-prefix-and-optional-marker)
- [Delete Versions In A Score Range](#delete-versions-in-a-score-range)
- [Reverse Version Listing Before A Marker](#reverse-version-listing-before-a-marker)
- [Combine Cache Keyspaces](#combine-cache-keyspaces)
- [Exclude Internal Records](#exclude-internal-records)
- [Watch A D4N Control Key](#watch-a-d4n-control-key)
- [Append A Commit-Ordered Event](#append-a-commit-ordered-event)
- [Read One Extra Row For Continuation Tokens](#read-one-extra-row-for-continuation-tokens)
- [Practical Rules](#practical-rules)

```cpp
namespace lfdb = ceph::libfdb;
namespace q = lfdb::query;
namespace fdbc = lfdb::layer::content;
```

## Common Key Helpers

Use Content keys when the key is made from complete domain segments: bucket id,
object id, version id, block id, score, and so on. The Content layer handles
segment separation and escaping, and the resulting `compiled_key` can be passed
directly to libfdb operations.

```cpp
auto d4n_keyspace()
{
  return fdbc::keyspace("d4n");
}

auto block_keyspace()
{
  return d4n_keyspace() / "block";
}

auto object_block_keyspace(std::string_view bucket_id,
                           std::string_view object_name)
{
  return block_keyspace() / bucket_id / object_name;
}

auto version_keyspace(std::string_view bucket_id,
                      std::string_view object_name)
{
  return d4n_keyspace() / "version" / bucket_id / object_name;
}

auto version_score_key(std::string_view bucket_id,
                       std::string_view object_name,
                       std::string_view version)
{
  return d4n_keyspace() / "version-score" / bucket_id / object_name / version;
}

auto version_index_key(std::string_view bucket_id,
                       std::string_view object_name,
                       std::string_view encoded_score,
                       std::string_view version)
{
  return version_keyspace(bucket_id, object_name) / encoded_score / version;
}
```

S3 object listing by object-name prefix is different: the prefix is inside the
object-name string, not between complete Content segments. Until the Content
layer grows a partial-segment prefix type, keep that one shape as raw bytes and
use query intervals directly.

```cpp
std::string object_listing_subspace(std::string_view bucket_id)
{
  return fmt::format("d4n/object-listing/{}/", bucket_id);
}

std::string object_listing_key(std::string_view bucket_id,
                               std::string_view object_name)
{
  return object_listing_subspace(bucket_id) + std::string(object_name);
}

std::string d4n_control_key(std::string_view name)
{
  return fmt::format("d4n/control/{}", name);
}

std::string d4n_event_prefix(std::string_view bucket_id)
{
  return fmt::format("d4n/event/{}/", bucket_id);
}

std::string d4n_event_suffix(std::string_view object_name,
                             std::string_view event)
{
  return fmt::format("/{}/{}", object_name, event);
}
```

## Build A Block Directory Key

You want one stable key for a cached block, grouped by bucket and object.

```cpp
fdbc::compiled_key BlockDirectory::build_index(CacheBlock *block)
{
  return block_keyspace()
       / block->cacheObj.bucketName
       / block->cacheObj.objName
       / fmt::format("{:020}", block->blockID)
       / fmt::format("{:020}", block->size);
}
```

The Content layer owns delimiter and escaping rules. The fixed-width numeric
strings preserve lexicographic order until libfdb grows typed numeric segments.

## Read All Blocks For One Object

You want the materialized block list for one object.

```cpp
const auto object_blocks = object_block_keyspace(bucket_id, object_name);
const auto blocks = lfdb::collect<CacheBlock>(FDBconn,
                                              fdbc::prefix(object_blocks));
```

Use `collect()` when a fully materialized result is exactly what the caller
wants. For a complete Content keyspace, `fdbc::prefix(object_blocks)` is the
range.

## Stream Blocks In A Transaction

You want to scan blocks while participating in a caller-owned transaction.

```cpp
const auto object_blocks = object_block_keyspace(bucket_id, object_name);

for (const auto& [key, block] :
     lfdb::scan<CacheBlock>(txn, fdbc::prefix(object_blocks))) {
  process_block(key, block);
}
```

Use `scan(txn, ...)` when the caller owns transaction lifetime. Use
`scan(dbh, ...)` for a managed flat stream, and `blocks(dbh, ...)` when
block-at-a-time processing is the point.

## List Objects With Prefix And Optional Marker

You want one object-listing page under an S3 object-name prefix, optionally
starting after a marker.

```cpp
const auto prefix_begin =
  object_listing_subspace(bucket_id) + std::string(prefix);

const auto page_options =
  q::query_options{ .result_limit = static_cast<int>(count + 1) };

auto read_page = [&](auto query) {
  return lfdb::collect<CacheObject>(FDBconn,
                                    q::with_options(std::move(query),
                                                    page_options));
};

const auto objects = marker.empty()
                   ? read_page(q::prefix(prefix_begin))
                   : read_page(q::prefix_starting_after(
                       prefix_begin,
                       object_listing_key(bucket_id, marker)));
```

The prefix query states the valid namespace. The marker helper adds the
exclusive lower bound. The range limit stays attached with `q::with_options()`
instead of being hidden in mutable selector state.

If marker inclusivity is runtime policy, spell the bound explicitly:

```cpp
const auto prefix_query = q::prefix(prefix_begin);
const auto marker_key = object_listing_key(bucket_id, marker);
const auto lower = marker_inclusive ? q::closed(marker_key)
                                    : q::open(marker_key);

const auto selector =
  q::intersection(prefix_query,
                  q::between(lower, q::open(q::successor(prefix_begin))));

if (q::is_empty(selector)) {
  return -ENOENT;
}
```

## Delete Versions In A Score Range

You want to delete only versions whose encoded scores fall in a closed score
range.

```cpp
const auto versions = version_keyspace(bucket_id, object_name);
const auto first_score = versions / encode_score(min);
const auto last_score = versions / encode_score(max);

const auto score_range =
  q::intersection(fdbc::prefix(versions),
                  q::between(first_score, q::prefix(last_score).end_key));

for (const auto& key :
     lfdb::scan<std::string>(txn, score_range) | std::views::keys) {
  lfdb::erase(txn, key);
}
```

This works when encoded scores sort lexicographically. FoundationDB reads only
the selected score range; the loop does not parse and discard unrelated version
keys.

## Reverse Version Listing Before A Marker

You want newest-first version listing, optionally ending before a marker version.
D4N still owns the marker-score lookup; libfdb owns the key interval.

```cpp
const auto versions = version_keyspace(bucket_id, object_name);
const auto page_options =
  q::query_options{
    .result_limit = static_cast<int>(count + 1),
    .reverse_order = true };

auto read_versions = [&](auto query) {
  return lfdb::collect<CacheObjectVersion>(FDBconn,
                                           q::with_options(std::move(query),
                                                           page_options));
};

if (marker_version.empty()) {
  return read_versions(fdbc::prefix(versions));
}

std::string marker_score;
const auto score_key = version_score_key(bucket_id, object_name, marker_version);

if (!lfdb::get(FDBconn, score_key, marker_score)) {
  throw marker_not_found{};
}

const auto marker_key = version_index_key(bucket_id,
                                          object_name,
                                          marker_score,
                                          marker_version);

return read_versions(q::ending_before(fdbc::prefix(versions), marker_key));
```

Reverse order and page size are read options. The key expression remains about
which versions are eligible.

## Combine Cache Keyspaces

You want to scan several cache tiers as one logical selection.

```cpp
const auto hot_cache = d4n_keyspace() / "cache" / "hot";
const auto warm_cache = d4n_keyspace() / "cache" / "warm";

const auto active_cache =
  q::set_union(fdbc::prefix(hot_cache),
               fdbc::prefix(warm_cache));

for (const auto& [key, object] :
     lfdb::scan<CacheObject>(FDBconn, active_cache)) {
  refresh_cache_object(key, object);
}
```

The query compiler emits normalized intervals for the scan. D4N does not need
separate scan loops for each tier.

## Exclude Internal Records

You want all records under a prefix except records in an internal subspace.

```cpp
const auto records = d4n_keyspace() / "record";
const auto internal_records = records / ".internal";

const auto visible_records =
  q::difference(fdbc::prefix(records),
                fdbc::prefix(internal_records));

for (const auto& [key, record] :
     lfdb::scan<Record>(FDBconn, visible_records)) {
  emit_record(key, record);
}
```

Making subtraction explicit keeps string filtering out of scan loops.

## Watch A D4N Control Key

You want one process to react when a small control key changes.

```cpp
std::jthread policy_watch {
  [dbh](std::stop_token stop_token) {
    const auto policy_key = d4n_control_key("cache-policy");

    lfdb::watched_loop(dbh, policy_key, stop_token,
      [](std::string_view key) {
        reload_d4n_control(key);
      });
  }
};
```

Transaction watches are narrow invalidation signals. They do not return the new
value; read current state separately after the watch fires.

## Append A Commit-Ordered Event

You want a D4N event/audit/debug record whose key sorts by FoundationDB commit
order without a client clock or a separate sequence key.

```cpp
lfdb::versionstamp stamp;

lfdb::set(dbh,
          lfdb::versioned(d4n_event_prefix(bucket_id),
                          d4n_event_suffix(object_name, "put"),
                          stamp),
          encoded_event);

const auto& committed_order = stamp.resolved_bytes();
```

The versioned-key helper takes raw prefix/suffix byte strings. Keep D4N escaping
or normalization inside the key helper if arbitrary object names flow into it.
A single `set()` cannot versionstamp both key and value.

## Read One Extra Row For Continuation Tokens

You want D4N pagination state, which is application policy layered over a normal
libfdb range limit.

```cpp
template <typename ValueT>
struct listed_page final
{
  std::vector<std::pair<std::string, ValueT>> rows;
  std::optional<std::string> continuation_token;
};

template <typename ValueT, typename TokenFnT>
listed_page<ValueT> read_page(lfdb::database_handle dbh,
                              auto query,
                              const std::size_t count,
                              TokenFnT token_from_key)
{
  listed_page<ValueT> out;

  if (0 == count) {
    return out;
  }

  const auto page_query =
    q::with_options(std::move(query),
                    q::query_options{
                      .result_limit = static_cast<int>(count + 1) });

  for (auto row : lfdb::scan<ValueT>(dbh, page_query)) {
    if (count == std::size(out.rows)) {
      out.continuation_token = std::invoke(token_from_key, row.first);
      break;
    }

    out.rows.push_back(std::move(row));
  }

  return out;
}
```

The FDB range limit is library-level. Continuation-token generation belongs in
D4N because it encodes S3 pagination semantics.

## Practical Rules

- Use Content keys for complete domain segments.
- Use raw byte keys when the query prefix cuts through one string segment.
- Use `q::prefix()` or `fdbc::prefix()` for prefix ranges.
- Use `q::open()`, `q::closed()`, and cursor helpers for marker bounds.
- Use `q::with_options()` to attach read limits, reverse order, target bytes,
  and streaming mode.
- Use `scan(txn, ...)` for caller-owned transactions.
- Use `scan(dbh, ...)` for managed flat streams.
- Use `blocks(dbh, ...)` when block-at-a-time processing is useful.
- Use `collect<ValueT>(dbh, ...)` only when the caller wants materialization.
- Use watches for narrow key-change notification, then read current state.
- Use versionstamps for commit-ordered records.
- Keep marker lookup, continuation tokens, S3 pagination, and object/version
  policy in D4N.
