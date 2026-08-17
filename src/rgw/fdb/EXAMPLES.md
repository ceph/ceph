# libfdb Examples

"Take a thousand days of practice for forging, and ten thousand days of practice for refining."
    -- Miyamoto Musashi, Go Rin No Sho (~1645)

Welcome, traveller! Grab your favorite walking stick, and let us journey into
the realm of libfdb!

While this is not proper documentation, hopefully this "cookbook-style" set
of mini-examples will help you on your libfdb path.

Errata: Please report errata, or contact with examples you would like to see!

These examples use a short namespace alias for readability and also to save
typing with one's poor fingers! Yoikes!

See examples/libfdb/ for some working, compilable simple examples.

```cpp
namespace lfdb = ceph::libfdb;
namespace q = lfdb::query;

using namespace std::string_literals;
```

## Running Tests And Benchmarks

From the build directory, run the libfdb tests with:

```sh
./bin/unittest_fdb
./bin/unittest_fdb_ceph
```

Benchmarks are hidden from default test runs. Run all libfdb benchmarks with:

```sh
./bin/unittest_fdb_ceph "[benchmark]"
```

## General Recipes

```cpp
// Use a database_handle when you desire a single logical operation. Behind
// the scenes, libfdb will create and complete its own transaction for you.
// Database-handle operations may retry after recoverable FoundationDB errors,
// so transaction callbacks may be activated more than once.
lfdb::set(dbh, "person/barbara-moo/name", "Barbara Moo");
```

```cpp
// Pass a transaction handle when several operations must be grouped in the
// same transaction. Do not use the transaction after commit().
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/barbara-moo/name", "Barbara Moo");
lfdb::set(txn, "person/barbara-moo/book", "Accelerated C++");

if (!lfdb::commit(txn)) {
  // Retry the transaction body with a fresh or recovered transaction.
}
```

## Version Stamps

```cpp
// Store a value with a versioned key:
lfdb::versionstamp stamp;

lfdb::set(dbh,
          lfdb::versioned("person/konrad-zuse/event/", "/created", stamp),
          "created");
```

```cpp
// Store a transaction-versioned value:
lfdb::versionstamp stamp;

lfdb::set(dbh,
          "person/barbara-liskov/version",
          lfdb::versioned("", stamp));
```

```cpp
// Get a version stamp from a committed transaction:
auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp stamp;

lfdb::set(txn, "person/alonzo-church/name", "Alonzo Church");

if (lfdb::commit(txn, stamp)) {
  const auto& version_stamp_bytes = stamp.resolved_bytes();
}
```

```cpp
// Use one version stamp for several operations in the same transaction:
auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp stamp;

lfdb::set(txn,
          lfdb::versioned("person/grace-hopper/event/", "/created", stamp),
          "created");
lfdb::set(txn,
          lfdb::versioned("person/grace-hopper/event/", "/updated", stamp),
          "updated");
lfdb::set(txn,
          "person/grace-hopper/version",
          lfdb::versioned("", stamp));

if (lfdb::commit(txn) && stamp.is_resolved()) {
  const auto& version_stamp_bytes = stamp.resolved_bytes();
}
```

Note that version stamping a key or a value is strictly an either/or proposition: there is no call
to set a stamped key and value all at once.

Version stamps become readable and orderable only after commit resolution.
Trying to read an unresolved stamp is a `std::invalid_argument`; trying to reuse a
resolved stamp as a new commit output is a `std::invalid_argument`.

## Transaction Versions

Transaction versions let application code connect a successful write to a later
consistent read. `committed_version()` is available after a successful commit,
`set_read_version()` pins another transaction to that version, and
`read_version()` reports the version an active transaction reads from. This is a
consistency tool, not durable historical storage. Set the read version before
performing reads in that transaction:

```cpp
auto write_txn = lfdb::make_transaction(dbh);

lfdb::set(write_txn, "person/grace-hopper/name", "Grace Hopper");

if (lfdb::commit(write_txn)) {
  const auto version = lfdb::committed_version(write_txn);

  auto read_txn = lfdb::make_transaction(dbh);
  lfdb::set_read_version(read_txn, version);

  std::string name;
  if (lfdb::get(read_txn, "person/grace-hopper/name", name)) {
    const auto observed_version = lfdb::read_version(read_txn);
    record_observation(name, observed_version);
  }
}
```

## Setup

```cpp
// Open the default FoundationDB database.
auto dbh = lfdb::create_database();
```

FoundationDB clients can connect through a default cluster file, an explicit
cluster file path, or the connection string normally stored inside a cluster
file. libfdb keeps caller-supplied sources explicit: a
`std::filesystem::path` source opens a cluster file, while a string-like source
opens a FoundationDB connection string. Use `create_database()` for
FoundationDB's default cluster-file resolution; explicit source wrappers require
non-empty inputs. See the
FoundationDB [cluster file documentation](https://apple.github.io/foundationdb/administration.html#cluster-files)
and the C API database-opening documentation:
<https://apple.github.io/foundationdb/api-c.html#database>.

```cpp
// Open a database with explicit database and network options. Explicit database
// options are used as passed. Flag-only options use
// lfdb::option_flag because they have no value. Network options are applied
// only during the first FoundationDB network initialization; later calls to
// create_database() cannot change them.
lfdb::database_options dbopts{
  { FDB_DB_OPTION_TRANSACTION_TIMEOUT, std::int64_t{5000} },
};

lfdb::network_options netopts{
  { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag },
};

auto dbh = lfdb::create_database(dbopts, netopts);
```

```cpp
// Open a database with an explicit cluster file plus database/network options.
auto dbh = lfdb::create_database(
  lfdb::connection_source{std::filesystem::path{"/path/to/fdb.cluster"}},
  dbopts,
  netopts);
```

```cpp
// Open a database from a FoundationDB connection string.
auto dbh = lfdb::create_database(
  lfdb::connection_source{"description:id@127.0.0.1:4500"});
```

## API Namespace

`lfdb::api` reports facts about the FoundationDB client API available to this
process. These calls do not require a database handle. See the FoundationDB C API
documentation:
<https://apple.github.io/foundationdb/api-c.html>.

```cpp
const auto client = lfdb::api::client_version();
const auto max_api = lfdb::api::max_version();
```

## System Namespace

`lfdb::system` contains FoundationDB system-level observations and operational
surface. Read-only functions report live database/client state. Sharp
operational functions belong here too, rather than in the core key/value API,
but they should only be called by code that is explicitly managing a
FoundationDB deployment. See the FoundationDB C API entries for these database
functions:
<https://apple.github.io/foundationdb/api-c.html#c.fdb_database_get_client_status>
and
<https://apple.github.io/foundationdb/api-c.html#c.fdb_database_get_main_thread_busyness>.

```cpp
const auto busyness = lfdb::system::main_thread_busyness(dbh);
const auto protocol = lfdb::system::server_protocol(dbh);
const auto status_json = lfdb::system::client_status_json(dbh);
```

The remaining functions are operational controls, not ordinary application data
operations:

```cpp
lfdb::system::reboot_worker(dbh, "127.0.0.1:4500", false, 60);
lfdb::system::force_recovery_with_data_loss(dbh, "dc1");
lfdb::system::create_snapshot(dbh, "snapshot-id", "snapshot-command");
```

## Single-Key Operations

Single-key `get()` returns whether the key was found.

```cpp
// Store and retrieve one value by key.
lfdb::set(dbh, "person/konrad-zuse/name", "Konrad Zuse");

std::string name;
if (lfdb::get(dbh, "person/konrad-zuse/name", name)) {
  // use name
}
```

```cpp
// Use a callback when the raw serialized bytes must be copied or decoded
// immediately. The span is only valid during the callback.
lfdb::get(dbh, "person/konrad-zuse/name",
          [](std::span<const std::uint8_t> bytes) {
            // copy or decode bytes here
          });
```

## Key Existence And Erase

```cpp
// Check for a key and erase it if it exists.
if (lfdb::key_exists(dbh, "person/jose-capablanca/title")) {
  lfdb::erase(dbh, "person/jose-capablanca/title");
}
```

## Atomic Mutations

Atomic mutations tell FoundationDB to transform a value at commit time on the
server side, without reading the current value back to the client. They are
useful for frequently updated counters, flags, and other small raw-byte values.
See the FoundationDB atomic operations guide:
https://apple.github.io/foundationdb/developer-guide.html#atomic-operations

```cpp
// Increment a counter without a read-modify-write round trip:
lfdb::atomic::add(dbh, "stats/person-count", std::uint64_t{1});
```

```cpp
// Group atomic mutations with ordinary writes in the same transaction:
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/grace-hopper/name", "Grace Hopper");
lfdb::atomic::add(txn, "stats/person-count", std::uint64_t{1});
lfdb::atomic::bit_or(txn, "stats/person-flags", std::uint64_t{0x01});

if (!lfdb::commit(txn)) {
  retry_person_update();
}
```

Numeric atomic mutations use FoundationDB's little-endian integer parameter
encoding. The C++ integral type's width becomes the FoundationDB parameter
width. They operate on raw FDB values, not zpp_bits object encoding:

```cpp
lfdb::atomic::max(dbh, "stats/high-watermark", std::uint64_t{128});
lfdb::atomic::min(dbh, "stats/low-watermark", std::uint64_t{4});
```

Byte-wise atomic mutations operate on raw byte strings:

```cpp
const std::string_view zero_bytes("\0\0\0\0\0\0\0\0", 8);

lfdb::atomic::byte_min(dbh, "stats/first-name", "Ada");
lfdb::atomic::byte_max(dbh, "stats/last-name", "Zuse");
lfdb::atomic::append_if_fits(dbh, "log/tail", "next-record\n");
lfdb::atomic::compare_and_clear(dbh, "stats/person-count", zero_bytes);
```

## Multi-Key Writes

```cpp
// Write key/value pairs from an STL associative container in one transaction.
std::map<std::string, std::string> people{
  { "person/saladin/name", "Saladin" },
  { "person/al-khwarizmi/name", "Al-Khwarizmi" },
  { "person/albrecht-duerer/name", "Albrecht Duerer" },
};

lfdb::set(dbh, people);
```

## Multi-Key Reads

Selector and query-expression `get()` overloads return the number of key/value
pairs emitted to the output container or iterator.

```cpp
// Read a key range into an STL associative container.
std::map<std::string, std::string> people;

const auto nread = lfdb::get(dbh, q::prefix("person/"), people);
```

## Key Ordering

```cpp
// FoundationDB keys are ordered lexicographically by byte string. Choose key
// formats so lexical order matches the scan order you want. Numeric suffixes
// should usually be fixed-width and zero-padded.
lfdb::set(dbh, "person/000001/name", "Barbara Moo");
lfdb::set(dbh, "person/000010/name", "Konrad Zuse");
```

## Selectors And Queries

FoundationDB keys are byte strings ordered lexicographically. Most libfdb range
queries should start from that fact: build keys so related records share a
prefix, then compose prefixes and bounds into the query you mean. `lfdb::select`
is the simple interval form; `lfdb::query` adds a small interval algebra for
prefixes, intersections, unions, differences, complements, singleton keys, and
explicit open/closed boundaries.

### What To Use When

| Situation | Use | Result Shape | Why |
| --- | --- | --- | --- |
| Store one key/value pair | `lfdb::set(dbh, key, value)` | `void` | One logical write with automatic transaction handling. |
| Apply a server-side value mutation | `lfdb::atomic::add(dbh, key, n)` | `void` | Updates counters or flags without a read-modify-write round trip. |
| Store many key/value pairs already in a container | `lfdb::set(dbh, kvs)` | `void` | Writes the whole range in one managed transaction without spelling out iterators. |
| Read one exact key | `lfdb::get(dbh, key, value)` | `bool` | Returns `true` only when the key exists and decodes into `value`. |
| Read one exact key without adding a read conflict | `lfdb::get(dbh, key, value, lfdb::read_mode::snapshot)` | `bool` | Useful for advisory reads where the value is not part of the transaction's correctness condition. |
| Read one exact key as bytes | `lfdb::get(dbh, key, callback)` | `bool` | Lets the callback copy or decode the raw value while the FDB buffer is valid. |
| Read a small range into an existing output | `lfdb::get(dbh, query, out)` | `std::size_t` | Materializes decoded string pairs, publishes them after a successful managed read, and reports how many records were found. |
| Read a flat stream in an existing transaction | `lfdb::scan(txn, query)` | generator of key/value pairs | Keeps transaction lifetime under caller control. |
| Read a flat stream without adding read conflicts | `lfdb::scan(txn, query, lfdb::read_mode::snapshot)` | generator of key/value pairs | Leaves specialized read-then-write policy visible at the call site. |
| Read a flat stream with managed transactions | `lfdb::scan(dbh, query)` | generator of key/value pairs | Hides transaction-window management while preserving streaming syntax. |
| Read directly into a new container | `lfdb::collect<T>(dbh, query)` | vector of key/value pairs | Best when the caller really wants a fully materialized result. |
| Read directly into a chosen container type | `lfdb::collect<T, AssocT>(dbh, query)` | `AssocT` | Keeps materialization explicit when the caller wants a map or custom container. |
| Process very large result sets | `lfdb::blocks<T>(dbh, query)` | generator of containers | Reads block-at-a-time to avoid one oversized, aging transaction. |
| Select everything under one key prefix | `q::prefix(prefix)` | `lfdb::select` | Handles FDB prefix successor rules without hand-built byte bounds. |
| Select an explicit half-open key interval | `q::between(begin, end)` | `lfdb::select` | Matches the usual FoundationDB `[begin, end)` range shape. |
| Combine or subtract selections | `q::intersection()`, `q::set_union()`, `q::difference()` | query expression or selector | Lets the query compiler normalize intervals before execution. |
| Add an explicit conflict check | `lfdb::mark_conflict_read(txn, query)` | `void` | Records transaction correctness dependencies that ordinary operations did not express. |
| Run several dependent operations atomically | `lfdb::make_transactor(dbh)` | callable transaction runner | Replays retryable transactions while keeping user code explicit. |

### Managed Range Reads

Managed range `get()` is for small, fully materialized reads. It gathers results
inside the managed transaction and publishes them to the caller's output only
after the read succeeds. Existing output is preserved and new results are
appended.

```cpp
std::vector<std::pair<std::string, std::string>> people{
  { "already/loaded", "kept" },
};

const auto nread = lfdb::get(dbh, q::prefix("person/"), people);
// nread is the number of records read from FoundationDB; people now also
// contains the newly-read records.
```

For very large reads, prefer `lfdb::blocks()` so work stays block-at-a-time
instead of collecting one large result before publishing it.

### Snapshot Reads

Snapshot reads do not add read conflict ranges. The FoundationDB developer guide
describes them here: <https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads>.
Use them for advisory reads, background maintenance, cleanup discovery, or other
places where another transaction changing the read keys should not by itself make
this transaction fail.

```cpp
object_metadata metadata;
if (lfdb::get(dbh, "object/catalog/project-a/report.pdf", metadata,
              lfdb::read_mode::snapshot) &&
    metadata.expired(cutoff)) {
  schedule_metadata_refresh(metadata);
}
```

The same mode works for range-shaped reads. This is useful when discovering
best-effort cleanup candidates where stale discoveries are acceptable:

```cpp
for (const auto& [key, metadata] :
     lfdb::scan<object_metadata>(txn, q::prefix("object/catalog/project-a/"),
                                 lfdb::read_mode::snapshot)) {
  if (metadata.expired(cutoff)) {
    schedule_metadata_refresh(metadata);
  }
}
```

Snapshot reads are a lower-isolation tool. If a later mutation depends on what
was read, keep that choice explicit in an ordinary transaction rather than
hiding it behind a convenience helper.

### Explicit Conflict Ranges

Conflict ranges are key ranges FoundationDB checks to decide whether a
transaction can still commit safely. Ordinary reads and writes add the usual
conflict ranges for their keys, but applications sometimes need to express an
additional dependency. Use `mark_conflict_read()` when this transaction must
retry if another transaction later writes an overlapping key or range. Use
`mark_conflict_write()` when this transaction must be treated as a writer for an
overlapping reader, even if it does not write that exact key itself:

```cpp
auto txn = lfdb::make_transaction(dbh);

lfdb::mark_conflict_read(txn, "object/index/marker");
lfdb::set(txn, "object/index/update", "pending");

if (not lfdb::commit(txn)) {
  retry_update();
}
```

The same functions accept exact keys, begin/end ranges, selectors, and query
expressions. Empty conflict expressions are rejected:

```cpp
auto txn = lfdb::make_transaction(dbh);

lfdb::mark_conflict_read(txn, "object/index/a", "object/index/z");
lfdb::mark_conflict_write(txn, q::prefix("object/cache/tenant-a/"));
```

### Prefix Selection

`q::prefix()` selects every key with a shared key prefix. This is the most
common FoundationDB access pattern, because related records are usually grouped
under prefixes such as `person/`, `bucket/index/`, or `object/metadata/`.

```cpp
auto people = q::prefix("person/");
```

A prefix at the 0xFF boundary normalizes to an empty selection in the ordinary
FDB keyspace; `q::successor()` still throws when no finite successor exists.

### Selectively Selected Selectable Section

The helpers in `lfdb::query` build FoundationDB key ranges without making
callers hand-code byte bounds. Start with simple selectors: prefixes, bounded
ranges, pagination, and options. When those are not enough, compose selectors
with the query algebra.

Read everything under one prefix:

```cpp
for (const auto& [key, person] :
     lfdb::scan<person_record>(dbh, q::prefix("person/"))) {
  show(person);
}
```

Read the next page under a prefix, after the previous marker:

```cpp
const auto query =
  q::prefix_starting_after("person/", "person/" + marker);

auto page = lfdb::collect<person_record>(dbh, query);
```

Read an explicit half-open key range:

```cpp
const auto query =
  q::between("person/name/ada", "person/name/grace");

auto people = lfdb::collect<person_record>(dbh, query);
```

Read a limited page in reverse order:

```cpp
const auto query =
  q::with_options(q::prefix("person/by-created/"),
                  q::query_options{
                    .result_limit = 100,
                    .reverse_order = true,
                  });

auto people = lfdb::collect<person_record>(dbh, query);
```

Read public records while excluding an internal subspace:

```cpp
const auto query =
  q::difference(q::prefix("person/"),
                q::prefix("person/.internal/"));

auto people = lfdb::collect<person_record>(dbh, query);
```

Page through that filtered result:

```cpp
const auto query =
  q::starting_after(
    q::difference(q::prefix("person/"),
                  q::prefix("person/.internal/")),
    page_marker);

auto people = lfdb::collect<person_record>(dbh, query);
```

The same read APIs consume both simple selectors and composed query expressions,
so the code that reads results does not have to change as the selection gets
more precise.

## Content Layer

Raw FoundationDB keys are byte strings ordered lexicographically. That is powerful,
but it also means an ad hoc key format can accidentally make common scans awkward
or wrong. The Content layer is a small key compiler: compose domain segments, get
one compiled byte key, and pass that key to the ordinary libfdb operations.

Use a namespace alias for readability:

```cpp
namespace fdbc = ceph::libfdb::layer::content;
```

### Normal Interface: Operators

The canonical way to build a keyspace is similar to writing a path:
```cpp
const std::string bucket_id = "bucket.8409.12";
const std::string version = "v0000000000000017";
const std::string object_name = "photos/2026/beach.jpg";

const auto object_head =
  fdbc::keyspace("object-cache") / "object" / bucket_id / version / object_name;

lfdb::set(dbh, object_head, object_metadata);
```

...the result, in object_head above, is a compiled key that follows FoundationDB rules.

Content keys are meant to flow into libfdb operations directly; callers should not
need to extract bytes from them.

Exact lookup uses the compiled key:

```cpp
std::string metadata;

if (lfdb::get(dbh, object_head, metadata)) {
  // cache hit
}
```

Build deeper keys by adding more segments:

```cpp
const auto block_key =
  object_head
  / "block"
  / fmt::format("{:020}", offset)
  / fmt::format("{:020}", length);

lfdb::set(dbh, block_key, block_data);
```

The fixed-width numeric strings are deliberate for now: Content layer string
segments preserve bytewise ordering, so numeric values represented as strings
must sort lexicographically the same way they sort numerically. A future typed
numeric segment should make that mechanical.

Here is the same idea applied to a block directory key. Ad hoc string keys often
need delimiter escaping by hand; the Content layer treats bucket, object, block
id, and size as separate key segments.

```cpp
fdbc::compiled_key block_index_key(const CacheBlock *block)
{
  return fdbc::keyspace("object-cache") / "block" / block->bucket_name /
         block->object_name / fmt::format("{:020}", block->block_id) /
         fmt::format("{:020}", block->size);
}
```

Scan all cached blocks for one object by selecting the object-block prefix:

```cpp
const auto object_blocks =
  fdbc::keyspace("object-cache") / "block" / bucket_id / object_name;

for (const auto& [key, block] : lfdb::scan<CacheBlock>(dbh, fdbc::prefix(object_blocks))) {
  // consume block
}
```

### Normal Interface: Functions

The function form is the same normal interface when a call expression is clearer
than a chain:

```cpp
const auto object_head = fdbc::key("object-cache",
                                   "object",
                                   bucket_id,
                                   version,
                                   object_name);

const auto block_key = fdbc::key("object-cache",
                                 "object",
                                 bucket_id,
                                 version,
                                 object_name,
                                 "block",
                                 fmt::format("{:020}", offset),
                                 fmt::format("{:020}", length));
```

### Choosing `keyspace()` or `key()`

The content key builder is effectively a small key compiler. Both `keyspace()`
and `key()` target the same output type, `fdbc::compiled_key`, so their results
can be used interchangeably by libfdb operations.

You do not technically have to use `keyspace()`:

```cpp
auto k = fdbc::key("tenant", "bucket", "object");
```

This is equivalent to the path-style form:

```cpp
auto k = fdbc::keyspace("tenant") / "bucket" / "object";
```

`keyspace()` is mainly ergonomic. It makes the root segment visually clear and
lets callers assemble keys piecewise:

```cpp
auto tenant = fdbc::keyspace("tenant");
auto bucket = tenant / bucket_id;
auto object = bucket / object_name;
```

This is not raw string concatenation. `/` is segment composition with escaping,
so each segment remains unambiguous in the compiled key.

For prefix scans, the compiled prefix can come from either spelling:

```cpp
fdbc::prefix(fdbc::keyspace("tenant") / bucket_id);
fdbc::prefix(fdbc::key("tenant", bucket_id));
```

Separating key assembly from key use is also performance-oriented: once a key or
key prefix is compiled, it can be reused without rebuilding those bytes each
time.

### Detailed Interface: Assembly

`assemble()` is the explicit compiler entry point used under the nicer interfaces.
Use it when you want the validation/lowering step to be visible, for example in
tests, generated schemas, or other code that builds key layouts as data.

```cpp
const auto object_head = fdbc::assemble("object-cache",
                                        "object",
                                        bucket_id,
                                        version,
                                        object_name);
```

Invalid segment types are rejected at compile time, and invalid keyspace roots
throw during assembly:

```cpp
static_assert(fdbc::key_segments<std::string_view, std::string_view>);
static_assert(!fdbc::key_segments<int>);

const auto object_records = fdbc::assemble("object-cache", "object");
```

Only the root segment has the non-empty and non-`0xFF` root restriction.
Subsequent segments are encoded as data under that root, so empty strings or
segments beginning with `0xFF` remain valid when the domain needs them.

The lowering code is written to be constexpr-friendly, but the current compiled
target owns a `std::string`. A future fixed-size/literal compiled target would
let literal-only assembly produce a fully constexpr key.

### Selecting Content Keys

Use `fdbc::prefix()` for a full prefix range. It returns a normal `lfdb::select`,
so it works with the ordinary range-read APIs:

```cpp
// All cached records for one object version.
const auto object_records =
  fdbc::keyspace("object-cache")
  / "object"
  / bucket_id
  / version
  / object_name;

std::vector<std::pair<std::string, std::string>> records;

const auto nread = lfdb::get(dbh, fdbc::prefix(object_records), records);
```

For a subrange, compose the lower and upper bounds and use the ordinary selector
constructors:

```cpp
const auto first_block =
  object_records / "block" / fmt::format("{:020}", first_offset);

const auto last_block =
  object_records / "block" / fmt::format("{:020}", last_offset);

auto blocks = q::between(first_block, last_block);

lfdb::get(dbh, blocks, records);
```

The explicit selector is half-open: the begin key is included and the end key is
excluded. Use `lfdb::inclusive()` or `lfdb::exclusive()` when the boundary shape
needs to be different.

### Navigating Stored Keys

Key selectors navigate among keys that actually exist in FoundationDB. They do
not compute byte-string successors or predecessors. See the FoundationDB
[key selector guide](https://apple.github.io/foundationdb/developer-guide.html#key-selectors)
for the underlying model.

Assume the database contains these keys:

`0 1 2 3 4`

| Selector | Meaning | Result for anchor `2` | Result for anchor `2.5` |
|---|---|---:|---:|
| `lfdb::lower(key)` | greatest stored key `< key` | `1` | `2` |
| `lfdb::floor(key)` | greatest stored key `<= key` | `2` | `2` |
| `lfdb::ceiling(key)` | least stored key `>= key` | `2` | `3` |
| `lfdb::higher(key)` | least stored key `> key` | `3` | `3` |

```cpp
auto txn = lfdb::make_transaction(dbh);

auto previous = lfdb::get_key(txn, lfdb::lower("2"));
auto current_or_previous = lfdb::get_key(txn, lfdb::floor("2"));
auto current_or_next = lfdb::get_key(txn, lfdb::ceiling("2"));
auto next = lfdb::get_key(txn, lfdb::higher("2"));
```

Selectors compose with content keys because content keys compile to ordinary
FoundationDB keys. The resolved key is still just a key, so use the query
algebra when you need to test whether it belongs to a content prefix:

```cpp
const auto objects = fdbc::keyspace("object-cache") / "object" / bucket_id;
const auto object_b = objects / "b";

const auto candidate = lfdb::get_key(dbh, lfdb::ceiling(object_b));

if (q::contains(fdbc::prefix(objects), candidate)) {
  load_object(candidate);
}
```

### Scanning A Selection

`scan()` is the ordinary flat key/value traversal interface. With a transaction
handle, it reads through the caller's transaction. Use this when the query is
expected to fit within one transaction and/or you want control over the
transaction lifetime and options.

```cpp
auto txn = lfdb::make_transaction(dbh);

for (const auto& [key, value] : lfdb::scan(txn, q::prefix("person/"))) {
  fmt::println("{}: {}", key, value);
}
```

With a database handle, `scan()` manages transaction windows internally and
still presents one flat key/value stream.

```cpp
for (const auto& [key, value] : lfdb::scan(dbh, q::prefix("person/"))) {
  fmt::println("{}: {}", key, value);
}
```

Use `collect()` when a materialized container is exactly what the caller needs:

```cpp
auto people = lfdb::collect<person_record>(dbh, q::prefix("person/"));
```

Use `for_each()` when the operation is naturally callback-shaped and you do not
need a composable generator. Passing a transaction handle keeps the transaction
boundary explicit:

```cpp
auto txn = lfdb::make_transaction(dbh);

lfdb::for_each(txn, q::prefix("person/"), [](auto&& row) {
  const auto& [key, value] = row;
  fmt::println("{}: {}", key, value);
});
```

The database-handle overload is a convenience for bounded, retryable work that
should run in one managed transaction. The callback may run again if the
transaction is retried, so keep it replay-safe:

```cpp
lfdb::for_each<person_record>(dbh, q::prefix("person/"), [](auto&& row) {
  if (row.second.name.empty()) {
    throw std::runtime_error("stored person has no name");
  }
});
```

Use `transform()` when each input row produces one output value:

```cpp
auto txn = lfdb::make_transaction(dbh);
std::vector<std::string> names;

lfdb::transform<person_record>(
  txn,
  q::prefix("person/"),
  [](auto&& row) {
    return std::move(row.second.name);
  },
  std::back_inserter(names));
```

When the transformed values should be materialized directly, let `transform()`
return the vector. The transform function is evaluated inside managed
transaction work, so it should also be replay-safe:

```cpp
auto names = lfdb::transform<person_record>(
  dbh,
  q::prefix("person/"),
  [](auto&& row) {
    return std::move(row.second.name);
  });
```

Use `erase_if()` when the delete decision depends on the decoded row. It returns
the number of keys cleared; database-handle predicates are evaluated inside the
managed transaction loop:

```cpp
const auto removed = lfdb::erase_if<person_record>(
  dbh, q::prefix("person/"),
  [](const auto& row) {
    return row.second.disabled;
  });
```

Selector-shaped scans can also ask for a bounded page. The returned page
contains only rows the caller requested; the extra sentinel row used to detect
continuation is not decoded or returned.

```cpp
auto people = lfdb::select { "person/" };
const auto page = lfdb::scan<person_record>(dbh, people, lfdb::page{100});

if (page.has_more) {
  // Resume from the last returned row.
}
```

### Block Traversal

`blocks()` is for truly large scans. Given a database handle, libfdb internally
plans the range work and manages transactions for each block/window. Use it
when a single transaction may get too old, or when the application naturally
wants to process bounded groups of rows.

```cpp
for (const auto& block : lfdb::blocks(dbh, q::prefix("object/metadata/"))) {
  for (const auto& [key, value] : block) {
    fmt::println("{}: {}", key, value);
  }
}
```

For example, a cache maintenance pass might walk a large object-metadata
keyspace block-at-a-time and only keep one planned block in memory:

```cpp
for (const auto& block : lfdb::blocks<object_metadata>(
       dbh, q::prefix("cache/object/head/"))) {
  for (const auto& [key, metadata] : block) {
    if (metadata.expired(cutoff)) {
      schedule_eviction(key);
    }
  }
}
```

The same pattern works for export or auditing jobs where progress can be
recorded between blocks:

```cpp
std::uint64_t exported = 0;

for (const auto& block : lfdb::blocks<object_metadata>(
       dbh, q::prefix("tenant/acme/object/"))) {
  write_export_batch(block);
  exported += std::size(block);
  lfdb::set(dbh, "export/acme/progress", exported);
}
```

It may also be useful to group flat `scan()` output into discrete groups of N
items. That is different from `blocks()`: the scan has already chosen its
transaction behavior, and `std::views::chunk()` only changes the shape seen by
the caller.

```cpp
// Stream groups of 100:
auto txn = lfdb::make_transaction(dbh);
auto keys = lfdb::scan(txn, q::prefix("key_"));

for (const auto& chunk : keys | std::views::chunk(100)) {
  for (const auto& [key, value] : chunk) {
    // ...
  }
}
```

### Estimating Range Size

`approximate_range_size()` asks FoundationDB for an approximate byte size of a
selection. FoundationDB calculates this from server-side sampling, so it is most
useful for planning large ranges rather than measuring small ranges exactly. The
C API documentation notes that estimates over roughly 3 MB can be considered
accurate enough for many planning purposes:
<https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_get_estimated_range_size_bytes>.

```cpp
const auto object_records =
  fdbc::keyspace("object-cache") / "object" / bucket_id;

const auto bytes = lfdb::approximate_range_size(dbh, fdbc::prefix(object_records));

if (block_planning_threshold < bytes) {
  for (const auto& block : lfdb::blocks<object_metadata>(
         dbh, fdbc::prefix(object_records))) {
    process_block(block);
  }
}
```

The helper accepts the same selection forms as `scan()` and `blocks()`, so query
algebra composes normally:

```cpp
auto visible =
  q::difference(q::prefix("object/"),
                q::prefix("object/private/"));

auto bytes = lfdb::approximate_range_size(dbh, visible);
```

### Explicit Key Ranges

Use `q::between()` when the selection is not a prefix. The default is the same
half-open range shape FoundationDB normally expects: begin included, end
excluded.

```cpp
auto medieval_people = q::between("person/charlemagne", "person/saladin/");
```

Use explicit interval notation when you need different endpoint inclusivity:

```cpp
auto medieval_people = q::between(
  q::closed("person/al-khwarizmi"),
  q::open("person/saladin/"));
```

### Reverse Selection

Attach range options with `q::with_options()`.

```cpp
auto people = q::with_options(q::prefix("person/"),
                              q::query_options{ .reverse_order = true });

auto txn = lfdb::make_transaction(dbh);

for (const auto& [key, value] : lfdb::scan(txn, people)) {
  // process results from high keys to low keys
}
```

### Query Algebra

Underneath the friendly selection helpers, libfdb queries are a compiled
interval algebra. A query expression emits zero or more canonical `lfdb::select`
intervals, and the normal operations consume those expressions directly:
`lfdb::get()`, `lfdb::erase()`, `lfdb::scan()`, `lfdb::blocks()`, and
`lfdb::collect()`.

In this context, an interval is a single contiguous span of lexicographic keys.

A closed bound includes its endpoint; an open bound excludes it. `q::prefix(x)`
is the interval from `x` up to, but not including, `q::successor(x)`.
Intersection keeps only overlapping keys, union combines selected keys,
difference subtracts one query from another, and complement means "everything
outside this query".

For deeper background, Allen's classic paper on interval reasoning is a useful
reference, though libfdb uses a considerably smaller subset:
[Maintaining Knowledge about Temporal Intervals](https://cse.unl.edu/~choueiry/Documents/Allen-CACM1983.pdf).

#### Prefix And Cursor Selection

This is the common "list records under a prefix, optionally starting after a
cursor" shape. The prefix query defines the valid record namespace; the cursor
query is intersected with it, so cursors before the prefix "clamp" to the prefix
and cursors past the prefix compile to an empty selection.

```cpp
auto record_subspace = [](std::string_view collection_id) {
  return std::string(collection_id) + "/records/";
};

const auto base = record_subspace(collection_id);
const auto prefix_key = base + std::string(prefix);
auto query = q::prefix(prefix_key);

if (!cursor.empty()) {
  const auto cursor_key = base + std::string(cursor);
  const auto lower = cursor_inclusive ? q::closed(cursor_key)
                                      : q::open(cursor_key);

  query = q::intersection(
    query,
    q::between(lower, q::open(q::successor(prefix_key))));
}

if (q::is_empty_expression(query)) {
  return -ENOENT;
}

const auto nread = lfdb::get(dbh, query, std::back_inserter(records));

if (0 == nread) {
  return -ENOENT;
}
```

#### Reverse Revision Selection

For a reverse revision list, build the same lexicographic interval and attach
`reverse_order` with `q::with_options()`.

```cpp
auto revision_subspace = [](std::string_view collection_id,
                            std::string_view record_id) {
  return std::string(collection_id) + "#" + std::string(record_id) + "/revisions/";
};

auto revision_key = [&](std::string_view score, std::string_view revision) {
  return revision_subspace(collection_id, record_id) +
         std::string(score) + "/" + std::string(revision);
};

const auto revisions = q::with_options(
  q::between(revision_subspace(collection_id, record_id),
             revision_key(cursor_score, cursor_revision)),
  q::query_options{ .reverse_order = true });

auto revisions_out = lfdb::collect<revision_record>(dbh, revisions);
```

#### Ranked Subranges

If encoded ranks sort lexicographically, a ranked query can select only the
needed rank range instead of scanning the whole revision prefix and filtering
client-side.

```cpp
const auto rank_base = revision_subspace(collection_id, record_id);
const auto begin_prefix = rank_base + std::string(min_rank) + "/";
const auto end_prefix = rank_base + std::string(max_rank) + "/";

const auto rank_range = q::between(begin_prefix, q::successor(end_prefix));
```

#### Combining Queries

Scans accept query expressions directly. This is usually the most natural way
to scan compound queries.

```cpp
const auto active_cache =
  q::set_union(q::prefix("cache/hot/"),
               q::prefix("cache/warm/"));

for (const auto& [key, value] : lfdb::scan(dbh, active_cache)) {
  process(key, value);
}
```

The transaction-backed scan overload also accepts query expressions:

```cpp
auto txn = lfdb::make_transaction(dbh);

for (const auto& [key, value] : lfdb::scan(txn, active_cache)) {
  process(key, value);
}
```

Use difference to subtract a reserved subspace:

```cpp
const auto visible_records =
  q::difference(q::prefix("collection/records/"),
                q::prefix("collection/records/.internal/"));
```

Intersect composite queries with page windows or other bounds to narrow the
result without hand-editing byte ranges. This is useful for prefix scans with
markers, cursors, or authorization windows:

```cpp
const auto visible_objects =
  q::difference(q::prefix("bucket/objects/"),
                q::prefix("bucket/objects/.hidden/"));

const auto page =
  q::intersection(visible_objects,
                  q::between("bucket/objects/a", "bucket/objects/z"));

for (const auto& [key, object] : lfdb::scan<object_record>(dbh, page)) {
  process(key, object);
}
```

Use complement when the natural expression is "everything except this keyspace":

```cpp
const auto public_keys = q::complement(q::prefix("tenant/private/"));
```

Unbounded helpers are still bounded by FoundationDB's ordinary byte keyspace:

```cpp
// Reads keys from "tenant/public/" through the end of ordinary FDB keys.
auto public_tail = q::from(q::lower_at_or_after("tenant/public/"));

// Complement is relative to that same public keyspace, not mathematical infinity.
auto non_private = q::complement(q::prefix("tenant/private/"));
```

Singleton queries are closed on both ends, so they remove one exact key cleanly:

```cpp
const auto without_tombstone =
  q::difference(q::prefix("record/"),
                q::singleton("record/tombstone"));
```

When you specifically need the emitted intervals, use `q::for_each_interval()`:

```cpp
q::for_each_interval(active_cache, [](const lfdb::select& interval) {
  inspect(interval);
});
```

## STL Containers As Values

```cpp
// Store an STL container as one serialized value.
std::vector roles{ "compiler"s, "systems"s, "naval-officer"s };

lfdb::set(dbh, "person/grace-hopper/roles", roles);

std::vector<std::string> out_roles;
lfdb::get(dbh, "person/grace-hopper/roles", out_roles);
```

## Associative Containers As Values

```cpp
// Store an associative container as one serialized value.
std::map<std::string, std::string> profile{
  { "name", "Maria Theresa" },
  { "title", "Archduchess of Austria" },
};

lfdb::set(dbh, "person/maria-theresa/profile", profile);

std::map<std::string, std::string> out_profile;
lfdb::get(dbh, "person/maria-theresa/profile", out_profile);
```

## User Types As Values

```cpp
// Store a user-defined type as one serialized value.
struct person_profile
{
  // User-defined types need to describe their serialized members.
  using serialize = zpp::bits::members<3>;

  std::string name;
  std::string field;
  std::vector<std::string> tags;
};

auto profile = person_profile{
  .name = "Edsger Dijkstra",
  .field = "computer science",
  .tags = std::vector{ "algorithms"s, "formal-methods"s },
};

lfdb::set(dbh, "person/edsger-dijkstra/profile", profile);

person_profile out_profile;
lfdb::get(dbh, "person/edsger-dijkstra/profile", out_profile);
```

## Manual Transactions

```cpp
// Group multiple operations in one explicit transaction.
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/matilda-of-tuscany/name", "Matilda of Tuscany");
lfdb::set(txn, "person/matilda-of-tuscany/title", "Margravine");

if (!lfdb::commit(txn)) {
  // Retry the transaction body.
}
```

`approximate_commit_bytes()` asks FoundationDB to estimate the transaction's
size as if it were committed now. Use it as a batching signal instead of trying
to recreate FoundationDB's accounting for mutations, range clears, and conflict
ranges.

```cpp
auto txn = lfdb::make_transaction(dbh);

for (const auto& object : objects_to_index) {
  lfdb::set(txn, object.index_key, object.index_record);

  if (soft_limit < lfdb::approximate_commit_bytes(txn)) {
    finish_batch(std::move(txn));
    txn = lfdb::make_transaction(dbh);
  }
}
```

## Manual Transactions With Options

```cpp
// Create an explicit transaction with transaction options.
lfdb::transaction_options opts{
  { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
};

auto txn = lfdb::make_transaction(dbh, opts);

lfdb::set(txn, "person/hypatia/name", "Hypatia");

if (!lfdb::commit(txn)) {
  // Retry the transaction body.
}
```

`prepare_replay()` is the lower-level hook for application-managed replay. Most
callers should use a transactor instead; use this when the application needs to
keep its own progress marker before re-running part of a larger operation.
`commit()` already prepares replay before returning `false`; call
`prepare_replay()` only for retryable errors that escape before commit.

```cpp
auto txn = lfdb::make_transaction(dbh);

try {
  rebuild_cache_index(txn, marker);

  if (!lfdb::commit(txn)) {
    retry_from(marker);
  }
}
catch (const lfdb::libfdb_exception& e) {
  if (lfdb::prepare_replay(txn, e.fdb_error_value)) {
    retry_from(marker);
  }
}
```

## Transactors: Replayable Transactions

Transactors are function objects created with `make_transactor()`. Creating a
transactor does not start a transaction; calling `operator()` creates the
transaction, invokes the body, and commits it.

The body may be called more than once after retryable FoundationDB errors. Keep
it deterministic and free of non-idempotent external side effects. If recovery
is not possible, or if user code throws, the exception escapes to the caller.

```cpp
// Use a transactor when the transaction body should be replayed after retryable
// FoundationDB errors.
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn) {
  lfdb::set(txn, "person/eleanor-of-aquitaine/name", "Eleanor of Aquitaine");
  lfdb::set(txn, "person/eleanor-of-aquitaine/title", "Duchess of Aquitaine");
});
```

### Transactor options

```cpp
// Options are applied to each transaction the transactor creates.
lfdb::transaction_options opts{
  { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
};

auto txr = lfdb::make_transactor(dbh, opts);

txr([](auto& txn) {
  lfdb::set(txn, "person/zenobia/name", "Zenobia");
});
```

```cpp
/* Retryable FoundationDB errors may replay the transaction body. */
auto txr = lfdb::make_transactor(dbh);

try {
  txr([](auto& txn) {
    // User exceptions propagate; the body is not committed.
    validate_profile_update();

    lfdb::set(txn, "person/jose-capablanca/title",
              std::vector{ "Original Grandmaster"s, "World Chess Champion"s });
  });
}
catch (const lfdb::libfdb_exception& e) {
  /* libfdb operation failure: FDB status failure, retry exhaustion, decode
   * failure, or invalid data at the FoundationDB boundary. */
}
catch (const std::invalid_argument& e) {
  /* The caller passed an argument that violates the libfdb API contract. */
}
catch (const std::exception& e) {
  /* Application or other runtime error. */
}
```

## Putting It All Together

These examples assume the local libfdb feature stack is available: Content keys,
query algebra, replayable transactors, and versionstamps. They are intentionally
small, but the point is realistic composition: let each library feature remove
one piece of hand-written database plumbing.

### Write Object State And An Ordered Event

```cpp
struct cache_metadata
{
  std::string etag;
  std::string storage_class;
  std::uint64_t size = 0;
};

auto object_keyspace(std::string_view bucket_id, std::string_view object_name)
{
  return fdbc::keyspace("d4n") / "object" / bucket_id / object_name;
}

std::string object_event_prefix(std::string_view bucket_id)
{
  return fmt::format("d4n/event/{}/", bucket_id);
}

std::string object_event_suffix(std::string_view object_name,
                                std::string_view event)
{
  return fmt::format("/{}/{}", object_name, event);
}

auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp event_version;
const auto object = object_keyspace(bucket_id, object_name);

lfdb::set(txn, object / "head", metadata);
lfdb::set(txn, object / "body" / fmt::format("{:020}", block_id), block);
lfdb::set(txn,
          lfdb::versioned(object_event_prefix(bucket_id),
                          object_event_suffix(object_name, "put"),
                          event_version),
          "put");

if (lfdb::commit(txn)) {
  const auto& committed_order = event_version.resolved_bytes();
}
```

Content keys keep the object records grouped. The explicit transaction keeps
the state update and event append atomic. The versionstamped event key is commit
ordered without an application sequence counter.

### Page A Prefix With A Marker

```cpp
auto object_listing_prefix(std::string_view bucket_id,
                           std::string_view prefix)
{
  return fmt::format("d4n/list/{}/{}", bucket_id, prefix);
}

auto object_listing_key(std::string_view bucket_id,
                        std::string_view object_name)
{
  return fmt::format("d4n/list/{}/{}", bucket_id, object_name);
}

const auto listing_prefix = object_listing_prefix(bucket_id, prefix);
const auto page_options =
  q::query_options{ .result_limit = static_cast<int>(page_size + 1) };

auto read_page = [&](auto query) {
  return lfdb::collect<cache_metadata>(dbh,
                                       q::with_options(std::move(query),
                                                       page_options));
};

auto page = marker.empty()
          ? read_page(q::prefix(listing_prefix))
          : read_page(q::prefix_starting_after(
              listing_prefix,
              object_listing_key(bucket_id, marker)));
```

The prefix helper gives the right upper bound, the marker helper gives the right
exclusive lower bound, and the range limit stays attached to the query instead
of being patched into a selector by hand.

### Prune Visible Cache Records

```cpp
const auto cache = fdbc::keyspace("d4n") / "cache" / bucket_id;
const auto reserved = cache / ".internal";
const auto tombstones = cache / ".tombstone";
const auto cutoff_key = cache / fmt::format("{:020}", cutoff_epoch);

const auto visible_cache =
  q::difference(fdbc::prefix(cache),
                q::set_union(fdbc::prefix(reserved),
                             fdbc::prefix(tombstones)));

const auto old_visible_cache = q::ending_before(visible_cache, cutoff_key);

auto txr = lfdb::make_transactor(dbh);
txr([&](auto& txn) {
  for (const auto& [key, value] : lfdb::scan<cache_metadata>(txn,
                                                             old_visible_cache)) {
    archive_cache_record(key, value);
    lfdb::erase(txn, key);
  }
});
```

The query expression says exactly what should be touched: cache records, minus
reserved subspaces, further trimmed by a cursor. The scan loop does work; it
does not reimplement filtering.

## Transaction Watches (Triggers)

Transaction watches (watches, triggers) ask FoundationDB to report a change
to a key (relative to the transaction that created the watch). If watches are
unsupported by the local FoundationDB configuration, or the transaction cannot
create watches, waiting on the watch throws `lfdb::libfdb_exception`.

Note that watches do not report the triggering value. If you need the value,
read it in a separate transaction. For the underlying semantics, see the
FoundationDB Developer Guide and the C API entry for ``fdb_transaction_watch()``.

See also:
- https://apple.github.io/foundationdb/developer-guide.html#watches
- https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_watch

Here are some illustrative examples using transaction watches:

```cpp
// Create a one-shot watch that becomes ready when the key changes:
auto watch = lfdb::make_watch(dbh, "person/jose-capablanca/title");

lfdb::set(dbh, "person/jose-capablanca/title", "World Chess Champion");

if (lfdb::watch_event::changed == watch.wait_for_event()) {
  handle_title_change();
}
```

```cpp
// Create a watch inside a transaction when it must share that transaction's
// read version. Commit the transaction before waiting on the watch:
auto txn = lfdb::make_transaction(dbh);
auto watch = lfdb::make_watch(txn, "person/jose-capablanca/title");

if (lfdb::commit(txn)) {
  watch.wait();
}
```

```cpp
// Cancel a watch that is no longer needed:
auto watch = lfdb::make_watch(dbh, "person/jose-capablanca/title");

watch.cancel();

if (lfdb::watch_event::cancelled == watch.wait_for_event()) {
  handle_watch_cancelled();
}
```

libfdb exposes watch readiness (`ready()`), explicit cancellation (`cancel()`),
blocking waits (`wait_for_event()`), and cooperative cancellation
(`wait_for_event(stop_token)`) so applications can choose the timeout strategy
that fits their scheduler. The same primitives can support polling, event-loop
integration, stop-token cancellation, or a dedicated watch thread. You do not
need to create an extra thread to put a timeout around a watch wait:

```cpp
bool wait_until_ready(lfdb::watch_handle& watch, auto timeout)
{
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  while (not watch.ready() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return watch.ready();
}

auto watch = lfdb::make_watch(dbh, "person/jose-capablanca/title");

if (not wait_until_ready(watch, std::chrono::seconds(5))) {
  watch.cancel();
  handle_watch_timeout();
  return;
}

if (lfdb::watch_event::changed == watch.wait_for_event()) {
  handle_title_change();
}
```

```cpp
// Let a jthread stop_token cancel a blocked transaction watch wait:
std::jthread watch_thread {
  [dbh](std::stop_token stop_token) {
    auto watch = lfdb::make_watch(dbh, "person/jose-capablanca/title");

    if (lfdb::watch_event::cancelled == watch.wait_for_event(stop_token)) {
      return;
    }

    handle_title_change();
  }
};

watch_thread.request_stop();
```

`watched_loop()` is a gadget for repeated watch handling. Its callback takes
the watched key as a `std::string_view`. The helper blocks; applications should
own any thread, executor, shutdown, or callback error policy around it:

```cpp
std::jthread watch_thread {
  [dbh](std::stop_token stop_token) {
    lfdb::watched_loop(dbh, "person/jose-capablanca/title", stop_token,
      [](std::string_view key) {
        handle_title_change(key);
      });
  }
};
```

Use a manual approach when you need direct control over each one-shot watch:

```cpp
// Reset (re-arm) the transaction watch manually after each event:
std::jthread watch_thread {
  [dbh](std::stop_token stop_token) {
    while (not stop_token.stop_requested()) {
      auto watch = lfdb::make_watch(dbh, "person/jose-capablanca/title");

      if (lfdb::watch_event::cancelled == watch.wait_for_event(stop_token)) {
        break;
      }

      handle_title_change();
    }
  }
};
```

## Appendix: Exception Summary

libfdb tries to keep its exception surface small. Ordinary library operation
failures are reported as `lfdb::libfdb_exception`; direct caller contract
violations use standard exceptions.

The categories are intentionally coarse: they are meant to be useful to
callers, not to divide every possible misuse into a separate exception type. For
example, some versionstamp state violations are treated as `std::invalid_argument`
to avoid a crazy taxonomy explosion.

| Condition | Exception |
| --- | --- |
| FoundationDB C API returns `fdb_error_t` | `lfdb::libfdb_exception` |
| FoundationDB option setup fails | `lfdb::libfdb_exception` |
| Retry recovery fails or retry limit is exceeded | `lfdb::libfdb_exception` |
| zpp_bits cannot decode stored bytes into the requested type | `lfdb::libfdb_exception` |
| Invalid result at the FoundationDB boundary | `lfdb::libfdb_exception` |
| Caller uses libfdb after shutdown | `lfdb::libfdb_exception` |
| Caller passes an impossible selector prefix or invalid versionstamp bytes | `std::invalid_argument` |
| Caller reads, compares, reuses, or overwrites a versionstamp in the wrong state; or asks for a committed version before commit | `std::invalid_argument` |
