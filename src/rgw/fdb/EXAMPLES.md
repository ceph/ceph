# libfdb Examples

"Take a thousand days of practice for forging, and ten thousand days of practice for refining."
    -- Miyamoto Musashi, Go Rin No Sho (~1645)

Welcome, traveller! Grab your favorite walking stick, and let us journey into
the realm of libfdb!

While this is not proper documentation, hopefully this "cookbook-stye" set
of mini-examples will help you on your libfdb path.

Errata: Please report errata, or contact with examples you would like to see!

These examples use a short namespace alias for readability and also to save
typing with one's poor fingers! Yoikes!

See examples/libfdb/ for some working, compilable simple examples.

```cpp
namespace lfdb = ceph::libfdb;
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
/* Use a database_handle when you desire a single logical operation. Behind
 * the scenes, libfdb will create and complete its own transaction for you.
 * Database-handle operations may retry after recoverable FoundationDB errors,
 * so callbacks and output iterators may be activated more than once. */
lfdb::set(dbh, "person/barbara-moo/name", "Barbara Moo");
```

```cpp
/* Pass a transaction handle when several operations must be grouped in the
 * same transaction. Do not use the transaction after commit(). */
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/barbara-moo/name", "Barbara Moo");
lfdb::set(txn, "person/barbara-moo/book", "Accelerated C++");

if (!lfdb::commit(txn)) {
  /* Retry the transaction body with a fresh or recovered transaction. */
}
```

## Setup

```cpp
/* Open the default FoundationDB database. */
auto dbh = lfdb::create_database();
```

```cpp
/* Open a database with explicit database and network options. Explicit database
 * options are used as passed. Flag-only options use lfdb::option_flag because
 * they have no value. Network options are applied only during the first
 * FoundationDB network initialization; later calls to create_database() cannot
 * change them. */
lfdb::database_options dbopts{
  { FDB_DB_OPTION_TRANSACTION_TIMEOUT, std::int64_t{5000} },
};

lfdb::network_options netopts{
  { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag },
};

auto dbh = lfdb::create_database(dbopts, netopts);
```

```cpp
/* Open a database with an explicit cluster file plus database/network options. */
auto dbh = lfdb::create_database("/path/to/fdb.cluster", dbopts, netopts);
```

## Single-Key Operations

```cpp
/* Store and retrieve one value by key. */
lfdb::set(dbh, "person/konrad-zuse/name", "Konrad Zuse");

std::string name;
if (lfdb::get(dbh, "person/konrad-zuse/name", name)) {
  /* use name */
}
```

```cpp
/* Use a callback when the raw serialized bytes must be copied or decoded
 * immediately. The span is only valid during the callback. */
lfdb::get(dbh, "person/konrad-zuse/name",
          [](std::span<const std::uint8_t> bytes) {
            /* copy or decode bytes here */
          });
```

## Key Existence And Erase

```cpp
/* Check for a key and erase it if it exists. */
if (lfdb::key_exists(dbh, "person/jose-capablanca/title")) {
  lfdb::erase(dbh, "person/jose-capablanca/title");
}
```

## Multi-Key Writes

```cpp
/* Write key/value pairs from an STL associative container in one transaction. */
std::map<std::string, std::string> people{
  { "person/saladin/name", "Saladin" },
  { "person/al-khwarizmi/name", "Al-Khwarizmi" },
  { "person/albrecht-duerer/name", "Albrecht Duerer" },
};

lfdb::set(dbh, std::begin(people), std::end(people));
```

## Multi-Key Reads

```cpp
/* Read a key range into an STL associative container. */
std::map<std::string, std::string> people;

lfdb::get(dbh, lfdb::select { "person/" }, people);
```

## Key Ordering

```cpp
/* FoundationDB keys are ordered lexicographically by byte string. Choose key
 * formats so lexical order matches the scan order you want. Numeric suffixes
 * should usually be fixed-width and zero-padded. */
lfdb::set(dbh, "person/000001/name", "Barbara Moo");
lfdb::set(dbh, "person/000010/name", "Konrad Zuse");
```

## Prefix Selection

"select" has two constructor forms. The one-argument form is usually the one
you want: it selects every key with a shared prefix. This is a natural fit for
FoundationDB key design, where related records are commonly grouped under a
prefix such as `person/`, `bucket/index/`, or `object/metadata/`.

```cpp
/* Select all keys beginning with "person/". */
auto people = lfdb::select { "person/" };
```

Using a key beginning with 0xFF will result in unpredictable behavior.

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

The canonical way to build a keyspace is similar to writing a path
```cpp
const std::string bucket_id = "bucket.8409.12";
const std::string version = "v0000000000000017";
const std::string object_name = "photos/2026/beach.jpg";

const auto object_head =
  fdbc::keyspace("d4n") / "cache" / "object" / bucket_id / version / object_name;

lfdb::set(dbh, object_head, object_metadata);
```

...the result, in object_head above, is a compiled key that follows FoundationDB rules.

Content keys are meant to flow into libfdb operations directly; callers should not
need to extract bytes from them.

Exact lookup uses the compiled key:

```cpp
std::string metadata;

if (lfdb::get(dbh, object_head, metadata)) {
  /* cache hit */
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

Here is the same idea applied to a D4N-style block directory key. The legacy
form had to URL-encode delimiter characters by hand; the Content layer treats
bucket, object, block id, and size as separate key segments.

```cpp
fdbc::compiled_key BlockDirectory::build_index(CacheBlock *block)
{
  return fdbc::keyspace("d4n") / "block" / block->cacheObj.bucketName /
         block->cacheObj.objName / fmt::format("{:020}", block->blockID) /
         fmt::format("{:020}", block->size);
}
```

Scan all cached blocks for one object by selecting the object-block prefix:

```cpp
const auto object_blocks =
  fdbc::keyspace("d4n") / "block" / bucket_id / object_name;

for (auto&& block : lfdb::block_generator<CacheBlock>(
       dbh, fdbc::prefix(object_blocks))) {
  // consume block
}
```

### Normal Interface: Functions

The function form is the same normal interface when a call expression is clearer
than a chain:

```cpp
const auto object_head = fdbc::key("d4n",
                                   "cache",
                                   "object",
                                   bucket_id,
                                   version,
                                   object_name);

const auto block_key = fdbc::key("d4n",
                                 "cache",
                                 "object",
                                 bucket_id,
                                 version,
                                 object_name,
                                 "block",
                                 fmt::format("{:020}", offset),
                                 fmt::format("{:020}", length));
```

### Detailed Interface: Assembly

`assemble()` is the explicit compiler entry point used under the nicer interfaces.
Use it when you want the validation/lowering step to be visible, for example in
tests, generated schemas, or other code that builds key layouts as data.

```cpp
const auto object_head = fdbc::assemble("d4n",
                                        "cache",
                                        "object",
                                        bucket_id,
                                        version,
                                        object_name);
```

Invalid segment types are rejected at compile time, and invalid keyspace values
throw during assembly:

```cpp
static_assert(fdbc::key_segments<std::string_view, std::string_view>);
static_assert(!fdbc::key_segments<int>);

const auto d4n_objects = fdbc::assemble("d4n", "cache", "object");
```

The lowering code is written to be constexpr-friendly, but the current compiled
target owns a `std::string`. A future fixed-size/literal compiled target would
let literal-only assembly produce a fully constexpr key.

### Selecting Content Keys

Use `fdbc::prefix()` for a full prefix range. It returns a normal `lfdb::select`,
so it works with the ordinary range-read APIs:

```cpp
/* All cached D4N records for one object version. */
const auto object_records =
  fdbc::keyspace("d4n")
  / "cache"
  / "object"
  / bucket_id
  / version
  / object_name;

std::vector<std::pair<std::string, std::string>> records;

lfdb::get(dbh, fdbc::prefix(object_records), std::back_inserter(records));
```

For a subrange, compose the lower and upper bounds and use the ordinary selector
constructors:

```cpp
const auto first_block =
  object_records / "block" / fmt::format("{:020}", first_offset);

const auto last_block =
  object_records / "block" / fmt::format("{:020}", last_offset);

auto blocks = lfdb::select { first_block, last_block };

lfdb::get(dbh, blocks, std::back_inserter(records));
```

The explicit selector is half-open: the begin key is included and the end key is
excluded. Use `lfdb::inclusive()` or `lfdb::exclusive()` when the boundary shape
needs to be different.

## Explicit Key Ranges

```cpp
/* Select a half-open lexicographic key range: begin is included, end is
 * excluded. */
auto medieval_people = lfdb::select { "person/charlemagne", "person/saladin/" };
```

## Pair Generator

`pair_generator()` reads a range through a transaction supplied by the caller.
Use it when the query is expected to fit within one transaction and/or you want
control over the transaction's lifetime and options.

```cpp
auto txn = lfdb::make_transaction(dbh);

for (const auto& [key, value] : lfdb::pair_generator(txn, lfdb::select { "person/" })) {
  fmt::println("{}: {}", key, value);
}
```

To get results in reverse order, set the reverse_order property in the selector:

```cpp
auto people = lfdb::select { "person/" };
people.options.reverse_order = true;
auto txn = lfdb::make_transaction(dbh);

for (const auto& [key, value] : lfdb::pair_generator(txn, people)) {
  /* process results from high keys to low keys */
}
```

It may be useful to group pair_generator()'s output into discrete groups of N items. One way to do that is
with a chunk_view:

```cpp
// Stream groups of 100:
auto txn = lfdb::make_transaction(dbh);
auto keys = lfdb::pair_generator(txn, lfdb::select { "key_" });

for (const auto& chunk : keys | std::views::chunk(100)) {
  for (const auto& [key, value] : chunk) {
    // ...
  }
}
```

To get results in reverse order, set the reverse_order property in the selector:

```cpp
auto people = lfdb::select { "person/" };
people.options.reverse_order = true;

for (const auto& [key, value] : lfdb::pair_generator(dbh, people)) {
  /* process results from high keys to low keys */
}
```

While block_generator() provides a way to get blocks of results, it also has different
request behavior than pair_generator(); it may therefore be useful to group pair_generator()'s
output into chunks. One way to do that is with a chunk_view:

```cpp
// Stream groups of 100:
auto keys = lfdb::pair_generator(dbh, lfdb::select { "key_" });

for (const auto& chunk : keys | std::views::chunk(100)) {
  for (const auto& [key, value] : chunk) {
    // ...
  }
}
```

## Block Generator

`block_generator()` is useful for reads that may become very large. Given a database
handle, it internally manages transactions for each planned block/window. Use it for very
large scans where a single transaction may get too old or where block-at-a-time
processing is preferable.

```cpp
/* Use block_generator() for large range scans where split planning and
 * block-at-a-time processing are useful. */
for (auto&& block : lfdb::block_generator(dbh, lfdb::select { "object/metadata/" })) {
  for (const auto& [key, value] : block) {
    fmt::println("{}: {}", key, value);
  }
}
```

## STL Containers As Values

```cpp
/* Store an STL container as one serialized value. */
std::vector roles{ "compiler"s, "systems"s, "naval-officer"s };

lfdb::set(dbh, "person/grace-hopper/roles", roles);

std::vector<std::string> out_roles;
lfdb::get(dbh, "person/grace-hopper/roles", out_roles);
```

## Associative Containers As Values

```cpp
/* Store an associative container as one serialized value. */
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
/* Store a user-defined type as one serialized value. */
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
/* Group multiple operations in one explicit transaction. */
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/matilda-of-tuscany/name", "Matilda of Tuscany");
lfdb::set(txn, "person/matilda-of-tuscany/title", "Margravine");

if (!lfdb::commit(txn)) {
  /* Retry the transaction body. */
}
```

## Manual Transactions With Options

```cpp
/* Create an explicit transaction with transaction options. */
lfdb::transaction_options opts{
  { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
};

auto txn = lfdb::make_transaction(dbh, opts);

lfdb::set(txn, "person/hypatia/name", "Hypatia");

if (!lfdb::commit(txn)) {
  /* Retry the transaction body. */
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
/* Use a transactor when the transaction body should be replayed after retryable
 * FoundationDB errors. */
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn) {
  lfdb::set(txn, "person/eleanor-of-aquitaine/name", "Eleanor of Aquitaine");
  lfdb::set(txn, "person/eleanor-of-aquitaine/title", "Duchess of Aquitaine");
});
```

### Transactor options

```cpp
/* Options are applied to each transaction the transactor creates. */
lfdb::transaction_options opts{
  { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
};

auto txr = lfdb::make_transactor(dbh, opts);

txr([](auto& txn) {
  lfdb::set(txn, "person/zenobia/name", "Zenobia");
});
```

```cpp
/* Retryable FoundationDB errors are handled before control returns here. */
auto txr = lfdb::make_transactor(dbh);

try {
    txr([](auto& txn) {
        /* User exceptions propagate; the body is not committed. */
        validate_profile_update();

        lfdb::set(txn, "person/jose-capablanca/title",
                  std::vector{ "Original Grandmaster"s, "World Chess Champion"s });
    });
}
catch (const lfdb::libfdb_exception& e) {
    /* FoundationDB reported an error that libfdb could not recover from. */
}
catch (const std::exception& e) {
    /* Application or system error from user code. */
}
```

## Putting It All Together

These examples assume the local libfdb feature stack is available: Content keys,
query algebra, replayable transactors, transaction watches, and versionstamps.
They are intentionally small, but the point is realistic composition: let each
library feature remove one piece of hand-written database plumbing.

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

### Refresh On Control-Key Changes

```cpp
std::string cache_policy_key(std::string_view bucket_id)
{
  return fmt::format("d4n/control/{}/cache-policy", bucket_id);
}

std::jthread policy_thread {
  [dbh, bucket_id](std::stop_token stop_token) {
    const auto policy_key = cache_policy_key(bucket_id);

    lfdb::watched_loop(dbh, policy_key, stop_token,
      [dbh](std::string_view key) {
        cache_policy policy;

        if (lfdb::get(dbh, key, policy)) {
          install_cache_policy(policy);
        }
      });
  }
};
```

A watch is only the invalidation signal. The callback performs a normal read, so
it observes the current value and can use the same decoding path as every other
single-key lookup.
