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
/* Open a database with database and network options. Flag-only options use
 * lfdb::option_flag because they have no value. Network options are applied
 * only during the first FoundationDB network initialization; later calls to
 * create_database() cannot change them. */
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

lfdb::get(dbh,
          lfdb::select { "person/" },
          std::inserter(people, std::end(people)));
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

## Explicit Key Ranges

```cpp
/* Select a half-open lexicographic key range: begin is included, end is
 * excluded. */
auto medieval_people = lfdb::select { "person/charlemagne", "person/saladin/" };
```

## Pair Generator

```cpp
/* Use pair_generator() for ordinary range scans where processing one key/value
 * pair at a time is natural. */
std::map<std::string, std::string> people;

std::ranges::copy(lfdb::pair_generator(dbh, lfdb::select { "person/" }),
                  std::inserter(people, std::end(people)));
```

## Block Generator

For very large prefix scans, use the same one-argument selector with
`block_generator()`. This lets libfdb plan and read the range in blocks while
the call site still names the logical key family directly.

```cpp
/* Use block_generator() for large range scans where split planning and
 * block-at-a-time processing are useful. */
for (auto&& block : lfdb::block_generator(dbh, lfdb::select { "object/metadata/" })) {
  /* process block */
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
