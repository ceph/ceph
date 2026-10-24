# libfdb Examples

> "Take a thousand days of practice for forging, and ten thousand days of practice for refining."
>
> — Miyamoto Musashi, *Go Rin No Sho* (~1645)

Welcome, traveller. This is a cookbook-style tour of libfdb. It starts with
ordinary key/value operations and then moves toward transactions, selectors,
query algebra, content keys, watches, and system utilities.

That gives us three natural usage patterns:

| Need | Usual interface |
|---|---|
| A simple operation | Free functions taking a database handle |
| A logical transaction with managed recovery | A transactor |
| Explicit transaction boundaries and recovery | Free functions taking a transaction handle |

Errata: Please report errata, or contact with examples you would like to see.

See `examples/libfdb/` for small standalone programs.
Build the standalone example against an existing FoundationDB-enabled Ceph build:

```sh
make -C examples/libfdb -j8 CEPH_BUILD="$PWD/build"
./examples/libfdb/mini_example
```

The program writes and reads `example/people` and verifies the round trip.
It releases its handles and stops the client network before exiting, including
when an operation fails.

**Breaking API changes:**

- `commit(txn)` returns `void` on success, and `commit(with_result, txn)` reports
  commit-future errors through `std::expected<void, std::error_code>`. Both submit
  once. Callers using the former commit/transactor result records must migrate;
  see [Transactions](#transactions) for commit and recovery examples.
- `libfdb_exception` derives from `std::system_error`. Use `code()` for the
  authoritative error; the constructor taking a bare integer and the mutable
  `fdb_error_value` field are removed. See
  [Error Codes and Exceptions](#error-codes-and-exceptions).

The examples use aliases to keep the code readable:

```cpp
#include "rgw/fdb/fdb.h"

#include <iostream>
#include <map>

namespace lfdb = ceph::libfdb;
namespace q = lfdb::query;
namespace fdbc = lfdb::layer::content;

using namespace std::string_literals;
```

The examples use the spellings below. The compatibility names perform the same
operations and remain available for existing callers.

| Purpose | Examples use | Compatibility name |
| --- | --- | --- |
| Stream rows | `lfdb::scan()` | `lfdb::pair_generator()` |
| Stream blocks of rows | `lfdb::blocks()` | `lfdb::block_generator()` |
| Compile content keys | `fdbc::key()` | `fdbc::assemble()` |

The snippets share the database opened in Setup. Application record types and
functions such as `use_name()` stand for the application's own data and work.
The recovery examples below contain their entire transaction bodies.

Choose the failure interface at the operation boundary:

| Operation | Success | Failure |
| --- | --- | --- |
| Database-handle write helper or transactor | The logical operation committed, after any managed replay | An exception; an earlier attempt may have committed if its outcome was uncertain |
| `commit(txn)` | The one submitted attempt committed | Native failure throws `libfdb_exception`; recovery remains with the caller |
| `commit(with_result, txn)` | A successful `std::expected<void, std::error_code>` confirms commit | Commit-future errors are returned as values; waiting, local, contract, and allocation failures may throw |
| `stamp.resolved_bytes(with_result)` | Borrowed stamp bytes | A recorded stamp error, independent of commit success; pending access is a contract error |

An exception is not a general rollback guarantee. Managed replay includes
`commit_unknown_result` (1021) by default. Use an idempotent transaction design,
or select a policy that stops on uncertain outcomes. Ordinary C++ exceptions
from application code propagate unchanged. See [Transactions](#transactions),
[Versioned Data](#versioned-data), and the [exception summary](#appendix-exception-summary).

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
// Open a database with explicit database and network options. Flag-only options
// use lfdb::option_flag because they have no value. Network options are applied
// only during the first FoundationDB network initialization.
lfdb::database_options dbopts{
  { FDB_DB_OPTION_TRANSACTION_TIMEOUT, std::int64_t{5000} }
};

lfdb::network_options netopts{
  { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag }
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

The source type makes the distinction explicit: a string is a FoundationDB
connection string, while a cluster-file name must be passed as a
`std::filesystem::path`.

## Basic Operations

Use a `database_handle` when you want libfdb to create, commit, and retry a
single logical operation:

```cpp
lfdb::set(dbh, "person/barbara-moo/name", "Barbara Moo");
```

Single-key `get()` returns whether the key was found:

```cpp
lfdb::set(dbh, "person/konrad-zuse/name", "Konrad Zuse");

std::string name;
if (lfdb::get(dbh, "person/konrad-zuse/name", name)) {
  use_name(name);
}
```

Use a void callback when the raw serialized bytes must be copied or decoded
immediately. The span is only valid during the callback.

```cpp
lfdb::get(dbh, "person/konrad-zuse/name",
          [](std::span<const std::uint8_t> bytes) {
            copy_or_decode(bytes);
          });
```

Check for a key and erase it if it exists:

```cpp
if (lfdb::key_exists(dbh, "person/jose-capablanca/title")) {
  lfdb::erase(dbh, "person/jose-capablanca/title");
}
```

Write key/value pairs from an STL associative container in one managed
transaction:

```cpp
std::map<std::string, std::string> people{
  { "person/saladin/name", "Saladin" },
  { "person/al-khwarizmi/name", "Al-Khwarizmi" },
  { "person/albrecht-duerer/name", "Albrecht Duerer" }
};

lfdb::set(dbh, people);
```

Selector and query-expression `get()` overloads return the number of key/value
pairs emitted to the output container or iterator:

```cpp
std::map<std::string, std::string> people;

const auto nread = lfdb::get(dbh, q::prefix("person/"), people);
```

## Values And Serialization

libfdb stores C++ values as FoundationDB byte values. Built-in scalar/string
cases are ordinary `set()`/`get()` calls. STL containers and user-defined types
are serialized as one stored value.

```cpp
std::vector roles{ "compiler"s, "systems"s, "naval-officer"s };

lfdb::set(dbh, "person/grace-hopper/roles", roles);

std::vector<std::string> out_roles;
lfdb::get(dbh, "person/grace-hopper/roles", out_roles);
```

```cpp
std::map<std::string, std::string> profile{
  { "name", "Maria Theresa" },
  { "title", "Archduchess of Austria" }
};

lfdb::set(dbh, "person/maria-theresa/profile", profile);

std::map<std::string, std::string> out_profile;
lfdb::get(dbh, "person/maria-theresa/profile", out_profile);
```

User-defined types need to describe their serialized members:

```cpp
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
  .tags = std::vector{ "algorithms"s, "formal-methods"s }
};

lfdb::set(dbh, "person/edsger-dijkstra/profile", profile);

person_profile out_profile;
lfdb::get(dbh, "person/edsger-dijkstra/profile", out_profile);
```

## Transactions

Pass a transaction handle when several operations must be grouped in the same
transaction. After a confirmed commit, do not reuse the transaction for more
operations. Ordinary `commit()` returns `void` on success and throws
`libfdb_exception` on a FoundationDB failure:

```cpp
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/barbara-moo/name", "Barbara Moo");
lfdb::set(txn, "person/barbara-moo/book", "Accelerated C++");

lfdb::commit(txn);
```

Both `commit(txn)` and `commit(with_result, txn)` submit exactly once. Neither
prepares a retry, resets the transaction, or backs off. The tagged overload
returns `std::expected<void, std::error_code>`: success confirms the commit;
failure carries the native error, including terminal errors and
`commit_unknown_result` (1021). That error means the write may have committed.
Failure to wait for the commit result throws without managed replay; the native
commit outcome remains unknown. Local failures, allocation failures, and contract
violations may also throw.
The ordinary overload explicitly throws `libfdb_exception` for a failed result.

An error before commit submission leaves manual transaction boundaries with the
caller. For example, if encoding one record fails, earlier valid writes and
registered stamp outputs remain pending; the caller may finish those writes or
discard the transaction. A managed transactor instead abandons the failed body.

This synchronous key/value example writes a name, retries only errors classified
as definitely uncommitted, and reads the value back after confirmed success.
It uses the `dbh` opened in Setup and the includes and aliases above. Timeout
and retry limits apply across attempts on the same transaction:

```cpp
const std::string key = "example/sync-commit/name";
const std::string expected = "Frances Allen";
const lfdb::transaction_options options {
  {FDB_TR_OPTION_TIMEOUT, std::int64_t {5000}},
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}}
};

auto txn = lfdb::make_transaction(dbh, options);

try {
  for (;;) {
    // Rebuild the entire transaction body after each reset.
    lfdb::set(txn, key, expected);

    const auto result = lfdb::commit(lfdb::with_result, txn);

    if (result) {
      break;
    }

    if (not lfdb::retryable_not_committed(result.error())) {
      // Includes 1021: preserve the error and stop instead of blindly replaying.
      throw lfdb::libfdb_exception {result.error()};
    }

    // Prepare explicitly, then rebuild the body using the same native budgets.
    lfdb::reset_for_replay(txn, result.error());
  }
}
catch (const lfdb::libfdb_exception& error) {
  if (lfdb::maybe_committed(error.code())) {
    std::cerr << "The write may have committed; resolve its outcome before replay.\n";
  }

  std::cerr << "Commit/retry error: " << error.what() << '\n';
  throw;
}

// Use a separate read transaction, not the transaction that just committed.
std::string actual;
if (not lfdb::get(dbh, key, actual) or expected != actual) {
  throw std::runtime_error {"committed key/value did not round-trip"};
}

std::cout << key << " = " << actual << '\n';
```

Only `retryable_not_committed()` permits another attempt in this example;
`retryable()` also accepts 1021. `reset_for_replay()` runs native `on_error()`,
including its backoff and retry budget, then restores managed options. If
preparation fails, it throws the actual preparation error, which can differ
from the commit error. The application owns any history it needs to retain.

The catch reports uncertain results converted to exceptions above, and failures
from retry preparation, then rethrows. It does not start a fresh transaction to
bypass exhausted budgets. Other exceptions propagate too. An exception after
submission is not proof that the write did not commit. A stamp error is reported
separately after a confirmed commit; it never requests replay of that commit.

The final readback assumes no other writer changes this example key. A missing
or different value, or a read error, fails that check after the commit has
already succeeded; it does not undo the write. Reading the expected value after
an uncertain commit would not, by itself, prove that this particular attempt
performed the write. Resolving an uncertain outcome requires application
knowledge, such as a durable operation identifier, or a deliberate decision
that replaying the whole operation is safe.

`committed_version()` is available after a successful commit. `set_read_version()`
pins another transaction to that version, and `read_version()` reports the
version an active transaction reads from. This is a consistency tool, not
durable historical storage.

```cpp
auto write_txn = lfdb::make_transaction(dbh);

lfdb::set(write_txn, "person/grace-hopper/name", "Grace Hopper");

lfdb::commit(write_txn);
const auto version = lfdb::committed_version(write_txn);

auto read_txn = lfdb::make_transaction(dbh);
lfdb::set_read_version(read_txn, version);

std::string name;
if (lfdb::get(read_txn, "person/grace-hopper/name", name)) {
  record_observation(name, lfdb::read_version(read_txn));
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
    lfdb::commit(txn);
    txn = lfdb::make_transaction(dbh);
  }
}

lfdb::commit(txn);
```

Each commit submits once. If a later batch fails, earlier batches remain
committed; the application owns progress and resumption across those boundaries.
Use a transactor per batch when managed replay is appropriate.

Pass transaction options when constructing a transaction or transactor. Each
transaction owns an option snapshot. Managed replay reapplies the options that
FoundationDB resets after a successful `on_error()`:

```cpp
lfdb::transaction_options opts{
  { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
};

auto update = lfdb::make_transactor(dbh, opts);
update([](auto txn) {
  lfdb::set(txn, "person/hypatia/name", "Hypatia");
});
```

`reset_for_replay()` is the lower-level hook for application-managed replay. Most
callers should use a transactor instead; use this when the application needs to
keep its own progress marker before re-running part of a larger operation.
Call `reset_for_replay()` after deciding that the entire transaction body may
be replayed, for a failure from either the body or commit. A successful reset
prepares another attempt; a failed reset throws and ends this recovery loop.
Pass the complete error code: its category must identify a nonzero native FDB
error. A libfdb or application error is rejected before changing the transaction.

```cpp
const lfdb::transaction_options options {
  {FDB_TR_OPTION_TIMEOUT, std::int64_t {5000}},
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}}
};
auto txn = lfdb::make_transaction(dbh, options);

for (;;) {
  try {
    std::uint64_t count = 0;
    lfdb::get(txn, "example/manual/count", count);
    lfdb::set(txn, "example/manual/count", count + 1);
    lfdb::commit(txn);
    break;
  }
  catch (const lfdb::libfdb_exception& e) {
    if (not lfdb::retryable_not_committed(e.code())) {
      throw;
    }
    lfdb::reset_for_replay(txn, e.code());
  }
}
```

The catch covers both the read and commit. It permits replay only when the
current attempt is definitely uncommitted; it propagates 1021 so the application
can resolve whether the increment happened. The reset runs in the handler, so
a preparation failure escapes without starting another attempt. This example
assumes the stored unsigned count has room for one more increment.

Transactors are function objects created with `make_transactor()`. Creating a
transactor does not start a transaction; calling `operator()` creates the
transaction, invokes the body, and commits it. The body may be called more than
once after retryable FoundationDB errors. Keep it deterministic and free of
non-idempotent external side effects.

Synchronous transactors borrow the body for the duration of the call, including
when additional arguments are bound. Noncopyable lvalue bodies work directly,
and mutations of their state remain visible to the caller. Bound arguments
retain their existing owned/staged semantics for replay. Asynchronous
`transact()` operations own their bodies because execution may start later.

```cpp
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn) {
  lfdb::set(txn, "person/eleanor-of-aquitaine/name", "Eleanor of Aquitaine");
  lfdb::set(txn, "person/eleanor-of-aquitaine/title", "Duchess of Aquitaine");
});
```

### Return Values And Application Failures

A normal return from the transaction body tells libfdb to attempt a commit.
A negative status such as `-EINVAL` is only an application return value; it does
not abort the transaction. The transactor returns that value after a confirmed
commit, including any writes made before the return:

```cpp
#include <cerrno>

auto txr = lfdb::make_transactor(dbh);

const auto status = txr([](auto& txn) {
  lfdb::set(txn, "example/transactor-status", "committed");
  return -EINVAL;
});

// status == -EINVAL, but the key now holds "committed".
```

To reject an update before commit, throw an application exception and let it
escape the body. Catch or translate it into an error code outside the
transactor. Catching it inside the body and returning a negative status still
leads to a commit attempt:

```cpp
try {
  txr([](auto& txn) {
    lfdb::set(txn, "example/transactor-status", "rejected");
    throw std::invalid_argument {"invalid update"};
  });
}
catch (const std::invalid_argument& e) {
  handle_contract_error(e);
}

// This attempt did not commit; the key still holds "committed".
```

Retryable `lfdb::libfdb_exception` errors request recovery and replay instead;
use an application exception for application rejection. Throwing stops the
current attempt, but does not undo an earlier attempt whose commit outcome was
unknown.

### Staged Proxies

A transactor may run its body more than once. Mutating a captured output
variable directly can therefore append or count the same result several times.
Staged Proxies give each attempt a private value and publish only the attempt
whose commit is confirmed.

Choose the parameter form according to its role:

| Parameter | Use it for | Replay behavior |
| --- | --- | --- |
| `input` | An input that the transaction reads but does not modify. | The transactor copies it once and presents the same value to every attempt. |
| `lfdb::staged(output)` | An output that modifies or extends its existing value. | Each attempt lazily copies the existing value before its first mutation. |
| `lfdb::staged(output, std::in_place)` | An output rebuilt entirely from transaction results. | Each attempt starts with a fresh value; the previous contents are never copied. |

Treat ordinary bound parameters as read-only. Mutating one changes only the
transactor's private argument and that mutation carries into later attempts;
it does not update the caller's object.

In-place construction happens before each attempt, so a successful query that
finds no values still replaces stale output with an empty value.

Prefer an ordinary return value when the callback produces one new value.
Staging is useful for output parameters or several existing local values.

```cpp
// Recompute a D4N bucket's cached-byte count without counting a replay twice.
std::uint64_t cached_bytes = 0;
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn, auto& bytes, std::string_view bucket_id) {
  bytes = 0;

  for (const auto& block :
       lfdb::scan<CacheBlock>(txn, object_range(bucket_id)) |
         std::views::values) {
    bytes += block.size;
  }
}, lfdb::staged(cached_bytes), bucket_id);
```

If you declare a proxy ahead of time, move it into the transactor because it
represents exactly one invocation's staging state:

```cpp
// Rebuild a D4N block listing without copying its previous contents.
std::vector<CacheBlock> blocks;
auto blocks_prx = lfdb::staged(blocks, std::in_place);
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn, auto& blocks, std::string_view bucket_id) {
  for (auto block : read_blocks(txn, bucket_id)) {
    blocks.push_back(std::move(block));
  }
}, std::move(blocks_prx), bucket_id);
```

The proxy covers assignment, `+=`, common sequence updates, and
`insert_or_assign()`; use `get_target()` for an uncommon operation. Mutating
operations deliberately return no iterator or reference into attempt-local
storage.

Gotchas:

- An ordinarily staged target must be copy-constructible; an in-place target
  must be default-initializable. Both must be nothrow move-assignable, alive for
  the call, and not accessed concurrently. Binding the same target more than
  once is rejected with an exception; overlapping targets are also invalid.

- Accept the proxy by lvalue reference (`auto& output`, as above). Do not
  capture and mutate the original target or retain the proxy after the callback.
  Any pointer, reference, or iterator obtained through `get_target()` is
  attempt-local and must not escape the callback.

- Staging protects only the bound C++ value. Logging, I/O, other captured state,
  and non-idempotent database writes after `commit_unknown_result` still need
  their own replay-safe design.

Any outcome without a confirmed commit leaves the staged target unchanged.
Transactors return the body's value after a confirmed commit and throw when
recovery fails or retry limits are exhausted.

Automatic replay uses FoundationDB's transaction retry limit and timeout, with
no additional libfdb attempt limit. Configure these options when an operation
must finish within a budget; unlimited native settings can allow indefinite
replay. A retry limit of zero permits the initial attempt without replay.

Pass `retry_if(callable)` when the application needs to choose replay. The
callable receives `transaction_failure {code, phase}`; `phase` is
`transaction_phase::body` or `transaction_phase::commit`. It sees the original
native error before preparation, including terminal errors for observation.
For example, stop rather than replay an uncertain commit:

```cpp
auto update = lfdb::make_transactor(dbh);
update(lfdb::retry_if([](const lfdb::transaction_failure& failure) {
  return lfdb::retryable_not_committed(failure.code);
}), [](auto txn) {
  lfdb::set(txn, "person/frances-allen/name", "Frances Allen");
});
```

Returning false propagates the original error without preparation. Returning
true requests native preparation and replay; it cannot override native
eligibility, budgets, cancellation, or a confirmed commit. Ordinary calls keep
automatic replay of native-eligible failures, including 1021. Stamp failure
after a confirmed commit never invokes this policy.

The selector owns one callable for the logical invocation and reuses it across
attempts. Mutable and move-only callables work; use `std::ref` to borrow an
application-owned callable explicitly. Keep counters, metrics, and any history
you need there. Local and ordinary user errors bypass native policy. Exceptions
from the policy propagate unchanged and never invoke it recursively. A failed
preparation reports its actual error without calling the policy again.

### Resolving An Uncertain Commit

For an operation that must be applied once, store a durable operation identifier
in the same transaction as its effects. This example uses the default replay
policy, including 1021, and increments the count only when its marker is absent:

```cpp
const std::string operation_id = "job-42";
const std::string marker = "example/once/applied/" + operation_id;
const lfdb::transaction_options options {
  {FDB_TR_OPTION_TIMEOUT, std::int64_t {5000}},
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}}
};
auto update = lfdb::make_transactor(dbh, options);

update([&marker](auto& txn) {
  if (lfdb::key_exists(txn, marker)) {
    return;
  }

  std::uint64_t count = 0;
  lfdb::get(txn, "example/once/count", count);
  lfdb::set(txn, "example/once/count", count + 1);
  lfdb::set(txn, marker, true);
});
```

Generate a distinct identifier for each logical operation and retain it across
attempts, later reconciliation, and process restarts. Never reuse one identifier
for different work. Serializable marker reads prevent concurrent attempts with
the same identifier from both applying the increment. Keep the marker as long
as the operation may be retried, and assume the count has room to increment.

If this call throws after an uncertain attempt, retrying the same logical
operation with the same identifier remains safe. FoundationDB guarantees that
an attempt reporting 1021 is no longer in flight. Other errors, such as timeout
after submission, can leave a commit in flight; an absent marker in a separate
read then does not prove that the commit cannot still finish. See the pinned
[FoundationDB failure guarantees](https://github.com/apple/foundationdb/blob/7.3.79/documentation/sphinx/source/developer-guide.rst#L824-L843).

This design covers database effects; external messages or payments need their
own deduplication or a transactional outbox. The library does not retain attempt
counts, cumulative uncertainty, or reconciliation history for you.

### Transaction Options And Failure Boundaries

Options are applied to each transaction the transactor creates:

```cpp
lfdb::transaction_options opts{
  { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
};

auto txr = lfdb::make_transactor(dbh, opts);

txr([](auto& txn) {
  lfdb::set(txn, "person/zenobia/name", "Zenobia");
});
```

```cpp
auto txr = lfdb::make_transactor(dbh);

try {
  txr([](auto& txn) {
    validate_profile_update();

    lfdb::set(txn, "person/jose-capablanca/title",
              std::vector{ "Original Grandmaster"s, "World Chess Champion"s });
  });
}
catch (const lfdb::libfdb_exception& e) {
  handle_libfdb_error(e);
}
catch (const std::exception& e) {
  handle_application_error(e);
}
```

## Reads, Selectors, And Queries

FoundationDB keys are byte strings ordered lexicographically. Choose key formats
so lexical order matches the scan order you want; numeric suffixes should
usually be fixed-width and zero-padded.

```cpp
lfdb::set(dbh, "person/000001/name", "Barbara Moo");
lfdb::set(dbh, "person/000010/name", "Konrad Zuse");
```

Most range queries should start from that ordering fact: build keys so related
records share a prefix, then compose prefixes and bounds into the query you
mean. `lfdb::select` is the simple interval form; `lfdb::query` adds a small
interval algebra for prefixes, intersections, unions, differences, complements,
singleton keys, and explicit open/closed boundaries.

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
| Select an explicit half-open key interval | `q::between(lower, upper)` | `lfdb::select` | Matches the usual FoundationDB `[lower, upper)` range shape. |
| Combine or subtract selections | `q::intersection()`, `q::set_union()`, `q::difference()` | query expression or selector | Lets the query compiler normalize intervals before execution. |
| Add an explicit conflict check | `lfdb::mark_conflict_read(txn, query)` | `void` | Records transaction correctness dependencies that ordinary operations did not express. |
| Run several dependent operations atomically | `lfdb::make_transactor(dbh)` | callable transaction runner | Replays retryable transactions while keeping user code explicit. |
| Publish local output after a replayable transaction | `lfdb::staged(output)` | callback proxy | Prevents retries from applying the same local mutation more than once. |

### Key Selectors

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

### Prefixes And Explicit Ranges

`q::prefix()` selects every key with a shared key prefix. This is the most
common FoundationDB access pattern, because related records are usually grouped
under prefixes such as `person/`, `bucket/index/`, or `object/metadata/`.

```cpp
auto people = q::prefix("person/");
```

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

Attach range options with `q::with_options()`:

```cpp
auto people = q::with_options(q::prefix("person/"),
                              q::query_options{ .reverse_order = true });

for (const auto& [key, value] : lfdb::scan(dbh, people)) {
  process_reverse_result(key, value);
}
```

### Query Algebra

`compile_intervals(expr)` and `compile_intervals<N>(expr)` return the same
executable interval values, including their query options. The latter stores
at most `N` intervals without allocating container storage and throws
`libfdb_exception` with `std::errc::value_too_large` if that capacity is exceeded.
Interval keys still own strings.
Geometric predicates such as `is_universal()` ignore execution options.

Union and intersection preserve matching options. When only one operand has
non-default options, those options apply to the result. Conflicting non-default
options throw `libfdb_exception` with `std::errc::invalid_argument`. Configure the
operands consistently, or combine unconfigured geometry and apply
`with_options()` to the result.
Difference retains its left operand's options; complement retains its operand's
options.

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

Start with simple selectors, then compose when the problem demands it:

```cpp
for (const auto& [key, person] :
     lfdb::scan<person_record>(dbh, q::prefix("person/"))) {
  show(person);
}
```

```cpp
const auto visible_records =
  q::difference(q::prefix("collection/records/"),
                q::prefix("collection/records/.internal/"));

for (const auto& [key, value] : lfdb::scan(dbh, visible_records)) {
  process(key, value);
}
```

Intersect composite queries with page windows or authorization windows without
hand-editing byte ranges:

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
q::for_each_interval(public_keys, [](const lfdb::select& interval) {
  inspect(interval);
});
```

Most callers only need to ask about the selection, not materialize its
canonical intervals. The relationship predicates work directly with composed
expressions:

```cpp
const auto objects = q::prefix("bucket/objects/");
const auto hidden = q::prefix("bucket/objects/.hidden/");
const auto visible = q::difference(objects, hidden);
const auto page = q::between("bucket/objects/a", "bucket/objects/z");

if (q::contains(visible, object_key)) {
  load_object(object_key);
}

if (q::intersects(visible, page)) {
  read_page(q::intersection(visible, page));
}

assert(q::encloses(objects, visible));
assert(q::is_disjoint(hidden, visible));
```

`q::is_empty_expression()` answers whether an expression selects no ordinary
FoundationDB keys. `q::interval_count()` instead counts the canonical,
nonoverlapping intervals an expression emits; use it only when the count itself
is needed. These predicates are `constexpr` and can participate in constant
evaluation when the expression's value types permit it. Complements and
unbounded expressions are still interpreted relative to FoundationDB's
ordinary keyspace.

### Prefix Cursors

This is the common "list records under a prefix, optionally starting after a
cursor" shape. The prefix query defines the valid record namespace; the cursor
query is intersected with it, so cursors before the prefix clamp to the prefix
and cursors past the prefix compile to an empty selection.

```cpp
auto record_subspace = [](std::string_view collection_id) {
  return std::string(collection_id) + "/records/";
};

const auto base = record_subspace(collection_id);
const auto prefix_key = base + std::string(prefix);

auto read_records = [&](auto&& selection) {
  return lfdb::get(dbh,
                   std::forward<decltype(selection)>(selection),
                   std::back_inserter(records));
};

const auto nread = [&] {
  if (cursor.empty()) {
    return read_records(q::prefix(prefix_key));
  }

  const auto cursor_key = base + std::string(cursor);
  const auto lower = cursor_inclusive ? q::closed(cursor_key)
                                      : q::open(cursor_key);

  return read_records(
    q::intersection(q::prefix(prefix_key),
                    q::between(lower, q::open(q::successor(prefix_key)))));
}();

if (0 == nread) {
  handle_empty_page();
}
```

### Reverse And Ranked Subranges

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

If encoded ranks sort lexicographically, a ranked query can select only the
needed rank range instead of scanning the whole revision prefix and filtering
client-side.

```cpp
const auto rank_base = revision_subspace(collection_id, record_id);
const auto begin_prefix = rank_base + std::string(min_rank) + "/";
const auto end_prefix = rank_base + std::string(max_rank) + "/";

const auto rank_range = q::between(begin_prefix, q::successor(end_prefix));
```

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

The same mode works for range-shaped reads:

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

lfdb::commit(txn);
```

The same functions accept exact keys, begin/end ranges, selectors, and query
expressions. Empty conflict expressions are rejected:

```cpp
auto txn = lfdb::make_transaction(dbh);

lfdb::mark_conflict_read(txn, "object/index/a", "object/index/z");
lfdb::mark_conflict_write(txn, q::prefix("object/cache/tenant-a/"));
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

The helper accepts the same selection forms as `scan()` and `blocks()`:

```cpp
auto visible =
  q::difference(q::prefix("object/"),
                q::prefix("object/private/"));

auto bytes = lfdb::approximate_range_size(dbh, visible);
```

## Scanning And Traversal

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

Each database-handle scan window uses an independently retried transaction, so
later windows may observe a newer read version. `collect(dbh, ...)` retains that
streaming contract while assembling its return value. Use an explicit transaction
when the whole traversal must share one read version.

Range `get(dbh, selection, output)` instead reads the entire selection in one
managed transaction, retries before publishing, and then appends or merges the
materialized rows into the output. This is useful for bounded reads requiring one
read version. Existing map values remain authoritative on key collisions. A
publication failure can still leave partial output; materializing first protects
against publishing rows from a failed database attempt.

Use `for_each()` when the operation is naturally callback-shaped and you do not
need a composable generator. The callback is a row consumer and must return
`void`; traversal returns the number of rows delivered. Use `transform()` when
each row should produce a value. Synchronous callbacks are borrowed for the call,
including noncopyable lvalues; their mutable state remains visible to the caller.

```cpp
auto txn = lfdb::make_transaction(dbh);

lfdb::for_each(txn, q::prefix("person/"), [](auto&& row) {
  const auto& [key, value] = row;
  fmt::println("{}: {}", key, value);
});
```

The database-handle overload consumes the same independently retried windows as
`scan(dbh, ...)`. A delivered callback is never replayed by the helper. Callback
exceptions propagate unchanged, including native-category `libfdb_exception`
objects. If the caller puts the traversal inside a transactor, an outer replay
can still invoke the whole traversal again:

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

lfdb::transform<person_record>(txn,
                               q::prefix("person/"),
                               std::back_inserter(names),
                               [](auto&& row) {
                                 return std::move(row.second.name);
                               });
```

When the transformed values should be materialized directly, let `transform()`
return the vector. Database-handle transforms use the same window and callback
contract as `for_each()`. Explicit-output forms stream into the output and return
its advanced iterator; a later failure leaves earlier output in place. The
allocating form returns its vector only after traversal succeeds:

```cpp
auto names = lfdb::transform<person_record>(
  dbh,
  q::prefix("person/"),
  [](auto&& row) {
    return std::move(row.second.name);
  });
```

Reference results are consumed while the source row is alive; the vector form
materializes values before advancing. Pointer and view results retain their
normal referent-lifetime requirements. The callable is always last. Pass an
explicit read mode immediately before it:

```cpp
auto names = lfdb::transform<person_record>(
  dbh, q::prefix("person/"), lfdb::read_mode::snapshot,
  [](auto&& row) { return std::move(row.second.name); });
```

Use `erase_if()` when the delete decision depends on the decoded row. It returns
the number of keys cleared; database-handle predicates are evaluated inside the
managed write transaction and may run again after a native database failure.
Predicate and decoder exceptions propagate unchanged without requesting replay:

```cpp
const auto removed = lfdb::erase_if<person_record>(
  dbh, q::prefix("person/"),
  [](const auto& row) {
    return row.second.disabled;
  });
```

Selector-shaped scans can ask for a bounded page. The returned page contains
only rows the caller requested; the extra sentinel row used to detect
continuation is not decoded or returned.

```cpp
auto people = lfdb::select{ "person/" };
const auto page = lfdb::scan<person_record>(dbh, people, lfdb::page{100});

if (page.has_more) {
  resume_after(page.rows.back().first);
}
```

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
the caller. Move the generator into the view because it owns a single traversal.

```cpp
auto txn = lfdb::make_transaction(dbh);
auto keys = lfdb::scan(txn, q::prefix("key_"));

for (const auto& chunk : std::move(keys) | std::views::chunk(100)) {
  for (const auto& [key, value] : chunk) {
    process(key, value);
  }
}
```

## Content Layer

Raw FoundationDB keys are byte strings ordered lexicographically. That is
powerful, but it also means an ad hoc key format can accidentally make common
scans awkward or wrong. The Content layer is a small key compiler: compose
domain segments, get one compiled byte key, and pass that key to the ordinary
libfdb operations.

The canonical way to build a keyspace is similar to writing a path:

```cpp
const std::string bucket_id = "bucket.8409.12";
const std::string version = "v0000000000000017";
const std::string object_name = "photos/2026/beach.jpg";

const auto object_head =
  fdbc::keyspace("object-cache") / "object" / bucket_id / version / object_name;

lfdb::set(dbh, object_head, object_metadata);
```

The result is a compiled key that follows FoundationDB rules. Content keys are
meant to flow into libfdb operations directly; callers should not need to
extract bytes from them.

Exact lookup uses the compiled key:

```cpp
std::string metadata;

if (lfdb::get(dbh, object_head, metadata)) {
  record_cache_hit(metadata);
}
```

Build deeper keys by adding more segments:

```cpp
const auto block_key =
  object_head / "block" / fmt::format("{:020}", offset) / fmt::format("{:020}", length);

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
  consume_block(key, block);
}
```

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

The content key builder is effectively a small key compiler. Both `keyspace()`
and `key()` target the same output type, `fdbc::compiled_key`, so their results
can be used interchangeably by libfdb operations.

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
so each segment remains unambiguous in the compiled key. Separating key assembly
from key use is also performance-oriented: once a key or key prefix is compiled,
it can be reused without rebuilding those bytes each time.

For prefix scans, the compiled prefix can come from either spelling:

```cpp
fdbc::prefix(fdbc::keyspace("tenant") / bucket_id);
fdbc::prefix(fdbc::key("tenant", bucket_id));
```

Use `key()` to build the complete object key:

```cpp
const auto object_head = fdbc::key("object-cache", "object", bucket_id, version, object_name);
```

Invalid segment types are rejected at compile time, and invalid keyspace roots
throw during assembly:

```cpp
static_assert(fdbc::key_segments<std::string_view, std::string_view>);
static_assert(!fdbc::key_segments<int>);

const auto object_records = fdbc::key("object-cache", "object");
```

The root segment must be non-empty. A root beginning with `0xFF` is valid:
segment encoding starts with a tuple type byte, so the compiled key remains in
ordinary FoundationDB keyspace. Later segments may also be empty or begin with
`0xFF`.

The lowering code is written to be constexpr-friendly, but the current compiled
target owns a `std::string`. A future fixed-size/literal compiled target would
let literal-only assembly produce a fully constexpr key.

Use `fdbc::prefix()` for a full prefix range. It returns a normal `lfdb::select`,
so it works with the ordinary range-read APIs:

```cpp
const auto object_records =
  fdbc::keyspace("object-cache") / "object" / bucket_id / version / object_name;

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

## Versioned Data

Version stamps let FoundationDB write commit-versioned bytes into keys or
values. A stamp is a shared result handle: copies observe the same completion.
`is_ready()` means it holds bytes or an error; `has_value()` means it holds bytes.
Inspect the result after operation completion, or use external synchronization;
the shared handle does not synchronize concurrent access to its result.
Reading a pending stamp throws `libfdb_exception` with
`std::errc::invalid_argument`. A failed stamp throws
`libfdb_exception` with its recorded code from `resolved_bytes()`, or returns
that code from `resolved_bytes(with_result)`.

Commit and stamp outcomes are independent. A read-only transaction can commit
successfully while its stamp reports `no_commit_version`. A stamp retrieval
failure after confirmed commit leaves commit successful. Do not replay a
confirmed transaction to recover its stamp.

The ordinary accessor borrows a const array reference; the tagged accessor
returns `std::expected<std::span<const std::uint8_t, 10>, std::error_code>`.
Keep a copy of the stamp handle alive while using borrowed bytes. Access from
temporary handles is deleted. Valid bytes are immutable; parsing the identical
bytes again is permitted without replacing their storage. Parsing malformed
bytes, overwriting valid bytes, and parsing into an active or failed output are
contract errors reported as `libfdb_exception` with `std::errc::invalid_argument`.

A failed output retains its error until explicitly registered as an output
again. Registration clears the shared failure for the new attempt; an error
copied from an earlier expected result remains independent. An output omitted
from replay retains its previous failure. Abandoning a registered pending output
records `std::errc::operation_canceled`. One pending handle may serve several
operations in one transaction; registering it with another transaction is
rejected before changing native data. A handle holding bytes cannot be reused
as a commit output.

Managed failure and stopped completion settle abandoned registered outputs before
reporting completion, even if an application retains the transaction handle.
A known libfdb failure records its code; other abandonment records standard
`operation_canceled`. A manual operation that fails before submitting commit
leaves existing outputs registered for the caller's transaction.

```cpp
lfdb::versionstamp stamp;

lfdb::set(dbh,
          lfdb::versioned("person/konrad-zuse/event/", "/created", stamp),
          "created");
```

```cpp
lfdb::versionstamp stamp;

lfdb::set(dbh,
          "person/barbara-liskov/version",
          lfdb::versioned("", stamp));
```

```cpp
auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp stamp;

lfdb::set(txn, "person/alonzo-church/name", "Alonzo Church");

lfdb::commit(txn, stamp);
const auto& version_stamp_bytes = stamp.resolved_bytes();
```

Handle a stamp failure separately from the commit. A read-only transaction
illustrates the distinction without requiring a fault:

```cpp
auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp stamp;
std::string name;
lfdb::get(txn, "person/alonzo-church/name", name);

lfdb::commit(txn, stamp);
// Commit succeeded. Any error below concerns only the requested stamp.
const auto bytes = stamp.resolved_bytes(lfdb::with_result);
if (not bytes) {
  std::cerr << "Committed, but no stamp: " << bytes.error().message() << '\n';
}
```

Here the stamp reports `fdb_error_code::no_commit_version`. On a write transaction,
use the same check before consuming bytes. Reporting or recovering unavailable
stamp metadata must not replay the already committed transaction. The ordinary
accessor can throw at this point; keep that error boundary separate from any
manual commit retry loop.

Use one versionstamp for several operations in the same transaction when those
records should share the same commit order:

```cpp
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

lfdb::commit(txn);
const auto& version_stamp_bytes = stamp.resolved_bytes();
```

Note that version stamping a key or a value is strictly an either/or
proposition: there is no call to set a stamped key and value all at once.

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
auto txn = lfdb::make_transaction(dbh);

lfdb::set(txn, "person/grace-hopper/name", "Grace Hopper");
lfdb::atomic::add(txn, "stats/person-count", std::uint64_t{1});
lfdb::atomic::bit_or(txn, "stats/person-flags", std::uint64_t{0x01});

lfdb::commit(txn);
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

## Watches / Triggers

Transaction watches ask FoundationDB to report a change to a key relative to
the transaction that created the watch. If watches are unsupported by the local
FoundationDB configuration, or the transaction cannot create watches, waiting on
the watch throws `lfdb::libfdb_exception`.

Watch keys use the same key concept as `get()` and `set()`: string literals,
`std::string_view`, strings, and compiled content keys all work directly. For
example, `make_watch(dbh, fdbc::key("people", "person", "title"))` watches a
compiled key. `watched_loop()` owns a copy of the resolved key bytes for its
repeated watches and passes those bytes to its callback as a string view.

Note that watches do not report the triggering value. If you need the value,
read it in a separate transaction. For the underlying semantics, see the
FoundationDB Developer Guide and the C API entry for `fdb_transaction_watch()`.

See also:
- https://apple.github.io/foundationdb/developer-guide.html#watches
- https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_watch

```cpp
auto watch = lfdb::make_watch(dbh, "person/jose-capablanca/title");

lfdb::set(dbh, "person/jose-capablanca/title", "World Chess Champion");

if (lfdb::watch_event::changed == watch.wait_for_event()) {
  handle_title_change();
}
```

Create a watch inside a transaction when it must share that transaction's read
version. Commit the transaction before waiting on the watch:

```cpp
auto txn = lfdb::make_transaction(dbh);
auto watch = lfdb::make_watch(txn, "person/jose-capablanca/title");

lfdb::commit(txn);
watch.wait();
```

```cpp
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
the watched key as a `std::string_view` and returns `void`. The helper blocks;
applications should own any thread, executor, shutdown, or callback error
policy around it:

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

## API And System Utilities

`lfdb::api` reports facts about the FoundationDB client API available to this
process. These calls do not require a database handle. See the FoundationDB C API
documentation:
<https://apple.github.io/foundationdb/api-c.html>.

```cpp
const auto client = lfdb::api::client_version();
const auto max_api = lfdb::api::max_version();
```

`lfdb::system` contains FoundationDB system-level observations and operational
surface. Read-only functions report live database/client state. Sharp
operational functions belong here too, rather than in the core key/value API,
but they should only be called by code that is explicitly managing a
FoundationDB deployment. See the FoundationDB C API entries for these database
functions:
<https://apple.github.io/foundationdb/api-c.html#c.fdb_database_get_client_status>
and
<https://apple.github.io/foundationdb/api-c.html#c.fdb_database_get_main_thread_busyness>.
`client_network_load()` reports load on FoundationDB's client network thread:
zero is idle, while one or greater indicates saturation.

```cpp
const auto client_load = lfdb::system::client_network_load(dbh);
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

## Putting It All Together

These examples assume the current libfdb feature stack is available: Content
keys, query algebra, replayable transactors, versionstamps, snapshots, conflict
ranges, and atomic mutations. They are intentionally compact; the point is to
let library features remove hand-written database plumbing.

### Content-Key Object Layout

```cpp
struct cache_metadata
{
  std::string etag;
  std::string storage_class;
  std::uint64_t size = 0;
};

struct object_block
{
  std::uint64_t offset = 0;
  std::string bytes;
};

auto object_root = [](std::string_view bucket_id, std::string_view object_name) {
  return fdbc::keyspace("d4n") / "object" / bucket_id / object_name;
};

const auto object = object_root(bucket_id, object_name);

lfdb::set(dbh, object / "head", metadata);
lfdb::set(dbh, object / "block" / fmt::format("{:020}", block.offset), block);
```

Content keys keep related records grouped without raw delimiter escaping. The
same compiled prefix can be reused for lookup, range scan, pagination, or block
planning.

### Versioned Object Update

```cpp
auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp event_version;
const auto object = object_root(bucket_id, object_name);

lfdb::set(txn, object / "head", metadata);
lfdb::set(txn, object / "block" / fmt::format("{:020}", block.offset), block);
lfdb::set(txn,
          lfdb::versioned("d4n/event/" + std::string(bucket_id) + "/",
                          "/" + std::string(object_name) + "/put",
                          event_version),
          "put");
lfdb::atomic::add(txn, "d4n/stats/object-writes", std::uint64_t{1});

lfdb::commit(txn);
publish_ordered_event(event_version.resolved_bytes());
```

The explicit transaction keeps the state update, event append, and counter
mutation atomic. The versionstamped event key is commit ordered without an
application sequence counter. If commit throws, propagate its outcome to the
application's recovery boundary. Stamp access and publication happen after
confirmed commit; their failure does not permit replaying these mutations.
In particular, blindly replaying 1021 can append another event and increment
the counter again. Use a durable operation marker when automatic replay is needed.

### Prefix Page With Marker

```cpp
auto listing_key = [](std::string_view bucket_id, std::string_view object_name) {
  return fdbc::keyspace("d4n") / "listing" / bucket_id / object_name;
};

const auto listing = fdbc::keyspace("d4n") / "listing" / bucket_id;

auto read_page = [&](auto&& selection) {
  auto rows = lfdb::scan<object_summary>(
    dbh, std::forward<decltype(selection)>(selection));

  return lfdb::collect(std::move(rows), lfdb::page{page_size});
};

const auto page = marker.empty()
                ? read_page(fdbc::prefix(listing))
                : read_page(q::starting_after(fdbc::prefix(listing),
                                              listing_key(bucket_id, marker)));

for (const auto& [key, summary] : page.rows) {
  emit_object(summary);
}

if (page.has_more && not page.rows.empty()) {
  remember_marker(page.rows.back().first);
}
```

The selector expresses the listing subspace, the marker narrows it, and the page
helper keeps the continuation check out of the application loop.

### Snapshot Audit Read

```cpp
const auto objects = fdbc::keyspace("d4n") / "object" / bucket_id;

auto txn = lfdb::make_transaction(dbh);

for (const auto& [key, metadata] :
     lfdb::scan<cache_metadata>(txn, fdbc::prefix(objects), lfdb::read_mode::snapshot)) {
  if (metadata.etag.empty()) {
    report_stale_object(key, metadata);
  }
}
```

Snapshot mode is useful for advisory scans where another transaction changing a
read key should not by itself make this transaction fail. This example reports
records with missing ETags; it does not make a concurrent update safe to overwrite.

### Conflict-Protected Promotion

```cpp
auto txn = lfdb::make_transaction(dbh);
const auto object = object_root(bucket_id, object_name);
const auto head = object / "head";
const auto versions = object / "versions";

const auto previous = lfdb::collect<object_version>(txn,
                                                   fdbc::prefix(versions),
                                                   lfdb::read_mode::snapshot);

lfdb::mark_conflict_read(txn, head);
lfdb::set(txn, head, choose_new_head(previous));

lfdb::commit(txn);
```

The snapshot scan avoids broad read conflicts, while the explicit conflict range
keeps the narrow correctness dependency visible. This assumes every writer of
the version records also changes `head` in the same transaction; otherwise,
read those records serializably or mark their range as a conflict dependency.

### Maintenance Pass

```cpp
const auto cache = fdbc::keyspace("d4n") / "cache" / bucket_id;
const auto reserved = cache / ".internal";
const auto tombstones = cache / ".tombstone";

const auto visible_cache =
  q::difference(fdbc::prefix(cache),
                q::set_union(fdbc::prefix(reserved),
                             fdbc::prefix(tombstones)));

const auto old_visible_cache = q::ending_before(
  visible_cache,
  cache / fmt::format("{:020}", cutoff_epoch));

auto txr = lfdb::make_transactor(dbh);

for (const auto& block : lfdb::blocks<cache_metadata>(dbh, old_visible_cache)) {
  txr([&](auto& txn) {
    for (const auto& [key, metadata] : block) {
      lfdb::set(txn, "d4n/archive/" + key, metadata);
      lfdb::erase(txn, key);
    }
  });
}
```

The query expression says exactly what should be touched: cache records, minus
reserved subspaces, further trimmed by a cursor. `blocks()` keeps discovery
bounded, while the transactor keeps each mutation block replayable. The body
only does transaction-local work, so replay does not repeat external side
effects. This compact recipe assumes the discovered records remain unchanged
until archived. For mutable records, read and validate each current value inside
the transactor before archiving and deleting it.

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

## Appendix: Exception Summary

Library-generated failures, including rejected arguments and invalid operation
states, use `lfdb::libfdb_exception`. Its error code distinguishes native errors,
library-specific failures, and standard conditions such as
`std::errc::invalid_argument` or `std::errc::value_too_large`; callers need one
exception handler while retaining the failure's category, code, and context.

Code that caught `std::invalid_argument` or `std::length_error` for a libfdb
validation failure should now catch `lfdb::libfdb_exception` and inspect `code()`
when it needs that distinction.

Exceptions thrown by application serializers, converters, callbacks, or native
continuations retain their original type and identity. Standard allocation
failures also propagate normally. The generic interval core remains independent
of FoundationDB; the libfdb query boundary translates its fixed-capacity failure
into `libfdb_exception`.

| Condition | Exception |
| --- | --- |
| FoundationDB C API reports an operational failure | `lfdb::libfdb_exception`; tagged commit returns commit-future errors as values |
| FoundationDB option setup fails | `lfdb::libfdb_exception` |
| Retry recovery fails or retry limit is exceeded | `lfdb::libfdb_exception` |
| zpp_bits returns an encoding or decoding failure | `lfdb::libfdb_exception` with the original standard error code and category |
| A user serializer, converter, or callback throws | The original exception, with its type and contents preserved |
| Invalid result at the FoundationDB boundary | `lfdb::libfdb_exception` |
| Caller uses libfdb after shutdown | `lfdb::libfdb_exception` |
| Caller passes an impossible selector prefix or invalid versionstamp bytes | `lfdb::libfdb_exception` with `std::errc::invalid_argument` |
| Caller accesses a pending stamp, reuses a stamp holding bytes, registers an output with two transactions, overwrites immutable bytes, or asks for a committed version before commit | `lfdb::libfdb_exception` with `std::errc::invalid_argument` |
| Stamp completed with an error | `resolved_bytes()` throws `lfdb::libfdb_exception`; `resolved_bytes(with_result)` returns the recorded error |

### Error Codes and Exceptions

`lfdb::libfdb_exception` derives from `std::system_error`, exposing one
authoritative error through `code()` and diagnostic text through `what()`.
The message-only constructor is retained for general local library failures;
it assigns the nonzero `lfdb::libfdb_error_code::operation_failed` code in
`lfdb::libfdb_error_category()`. When a native or more specific local code is
available, supply that code explicitly, with any useful operation context.

Native errors use `lfdb::fdb_error_category()`, whose diagnostic name is
"FoundationDB". Library-specific codes use `lfdb::libfdb_error_category()`, named
"libfdb". Standard conditions retain their standard category. The constructor
`libfdb_exception(std::errc, context)` accepts a standard condition directly.
The native `enum struct fdb_error_code : int` retains the pinned FoundationDB
names and numbers; unknown native integers remain representable. Both enums
convert to `std::error_code`. At a raw C API boundary, construct
`std::error_code(raw, lfdb::fdb_error_category())` explicitly. The old bare-int
exception constructor and mutable raw-code field have been removed.

`lfdb::retryable(code)`, `lfdb::maybe_committed(code)`, and
`lfdb::retryable_not_committed(code)` query the loaded C client's predicates
only for native-category codes. Equal integers in a local or standard category
do not become native failures. Predicate results concern the native error;
false does not generally establish that an earlier submitted attempt rolled back.

Serialization and conversion distinguish returned failure statuses from thrown
exceptions:

| Failure source | Reporting |
| --- | --- |
| A zpp_bits archive returns a failure status | `lfdb::libfdb_exception` carrying the original standard error code and category, with operation context |
| A user serializer, converter, or callback throws an exception | The original exception propagates unchanged, preserving its type and contents |
| FoundationDB returns a native failure | The throwing interface reports `lfdb::libfdb_exception` carrying the exact native code in `lfdb::fdb_error_category()` |

The returned-status rule applies consistently to encoding, decoding, direct
conversions, managed writes, typed point reads, and typed scans. A catch for the
standard `std::system_error` base type also handles these library exceptions.

Database-handle read and write helpers propagate a thrown serializer, converter,
or callback exception at the operation boundary. A native code on that user
exception does not make it an internal retry request. A transactor applies its
policy to native `libfdb_exception` errors escaping its body; use an application
exception type to reject application work.

The conversion boundary inspects the archive's returned status directly.
It does not catch and reconstruct exceptions from `what()`. Exceptions thrown
by user code retain their identity even when they derive from
`std::system_error`. Serializer status codes retain their standard category;
FoundationDB retry predicates apply only to codes in the native category.
