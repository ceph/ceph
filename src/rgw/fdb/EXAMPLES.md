<!--
Ceph - scalable distributed file system

Copyright (C) 2026 International Business Machines Corp. (IBM)

This is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License version 2.1, as published by the Free Software
Foundation. See file COPYING.
-->

# libfdb Examples

> "Take a thousand days of practice for forging, and ten thousand days of practice for refining."
>
> — Miyamoto Musashi, *Go Rin No Sho* (~1645)

Welcome, traveller. This is a cookbook-style tour of libfdb. It starts with
ordinary key/value operations and then moves toward transactions, selectors,
query algebra, content keys, watches, and system utilities.

For a complete Ceph-specific use of these pieces, see
[bounded metadata archiving](../CEPH_FDB_EXAMPLES.md): Ceph buffer-list payloads,
composed key selection, borrowed typed helpers, and staged publication after
one confirmed batch commit.

Choose the interface according to transaction ownership:

| Need | Usual interface |
|---|---|
| A simple operation | Free functions taking a database handle |
| A logical transaction with managed recovery | A transactor |
| A composed operation within an existing transaction | `txr(txn, body, args...)` |
| Explicit transaction boundaries and recovery | Free functions taking a transaction handle |

Errata: Please report errata, or contact with examples you would like to see.

See `examples/libfdb/` for small standalone programs.
Build the standalone example against an existing FoundationDB-enabled Ceph build:

```sh
make -C examples/libfdb -j8 CEPH_BUILD="$PWD/build"
./examples/libfdb/mini_example
```

If Ceph uses an external zpp_bits checkout, pass the same source directory that
its CMake configuration uses for `zpp_bits_SOURCE_DIR`. Force a rebuild when
changing dependency paths:

```sh
make -B -C examples/libfdb -j8 CEPH_BUILD="$PWD/build" \
  ZPP_BITS_SOURCE_DIR=/path/to/zpp_bits
```

The program demonstrates ordinary manual completion, owned and borrowed Transactor
calls, `and_then`/`transform` composition, staged publication after confirmed commit,
central failure reporting, explicit uncertain replay
of an idempotent replacement, typed application rejection, a verified round trip,
and range and interval-set assignment:

```text
manual commit: confirmed
managed transaction: stored 2 people
borrowed transaction: prepared 2 people before commit
caller commit: confirmed
composed transaction: stored 2 people
staged publication: loaded 2 people after commit
explicit replay_unknown: replaced 2 people
application rejection: empty name, no commit submitted
read 2 people, some of them are likely nifty
intersection assignment: selected 1 key
set operators: selected 1 key
```

It releases its handles and stops the client network before exiting, including
when an operation fails.

**Breaking API changes:**

- Ordinary `commit(txn)` and `commit(txn, stamp)` return
  `transaction_result<void>`. An owning libfdb Transactor call returns
  `transaction_result<T, E>`; no `with_result` selector or throwing completion
  overload remains. Both retain structured failure and commit knowledge.
- An unexpected application `std::expected<T, E>` body result rejects the attempt without
  commit or staged publication. Ordinary values, including false and negative
  integers, authorize commit.
- `txr(txn, body, args...)` borrows the supplied transaction and returns
  `std::expected<T, transaction_failure<E>>` with working data. Returning an expected
  with that library failure type preserves its classification for the outer owner;
  native failures remain eligible for recovery instead of becoming application data.
- Managed execution now defaults to `retry_policy::no_replay_unknown`. Choose
  `retry_policy::replay_unknown` explicitly when repeated database effects are
  tolerated or deduplicated. These replace the old policy enumerators
  `retryable_not_committed` and `retryable`, respectively; native error predicates
  retain their existing names.
- Owning result values and application errors must have nonthrowing moves and
  destruction. References and staged proxies cannot be transaction results.
  See [Transactions](#transactions) for migration and recovery examples.
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

Choose the boundary that owns the transaction:

| Operation | Success | Failure |
| --- | --- | --- |
| Database-handle read/write helpers | Existing value or void contract; writes commit with native replay | Existing exception contract |
| `commit(txn)` and versionstamp form | One confirmed commit, with status | Structured cause, phase and status; no automatic recovery |
| `txr(body, args...)` | The final attempt's owning value and status | Application rejection, native failure/recovery data or a captured exception |
| `txr(txn, body, args...)` | Working value; commit remains with the caller | The same failure family, without local recovery or commit |
| `stamp.resolved_bytes(with_result)` | Borrowed stamp bytes | A recorded stamp error, independent of commit success; pending access is a contract error |

A returned failure or exception is not a rollback guarantee. Managed replay
defaults to stopping when commit effects are uncertain. Inspect the returned
status; explicit `replay_unknown` permits eligible recovery after uncertainty.
Design replayable database effects deliberately.
Leaf operations can throw before manual commit is called. An owning Transactor call
catches failures across creation, binding, the body, and completion; a borrowed call
captures handle validation and body failures without taking over transaction cleanup.

The shorter examples outside the transaction chapter use
`lfdb::commit(txn).value()` when they choose to stop by throwing on failure.
That is the standard `std::expected` accessor: its
`std::bad_expected_access<transaction_failure<>>` retains the complete error.
It is caller work after result delivery, not another libfdb completion mode.

## Setup

Choose one setup form before the first FoundationDB network initialization;
the snippets below are alternatives. Network options take effect only during
that first initialization.

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

Pass a `network_options` object when only the client network needs configuration.
This keeps default cluster-file resolution and supplies empty database options:

```cpp
const lfdb::network_options netopts{
  { FDB_NET_OPTION_TRACE_FORMAT, std::string{"json"} }
};

auto dbh = lfdb::create_database(netopts);
```

`lfdb::create_database(lfdb::network_options{})` accepts an explicitly typed temporary.
`create_database({})` retains its existing meaning of empty database options;
`create_database()` is the simplest form when neither set of options is needed.

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

## Options

`network_options`, `database_options`, and `transaction_options` are owning
`option_list<CodeT>` sequences. Entries apply in written order, including repeated
codes and identical flags. This supports additive transaction tags and settings
whose order matters.

**Migration from `option_map`:** the sorted, unique-key container and its alias
are removed. Replace map-specific `find()`, `insert_or_assign()`, and `operator[]`
calls with sequence operations. Braced initializers still work, but now retain
duplicates and apply in written order. Review their ordering as well as explicit
map-style edits; `size()` counts applications rather than distinct codes.

```cpp
lfdb::transaction_options options;
options.reserve(4);
options.emplace_back(FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3});
options.append({
  {FDB_TR_OPTION_TAG, "a"},
  {FDB_TR_OPTION_AUTO_THROTTLE_TAG, "b"},
  {FDB_TR_OPTION_TAG, "c"}
});
const std::array more {
  lfdb::transaction_options::value_type {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
};
options.append_range(more);

// Change both TAG entries without moving their positions:
const auto replaced = options.replace_all(FDB_TR_OPTION_TAG, "batch"); // 2

// Replace a former options[FDB_TR_OPTION_TIMEOUT] = 5000 assignment:
options.erase(FDB_TR_OPTION_TIMEOUT);
options.emplace_back(FDB_TR_OPTION_TIMEOUT, std::int64_t {5000});
```

`push_back(pair)` and `emplace_back(code, value)` append one entry;
`std::back_inserter()` works with range algorithms. `append_range()` accepts
single-pass ranges and preserves their order. Initializer-list elements are
copied; move iterators transfer owned values. Merely passing an rvalue container
does not move its elements. An appended range must not overlap its destination.

`replace_all()` returns the number of matching entries, including unchanged
matches. It inserts nothing when the code is absent. `erase(code)` removes every
matching entry and returns the count; `clear()` removes everything. Iteration is
const, and insertion and erasure follow vector invalidation rules.

Transactions, Transactors, and lazy database scans own their option snapshots.
Changing the original list affects later construction. A consumed list can
transfer its storage to `make_transaction()`. Validation happens when options
are applied; native failures retain their error code and category.

On replay, libfdb reapplies nonpersistent entries in recorded order. FoundationDB
retains timeout, retry limit, maximum retry delay, and authorization-token options.
The library's ordinary recovery machinery remains responsible for these rules.

Database-handle helpers accept options immediately after the handle:

```cpp
lfdb::set(dbh, options, "person/example/name", "Example");
const auto rows = lfdb::collect(dbh, options, q::prefix("person/"));
const auto partitions = lfdb::partitions(dbh, options, q::prefix("person/"), 1024 * 1024);
```

The same argument order applies to point and range reads, scans, paged reads,
watches, atomic mutations, transformations, and erasure. Helpers without an
options argument use an empty list. Use an explicit transaction when several
operations must share one configured transaction and read version.

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

A transaction handle groups operations; ordinary completion submits once:

~~~cpp
auto txn = lfdb::make_transaction(dbh);
lfdb::set(txn, "person/frances-allen/name", "Frances Allen");
const auto committed = lfdb::commit(txn);
if (not committed) {
  const auto& failure = committed.error();
  std::cerr << "Commit confirmed: " << false
            << ", may finish later: " << failure.status.may_commit_later() << '\n';
}
~~~

Inspect the cause as well as commit knowledge. Native/library codes retain their
`std::error_code` category; arbitrary C++ exceptions retain their identity:

~~~cpp
if (not committed) {
  const auto& failure = committed.error();
  if (const auto *native = std::get_if<lfdb::fdb_error>(&failure.cause)) {
    std::cerr << native->code.category().name() << ": " << native->code.message() << '\n';
  }
  if (const auto *exception = std::get_if<std::exception_ptr>(&failure.cause)) {
    // Explicit caller choice; completion itself has already returned.
    std::rethrow_exception(*exception);
  }
}
~~~

`commit(txn, stamp)` has exactly the same result contract. Neither form resets
the transaction, calls native `on_error`, or replays its body. If the caller
chooses manual replay, it must rerun the entire dependent read/write sequence
after `reset_for_replay(txn, native.code)` succeeds. Reusing the same transaction
preserves FoundationDB's retry/timeout budgets. Do not blindly retry just the
commit, replace an exhausted transaction, or treat a terminal code as a rollback
guarantee. Prefer a libfdb Transactor for whole-body replay.

A reusable Transactor keeps configuration. An ordinary call creates its own
transaction; an explicit transaction argument selects borrowed execution, described
below. The owning call borrows its synchronous body and owns bound arguments once
for reuse across attempts:

~~~cpp
const auto txr = lfdb::make_transactor(dbh);
const auto updated = txr([](auto& txn, const std::string& name) {
  lfdb::set(txn, "person/frances-allen/name", name);
  return name.size();
}, std::string {"Frances Allen"});

if (updated) {
  std::cout << "Committed " << updated->value << " characters in "
            << updated->status.attempts << " attempt(s)\n";
}
~~~

`const txr` fixes configuration; it does not make its new transaction immutable.
Mutable and noncopyable lvalue bodies work. Ordinary bound inputs should be
treated as read-only: mutations otherwise survive replay. Borrowing with
`std::ref` retains the caller's lifetime obligations.

The body and its helpers leave completion and replay to this owner. Do not
commit or reset the supplied handle inside a Transactor body; pass that same
handle to dependent operations and return their result to the outer owner.

### Return Values And Application Failures

A void or ordinary value return authorizes commit. In
`std::expected<T, E>`, a value authorizes commit and `std::unexpected(E)`
rejects the attempt. The error is application data even when E is
`std::error_code`; its category does not turn a rejection into a native retry.
The explicit `transaction_failure<E>` error type instead propagates a library
failure with its existing classification, as shown below.

Give the body an explicit expected return type, or return a typed expected local so
the function or lambda can infer its result. A bare unexpected<E> return is rejected
at compile time rather than silently committed as an ordinary payload.

~~~cpp
enum struct update_error { empty_name };

auto update_name(lfdb::transaction_handle& txn, const std::string& name)
  -> std::expected<std::size_t, update_error>
{
  if (name.empty()) {
    return std::unexpected(update_error::empty_name);
  }
  lfdb::set(txn, "person/frances-allen/name", name);
  return name.size();
}

const auto txr = lfdb::make_transactor(dbh);
if (const auto result = txr(update_name, std::string {"Frances Allen"}); not result) {
  if (const auto *rejection =
        std::get_if<lfdb::application_error<update_error>>(&result.error().cause)) {
    std::cerr << "Update rejected: " << static_cast<int>(rejection->error) << '\n';
  }
}
~~~

A negative domain outcome may still be a successful committed value when the
body deliberately performs cleanup. Return that outcome as T; use unexpected
only when the entire attempt must be rejected. Native operations still use
their existing leaf exception contract. User serializer/decoder exceptions
never become replay requests merely because they carry a native-looking code.

An owning call's `transaction_result<T, E>` is
`std::expected<committed_value<T>, transaction_failure<E>>`.
Void bodies omit the payload; bodies without an application error channel omit
`application_error<E>` from the failure variant. Success and failure both carry
`transaction_status`. Invocation and final delivery are nonthrowing once caller
arguments have been constructed. Factory/options construction and subsequent
caller processing can still throw.

T and E need nonthrowing moves and destruction. An explicit owning pointer can
carry an otherwise immovable object. A movable view or generator is not
automatically safe: consume transaction-bound rows before commit and return
owning data. Staged publication likewise cannot throw after confirmed commit.

### Reusing A Transaction And Composing Results

An explicit handle selects one borrowed invocation:

| Body result | Owning call | Borrowed call |
| --- | --- | --- |
| `T` or `void` | `transaction_result<T>` | `std::expected<T, transaction_failure<>>` |
| `std::expected<T, E>` | `transaction_result<T, E>` | `std::expected<T, transaction_failure<E>>` |
| `std::expected<T, transaction_failure<E>>` | `transaction_result<T, E>` | The same expected type |

The borrowed call does not commit, reset, replay, finish versionstamps, or apply
the Transactor's creation options. It borrows the body and arguments directly,
including mutable noncopyable bodies and immovable lvalue inputs. Its failure
describes validation or body execution; successful working data has no commit status.
Caller argument construction still occurs before the invocation's exception boundary.
A null borrowed handle returns a setup failure with zero body attempts; its body
is not invoked. A valid borrowed invocation records one body attempt.

`and_then()` propagates the common error type and can change the success type.
The following complete operation reuses one body under both ownership forms:

~~~cpp
enum struct profile_error { missing, empty };

auto read_profile(lfdb::transaction_handle& txn, const std::string& key)
{
  std::expected<std::map<std::string, std::string>, profile_error> fields {std::in_place};
  if (not lfdb::get(txn, key, *fields)) {
    fields = std::unexpected(profile_error::missing);
  }
  return fields;
}

auto write_profile(lfdb::transaction_handle& txn, const std::string& key,
                   const std::map<std::string, std::string>& fields)
  -> std::expected<std::size_t, profile_error>
{
  if (fields.empty()) {
    return std::unexpected(profile_error::empty);
  }
  lfdb::set(txn, key, fields);
  return fields.size();
}

void reuse_profile(const lfdb::database_handle& dbh)
{
  const std::string key = "example/reuse/profile";
  lfdb::set(dbh, key, std::map<std::string, std::string> {{"name", "Frances"}});
  const auto txr = lfdb::make_transactor(dbh);
  auto update_profile = [&](auto& txn) {
    return txr(txn, read_profile, key).and_then([&](auto&& fields) {
      fields["name"] = "Frances Allen";
      return txr(txn, write_profile, key, fields);
    });
  };
  auto report_status = [](const auto& failure) {
    std::cerr << "Failure after " << failure.status.attempts
              << " attempt(s); uncertain: " << failure.status.has_unknown_commit() << '\n';
  };

  auto caller_txn = lfdb::make_transaction(dbh);
  const auto working = txr(caller_txn, update_profile);
  if (not working) {
    report_status(working.error());
    return;
  }
  std::cout << "Prepared " << *working << " fields before commit\n";
  if (const auto committed = lfdb::commit(caller_txn); not committed) {
    report_status(committed.error());
    return;
  }

  const auto completed = txr(update_profile);
  if (not completed) {
    report_status(completed.error());
    return;
  }
  std::cout << "Committed " << completed->value << " fields in "
            << completed->status.attempts << " attempt(s)\n";
}

reuse_profile(dbh);
~~~

The [standalone program](../../../examples/libfdb/mini_example.cc) includes a central
reporter for application errors, native codes, original exceptions, recovery errors,
and earlier commit uncertainty. The same reporter accepts ordinary commit failures
and both Transactor failure forms. Apply application error translation after the
owning invocation finishes; translating an intermediate native failure would hide
it from the owner's recovery path.

A returned `transaction_failure<E>` is already classified. Native causes use the
existing owner's recovery path; `application_error<E>` remains rejection even when
its payload contains a native-looking error code. The owner retains its logical
attempt count and earlier uncertainty. A returned failed recovery remains terminal.
Propagate failures from operations participating in this transaction; separately
committing transactions need their own application protocol.

Standard `and_then()` requires identical error types. `transform()` is concise when
the next step is a native operation without an application rejection channel:

~~~cpp
const auto txr = lfdb::make_transactor(dbh);
const auto completed = txr([&](auto& txn) {
  return txr(txn, read_profile, std::string {"example/reuse/profile"})
    .transform([&](auto&& fields) {
      fields["name"] = "Frances Allen";
      lfdb::set(txn, "example/reuse/profile", fields);
    });
});
if (not completed) {
  std::cerr << "Profile update failed; uncertain: "
            << completed.error().status.has_unknown_commit() << '\n';
}
~~~

Neither standard monadic operation catches exceptions thrown by its callback.
Keep the whole chain inside an owning or borrowed Transactor invocation so those
exceptions become structured failures. Put staged arguments on the owning call;
borrowed calls return working values and reject explicit staged bindings.

### Staged Proxies

An owning Transactor call may run its body more than once. Mutating a captured output
variable directly can therefore append or count the same result several times.
Staged Proxies give each attempt a private value and publish only the attempt
whose commit is confirmed.

Choose the parameter form according to its role:

| Parameter | Use it for | Replay behavior |
| --- | --- | --- |
| `input` | An input that the transaction reads but does not modify. | The transactor stores it once and presents the same value to every attempt. |
| `lfdb::staged(output)` | An output that modifies or extends its existing value. | Each attempt lazily copies the existing value before its first mutation. |
| `lfdb::staged(std::in_place, output)` | An output rebuilt entirely from transaction results. | Each attempt starts with a fresh value; the previous contents are never copied. |

Treat ordinary bound parameters as read-only. Mutating one changes only the
transactor's private argument and that mutation carries into later attempts;
it does not update the caller's object.

In-place construction happens before each attempt, so a successful query that
finds no values still replaces stale output with an empty value.

Prefer an owning return value when the transaction body produces one new value.
Staging is useful for output parameters or several existing local values.
The standalone example composes a borrowed read with `transform()` inside an
owning call and publishes its vector through `staged(std::in_place, output)`
only after that call confirms commit.

```cpp
// Recompute a bucket's cached-byte count without counting a replay twice:
std::uint64_t cached_bytes = 0;
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn, auto& bytes, std::string_view bucket_id) {
  bytes = 0;

  for (const auto& block :
       lfdb::scan<block_info>(txn, object_range(bucket_id)) |
         std::views::values) {
    bytes += block.size;
  }
}, lfdb::staged(cached_bytes), bucket_id).value();
```

If you declare a proxy ahead of time, move it into the transactor because it
represents exactly one invocation's staging state:

```cpp
// Rebuild a block listing without copying its previous contents:
std::vector<block_info> blocks;
auto blocks_prx = lfdb::staged(std::in_place, blocks);
auto txr = lfdb::make_transactor(dbh);

txr([](auto& txn, auto& blocks, std::string_view bucket_id) {
  for (auto block : read_blocks(txn, bucket_id)) {
    blocks.push_back(std::move(block));
  }
}, std::move(blocks_prx), bucket_id).value();
```

The proxy covers assignment, `+=`, common sequence updates, and
`insert_or_assign()`; use `get_target()` for an uncommon operation. Mutating
operations deliberately return no iterator or reference into attempt-local
storage.

Gotchas:

- An ordinarily staged target must be copy-constructible; an in-place target
  must be default-initializable. Both must be nothrow move-assignable, alive for
  the call, and not accessed concurrently. Binding the same target more than
  once is rejected as a setup failure; overlapping targets are also invalid.

- Accept the proxy by lvalue reference (`auto& output`, as above). Do not
  capture and mutate the original target or retain the proxy after the callback.
  Any pointer, reference, or iterator obtained through `get_target()` is
  attempt-local and must not escape the callback.

- Staging protects only the bound C++ value. Logging, I/O, other captured state,
  and non-idempotent database writes after `commit_unknown_result` still need
  their own replay-safe design.

Any outcome without a confirmed commit leaves the staged target unchanged.
Inspect the Transactor result before using a successful publication. Duplicate
target binding is a setup failure in that result, before any body attempt.

### Completion Status And Retry Decisions

`transaction_failure.phase` distinguishes setup, body, observed commit failure
and commit observation failure. Status describes the latest attempt and retains
earlier uncertainty:

| Status/query | Meaning |
| --- | --- |
| `not_submitted` | This attempt did not submit a commit |
| `not_committed` | The observed native error establishes noncommit |
| `committed` | This attempt's commit was confirmed |
| `unknown_result` | The observed 1021 is uncertain but quiescent under the transaction's normal options |
| `unconfirmed` | Commit was submitted; it may still finish |
| `has_unknown_commit()` | Current or earlier commit effects remain uncertain |
| `may_commit_later()` | Current or earlier submission may still finish |

Success of a later attempt does not erase an earlier unknown commit.
`first_unknown_commit_error` retains the first available error code;
`prior_unknown_commit` preserves earlier knowledge. Timeout/cancellation and
failure to wait can leave an in-flight commit even when the native
`MAYBE_COMMITTED` predicate is false. The expert option CAUSAL_WRITE_RISKY
also weakens normal 1021 quiescence. Native completion reads the actual
transaction options when classifying the outcome.

Risky causal writes can also permit a duplicate after a reported success.
`duplicate_commit_possible` records that conservative possibility, including
when INITIALIZE_NEW_DATABASE enables risky writes implicitly. Both status
queries remain true in that case even though the observed attempt committed.
The flag is evidence about the chosen guarantees, not an observed duplicate.
It can be true without a first error code because no failure was observed.
See the pinned [native commit implementation](https://github.com/apple/foundationdb/blob/341404ebaa376c5247929cc8543dfd4bc3cf9e1a/fdbclient/NativeAPI.actor.cpp#L6734)
and [option definitions](https://github.com/apple/foundationdb/blob/341404ebaa376c5247929cc8543dfd4bc3cf9e1a/fdbclient/vexillographer/fdb.options).

Both policies use native `on_error` for eligibility, backoff, retry limit and
timeout. They add no libfdb attempt budget. A zero native retry limit allows the
initial attempt only. The default, `retry_policy::no_replay_unknown`, stops when
this owner has uncertain commit effects:

~~~cpp
const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}},
  {FDB_TR_OPTION_TIMEOUT, std::int64_t {5000}}
};
const auto txr = lfdb::make_transactor(dbh, options);
if (const auto result = txr(update_name, std::string {"Frances Allen"}); not result) {
  const auto& failure = result.error();
  std::cerr << "Update failed after " << failure.status.attempts
            << " attempt(s); uncertain: " << failure.status.has_unknown_commit() << '\n';
}
~~~

The same default applies to managed database-handle convenience operations. Native
read-only recovery has no commit effects. Both policies leave an unsubmitted body
failure eligible for native recovery. "Unknown" includes `unknown_result`,
`unconfirmed`, and uncertainty retained from earlier attempts, as reported by
`has_unknown_commit()`. It does not mean merely an unrecognized error code.
A commit observation failure always stops without automatic replay. Versionstamp
retrieval failure after confirmed commit does not reverse success or invoke retry
policy. To permit recovery after an uncertain native commit result, choose
`retry_policy::replay_unknown`, as in the durable-marker example below.

For an additional application decision, pass `retry_if(callable)`:

~~~cpp
const auto result = txr(lfdb::retry_if([](const lfdb::transaction_failure<>& failure) {
  const auto& native = std::get<lfdb::fdb_error>(failure.cause);
  return native.code != lfdb::fdb_error_code::database_locked;
}), update_name, std::string {"Frances Allen"});
~~~

The callable sees native failures permitted by the uncertainty policy, with their phase/status,
before recovery.
False returns the original failure. True requests native preparation; it cannot
override eligibility, budgets or the configured uncertainty policy. One mutable,
possibly move-only callable is reused for the invocation. Policy exceptions
are captured in `fdb_error.retry_error` without replacing the original trigger.
Native recovery failure retains its separate code there. Recovery does not call
the decision callable recursively. Application rejection and user-origin
exceptions bypass native policy.

When a library failure was originally thrown, `fdb_error.exception` retains the
original exception and its diagnostic context alongside the authoritative code.
An observed native commit-future error need not have an exception object.

### Resolving An Uncertain Commit

For database effects that must be applied once, store a durable operation marker
in the same transaction as those effects. Keep its identifier stable across
attempts, later reconciliation and process restarts:

~~~cpp
void apply_once(lfdb::transaction_handle& txn, const std::string& marker)
{
  if (lfdb::key_exists(txn, marker)) {
    return;
  }
  std::uint64_t count = 0;
  lfdb::get(txn, "example/once/count", count);
  lfdb::set(txn, "example/once/count", count + 1);
  lfdb::set(txn, marker, true);
}

const std::string marker = "example/once/applied/job-42";
const auto txr = lfdb::make_transactor(dbh, options, lfdb::retry_policy::replay_unknown);
const auto result = txr(apply_once, marker);
if (const auto& status = result ? result->status : result.error().status; status.has_unknown_commit()) {
  std::cerr << "Reconciliation required; may finish later: "
            << status.may_commit_later() << '\n';
}
~~~

Use a distinct identifier for each logical operation and never reuse it for
different work. Serializable marker reads prevent two attempts with that
identifier from both applying the increment. Keep the marker as long as retry
remains possible and ensure the count has room to increment.
The explicit policy permits recovery; FoundationDB still decides whether recovery
can proceed. A returned failure retains the original cause, phase, and commit
history even when that recovery fails.

An absent marker in a separate read does not prove that a submitted commit
cannot still finish. Normal observed 1021 is quiescent; timeout, cancellation
and risky options need greater care. A later successful result still reports
earlier uncertainty. The result supplies evidence, not a reconciliation policy.
External messages or payments need their own deduplication or a transactional
outbox.

### Transaction Options And Failure Boundaries

The existing brace-list option syntax and simple defaults remain:

~~~cpp
const auto defaults = lfdb::make_transactor(dbh);
const auto configured = lfdb::make_transactor(dbh, {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}},
  {FDB_TR_OPTION_TIMEOUT, std::int64_t {5000}}
});
~~~

Options apply to every transaction created by that Transactor; native recovery
preserves persistent options and restores nonpersistent ones on the same
transaction. Do not recreate transactions to bypass exhausted native budgets.
Borrowed calls retain the supplied transaction's options; the configured replay
policy and per-invocation retry_if control owning execution only.
Set replay-sensitive and causal-write options through libfdb configuration.
Changes made directly on raw native handles are not tracked for restoration or
outcome classification.
A failure constructing configuration occurs before invocation. Failures creating
a transaction or binding arguments are reported with phase setup and zero body
attempts. A manual precondition failure reports that no commit was submitted.

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
| Tighten an existing single interval | `q::assign_intersection(range, bound)` or `range &= bound` | `lfdb::select&` | Updates the range while retaining unchanged endpoint storage. |
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

Union, intersection, and symmetric difference preserve matching options. When only one operand has
non-default options, those options apply to the result. Conflicting non-default
options throw `libfdb_exception` with `std::errc::invalid_argument`. Configure the
operands consistently, or combine unconfigured geometry and apply
`with_options()` to the result.
Difference retains its left operand's options; complement retains its operand's
options.

Use `assign_intersection()` to modify one concrete interval. `operator&=` delegates
to that same operation; `intersection()` produces a value and `intersects()` tests
whether the common range is nonempty. These operations describe ranges independently
of which keys are stored in the database:

```cpp
namespace q = ceph::libfdb::query;

auto people = q::prefix("person/");
q::assign_intersection(people, q::at_or_after("person/m"));
people &= q::before("person/z");

for (const auto& [key, value] : lfdb::scan(txn, people)) {
  fmt::println("{}: {}", key, value);
}
```

Both mutating forms return the first interval by reference and support constant
evaluation. Lvalue bounds remain unchanged; temporary bounds can donate tighter
endpoint strings. Unchanged endpoints retain their storage. Aliasing the first
interval, including `range &= std::move(range)`, is supported. On an exception the
first interval remains unchanged; an rvalue second operand may have been consumed.
The usual option-conflict and ordinary-keyspace rules still apply, including empty
results. Concrete value-producing intersection uses the same mutator on its local
first operand. This overload accepts concrete intervals; intersections involving
composite query expressions retain their existing lazy expression interface.

Selection construction can include `rgw/fdb/query.h` directly. Its shared key
concepts and key adaptation live in `rgw/fdb/key.h`, which uses only standard
headers. Scalar emptiness and interval-count checks borrow endpoint bytes.
Selector materialization moves rvalue intervals and copies borrowed intervals.

The full set-operator family delegates to the corresponding named functions:

| Operation | Value function | Value operator | Named mutator | Compound operator |
|---|---|---|---|---|
| Intersection | `intersection(a, b)` | `a & b` | `assign_intersection(a, b)` | `a &= b` |
| Union | `set_union(a, b)` | `a \| b` | `assign_set_union(a, b)` | `a \|= b` |
| Difference | `difference(a, b)` | `a - b` | `assign_difference(a, b)` | `a -= b` |
| Symmetric difference | `symmetric_difference(a, b)` | `a ^ b` | `assign_symmetric_difference(a, b)` | `a ^= b` |
| Complement | `complement(a)` | `~a` | `assign_complement(a)` | C++ has no `~=` |

A single interval cannot hold disconnected results. Use an explicit `interval_set`
for general mutation; it owns canonical, ordered, nonoverlapping core intervals and
one set of execution options, including when it is empty:

```cpp
q::interval_set visible {
  (q::prefix("object/") | q::prefix("archive/")) - q::prefix("object/private/")};
visible |= q::prefix("shared/");
visible -= q::prefix("archive/obsolete/");
visible &= q::before("shared/z");
visible ^= q::prefix("shared/temporary/");

for (const auto& [key, value] : lfdb::scan(txn, visible)) {
  fmt::println("{}: {}", key, value);
}

auto outside = ~visible;                 // An owning expression for the complement.
q::assign_complement(visible);           // Replace visible with its complement.
visible = ~visible;                      // Assign an arbitrary expression back to the set.
```

The named mutators and compound operators return the destination by reference.
General set mutation borrows its operands, builds a complete canonical replacement,
and publishes it by swapping after all throwing work succeeds. On failure the
destination and borrowed operands stay unchanged. Self-aliasing, including
`selected ^= std::move(selected)`, is supported. The single-interval `&=` overload
retains its endpoint-storage reuse; general sets materialize owned endpoint ranges
for the replacement. These operations support constant evaluation when their
domain and storage permit it. `with_options(interval_set, options)` retains the
mutable set type. `compile_intervals()` still exports the existing vector or
fixed-capacity collection. Core storage exposes const iteration to preserve its
canonical order; exported collections are independent values.
Geometric predicates borrow their operands and ignore execution-option conflicts.
The dynamic pure-core algebra accepts immutable, copy-constructible keys. Fixed-
capacity export uses preconstructed assignable slots and therefore additionally
requires assignable interval values; this requirement is checked by its constraints.

As with existing expressions, options apply to each emitted native interval;
`result_limit` does not become a whole-query row limit. Conflicting options for
union, intersection, and symmetric difference fail before mutation. Difference
retains the left options and complement retains the operand's options. Symmetric
difference selects keys present in exactly one operand. Complement is relative
to ordinary FDB keyspace, while `selection ^= window` toggles membership inside
the window and retains selected keys outside it. These meanings differ whenever
the original selection contains keys outside the window.

Value operators retain the named functions' eager single-interval intersection
and otherwise lazy, owning expression behavior. An rvalue operand transfers its
owned state; an lvalue must be copied when constructing an owning expression.
Prefer compound assignment when a mutable result is what the caller needs.
The pure algebra has the same operators and `interval::interval_set<Domain>`;
its complement uses the domain's algebraic universe. Mixed core/FDB operators
select the FDB adapter when either operand is an FDB interval or configured selection, and
emission clips to ordinary keyspace and discards empty clipped ranges.

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

#### Preparing bounded interval lists

When an expression's shape supplies a finite bound, let the library infer fixed
storage rather than guessing a capacity or counting first:

```cpp
const auto selected = q::difference(q::prefix("tenant/"), q::prefix("tenant/private/"));
static_assert(2 == q::interval_capacity_v<decltype(selected)>);
const auto prepared = q::compile_intervals(std::in_place, selected);

for (const auto& interval : prepared) {
  // Execute or inspect each prepared interval:
  inspect(interval);
}
```

`interval_capacity_v<Expression>` is an upper bound on emitted components, not
their actual count or the number of selected keys. If two operands have bounds
`p` and `q`, their union, intersection, difference, and symmetric difference need
at most `p + q` components. Complement is difference from one universal interval,
so a bound of `p` becomes at most `p + 1`. Empty or coalescing results can be much
smaller. Overflow and expressions whose size is known only at runtime yield
`std::dynamic_extent`; use ordinary `compile_intervals(expression)` for those.

The same inferred overload is available in the pure interval namespace and can
be used during constant evaluation. Fixed storage requires assignable interval
values. Dynamic algebra continues to support immutable, non-default-constructible
keys. Fixed interval-list storage does not remove allocations inside owned keys,
such as long strings. `compile_intervals<Capacity>()` remains available when the
caller deliberately supplies a bound, and rejects insufficient capacity.

Reuse an `interval_set` for repeated geometric work. Predicates and expression evaluation
borrow its existing canonical storage. Difference prepares a compound right
operand once, only if the left operand emits anything. Small expression trees
use bounded inline scratch internally; larger or unsuitable value types retain
dynamic storage.

#### Interval mathematics for reconciliation

Think of a selection as a set of eligible keys or positions. For `a <= b`, a
half-open byte extent `[a, b)` contains offsets `a` through `b - 1`, so its length is `b - a`.
Adjacent extents `[0, 5)` and `[5, 8)` have empty intersection and unite to
`[0, 8)`. This convention avoids counting their boundary twice. `[a, a)` is
empty; the closed singleton `[a, a]` contains one position.

Let the old byte coverage be `A = [0, 8)` and the desired coverage be
`B = [5, 12)`:

| Question | Algebra | Result |
| --- | --- | --- |
| What remains covered? | `A & B` | `[5, 8)` |
| What must be added? | `B - A` | `[8, 12)` |
| What must be removed? | `A - B` | `[0, 5)` |
| What changes in either direction? | `A ^ B` | `[0, 5)` union `[8, 12)` |
| What is covered by either plan? | `A \| B` | `[0, 12)` |

The pure algebra expresses this without a database or Ceph payload dependency:

```cpp
#include "rgw/fdb/interval.h"

#include <cassert>
#include <compare>
#include <cstdint>

namespace algebra = ceph::libfdb::interval;

struct byte_offset_domain final
{
  using value_type = std::uint64_t;

  static constexpr std::strong_ordering compare(const value_type lhs, const value_type rhs)
  {
    return lhs <=> rhs;
  }

};

void reconcile_byte_extents()
{
  using extent = algebra::query<byte_offset_domain>;

  const auto previous = extent::closed_open(0, 8);
  const auto desired = extent::closed_open(5, 12);
  const auto additions = desired - previous;
  const auto removals = previous - desired;
  const auto changed = previous ^ desired;

  assert(algebra::is_disjoint(additions, removals));
  assert(algebra::contains(additions, 8));
  assert(not algebra::contains(additions, 12));
  assert(algebra::contains(changed, 0));
  assert(not algebra::contains(additions, 0));

  // Apply only the edits needed to reach the desired coverage:
  algebra::interval_set<byte_offset_domain> applied {previous};
  applied -= removals;
  applied |= additions;

  assert(algebra::encloses(applied, desired));
  assert(algebra::encloses(desired, applied));
}
```

Additions and removals are disjoint, and
`(A - (A - B)) | (B - A)` restores `B`. The same calculation can drive extent
repair, coverage reconciliation, or changes between two key-selection policies.
Geometry only identifies the ranges; the application still performs its reads,
writes, or byte movement. Prefix operations additionally need a domain successor;
the byte-offset example needs only the strong ordering above. Keep unbounded
endpoints as bounds rather than overflowing a numeric endpoint to invent one.

Symmetric difference is a toggle, with the membership rule
`x in A ^ B` exactly when `x` is in one operand but not both:

```text
A ^ B = (A - B) | (B - A) = (A | B) - (A & B)
A ^ A = empty
(S ^ delta) ^ delta = S
```

Repeated XOR keeps positions present in an odd number of operands. Operand order
does not change that geometry, and applying the same toggle twice cancels it.

This makes XOR useful for a complete change set. It differs from finding holes
inside a window. For `S = [2, 8)` and `W = [5, 10)`, `W - S` is `[8, 10)`,
whereas `S ^ W` also retains `[2, 5)` outside `W`. A complement restricted to a
window is `W - S`, equivalently `W & ~S`. In the FDB adapter, bare `~S` ranges
over the entire ordinary keyspace `["", "\xFF")`; intersect with the intended
tenant/bucket scope when expressing a local exclusion.

Set identities help simplify and check a selection before issuing reads:

```text
A & (B | C) = (A & B) | (A & C)
~(A | B) = (~A) & (~B)
~(A & B) = (~A) | (~B)
A - B = A & ~B
```

These equalities describe membership in one common universe and ordered domain,
not C++ expression-object equality or execution-option equality. They apply to
unconfigured geometry or compatible options. Option conflicts can still throw,
and per-interval read limits can change the records a query actually returns.
Ask `encloses(A, B)` whether `B` is a subset of `A`, or check enclosure in both
directions for equal geometry; these predicates ignore execution options.

For finite byte coverage, `|A ^ B| = |A| + |B| - 2 * |A & B|`, where the bars
count byte positions. Here that is `8 + 7 - 2 * 3 = 9`. Summing lengths over
canonical disjoint extents avoids counting overlapping bytes twice.

`algebra::interval_count(changed)` is two in the pure example; use
`q::interval_count()` for FDB selections. These count canonical components, not
covered bytes, possible keys, or stored database rows. The changed coverage above
has two components and nine byte positions. Database record counts require reading the selected records;
an empty result set does not make its selection geometrically empty.

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

lfdb::commit(txn).value();
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

Use raw callbacks to inspect or copy encoded records without decoding them:

```cpp
std::size_t encoded_bytes = 0;
lfdb::for_each(lfdb::raw, dbh, q::prefix("person/"),
  [&encoded_bytes](std::span<const std::uint8_t> key,
                   std::span<const std::uint8_t> value) {
    encoded_bytes += key.size() + value.size();
  });
```

Both spans expire when the callback returns. Values contain the stored encoding,
including any serialization framing. Raw traversal borrows noncopyable consumers,
retains the transaction during callbacks, and returns the number of rows delivered.
An explicit read mode precedes the callback; options follow a database handle:
`for_each(raw, dbh, options, selection, read_mode::snapshot, callback)`.
The transaction-handle form shares its caller's transaction. The database form
uses bounded, independently retried windows; callbacks run after recovery and are
never replayed, while later windows can observe newer read versions. Exceptions
from callbacks propagate immediately, including native-looking libfdb exceptions.

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
objects. If the caller puts the traversal inside an owning Transactor call, an outer replay
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

For a generic range or a logical page that can span native windows, use
`collect(rows, page)`. The source remains in its caller's transaction when it
comes from `scan(txn, ...)`:

```cpp
const auto page = lfdb::collect(
  lfdb::scan(txn, people) | std::views::values,
  lfdb::page{100});

for (const auto& value : page.rows) {
  fmt::println("{}", value);
}
```

The page limit bounds returned rows; it is not a reservation request. Exact-sized
sources reserve up to their actual size, capped by the page limit. Unknown-size
streams grow as values arrive. Reservation waits for the first value, so an empty
generic range allocates no output-vector storage. The single-window scan above
reserves from its actual native row count and does not decode the sentinel.

When `__cpp_lib_ranges_reserve_hint` advertises C++26 library support, bounded
pages can also use `std::ranges::reserve_hint` for approximately sized sources.
The feature macro gates library availability; range concepts check applicability.
Libraries without this feature use exact `sized_range` information only. An
approximate hint is advice and may overestimate; unlimited unsized pages ignore
it, because an adaptor such as `views::take(n)` can advertise its cap as a hint.
Querying metadata does not traverse the source. Metadata/iteration/allocation
exceptions propagate, and vectors may round capacity above a reservation request.

Generic collection is `constexpr` for suitable sources and values. Allocation
must be released within the constant evaluation; database reads remain runtime:

```cpp
static_assert([] {
  const auto page = lfdb::collect(std::array{10, 20, 30}, lfdb::page{2});
  return page.has_more && 2 == page.size() && 10 == page.rows.front();
}());
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
fdbc::compiled_key block_index_key(const block_info *block)
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

for (const auto& [key, block] : lfdb::scan<block_info>(dbh, fdbc::prefix(object_blocks))) {
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

Pass a compiled prefix to `key()` when generating several children:

```cpp
const auto objects = fdbc::key("tenant", bucket_id);
const auto first = fdbc::key(objects, "objects", "first");
const auto second = objects / "second";

auto owned = objects;
owned.append("objects", "third");
owned /= "metadata";
const auto final = fdbc::key(std::move(owned), "version");
```

An lvalue prefix is preserved and copied directly into the new key. A consumed
prefix can reuse its storage; `append()` and `/=` extend an exclusively owned
key in place. Segment arguments to these mutating operations must not refer to
the key's own encoded bytes, because growing its storage can invalidate those
views. All forms retain the same segment escaping and ordering.

Assembly and batch `append()` size embedded-NUL escapes before reserving storage.
They reuse the same NUL inspection when encoding; a raw segment of `n` bytes with
`z` zero bytes occupies `n + z + 2` encoded bytes. Existing compiled-prefix bytes
already include their escapes and separators. Consuming `/` and `/=` retain normal
string growth for successive extensions. The aliasing restriction also applies to
consuming `key(std::move(prefix), ...)` and `/`.

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

lfdb::commit(txn, stamp).value();
const auto& version_stamp_bytes = stamp.resolved_bytes();
```

Handle a stamp failure separately from the commit. A read-only transaction
illustrates the distinction without requiring a fault:

```cpp
auto txn = lfdb::make_transaction(dbh);
lfdb::versionstamp stamp;
std::string name;
lfdb::get(txn, "person/alonzo-church/name", name);

lfdb::commit(txn, stamp).value();
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

lfdb::commit(txn).value();
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

lfdb::commit(txn).value();
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

lfdb::commit(txn).value();
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
  return fdbc::keyspace("example") / "object" / bucket_id / object_name;
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
          lfdb::versioned("example/event/" + std::string(bucket_id) + "/",
                          "/" + std::string(object_name) + "/put",
                          event_version),
          "put");
lfdb::atomic::add(txn, "example/stats/object-writes", std::uint64_t{1});

lfdb::commit(txn).value();
publish_ordered_event(event_version.resolved_bytes());
```

The explicit transaction keeps the state update, event append, and counter
mutation atomic. The versionstamped event key is commit ordered without an
application sequence counter. Propagate a failed commit result, including its
status, to the application's recovery boundary. This recipe's explicit value()
accessor throws bad_expected_access carrying that result's complete failure. Stamp access and publication happen after
confirmed commit; their failure does not permit replaying these mutations.
In particular, blindly replaying 1021 can append another event and increment
the counter again. Use a durable operation marker when automatic replay is needed.

### Prefix Page With Marker

```cpp
auto listing_key = [](std::string_view bucket_id, std::string_view object_name) {
  return fdbc::keyspace("example") / "listing" / bucket_id / object_name;
};

const auto listing = fdbc::keyspace("example") / "listing" / bucket_id;

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
const auto objects = fdbc::keyspace("example") / "object" / bucket_id;

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

lfdb::commit(txn).value();
```

The snapshot scan avoids broad read conflicts, while the explicit conflict range
keeps the narrow correctness dependency visible. This assumes every writer of
the version records also changes `head` in the same transaction; otherwise,
read those records serializably or mark their range as a conflict dependency.

### Maintenance Pass

```cpp
const auto cache = fdbc::keyspace("example") / "cache" / bucket_id;
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
      lfdb::set(txn, "example/archive/" + key, metadata);
      lfdb::erase(txn, key);
    }
  }).value();
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

At leaf-operation boundaries, library-generated failures, including rejected
arguments and invalid operation states, use `lfdb::libfdb_exception`. Manual
commit and Transactor invocation instead return their structured completion
results; their error records retain native category/code and commit knowledge. Its error code distinguishes native errors,
library-specific failures, and standard conditions such as
`std::errc::invalid_argument` or `std::errc::value_too_large`; callers need one
exception handler while retaining the failure's category, code, and context.

Code that caught `std::invalid_argument` or `std::length_error` for a libfdb
validation failure should now catch `lfdb::libfdb_exception` and inspect `code()`
when it needs that distinction.

Exceptions thrown by application serializers, converters, callbacks, or native
continuations retain their original type and identity. Standard allocation
failures also propagate normally at leaf boundaries; a Transactor captures them
in its result. The generic interval core remains independent
of FoundationDB; the libfdb query boundary translates its fixed-capacity failure
into `libfdb_exception`.

| Condition | Exception |
| --- | --- |
| FoundationDB C API reports an operational failure | Leaf operations throw `libfdb_exception`; transaction completion returns structured native failure |
| FoundationDB option setup fails | `lfdb::libfdb_exception` |
| Retry recovery fails or retry limit is exceeded | Transactor result retains the original failure and separate recovery code |
| zpp_bits returns an encoding or decoding failure | `lfdb::libfdb_exception` with the original standard error code and category |
| A user serializer, converter, or callback throws | Original exception at leaf boundaries; captured exception_ptr in a Transactor result, with identity preserved |
| Invalid result at the FoundationDB boundary | `lfdb::libfdb_exception` |
| Caller uses libfdb after shutdown | `lfdb::libfdb_exception` |
| Caller passes an impossible selector prefix or invalid versionstamp bytes | `lfdb::libfdb_exception` with `std::errc::invalid_argument` |
| Caller accesses a pending stamp, reuses a stamp holding bytes, registers an output with two transactions, overwrites immutable bytes, or asks for a committed version before commit | `lfdb::libfdb_exception` with `std::errc::invalid_argument` |
| Stamp completed with an error | `resolved_bytes()` throws `lfdb::libfdb_exception`; `resolved_bytes(with_result)` returns the recorded error |

### Error Codes and Exceptions

`lfdb::libfdb_exception` derives from `std::system_error`, exposing one
authoritative error through `code()` and diagnostic text through `what()`.
Construction requires an explicit error code, with optional diagnostic context.
Use `lfdb::libfdb_error_code::operation_failed` for a general local failure;
preserve the original code when reporting a native failure.

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
exception does not make it an internal retry request. An owning Transactor applies
its policy to native `libfdb_exception` errors escaping its body and to native
causes propagated in `transaction_failure<E>`. Ordinary expected body errors reject
application work; returned library failures preserve their existing classification.
Other user exceptions are captured with identity in owning and borrowed results.
Exceptions from leaf helpers still propagate to their own caller.

The conversion boundary inspects the archive's returned status directly.
It does not catch and reconstruct exceptions from `what()`. Exceptions thrown
by user code retain their identity even when they derive from
`std::system_error`. Serializer status codes retain their standard category;
FoundationDB retry predicates apply only to codes in the native category.
