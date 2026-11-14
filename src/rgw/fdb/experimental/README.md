# Experimental Concurrency

This extension is experimental. Its interfaces and native backend types may
change. Core libfdb headers do not include it. Select exactly one backend:

| Selection | Behavior |
| --- | --- |
| `OFF` | Default; no experimental interface target or stdexec dependency |
| `NVIDIA_STDEXEC` | NVIDIA stdexec senders, composition, and transaction bodies |
| `ASIO` | Deferred Asio operations and asynchronous transaction bodies; no stdexec dependency |

Enable FoundationDB with `WITH_RADOSGW_FDB=ON`, set
`LIBFDB_EXPERIMENTAL_CONCURRENCY`, and link `rgw_fdb_execution`. Include
`rgw/fdb/experimental/concurrency.h`; the older `rgw/fdb/execution.h` include
forwards here. The former `EXPERIMENTAL_NVIDIA_STDEXEC` boolean is replaced by
this selector. Selecting a backend with FoundationDB disabled is an error.

The extension currently requires C++23; its sender model follows C++26
execution conventions through the pinned stdexec implementation. Maintaining
direct stdexec composition is a design constraint. Asio adapter requirements
do not narrow the stdexec sender interface. Both backends use the same operation
factories and FoundationDB request transitions, including commit and replay
preparation. Execution, cancellation delivery, and native composition remain
backend-specific.

NVIDIA transaction bodies can return native `stdexec::task<T>` coroutines.
Supply their application scheduler through ordinary execution composition,
such as `stdexec::starts_on(scheduler, transact(...))`. Dependent body completion
queries retain the receiver's actual environment. The body can await individual
operations or `when_all()` groups, and its owning result is published after commit.
The Ceph terminal bridge alone does not supply a coroutine's start scheduler.

## Operations

Both backends provide the following operations in
`ceph::libfdb::experimental`:

| Operation | Successful result |
| --- | --- |
| `get(raw, txn, key[, mode])` | `point_read_result` |
| `read_window(raw, txn, cursor[, mode])` | `scan_window` |
| `for_each(raw, txn, selection[, scan_options], callback)` | Number of delivered rows |
| `for_each(raw, dbh, [transaction_options,] selection[, scan_options], callback)` | The same count, with managed windows |
| `read_version(txn)` | `std::int64_t` read version |
| `approximate_commit_bytes(txn)` | `std::int64_t` pending transaction size |
| `approximate_range_size(txn, selection)` | `std::int64_t` estimated stored bytes |
| `partitions(txn, selection, target_bytes)` | Owning `std::vector<select>` |
| `when_changed(watch)` | No values |
| `reset_for_replay(txn, error)` | no value |
| `commit(txn)` | no success payload |
| `commit(with_result, txn)` | `std::expected<void, std::error_code>` |
| `transact(dbh, [options,] body, args...)` | The body's owning successful values, after commit |
| `transact(retry_if(policy), dbh, [options,] body, args...)` | The same values, with an application replay decision |

Reads, scans, metadata requests, commits, and transaction bodies start only on
initiation. Creating or extracting an operation contacts no server.
`when_changed()` adopts a watch that has already been created; that underlying
watch may already be active.

Disjoint range estimates and split-point requests keep at most four native
futures outstanding. Each consumed result immediately refills its slot;
results are consumed in interval order, and partition output retains query
options and reverse ordering. A slow front request can still delay refill.

Point results and scan windows own the FoundationDB storage behind their byte
views. Keep the result alive and unmoved while using its views. Consuming a
window with `next()` also releases its rows. A missing key is a successful
point result with an empty `bytes()`, rather than an operation failure.
Point and window reads default to `read_mode::serializable`. Pass
`read_mode::snapshot` as the final argument to omit native read conflicts,
just as with synchronous reads. The chosen mode belongs to that read operation;
subsequent windows may choose their mode explicitly.
Experimental windows reuse the ordinary scan result owner and extraction.
Both paths copy unaligned row metadata into aligned storage; key and value
bytes continue to use FoundationDB's storage without copying. Aligned metadata
needs no extra allocation; unaligned metadata uses overwrite storage without
initializing records that are immediately copied.

Point operation descriptions own their keys. Borrowed keys and string lvalues
are copied once; passing an owned `std::string` rvalue transfers its buffer.
Transaction options are likewise copied from lvalues or transferred from
rvalues, then moved into the transaction that retains them for replay.
The ordered option lists preserve repeated codes and insertion
order in both backends. **This replaces the former map API (application programming
interface) and changes the application order of existing initializers**; see the
[options migration guide](../EXAMPLES.md#options).

Metadata operations retain their transaction and own their selection.
Transaction and target byte size validation and FoundationDB requests occur on
initiation. Range estimates sum canonical, nonoverlapping intervals in batches
of at most four outstanding futures. An empty selection yields zero bytes or
no partitions without a database request. Partition planning preserves
selection bounds, options, and traversal direction; `target_bytes` must be
positive. Estimates and split points are approximate. Planning returns
ordinary owning selectors and leaves scheduling to the caller. These
operations take an existing transaction; return one from `transact()` when
managed replay is needed.

`commit()` submits once without retry preparation. Ordinary commit has no success
payload and reports native failures through the existing error channel. Tagged
commit returns every native operational failure as an error value. Local failures
and contract violations retain the error channel and use `libfdb_exception`. Both
forms eagerly complete
requested version stamps independently: stamp failure after a confirmed commit
does not change successful commit disposition.

For manual replay, decide whether replay is appropriate, call
`lfdbx::reset_for_replay(txn, error)`, await its completion, then rebuild the
transaction body. It owns the handle, starts lazily, validates the complete
nonzero native error code before changing state, and uses the same recovery
mechanism as `transact()`. Once started, preparation runs to its actual outcome;
cancellation cannot leave an uncertain partially prepared transaction. The
synchronous spelling is `lfdb::reset_for_replay()`. `transact()` manages
native asynchronous preparation, backoff, restoration of options, and replay.
It prepares fresh staged state for each attempt and publishes body values and
staged outputs only after confirmed commit. Application errors retain their
original exception types; failed preparation preserves its actual error.

Automatic replay uses FoundationDB's transaction retry limit and timeout, with
no additional libfdb attempt limit. These options apply across attempts on the
same transaction. Unlimited native settings can allow indefinite replay; a
retry limit of zero permits the initial attempt without replay.

An optional `lfdb::retry_if(callable)` selector receives
`transaction_failure {code, phase}` before recovery, including terminal native
errors for observation. `transaction_phase::body` and `transaction_phase::commit`
identify the failure boundary. Return false to preserve the original error
without preparation, or true to request native preparation and replay. Native
eligibility, budgets, cancellation, and confirmed commits still constrain replay.
The ordinary default includes native-eligible 1021 failures.

The selector owns its callable and reuses it across attempts; mutable and
move-only callables work, and `std::ref` explicitly borrows application state.
Counts and history belong to that callable or the application. Local/user
failures bypass native policy; policy exceptions retain their identity and do
not re-enter classification. A failed preparation reports its actual error and
does not invoke policy again. The decision runs synchronously on the existing
completion context; keep it short and compose asynchronous reconciliation after
propagation rather than blocking a FoundationDB callback.

An explicit native-category `libfdb_exception` escaping the transaction body
denotes a native failure. Use an application exception type to reject application
work. Exceptions from serializers, converters, and helper callbacks retain
their identity and bypass native recovery at those operation boundaries.

After commit submission, cancellation cannot prove rollback. Both backends
preserve actual commit disposition and maybe-committed errors. An error after
submission does not establish that the database was unchanged. Staged local
output does not make non-idempotent database operations safe to replay after
an uncertain commit.

See the [manual recovery examples](../EXAMPLES.md#transactions),
[durable operation marker](../EXAMPLES.md#resolving-an-uncertain-commit), and
[independent stamp reporting](../EXAMPLES.md#versioned-data) for complete
transaction bodies and their recovery assumptions.

## Common Ceph Boundary

Include `rgw/ceph_fdb_execution.h` to use `wait(operation, optional_yield)` or
`async_wait(operation, token)`. A yield context suspends its coroutine;
`null_yield` blocks the caller. These adapters preserve original exceptions.

This complete operation sequence works under either backend:

```cpp
namespace lfdb = ceph::libfdb;
namespace lfdbx = lfdb::experimental;

lfdb::transaction_options options {
  {FDB_TR_OPTION_TAG, "metadata"},
  {FDB_TR_OPTION_TAG, "object"}
};
auto work = lfdbx::transact(
  dbh, std::move(options), [object_key](lfdb::transaction_handle& txn) {
    return lfdbx::get(lfdb::raw, txn, object_key);
  });

auto object = lfdbx::wait(std::move(work), y);
if (const auto bytes = object.bytes()) {
  consume_object(*bytes);
}
```

The metadata operations use the same boundary under either backend:

```cpp
auto selection = lfdb::query::prefix(object_key);
auto version = lfdbx::wait(lfdbx::read_version(txn), y);
auto pending_bytes = lfdbx::wait(lfdbx::approximate_commit_bytes(txn), y);
auto stored_bytes = lfdbx::wait(lfdbx::approximate_range_size(txn, selection), y);
auto ranges = lfdbx::wait(lfdbx::partitions(txn, selection, 1 << 20), y);
```

Operation factories do not create application worker threads. Applications
choose their execution contexts and concurrency limits. Serialize operations
that share a transaction's mutable state.

## Asynchronous Traversal

Experimental `for_each(raw, ...)` suspends on FoundationDB reads. It owns its
selection, callback, and handles until terminal completion, and reuses the
ordinary query cursor and result extraction. The callback receives key and
value byte spans. It can return `void` or native asynchronous work: a sender in
NVIDIA mode, or an `operation` with the documented Asio completion shape in
ASIO mode. Each row remains valid until that work completes. Callback results
are discarded, and the next row starts only after successful completion.
Use `std::ref(callback)` when intentionally borrowing a longer-lived callback.

This synchronous consumer works with either backend:

```cpp
auto count = lfdbx::wait(lfdbx::for_each(lfdb::raw, dbh,
  lfdb::query::prefix(object_key), [](auto key, auto value) {
    consume_object_row(key, value);
  }), y);
```

The transaction form uses the caller's transaction and leaves recovery to the
caller. The database form creates and retries a transaction for each window
before exposing any of its rows. Delivered callbacks are never retried by the
traversal. Later managed windows may observe newer versions, as with ordinary
managed scans. Neither form commits the read transaction. Empty selections
complete with zero rows and no database request. An error or stop ends traversal;
already-delivered callback effects remain visible.

`scan_options` controls read mode and optional single-window lookahead:

```cpp
auto work = lfdbx::for_each(lfdb::raw, txn, selection,
  lfdbx::scan_options {.mode = lfdb::read_mode::snapshot, .prefetch = true},
  [](auto key, auto value) { consume_object_row(key, value); });
```

The default consumes each window before starting the next read. With prefetch,
one next-window read overlaps consumption of the current window. At most two
windows are retained, and downstream work provides backpressure. Query result
limits and byte targets still apply; a byte target is not a hard memory bound.
The current owner remains alive until its last callback completes. On failure,
both branches finish before traversal releases their owners.

Prefetch changes when later reads are issued. Use it only when the consumer
does not require mutations or external side effects to precede those reads.
In particular, writes through an explicit transaction between rows must not
rely on being visible to a prefetched window. Independent managed transactions
do not share one fixed read version. Existing synchronous scans, pagination,
and streaming-mode defaults retain their contracts.

Native stdexec callbacks retain native composition. Transfer expensive work to
the application's scheduler, for example:

```cpp
auto work = lfdbx::for_each(lfdb::raw, txn, selection,
  lfdbx::scan_options {.prefetch = true}, [scheduler](auto key, auto value) {
    return stdexec::schedule(scheduler) | stdexec::then([key, value] {
      consume_object_row(key, value);
    });
  });
```

Supply the appropriate scheduler environment for coroutine callbacks returning
`stdexec::task`. Asio traversal preserves the associated executor, allocator,
and cancellation slot for its read and consumer operations. No traversal
operation creates an application worker pool.

## Native Access

`operation<NativeT>` retains the backend's actual type as `native_handle_type`.
`native_handle()` on an lvalue borrows that object; the owner must remain alive
and unmoved. On an rvalue it extracts the native object by value and transfers
ownership. Access through a const temporary is rejected. Access itself starts
no work. A native execution object is distinct from a FoundationDB raw pointer;
a deferred read or commit has no FoundationDB future to expose before it starts.

In NVIDIA mode the public operation itself models a sender. Completion
signatures, receiver connection, and environment queries are forwarded with
their native types; ordinary composition requires no extraction:

```cpp
namespace ex = stdexec;
auto work = ex::when_all(
  lfdbx::get(lfdb::raw, txn, metadata_key),
  lfdbx::get(lfdb::raw, txn, policy_key));
```

Metadata results also compose directly, without an adapter-specific result
envelope:

```cpp
auto metadata = stdexec::when_all(
  lfdbx::read_version(txn), lfdbx::approximate_commit_bytes(txn)) |
  stdexec::then([](std::int64_t version, std::int64_t pending_bytes) {
    return std::pair {version, pending_bytes};
  });
```

Native extraction and wrapping can be used for additional backend facilities:

```cpp
auto operation = lfdbx::get(lfdb::raw, txn, std::move(object_key));
using native_type = decltype(operation)::native_handle_type;
native_type native = std::move(operation).native_handle();
auto wrapped = lfdbx::operation {std::move(native)};
```

In ASIO mode the native object is an Asio deferred asynchronous operation.
Composition tokens and executors stay at this adapter boundary. For example,
convert an owning point result into a byte count using native deferred
composition, then return to the ordinary Ceph wait boundary:

```cpp
namespace asio = boost::asio;
auto native = lfdbx::get(lfdb::raw, txn, object_key).native_handle();
auto count = std::move(native) | asio::deferred(
  [](std::exception_ptr error, std::optional<lfdbx::point_read_result> result) {
    std::optional<std::size_t> size;
    if (result) {
      const auto bytes = result->bytes();
      size.emplace(bytes ? bytes->size() : 0);
    }

    return asio::deferred.values(std::move(error), std::move(size));
  });

auto size = lfdbx::wait(lfdbx::operation {std::move(count)}, y);
```

ASIO native bodies wrapped in `operation` must have completion signature
`void(std::exception_ptr, std::optional<ValueT>)`, with owning, nothrow-movable
values. No values use `std::tuple<>`; several values can use a tuple. These
adapter requirements do not apply to native stdexec composition or its error
types. Standard/stdexec algorithms do not directly accept ASIO operation
objects; no standard sender-conformance claim is made for that backend.

## Cancellation and Delivery

NVIDIA mode uses the receiver's stop token. Read, scan, metadata, and watch
cancellation is supported; submitted commits retain their outcome. Its Ceph
terminal adapter accepts one successful completion shape and an
`std::exception_ptr` error channel. Untransferred sender continuations may run
on FoundationDB's network thread; transfer application work to its chosen
scheduler before blocking work.

ASIO mode delivers completions through the handler's associated executor and
allocator and tracks outstanding work. Native operations support `terminal`
and `partial` cancellation. Reads can install conflict ranges, so `total`
cancellation is not promised. Cancellation requests before initiation are not
remembered. A request after initiation can stop an unstarted transaction body
or its outstanding read/watch; a submitted commit is protected. Cancellation
can lose a race with completion and has no bounded-latency guarantee. Keep the
execution context alive through pending operations. Executor destruction may
discard queued completion, releasing its owned state. Asio leaf operations
allocate stop state only for cancellable requests with a connected slot;
transactions connect child cancellation only when the caller connects a slot.
Cancellation setup failure releases uninitiated state. If a terminal post
cannot allocate, the adapter reports that exception directly through the
completion handler; this fallback may execute outside the associated executor.

## Validation

Build and run `unittest_fdb_execution` for `NVIDIA_STDEXEC`, or
`unittest_fdb_asio` for `ASIO`. The live tests require a reachable FoundationDB
cluster. They use process-specific key prefixes and clean up their own data.
Both suites include the shared concurrency contract tests. Tests cover native
access, result ownership, replay, staged publication,
exceptions, cancellation, and commit disposition. Shared metadata tests cover
lazy scalar reads, estimates spanning several batches, empty selections, and
partition coverage in both directions. Shared options tests cover copied and
moved snapshots, repeated entries, lazy initiation, replay, and native errors,
including deferred option validation. NVIDIA tests also verify native metadata
completion signatures and direct stdexec composition.

Run backend configurations sequentially, with an explicit job limit:

```sh
cmake --build build --parallel 8 --target unittest_fdb_asio
./build/bin/unittest_fdb_asio
```

Focused sanitizer validation with Clang 19.1.7 and libstdc++ 14.3.1 builds both
suites using `-O1 -g -fsanitize=address,undefined,vptr -fno-omit-frame-pointer
-fno-sanitize-recover=all` for compilation and linking. No UBSan (UndefinedBehaviorSanitizer) checks need to
be excluded. GCC 14.3.1 accepts address instrumentation, but its full UBSan
instrumentation rejects constexpr type comparisons in the pinned stdexec
dependency. The failure reproduces without libfdb or stdexec headers; disabling
only `vptr` does not resolve it. Use Clang for this combined sanitizer coverage.

The installed native client and other prebuilt dependencies remain
uninstrumented. With FoundationDB 7.3.71, leak detection reports a 512-byte native
allocation even in a minimal raw C client with orderly network shutdown;
validation with `ASAN_OPTIONS=detect_leaks=0` makes no leak-free claim. The full
NVIDIA suite also emits a stack-switching warning from ASan (AddressSanitizer). Focused replay lifetime
and stamp-boundary checks pass without that warning, as does the ASIO suite.


## Focused Performance Comparison

`bench_fdb_concurrency` is an opt-in executable, excluded from ordinary builds.
Build it in Release mode under each enabled backend, then run the same workload,
queue depth and operation count. No Ceph Object Gateway daemon or Ceph cluster is required; it
uses the FoundationDB C client and a reachable FoundationDB cluster. Run builds
and measurements sequentially.

```sh
cmake --build build-asio --parallel 8 --target bench_fdb_concurrency
./build-asio/bin/bench_fdb_concurrency point_owned_ryw 8 100000 5
cmake --build build-stdexec --parallel 8 --target bench_fdb_concurrency
./build-stdexec/bin/bench_fdb_concurrency point_owned_ryw 8 100000 5
```

The harness warms up first and reuses one executor throughout each sample.
Both modes use `async_wait` at the same terminal boundary, one executor thread,
and the same data sizes. CSV (comma-separated values) output includes operations/second,
p50/p95/p99 latency, and process CPU (central processing unit) time in microseconds
per operation, including the FoundationDB network thread. Each operation's latency
includes initiation and terminal delivery.
No per-operation blocking context is created.

| Workload | Work measured |
| --- | --- |
| `point_owned_ryw` | Ready read-your-writes point read with an owned string key |
| `point_borrowed_ryw` | Same read with a borrowed key copied into the description |
| `scan_ryw` | One 64-row read-your-writes window, including metadata extraction |
| `point_live` | Point read using a fresh transaction for every operation |
| `commit` | Fresh transaction writing a separate key per lane, then commit |
| `transact` | Asynchronous write/read body and confirmed commit per operation |

Depth is 1..64; operation count and optional repeat count must be positive.
Use shorter counts for live I/O. The harness uses process-specific key prefixes
and removes them on normal exit. Transactions in different lanes write distinct
keys; these workloads do not characterize contention or replay throughput.
Compare repeated samples in alternating backend order on the same cluster;
report variation and distinguish client overhead from server latency. The two
backends retain different internal scheduling policies, so this benchmark does
not establish equivalence of arbitrary native compositions or schedulers.
