# Developer’s guide

Datum helps turn byte handling and handler state into operations with explicit contracts.
For a port, first record the existing ordering, wire format, quota, lifetime, error, and
publication rules. Then choose vocabulary and operations that make those rules visible.
These ideas appear particularly well suited to Datum in Ceph, RGW (RADOS Gateway),
object classes, and SALAD. They are design tools, not a straitjacket:
use a direct loop or a different architecture when it expresses the contract better.

## Start with meaning

Distinguish an object name from its encoded storage key, encoded bytes from a decoded record,
and a selection boundary from a continuation token. The same string type may represent several
roles; give them clear names before introducing a new type. Existing key encodings and wire
formats remain contracts. Datum vocabulary describes their use without changing their bytes.

Start with supplied operations: `byte_extent` and `byte_mapping` for byte geometry,
`key::byte_query` for string-key selections, and `order_by` with field projections for ordering.
Use standard Ranges algorithms and views where their evaluation and lifetime rules fit.
Keep Ceph buffers in the maintained `<ceph-datum.h>` vocabulary and native records, codecs,
errno translation, reshard accounting, and persistence in native bindings.
See the [layer map](design.md#architectural-guidelines) and
[Ceph vocabulary lesson](../tutorial.md#18-bytes-with-a-ceph-accent).

## Decode once, then use the value

A validated record is useful input. Pass it to checking, accounting, and selection operations
rather than passing encoded bytes and decoding them again. Paired writes can similarly share
one encoding while retaining their separate storage effects and transaction requirements.

A lazy transformation does not cache each decoded value. In this fragment, `decode` is a pure
application function returning `std::expected<unsigned, std::errc>`:

```cpp
auto lazy = encoded | std::views::transform(decode);
auto current = std::ranges::begin(lazy); // Assume a nonempty source.
auto first = *current;
auto again = *current;                  // Invokes decode again at the same position.
```

The standard specifies invocation of the transformation on each
[iterator dereference](https://eel.is/c++draft/range.transform.iterator).
In an expected-returning function, materialize the result once, then reuse it:

```cpp
auto decoded = *current;
if (!decoded) {
    return std::unexpected(decoded.error());
}
const unsigned value = *decoded;        // Use this validated value in subsequent operations.
```

This guarantees one transformation for that visit, not for future passes or retries. For a
streaming handler, keep the value local while processing that row. If several passes need it,
retain decoded records explicitly. Materializing a complete page uses storage and may decode
rows beyond the reply quota; choose that behavior deliberately. Keep fetching and transaction
assumptions visible at the native boundary rather than hiding them in iterator operations.

## Own what must survive

Borrow for synchronous calls whose references do not escape. Retain an owner when a callback,
asynchronous completion, or result outlives the call. A `std::span` or `std::string_view` provides
access without extending the backing storage’s lifetime. Owning a view descriptor need not own
its payload. Datum’s `owning_value` contract requires truthful enrollment for custom types.

With `<ceph-datum.h>`, `datum::slice(bufferlist, extent)` retains native segment references;
`datum::copy(bufferlist, extent)` creates independent payload bytes. Retention follows the
buffer’s native lifetime rules and does not imply immutability or isolation. Externally backed
buffers keep their external lifetime obligations. Capture owned request or completion state
when work escapes, and return owned continuation state before page storage disappears.

## Specify progress separately from output

`visit_range` and `traversal::scan_range` distinguish an offered row, consumption, acceptance,
and stopping. Each decision contributes:

| Decision | Offered | Consumed | Accepted | Stops |
| --- | --- | --- | --- | --- |
| `accept` | 1 | 1 | 1 | No |
| `skip` | 1 | 1 | 0 | No |
| `accept_and_stop` | 1 | 1 | 1 | Yes |
| `reject_and_stop` | 1 | 0 | 0 | Yes |

Acceptance has the meaning chosen by the consumer. A native GC (garbage collection) listing
may charge a row against its quota without returning an entry. A removal lookahead can supply
a boundary while remaining unconsumed. Neither the number returned nor a quota counter can
automatically replace consumption. `visit_result` reports `offered`, `accepted`, and the
optional `stop`; `consumed()` and `stopped()` derive from those facts.

`scan_range` saves `positions.position(row)` before invoking the consumer, which may move the
row. On stop, `positions.next(saved, consumed)` constructs the continuation. An ordered scan
usually resumes exclusively after a consumed key and inclusively at a rejected key. The profile
must establish that this matches the backend’s order and replay rules. An owned token provides
neither a snapshot nor durable acknowledgement. See [Keep your place](../tutorial.md#10-keep-your-place).

## Decide failure and publication together

The [complete example](examples/developer_flow.cpp) decodes a toy page of unsigned decimal
values, prepares an owned reply, and only then prints it. Its application function
`prepare_reply(encoded, maximum)` returns `std::expected<page_reply, std::errc>`.
Successful preparation decodes each row once into owned records before `scan_range` applies the quota.
Zero values are skipped, and a full reply rejects the next offered row.
The program limits `{"0", "7", "9"}` to one reply entry, then rejects a malformed page.
Its [expected output](expected/developer_flow.txt) is:

```text
returned: 7
offered: 3, consumed: 2, accepted: 1
resume index: 2
malformed page: no reply published
```

The index refers to this unchanged toy source; a production binding supplies its own owned key
or opaque token. Malformed input anywhere in the page returns an error with no reply or partial
progress, even beyond the quota. Local staged values are discarded. Allocation can still throw.
This is a deliberate whole-page validation contract, not a model for every native listing.
Publication and storage effects stay outside preparation; `std::expected` itself supplies no
rollback. Sharing one encoding across writes likewise does not make the writes atomic.

**Available interfaces:** in this Datum checkout, `visit_range` and `scan_range` require an
`acceptance` callback result; callback exceptions propagate without a partial result.
`traversal::for_each_page` already accepts an expected-returning **reader**: read failure reports
the error, the attempted page’s resume state, and the count of previously visited pages.
Its visitor returns `void` or `page_action`; visitor exceptions propagate. Row consumption,
page progress, safe retry, and transaction commit therefore remain separate contracts.
Fallible visitation with error/progress results is a candidate in the object-class integration branch;
the example here does not depend on it.

Build the example in the ordinary [tutorial build](build-and-run.md):

```sh
cmake --build build --target developer_flow --parallel 8
ctest --test-dir build -R '^developer_flow$' --output-on-failure
```

## Choose the useful programming ideas

The mechanisms above suggest **value-oriented generic programming with a pure core and explicit
effect boundaries** as a useful description of Datum’s style. This is an interpretation of the
code, not a required paradigm or a claim that all callbacks are pure.

| Approach | Relationship to Datum | Practical consequence |
| --- | --- | --- |
| Data-oriented programming | [Sharvit’s principles](https://blog.klipse.tech/databook/2022/06/22/immutable-data.html) separate data and behavior, use generic data structures, require immutability, and separate schema from representation. Datum shares useful separation, but also uses concrete static types, writable views, and controlled mutation. | Pass useful data between operations; immutable maps or persistent containers are optional design choices. |
| Generic programming | [Musser and Stepanov](https://stepanovpapers.com/genprog.pdf) derive reusable algorithms from concrete algorithms and the operations their representations need. Datum’s ordered domains, ranges, projections, and callbacks follow this pattern. | Supply the smallest truthful contract; reuse standard algorithms before adding protocols. |
| Functional core, imperative shell | [Bernhardt’s architecture](https://www.destroyallsoftware.com/screencasts/catalog/functional-core-imperative-shell) separates value calculations from external effects. Datum’s interval algebra fits the core; native handlers arrange decoding, storage, transactions, and asynchronous work. | Test decisions with ordinary values, and keep publication explicit. A generic traversal can still run effectful callbacks. |
| Hardware-oriented data-oriented design | [Fabian’s account](https://www.dataorienteddesign.com/dodbook/node2.html) designs transformations around actual data and target hardware. Datum does not prescribe layout, batching, or cache behavior. | Decode-once and encoding reuse remove repeated work; layout and throughput still need workload-specific measurement. |

For a new shared operation, trace its producers and consumers and question every required
method, flag, mutable reference, and cached field. Recurring domain-independent protocols
deserve strong reusable core machinery when they simplify real callers; low-level composition
alone does not establish that the shared interface is sufficient. Keep that machinery pure of
native dependencies and policy. Validate ordering, quota boundaries, errors, lifetimes, and
publication against actual consumers and wire tests before calling a port complete.
