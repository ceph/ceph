# Datum

Datum is a powerful foundation for building clear, composable data-handling code in Ceph.
It provides shared operations for recurring jobs: gathering fragmented payloads, selecting byte
ranges, ordering records, and choosing fields for encoding or diagnostics.

Its standard-library core brings together byte geometry, interval queries, record projections,
comparisons, and traversal. These operations compose naturally with C++ Ranges, while domain
vocabularies connect them to native Ceph buffers and wire formats. Views, projections, and
explicit ownership help components pass data between operations and avoid unnecessary copies
while retaining control of I/O, transactions, and storage policy.

Read the [tutorial](tutorials/tutorial.md) for small, runnable examples, or use the
[documentation overview](../../../doc/dev/radosgw/datum/index.rst) to find the
tutorial, design, build instructions, and index together.

Examples abbreviate the library namespace with `namespace datum = ceph::salad::datum;`
and use the standard headers for the storage and algorithms shown.

## Standard range operations

Prefer standard views and Ranges algorithms for selecting, transforming, and moving
sequences. Include `<datum/range.h>` to append an input range to a growable destination:

```cpp
const std::array fragments {std::string_view {"pay"}, std::string_view {"load"}};
std::string contiguous;
datum::append_range(contiguous, fragments | std::views::join);
```

`append_range(destination, source)` appends in source order and returns the destination
by reference. Native C++23 `append_range` members are used when available; the fallback
supports standard `push_back` sequences, including strings, vectors, deques, and lists.
A custom destination needs just `push_back(value)`; it needs no iterator,
`value_type` alias, default construction, or movement.
Input ranges need only one traversal. The fallback preserves the container's usual
amortized growth rather than reserving an exact size on every append. An rvalue range
does not implicitly move its elements: use `source | std::views::as_rvalue` when
consuming their ownership is intended.

The source must not overlap the destination, except that strings retain their standard
support for overlapping character sources. String fallback staging uses the destination's
allocator. Failure effects follow the destination operation; elementwise insertion can
leave an appended prefix if a later element operation throws. The operation does not
roll back arbitrary container or user-defined member effects.

Other core operations already accept standard ranges under their established names:

| Task | Range interface |
| --- | --- |
| Copy a complete byte source | `copy(source, output)` |
| Copy selected contiguous or fragmented bytes | `copy(source, byte_extent, output)` |
| Scatter contiguous bytes into writable fragments | `scatter_bytes(source, fragments)` |
| Read the next bytes into ordinary storage | `byte_cursor::read_into(output)` |
| Send fragments to a synchronous consumer | `write_fragments(fragments, sink)` |
| Compose a dynamic sequence of key segments | `key::compose_range(segments)` |
| Append interval entries to bounded storage | `static_interval_set::append_range`, also through `interval_batch` |
| Normalize, query, or clip intervals | `compile_intervals`, `canonical_range`, and `clip_view` |
| Count or take selected coverage | `interval::measure(range, metric)`, `interval::take(range, quota, metric)` |
| Visit data and preserve continuation | `traversal::scan_range`, `traversal::for_each_page` |

Use the standard algorithms directly with Datum projections and comparisons.
Heterogeneous record profiles and comparison policies retain their typed argument packs;
a runtime sequence cannot substitute for the different member and policy types.

## Bytes, geometry, and standard Ranges

Common byte operations need no application domain declarations. Include `<datum/bytes.h>`:

```cpp
const std::string_view source {"Miso: meow"};
std::array<char, 16> output {};
const auto written = datum::copy(source, output); // Ten bytes; the tail stays unchanged.
datum::copy(source, 6, std::span {output}.first(4)); // Four bytes starting at offset six.
datum::copy(source, datum::byte_extent {6, 3}, output); // Write three bytes into the prefix.
```

`source` may be a contiguous byte range or a forward range of byte blocks. `output`
is writable contiguous storage, such as an array, vector, or span. The returned span
covers the bytes written and borrows the output storage; it does not keep that storage
alive. Both capacities are checked before writing; invalid source
bounds throw `std::out_of_range`, and insufficient output throws `std::length_error`.
Source and output must not overlap. Temporary owning output containers are rejected.
Synchronous copying accepts temporary source owners because it retains no references.
With just the source and output arguments, the complete source is copied and a larger
output keeps its unused tail unchanged. Array size is the byte count, so a string literal
includes its terminating NUL; use `std::string_view` to select text without that terminator.

`fragments(source)` exposes a contiguous source as a one-fragment view containing a standard span. It preserves
element constness and borrows the bytes. Pass a named owner or a borrowed range such as
`std::span`; temporary owning containers are rejected. Keep the fragment view alive while
using its iterators, and keep the backing storage alive while using an extracted span.

`byte_extent {offset, length}` describes a half-open range of bytes. `end_offset` checks
endpoint overflow; `subextent` selects relative to a source; `intersection` returns
an optional nonempty overlap. `byte_mapping {{offset, length}, backing_offset}` keeps
logical and backing positions together. Its `subextent` and `clip` advance both
positions consistently and check both endpoints. These values establish geometry,
not storage ownership or permission to access an object.

`interval::byte_query` supplies standard unsigned byte positions. Use
`interval::bounds(byte_extent)` to prepare a selection. A `clip_view` projection may
return a byte extent directly, including through a member pointer such as
`&byte_mapping::logical`; custom ordered domains remain available for other units.
For counting and limiting covered bytes, use the supplied `interval::bytes` metric with
`interval::measure` and `interval::take`. It provides checked unsigned arithmetic without
an application metric declaration; gaps contribute no bytes.

Byte traversal and clipping accept named standard views that maintain iteration
caches, including `filter_view`. Read cursors expose const spans; clipping exposes
const rows. Keep borrowed sources, backing storage, and any view descriptors on which their iterators depend
alive. Cursor `take` produces borrowed spans only when fragment references have
stable storage: owning blocks returned by value remain valid for synchronous
`copy` and `read_into`, but cannot escape as slices. A slice's
`byte_size()` counts bytes, whereas its Range elements are fragments. Joining spans
returned by value can be an input range; it need not be forward or contiguous.

## Byte sources and sinks

Byte operations compose through ordinary ranges; they do not discover or perform I/O.
Choose the source adapter and destination contract independently:

| Source | Fixed writable storage | Synchronous fragment sink |
| --- | --- | --- |
| Contiguous byte range | `copy(source, output)` | `write_fragments(fragments(source), sink)` |
| Forward range of byte blocks | `copy(blocks, output)` | `write_fragments(blocks, sink)` |
| Single-pass range of byte blocks | Consume incrementally in a sink | `write_fragments(blocks, sink)` |
| Native Ceph list | `copy(source, output)` through `ceph-datum.h` | `write_fragments(fragments(source), sink)` with the core included |

The fixed-storage operations accept `char`, `signed char`, `unsigned char`, and
`std::byte` independently on either side. They check capacities before writing.
Capacity checking traverses fragmented input before transferring it, so a generated
forward range must produce consistent blocks across traversals. `write_fragments`
needs only one pass and can consume generated owning blocks during each callback.

For a growable sequence with compatible element types, use
`append_range(output, blocks | std::views::join)`. A per-fragment sink can instead
normalize ordinary byte representations using the standard library:

```cpp
std::vector<std::byte> collected;
datum::write_fragments(blocks, [&](const auto& block) {
    datum::append_range(collected, std::as_bytes(std::span {block}));
});
```

Here, `blocks` is a range of contiguous byte blocks, including native spans from
`fragments(buffer)`. The spans borrow storage; `collected` owns copied bytes.
No application vocabulary or codec is needed.

`write_fragments` returns `void` and ignores sink return values. A byte-writing sink
must handle complete-block consumption, retries, and error reporting itself; a returned
count or error code does not stop traversal. An exception stops at that callback,
and earlier effects remain. Use `visit_range` when a consumer needs an explicit decision
to stop between blocks. Transport-specific partial progress and asynchronous lifetime
belong in the binding; retaining a borrowed span alone does not retain its bytes.

## Native vocabulary and comparison composition

Include `<ceph-datum.h>` and link `datum::ceph` for common Ceph buffers. The adapter
is usable in C++20 declarations without importing a codec or the newer core dialect.
Given a native `const ceph::buffer::list& buffer`, these operations need only ordinary storage
and Datum's supplied byte geometry:

```cpp
std::array<char, 4> output {};
datum::copy(buffer, 12, output);
const ceph::buffer::list independent = datum::copy(buffer, datum::byte_extent {12, 4});
const ceph::buffer::list shared = datum::slice(buffer, datum::byte_extent {12, 4});
```

`copy` into a new native list owns independent payload bytes. `slice` retains native
storage references, so a slice of Ceph-owned buffers survives clearing the source list.
It can span several native segments. Unlike `std::span`, the resulting list retains
shared payload ownership; it is an ordinary `ceph::buffer::list`, rather than a type
modeling `std::ranges::view`. Shared bytes can still change, and externally backed
buffers retain their native lifetime rules.
For C++23 core algorithms, expose the same native storage as a standard range of spans:

```cpp
const auto blocks = datum::fragments(shared);
datum::copy(blocks, output);
```

`fragments` borrows the list and bytes. Keep `shared` and the view descriptor alive while using
its iterators. A span obtained from the view needs its backing bytes to remain alive,
but no longer depends on the view descriptor. Borrowing storage is distinct from the
`std::ranges::borrowed_range` concept, which concerns iterator lifetime after the
range object is destroyed; this fragment view does not model that concept.
`read_contiguous(cursor, scratch)` borrows a single
native fragment when possible and gathers into scratch otherwise.

Ceph framing and RGW key wire profiles remain in optional `ceph/wire.h` and
`rgw/key.h` adapters. Explicit field profiles work in C++23 with
`datum::ceph` and `datum::zpp`; reflection is optional. Their codecs and language
requirements belong on implementation targets privately. Backend-specific query execution
and tests stay with their native consumer integrations.

Prefer standard tuple comparison and Ranges projections when one key describes the
order. `<datum/compare.h>` supplies composition for mixed or reusable policies:

```cpp
std::ranges::sort(entries, datum::order_by(
    datum::ascending(&entry::name), datum::descending(&entry::epoch)));
std::ranges::sort(pairs, {}, datum::nth_key<0>);
```

`order_by` supplies the Boolean predicate expected by standard sorting algorithms.
`ascending` and `descending` project each field in the requested direction. The predicate
shares the existing comparison engine and stops after the first nonequivalent key.

For a reusable three-way result, combine the same field policies with
`lexicographic(ascending(...), descending(...))`. It stops at the first nonequivalent
result, including unordered, and preserves the common standard comparison category.
`ascending` projects a key once per operand, and `reverse` exchanges operand roles.
`order_by(policy)` also adapts an existing strong or weak three-way policy, including a
composed `lexicographic` policy. Sorting needs no equality operator on records. A partial order needs an explicit
domain policy before sorting. These operations
own their callables; use `std::ref` for a synchronous borrowed mutable or noncopyable
provider. Record equality and ordering may describe different identities; do not
substitute one for the other.
For a floating-point projection, choose an explicit nonpartial policy, for example
`ascending(&entry::score, std::strong_order)`, when a total floating-point order is intended.

## Architectural guidelines

Interval algebra is Datum's mathematical foundation; data views, observation, and codecs
form separate layers above it. Native vocabulary connects those layers to Ceph types without
adding Ceph dependencies to the standard-library core.

Changes to Datum must preserve these boundaries:

1. **Separate structure, observation, ownership and authority.** Geometry and
   field profiles describe data; lenses and views observe it; owners preserve
   backing storage. Retaining storage does not establish immutability, semantic
   admission, a transaction lease or permission to publish. Consumers and their
   bindings own those guarantees, native types, reviewed wire formats and I/O.
   Concrete Ceph buffers, subsystem records and backend policy must never enter
   Datum's central mechanisms.
2. **Keep the mathematical kernel pure and the shared surface purposeful.**
   Ordered-domain algebra depends on ordering; distance belongs to metrics.
   Shared planning and traversal express data selections, progress and
   continuation facts. Transaction rules, locking, backend scheduling and
   distributed control belong in consumer bindings.
3. **Require demonstrated generality.** A central mechanism must express a
   useful data operation or invariant and demonstrate reuse across distinct
   consumer shapes, using isolated models when appropriate. Prefer composition
   of existing facilities. Keep exceptional cases in adapters, with abstract
   protocols that allow those adapters to implement their requirements. Each
   addition must earn its implementation, vocabulary and dependency costs.

| Layer | Public headers | CMake target |
| --- | --- | --- |
| Pure ordered-domain geometry | `interval/query.h`, `interval/clip.h`, `interval/metric.h` | `datum::core` |
| Native vocabulary outside core | `<ceph-datum.h>` | `datum::ceph` |
| Generic data and traversal | `extent.h`, `compare.h`, `key.h`, `bytes.h`, `range.h`, `budget.h`, `plan/query.h`, `traversal.h` | `datum::core` |
| Records, ownership and range projections | `record_definition.h`, `record.h`, `ownership.h`, `interval/range.h`, `traversal/scan.h` | `datum::core` (portable), `datum::reflection` (native C++26) |
| Generic bounded diagnostics | `render/display.h` | `datum::render` |
| Record and Datum diagnostic profiles | `render/record.h`, `render/datum.h` | `datum::render` |
| Optional codec and fmt adapters | `codec/zpp.h`, `render/fmt.h` | `datum::zpp`, `datum::fmt` |

`datum::core` requires C++23 but no reflection, concrete codec or Ceph headers.
`datum::reflection` enables optional native C++26 record reflection and adds the
GNU reflection opt-in when using GCC. Codec and
formatting targets add their own dependencies. `datum::fmt` uses Ceph's selected
`FMT_LIB`, including its system/bundled and header-only choices. Header inclusion and
record enrollment remain explicit; there is no umbrella that imports all layers.

Portable C++23 record declarations use `record_definition.h` and a field list:

```cpp
#define ENTRY_FIELDS(Field) \
    Field(int, key) \
    Field(unsigned, revision, 99)
DATUM_RECORD(entry, ENTRY_FIELDS);
#undef ENTRY_FIELDS
```

The optional third argument is a default member initializer expression. Use `Type{...}`
for braced construction and aliases for types containing commas. Ordinary aggregate
initialization can override each default; metadata and generated views do not evaluate
the source initializers. The producer declares the whole public aggregate from that list.
Use `DATUM_LOCAL_RECORD` with the same field-list form for a record declared inside a function.
Native C++26 keeps ordinary record declarations and reflection. Existing records with
constructors use explicit observations in C++23; native `view_of` also discovers their
public members.

`render/display.h` provides the bounded diagnostic engine; formatting adapters are
explicitly included. Include `render/fmt.h` and link `datum::fmt` for Ceph's selected fmt, or include
`render/std_format.h` and link `datum::std_format` for the optional standard adapter.
`DATUM_ENABLE_FMT=OFF` omits fmt setup for consumers that do not need that adapter; a
fmt-free standalone consumer also disables the combined native tests.

The portable C++23 renderer uses explicit `display_schema<T>` declarations: provide
the record's `name` and label each field with `field<Accessor>("label")`. Enum diagnostics
use an explicit `enum_schema<T>::name(value)` function. Automatic names, checked reflected
record enrollment and reflected enum tables remain available on the native C++26 path.
The bounded rendering, redaction and observation rules are shared by both paths.

`render/datum.h` supplies the same interval, query-plan and page-progress diagnostics in
portable C++23 builds. Its explicit profiles check member counts without constructing
records. Native C++26 keeps reflected enrollment and enum tables; native tests also check
the portable profiles' names, member order, reference types and complete enum tables.

For a complete C++23 record declared with `DATUM_RECORD`, include
`render/record.h`. A schema can derive from
`render::declared_display_schema<Record, 3, &Record::secret>` and supply its own `name`.
The helper checks the approved member count and redaction membership, then derives field
labels from the declaration. It requires no ownership, default construction or movement.
Selected or renamed fields, reference-member redaction and records outside that producer
use explicit schemas or projections. Native C++26 retains `reflected_display_schema`.

For a type also recognized by another formatter, select Datum diagnostics explicitly:

```cpp
auto argument = datum::render::display(value, {.bytes = 256, .depth = 8});
auto text = fmt::format("{}", argument);
```

The argument borrows an lvalue owner and copies its limits; it does not render or copy the
owner. It remains valid only while that owner does. Direct formatting remains available
for unambiguous enrolled types. Use the explicit argument for overlaps with Ceph's
`fmt_print()` protocol or automatic range formatting; no alternate protocol silently
replaces Datum's redaction. Format specifications must be empty. Diagnostic byte/depth
limits govern each displayed value rather than the surrounding log message.

Native buffer vocabulary and framing are opt-in adapters outside the pure core.
Subsystem records, backend execution, and production migrations belong in consumer integrations.

Each public entry header selects its own layer. Shared native reflection metadata lives in
`detail/record.h`; consumers do not include it directly. Record views, ordered
fields and wire descriptors share `record.h`, while `ownership.h` supplies payload
ownership and retained projections. `render/display.h` does not import geometry,
plans or page traversal; include `render/datum.h` for their diagnostic profiles.

## Geometry and data views

`key::compose(first, ...)` builds a key from fixed arguments or a tuple.
`key::compose_range(segments)` accepts an input range of string-like segments,
including generated temporary strings, and builds the same owned encoding in one pass.
The range must contain a nonempty first segment. No views into generated segments escape.

The NUL-escaped `key.h` profile rejects segments and segment prefixes that
start with `0xFF`: that byte would consume the preceding terminator and make the
encoding ambiguous. The first keyspace segment must also be nonempty. Empty later
segments, embedded NULs and interior `0xFF` bytes are supported. Admitted keys retain
their existing encoding. A binding needing unrestricted binary segments should
choose a different framing profile; byte-domain ordering and interval geometry
independently support all byte values. Previously emitted ambiguous keys cannot
be distinguished from their colliding interpretation by decoding alone.

`byte_cursor` in `bytes.h` borrows a stable lvalue fragment range. A bounded
`take()` returns a forward range of spans with independent positions, so advancing
the cursor does not change an earlier slice. Insufficient input throws before
advancement; `mark()`/`restore()` are scoped to the same live cursor. No payload
allocation or flattening occurs. The slice's byte count is `byte_size()`, distinct
from the number of range elements. Backing bytes must outlive every use.

```cpp
datum::byte_cursor input {fragments};
const auto payload = input.take(length);
datum::write_fragments(payload, [&](auto block) { output.write(block); });
```

`write_fragments()` consumes even single-pass fragment ranges through a borrowed synchronous sink;
it returns `void` after invoking the sink in source order, ignoring the sink's return values.
The binding decides whether to copy or retain the blocks. Cursor bookmarks and capacity preflight
still require stable, multipass fragments. This does not add fragment-native behavior to a
contiguous-only codec. Bulk gathering and incremental `read_into(output)` accept arrays, vectors, spans,
and other writable contiguous byte ranges directly. They share byte-copy primitives;
the bulk operation needs no cursor state.

`retained_view` in `ownership.h` stores an audited payload owner and a pure lens.
Construction takes a non-const rvalue owner; access recomputes the projection after
moves instead of retaining self-pointers. `byte_slice{offset, length}` is a checked
contiguous projection. This object stores an owner and lens, rather than an iterable
Ranges view; its `get()` operation returns the projection. The lens must derive its
borrow from the retained owner.
Backing lifetime does not establish immutability or a transaction lease; extracting
a projection directly from a temporary wrapper is rejected.
An intentional copy from a const source is explicit, for example
`retained_view{std::string{source}, lens}`. This explicit string copy may allocate or throw.
The retained view itself may be const; copying it uses the ordinary copy semantics of
its owner and lens.

The constructor's non-const owner rule applies in C++23 and C++26: `std::move` preserves
const qualification. A copy-only owner, or an owner whose move constructor copies internally,
can still copy from a non-const rvalue. An accepted call does not guarantee a cheap or
copy-free transfer.

An ordered domain supplies a copy-constructible point type and a strong-order
comparison. Point equality operators, default construction, assignment and
arithmetic are unnecessary. Domain ordering and lens purity are semantic laws;
concepts check the corresponding expressions and types. A boundary supplies domain identity, `kind()`, `key()`, and `inclusive()`.
The shared `is_finite`, `is_below_all`, and `is_above_all` predicates derive from its kind;
its key is available only when finite. Empty queries use reversed infinite bounds,
so no separate empty flag or point construction is needed.

`comparison_domain<T, Compare>` supplies the value type and a compile-time comparison object.
Its default, [`std::compare_three_way`](https://eel.is/c++draft/comparisons.three.way), retains
that object's native constraints; `ordered_domain` checks the supplied comparison's result.
Custom policies can order points without native comparison operators. Name derived profiles
`<semantic_role>_domain` to preserve distinct coordinate systems with the same representation.
`key::byte_string_domain` also supports derived profiles, retaining owning strings, borrowed
comparisons, and neutral prefix upper bounds. Backend keyspace restrictions stay with consumers.
Floating defaults return partial ordering; choose an explicit policy such as
[`std::strong_order`](https://eel.is/c++draft/cmp.alg) when its NaN and signed-zero ordering fits.

Prefix construction additionally requires `prefix_domain`: its `prefix_upper_bound()` supplies
an optional exclusive upper key. An absent upper key extends the prefix to positive infinity.
This bounds all keys with a prefix; it is separate from finding the next individual key.
For example, the byte prefix `"ab"` has upper bound `"ac"`. Ordinary interval operations and
open/closed endpoints need no stepping operation. An optional `empty_prefix()` returning `bool`
identifies the universal prefix. Both `query<D>::prefix()` and `prefix<D>()` use that policy in
the same construction path, observing the domain's prefix functions through const arguments.

```cpp
#include <datum/interval/query.h>

namespace iv = ceph::salad::datum::interval;

struct positions_domain : iv::comparison_domain<int> {};
using Q = iv::query<positions_domain>;

constexpr auto selected = iv::difference(Q::closed_open(0, 100), Q::singleton(40));
static_assert(iv::contains(selected, 39));
static_assert(!iv::contains(selected, 40));
```

`contains()` evaluates expression structure directly. Direct interval overlap
and enclosure compare cached bounds without copying points. General expression
relations normalize each side and sweep the canonical intervals. Union,
intersection and difference produce ordered, nonempty, coalesced output;
`compile_intervals()` retains that output in a `canonical_range` backed by a vector;
`compile_intervals<Capacity>()` uses bounded inline storage for an expression.

An ordinary callable receives scoped canonical intervals through
`for_each_interval()`. A sink exposing `emit_interval(lower, upper)` instead
receives scoped bounds, avoiding materialization at that boundary. A sink may
provide both; the bound interface takes precedence. Consumers must copy what
they retain. General normalization can still allocate and copy points internally.

Compound traversal over native intervals and admitted canonical sequences streams
intermediate bounds from the original leaf storage. A bound sink receives them
without intermediate allocation or endpoint copies; an ordinary callable still
receives canonical interval values. The internal cursors are input ranges, with
operation selection and composition resolved at compile time. Foreign interval
accessors keep their observation and materialization path. Explicit `evaluate()`
with a workspace retains intermediate values and enforces its work and entry
limits before publication. Default streaming traversal has no workspace or
work-budget bookkeeping.

`interval_count()` observes emitted bounds without retaining endpoint values.
Admitted canonical ranges count their elements without reading endpoint geometry;
a sized range supplies its count directly. Unsized counting accumulates in
`std::size_t`, independently of the iterator's difference type.
`is_empty_expression()` stops at the first certified result for native/admitted
intersection and difference expressions. Unneeded comparisons and observations
may be skipped; errors in performed observations propagate. Foreign compound
normalization retains its existing observation path and can still require
intermediate values, as in ordinary traversal.

```cpp
#include <datum/interval/range.h>

#include <array>
#include <print>

struct segment
{
    int begin;
    int end;
};
const std::array records {segment {0, 10}, segment {20, 30}};
const auto extent_of = [](const segment& value) {
    return Q::closed_open(value.begin, value.end);
};
iv::intervals_view intervals {records, extent_of};
static_assert(iv::interval_range_for<decltype(intervals), positions_domain>);

iv::interval_batch<positions_domain, 8> owned {intervals};
const auto progress = iv::visit_intervals(owned, [](const Q& value) {
    return iv::contains(value, 5) ? iv::acceptance::accept : iv::acceptance::reject_and_stop;
});
```

The view uses standard `views::all`/`transform` storage. For ordinary containers,
lvalue inputs are borrowed and movable rvalue inputs are owned. Existing views are
copied or moved into the projection, keeping their own borrowing or ownership behavior.
A const lens may return borrowed geometry;
references are read-only and owners must survive. Temporary range elements
require owning point values and materialization before the element dies.
Iterators also depend on the stored lens, so the view does not opt into
`std::ranges::borrowed_range`, even when its input borrows a container. Const iteration
must keep the same domain. The fixtures cover constructor lenses, proxies, move-only views and
points without default construction, assignment or equality operators.

`interval::clip_view{records, prepared_selection, geometry_lens}` observes stable
lvalue records and a canonical lvalue selection. An existing source view is
borrowed through `ref_view`; its lens always observes read-only records, including
when the source view has shallow constness. Construction checks that source
geometry is ordered and disjoint, allowing touching origins but rejecting overlap.
Iteration sweeps both ranges, yielding `cut_view` observations without
merging origins. A reference or borrowed geometry lens avoids endpoint copies;
a lens that constructs owning endpoints pays for that construction. No payload
copy or allocation is added by the view. Lenses may carry move-only state through
standard `transform_view` storage, and the result composes with range adaptors.
Cuts borrow the iterator's geometry cache and the selection: consume their bounds
before advancing. Temporary/proxy source rows and temporary selections are rejected.
Dereferencing a temporary clipping iterator or calling `begin()` on a temporary
clipping view is rejected too. Keep the view and a named iterator alive for a
single cut, or use range-for or a range algorithm for scoped consumption.
Geometry is retained once per visited origin; intersection bounds are formed on
dereference. Keep the cut value when consuming several fields, as in the loop below.

```cpp
const auto selected_coverage = iv::compile_intervals(Q::closed_open(5, 25));
const iv::clip_view cuts {records, selected_coverage, extent_of};
for (const auto& cut : cuts) {
    const auto& source = cut.source();
    std::println("source [{}, {}), selected [{}, {})", source.begin, source.end,
                 cut.lower().key(), cut.upper().key());
}
```

This prints selected bounds `[5, 10)` from the first record and `[20, 25)` from
the second. The gap between records contributes no cut.

`cut_view` models the interval protocol directly: use `contains(view, point)` and
the `lower()`/`upper()` member views. Explicit `cut{view}` retains endpoint values,
while `source()` still borrows the original record. A point such as `string_view`
retains its normal backing-storage lifetime. `interval_cut` remains an alias for
the borrowed view, and `bounds()` preserves existing scoped call sites.

```cpp
auto retained = cuts | iv::views::cut | std::ranges::to<std::vector>();
```

Direct retention from native borrowed bounds copies each finite endpoint once. Standard
collection may move elements; copy-only points implement those moves by copying.
Reserve and `emplace_back(view)` when direct construction matters. Vector growth
can add further element moves or copies.

For custom point domains, a geometry lens can return a small view whose accessors
express fixed endpoint semantics. Keeping those facts in the view type avoids
caching runtime endpoint flags. The record and its point storage still belong
to the caller:

```cpp
struct extent_view
{
    using domain_type = positions_domain;

    const segment& origin;

public:
    constexpr auto lower() const
    {
        return iv::boundary_ref<positions_domain>::closed(origin.begin);
    }

    constexpr auto upper() const
    {
        return iv::boundary_ref<positions_domain>::open(origin.end);
    }

};
const iv::clip_view borrowed_cuts {
    records, selected_coverage, [](const segment& row) { return extent_view {row}; }};
```

Source-preserving cuts compose with byte cursors without a shared native payload
type. The binding maps each cut's object coordinate into its packed payload
coordinate, advances `input.consume(offset - input.position())`, then consumes
`input.take(length)` through `write_fragments()`. Account for skipped origins
when computing packed offsets: object holes occupy no packed bytes. For retained
native buffers, a native owner-preserving cursor may be the appropriate sink;
borrowed spans alone do not transfer its owner. Ownership and output coverage
remain binding responsibilities.

`interval/metric.h` keeps distance separate from order. A metric supplies a quantity type, zero,
and distance. Measurement also needs checked addition; quota selection needs quantity ordering,
checked subtraction, and point advancement. Strong point and quantity types can differ; neither
point assignment nor built-in arithmetic is required. Use the supplied `interval::bytes`
for byte positions and counts; `unsigned_metric<T>` provides checked arithmetic for
other unsigned quantities.
`measure(coverage, metric)` and `take(coverage, quota, metric)` support
finite half-open canonical coverage. Holes consume no quota. Other inclusion
conventions and infinities throw explicitly; discrete stepping and storage
alignment remain external. Taking a quota returns a `canonical_range` owning its
interval values, without sorting or revalidating the result. Point payloads retain
their domain's ownership semantics. Constructing output points and growing the
backing vector can still copy and allocate.

`static_interval_set::append_range(source)` adds interval entries in source order;
its range constructor delegates to the same operation. `interval_batch` exposes
that operation while adding point ownership to the fixed-capacity inline container.
The member accepts single-pass ranges and projected interval views. It preserves
duplicates and empty entries; normalization remains a separate operation.
Capacity is checked before each dereference/projection. An exception leaves the
already appended prefix and does not inspect an extra unadmitted entry; beginning
or incrementing an input iterator may itself prefetch. The free `datum::append_range`
also dispatches to this member.
Its explicit constructor preserves source order, duplicates and empty entries.
Capacity limits entries, not point allocations, source prefetch or algebra
scratch. Ownership follows payloads recursively and does not imply immutability,
snapshot lifetime or wire enrollment. Opaque types need audited consumer profiles.

`visit_range()`, in `<datum/range.h>`, reports acceptance and stopping for an input range. An accepted
stop counts the entry; a rejected stop does not. Neither increments again.
`offered`, `consumed()`, and `accepted` are distinct: `skip` consumes without
accepting, and `reject_and_stop` leaves the offered row unconsumed. The optional `stop`
records the stopping decision; `stopped()` and `consumed()` derive from it and the counts.
`traversal::scan_range()` calls the profile's `position(row)` to save an owned position
before the consumer can move a row. On stop, `next(position, consumed)` chooses the
next pass's starting position. The profile chooses an ordered boundary or opaque state;
ordered resumes are exclusive after consumption and inclusive on rejection.
`position()` runs for each row, so use a cheap index/token when an owned key is unnecessary.
A returned token does not establish a snapshot, durable acknowledgement, or partial-page replay.
Exceptions propagate without inventing a result for partial external effects.

```cpp
const auto scan = datum::traversal::scan_range(
    rows, consumer,
    datum::traversal::ordered_positions {[](const Row& row) { return Bound::closed(row.key); }});
```

`resource_budget` charges cumulative input/payload bytes, elements, requested
allocation bytes and work; `enter_depth()` checks and unwinds current nesting.
Failed charges preserve that counter and throw before the corresponding work.
For ordinary admission, `try_charge(resource, amount)` returns whether the charge was applied.
Refusal leaves usage unchanged, including when the total would exceed its representable range.
Use `charge()` when refusal must abort the operation; changing its return type would let
unchecked guards continue after refusal. The bounded budget rejects invalid resource identifiers.
Codecs and allocators must cooperate: the counters do not observe arbitrary heap
allocations. The accounting model is cumulative requested bytes, not live memory.
Templated operations can accept `unlimited_budget` to elide charges entirely.

Byte cursor reads preflight the requested length, then copy and advance in one
fragment traversal. No flattening or allocation is introduced. If observing a
later fragment throws, already copied bytes and the cursor's consumed prefix
remain; a bookmark can restore the position when the binding permits replay.
Scalar diagnostic redaction skips field access when no size is needed.

`traversal::for_each_page()` separately handles explicit cursor progress,
cancellation, limits, throwing readers and `std::expected` failures. Readers and
visitors own their effects; traversal does not establish a durable commit.
Completion, stopping, cancellation, and page limits are ordinary result states.
An expected reader failure carries its error, resume state, and prior page count;
exceptions from a visitor still propagate without inventing a safe replay position.
Use an optional for ordinary absence, a Boolean for admission, and an expected when
the caller must distinguish a useful value from a handled failure. These mechanisms
do not imply rollback, and allocation or consumer callbacks can still throw.

## Canonical operands and evaluation

`compile_intervals(expression)` retains normalized point values. The range overload
normalizes unordered input, removes empties, and coalesces touching intervals when
at least one side includes their shared endpoint. Both return `canonical_range`;
workspace evaluation and covered quotas retain that same proof without revalidation.
These results can feed metrics, clipping, relations, and standard range pipelines directly.

`canonical_range{range}` checks that invariant on externally prepared forward ranges
with stable lvalue geometry. Admission uses standard `views::all` storage: ordinary
lvalue containers are borrowed, and movable rvalue containers are retained. Existing
views follow `views::all`'s copy/move rules. Explicit storage selection follows the same rule.
Copying an existing canonical range preserves its storage semantics, including an owned
container's copy. Pass compiled results directly to geometry operations, which borrow
canonical lvalues. External mutation must preserve normalization during observation.

| Operation | Storage behind the returned `canonical_range` |
| --- | --- |
| `compile_intervals(expression)` or `compile_intervals(raw_ranges)` | Owns a vector of normalized interval values |
| `compile_intervals<Capacity>(expression)` | Owns inline interval values, with the requested entry limit |
| `evaluate(expression, scratch)` | Borrows a read-only span into the workspace |
| `take(coverage, quota, metric)` | Owns a vector of selected interval values |
| `canonical_range{range}` for an unadmitted range | Checks existing geometry; storage follows `views::all` |

Each result supports read-only range iteration; sizing, indexing, and contiguous access
follow its backing storage. An owned vector result is a range, rather than a
`std::ranges::view`. Storage ownership and point-payload lifetime remain separate:
`owning_value` follows both. A standard `std::ranges::owning_view<R>` carries payload
ownership only when `R` is already an audited owning value.

```cpp
const auto prepared = iv::compile_intervals(expression);
iv::for_each_interval(iv::difference(prepared, exclusions), sink);

iv::workspace<positions_domain> scratch {{.intermediate_intervals = 256, .work = 4096}};
const auto result = iv::evaluate(iv::intersection(prepared, requested), scratch);
```

Lvalue canonical expression operands are borrowed; temporaries are retained.
Default observation of `difference(source, source)` for the same admitted object
returns an empty result without visiting endpoints, including noncontiguous ranges
and nested expressions. Explicit workspace evaluation keeps its preparation and
accounting, and unadmitted foreign accessors retain their ordinary error behavior.
Canonical sweeps are linear, and a connected union tree sorts at most once. Boundary sinks
borrow endpoints during each call; callable interval sinks receive point values.
Prepared relations do not normalize again. `visit_intervals` infers the range's
domain; explicit selection remains available. It observes canonical lvalues
directly, while foreign/prvalue geometry still materializes safely.

Direct intersection sweeps skip separated pairs before forming overlap bounds
and reuse upper-endpoint ordering for selection and advancement. Inclusivity,
endpoint association and workspace work accounting are preserved. Intersection
of two individual interval views is evaluated directly.

Workspace evaluation prepares the whole result before publishing. A budget
failure clears scratch and invokes no output consumer. Consumer exceptions may
leave consumer effects. The result is a canonical range over a read-only span into scratch, so ordinary
`auto` preserves observation without copying the result vector. Reset/re-evaluation
invalidates it; inputs must not borrow results from that same workspace.
Work units count entries, sorting comparisons and sweep steps, not every CPU operation;
intermediate limits count retained intervals, not endpoint bytes, vector capacity
or application authority. Buffer capacity is reused; endpoint-owned allocations
can recur. Output capacity alone never limits intermediate work.

## Query descriptions

`<datum/plan/query.h>` keeps an interval expression with requirements, residuals, and
controls, leaving their interpretation to the consumer. `make_query` starts with empty
annotation tuples; `with_requirement`, `with_residual`, and `with_control` append values.
Composition preserves references already held in those tuples.

`plan::query` checks the actual `intervals`, `requirements`, `residuals`, and `controls`
members. A plain record can supply them without nested metadata aliases:

```cpp
namespace plan = datum::plan;

struct byte_order
{
    iv::byte_query intervals;
    std::tuple<> requirements;
    std::tuple<> residuals;
    std::tuple<std::uint64_t> controls;
};

const byte_order order {iv::byte_query::closed_open(0, 8), {}, {}, {std::uint64_t {5}}};
static_assert(plan::resolved_query<byte_order>);
const auto coverage = plan::compile_intervals(order);
const auto selected = iv::take(coverage, plan::control<std::uint64_t>(order), iv::bytes);
```

Here, `iv` denotes `datum::interval`. `resolved_query` means the requirement tuple is
empty. A consumer resolves its own application requirements before constructing such a
description. Plan compilation compiles only the intervals; it does not apply residuals or
controls. The explicit `take` call gives the integer control its covered-byte meaning.

## Observation and wire schemas

Diagnostic schemas, enum schemas, projections and redaction fields are enrolled
in `datum::render`: specialize `display_schema`, `enum_schema` or
`display_projection`, with `reflected_display_schema` and `reflected_enum_schema`
for reviewed reflected descriptions. Standard formatting and the optional fmt
adapter share the same bounded character-sink implementation.

Explicit diagnostic tuples select each displayed or redacted member. Supply record
and member labels for a C++23 schema:

```cpp
namespace ceph::salad::datum::render {

template <>
struct display_schema<Entry>
{
    static constexpr std::string_view name = "Entry";
    static constexpr auto fields =
        std::tuple {field<&Entry::key>("key"), field<&Entry::version>("version"),
                    display_field<&Entry::value, display_field_policy::redacted> {"value"}};
};
} // ceph::salad::datum::render
```

Use `field<&Entry::key>("object_key")` for a different field label. With optional
C++26 reflection, record and member labels can be inferred when omitted, and an
explicit schema `name` overrides the inferred record label. Inferred type labels use
the underlying type rather than a caller's alias spelling; specializations use their template's name.
Unnamed record types need an explicit schema name; callable accessors need explicit
field labels. Additional record members remain
undisplayed until selected; this tuple does not enroll the record for binary I/O.

Structural and wire schemas live directly in `datum`. Start with
`binary_schema_t<Record>`: it selects direct public value members in declaration
order, so the wire layout follows the record declaration. The schema is a type
descriptor. Complete record facts come from a `DATUM_RECORD` declaration in
C++23 or reflection over an ordinary aggregate in C++26. No separate codec hooks
are required. The tutorial's [postcard lesson](tutorials/tutorial.md#12-send-a-postcard)
shows the complete C++23 declaration and a selected wire layout:

```cpp
using wire = datum::binary_schema_t<Entry>;
auto bytes = datum::to_bytes<wire>(entry);
auto restored = datum::from_bytes<wire>(bytes);
```

For a custom protocol, give the selected members in wire order. A nonempty
`binary_fields` selection infers its record from the first member pointer:

```cpp
using reply_wire = datum::binary_fields<&Entry::value, &Entry::key>;
```

Schema use requires distinct direct public members of that record; omissions are
intentional. The existing `binary_schema_t<Entry, &Entry::value, &Entry::key>` spelling
validates the explicit record and names the same descriptor for an unqualified record.
Explicit record qualifiers are preserved by a typed descriptor. An empty selection
cannot infer a record; use `binary_schema_t<Record>` for declaration order, including
an empty record. For a nonempty, unqualified record, this also names the same
descriptor as a complete declaration-order selection.
An inherited member pointer identifies its declaring class, so
`&Derived::inherited` infers the base record rather than `Derived`.

Fresh decoding uses ordinary value initialization, including default member
initializers, for omitted fields. Decoding into an existing record preserves its
omitted fields. Selected fields can be partly changed on failure. The typed schema
also works with fixed spans, explicit fragmented staging and `decode_bounded()`.
Bit fields and reference members require an explicit selection that omits them;
inherited state requires a separate record or projection.

The existing `binary_schema<Record>` enrollment and complete-coverage
`checked_binary_schema` remain available for record-based codec calls.
`binary_fields` builds on the shared `member_fields` structural profile;
`schema::fields` supplies its ordered observation and construction operations.
Native wire profiles can still repeat fields or use nonaggregate and borrowed
records. These uses do not establish schema eligibility, ownership, or permission
to default-construct a decoded record. Only value-returning decoding requires
default construction and movement; encoding and decoding into an existing record
do not.
Diagnostics keep independent confidentiality approval and redaction.

Reflected enum names become an ordinary constant table before runtime lookup.
This keeps reflection facts separate from runtime diagnostic traversal; see the
draft's [expansion statements](https://eel.is/c++draft/stmt.expand) and
[immediate functions](https://eel.is/c++draft/expr.const.imm).

The optional zpp adapter also supplies `datum::checked_members()`,
`datum::binary_record`, `datum::binary_member_count` and `datum::serialize_schema()`
for the consumer's ADL declarations and archive hooks. Consumers specialize
`datum::binary_field_codec<T>` when a field has a chosen external representation.
The same field codecs apply to direct schema types. No application timestamp or native buffer
type is recognized by the adapter. SALAD's time profiles and all SALAD record
enrollment remain in SALAD headers.

`to_bytes()` and `from_bytes()` retain the existing contiguous-buffer
convenience API, including aggregate envelopes and explicit codecs. Decode errors
retain `std::system_error` and its error code; trailing bytes report `std::errc::bad_message`.
These exceptions remain catchable as `std::runtime_error`. Callers that inspect codec status
directly can use the existing archive/`serialize_schema()` hooks without an exception conversion.
Decoding establishes representation,
not semantic admission; decoding into an existing value can partly mutate it on
failure. Contiguous inputs accept arrays, vectors, spans, and standard views directly.
Resizable output containers retain their clear-and-grow behavior. Fixed initialized
output, such as an array or span, returns the written subspan without resizing;
temporary owning destinations are rejected. The byte representation and complete-message
checks are shared by all these forms.
Fragmented byte ranges use explicit caller-provided contiguous staging for zpp.
Staging also accepts ordinary writable contiguous ranges and keeps its existing size,
including when supplied as a vector; it never selects the clear-and-grow output path.
`scatter_bytes` and whole-source `copy` preflight capacity before writing. Fixed codec
overflow may leave a written prefix. Sources and destinations must not overlap.
`decode_bounded<T, MaximumContainerBytes>(bytes, maximum_input_bytes)` returns a
candidate only after full decoding; the codec allocation limit is per container,
not an aggregate memory/admission budget. Its wire enrollment/order is unchanged.
An exceeded input-byte limit still throws `std::length_error`. A malformed representation is
an error even when checking candidate input is routine; the binding chooses how to expose
that failure. Missing records, rejected admission, and end-of-traversal are separate outcomes.

`view_of<Record>` provides named member projections and an ordinary lvalue-only
constructor. In C++23, `Record` uses the complete `DATUM_RECORD` declaration;
in C++26, reflection generates the view from an ordinary record declaration.
The default member profile is `const T&`;
consumers can specialize `member_view<T>` for other representations. Views are
named-field projections here; their names do not imply an iterable range or
ownership of the source record. Use an alias when a local name is helpful:

```cpp
using listing_view = datum::view_of<listing_record>;
```

Native reflected source records may have constructors; only their public nonstatic
data members are observed. Private state is not exposed. Records with base classes, unions,
anonymous members or bit fields require explicit projections. Viewing a native
record does not register a binary schema or infer its wire field order.

`member_lens<&Record::field>` observes that source member on the owner or its
matching `view_of<Record>`, including a named derived view. References and
consumer-selected member representations are preserved. Complete record facts map
the approved source member to the generated member during constant evaluation;
unrelated records with matching field names and temporary sources are rejected.
In portable C++23 builds, an explicit lens can also observe an ordinary record with
constructors. It requires a valid, non-null member-object pointer for that exact source
type. Available structural facts still check direct public membership. Generated views
and complete schemas retain their separate metadata requirements; observing selected
source fields establishes neither whole-record ownership nor schema completeness.
Generated members are initialized directly from their projections, without moving
an intermediate aggregate. Immovable value projections are usable too. Construction
is conditionally `noexcept` according to the accessors; explicitly moving an existing
view still moves its chosen representation. Throwing consumer projections remain usable.

For local conversions between known record types, prefer ordinary aggregate
initialization with named destination members:

```cpp
struct Row final
{
    std::string key;
    std::string payload;
};

Row row {.key = std::move(request.key), .payload = std::move(request.payload)};
```

The explicit moves consume the source strings. Omit them to copy the strings.
Use a field profile when a generic algorithm already needs selected fields and
their order.

`member_fields<&Record::first, &Record::second>` is the shared structural profile.
It also backs `binary_fields`, keeping approved wire order explicit. A typed
reader returns each lens's exact value type; `reconstruct<Record>(reader)` builds
an owned aggregate, with reads sequenced in profile order. Declaration-order
profiles initialize final members directly; reordered profiles use a statically
mapped tuple of field values. `construct(reader, factory)` handles audited opaque
owners and nondefault construction. `project<Target>(std::move(source))` consumes
selected fields once, rejecting duplicate members and borrowing targets.
Projection initializes target members positionally in profile order; the caller
must choose an order that matches the target aggregate. Reconstruction maps the
original record's members by identity.

With a concrete `codec`, generic reading and projection can use the same profile:

```cpp
using fields = datum::member_fields<&Request::key, &Request::payload>;
auto candidate = fields::reconstruct<Request>(
    [&](auto field) { return codec.read<typename decltype(field)::value_type>(input); });
auto row = fields::project<Row>(std::move(candidate));
```

These operations construct private values. Codec representation checks and
publication remain consumer responsibilities; exceptions destroy the candidate.

The existing ordered binary field list also supplies observation:

```cpp
using fields = datum::binary_schema<Record>::fields;
const datum::view_of view {owner};
fields::observe(view, [&archive](const auto& field) {
    datum::serialize_field(archive, field).or_throw();
});
```

`field_order<lens...>::visit()` passes lenses before reading fields so a
consumer can apply redaction or budgets first. `observe()` reads each field in
order. Both require a `void` consumer and allow exceptions; consumer effects may
leave a prefix on failure. There is no acceptance/resume or rollback promise.
The field list adds no codec enrollment for views; transformed representations
still require a reviewed consumer profile.

Public diagnostics use total byte/depth limits (4096/16 by default) alongside
per-text/range caps. `format_bounded()` reports written bytes and truncation;
`format_to()` returns the output position. Truncated diagnostics are prefixes.
No lens is dereferenced beyond the range display cap.
Nested observations support floating-point values, durations with their original
counts/units, pair/tuple values and map entries. Tuple/range element and depth
limits apply before nested observation; diagnostics remain separate from wire
encoding and explicit record confidentiality enrollment.
Explicit record schemas take precedence over projections, and both take precedence
over inferred range/text representations. Redacted sizes use bytes for byte values
and elements for other ranges. Time points retain their clock's duration units;
rendering does not convert potentially overflowing values to nanoseconds.
Recognition selects the first matching category from deferred predicates at compile
time. Later predicates and unselected rendering operations are not instantiated,
so an enrolled value can have unrelated interfaces that are ill-formed to inspect.
Reflected diagnostics read bit fields by value. Active union selection still needs
an explicit projection; reflection cannot infer the active member.

The byte endpoints support constant evaluation when the enrolled hooks and field
codecs do too. Dynamic temporary storage must be consumed within that evaluation:

```cpp
#include <datum/codec/zpp.h>

static_assert([] {
    const auto bytes = datum::to_bytes(std::uint64_t {41});

    return datum::from_bytes<std::uint64_t>(bytes) == 41;
}());
```

SALAD's domain keyspaces, validation/authority carriers, wire profiles and backend
adapters belong in SALAD. This foundation branch contains Datum alone; consumer
integration and migration are separately scoped work.
The generic libfdb predicate implementations have been folded into Datum and
modernized here; libfdb itself has not been changed or redirected to Datum.

## Reading compiler diagnostics

The byte operations expose their requirements as named concepts. A failed constraint
identifies the argument contract without making an invalid call appear supported:

| Diagnostic names | What to change |
| --- | --- |
| `byte_source`, `byte_block` | Supply contiguous bytes or forward fragments. Supported byte values are `char`, `signed char`, `unsigned char`, and `std::byte`; include `ceph-datum.h` for a native list. |
| `borrowed_byte_block` | `fragments` borrows storage. Give it a named owner or a span over storage that remains alive. |
| `writable_byte_output` | Supply writable output whose owner survives the returned span; a named array/vector or a writable span is suitable. |
| `writable_byte_fragments` | Supply fragment elements with writable bytes. A const array of writable spans works; spans of const bytes do not. |
| `invocable` on a comparison, `sorting_ordering_result` | Check the field projection and operand types. A sorting predicate needs a strong or weak order; supply an explicit policy for a partial ordering, such as floating-point values. |

In C++23, declaration-order schemas and generated record views require complete member
metadata. The diagnostic names `DATUM_RECORD` and its public header,
`<datum/record_definition.h>`, directly. For observing an existing record without that
metadata, use an explicit `member_fields` profile. Binary and diagnostic schemas retain
their independent field selections.

## Focused development

Datum is part of normal RGW builds: Ceph registers its targets whenever
`WITH_RADOSGW` is enabled, and the generated configuration header defines the
presence macro `HAVE_DATUM`. There is no separate Datum build switch. Portable C++23
and optional native C++26 requirements are scoped to Datum targets; unrelated Ceph targets retain
their existing language standard. Tests belong to Ceph's RGW test suite under
`src/test/rgw`, follow `WITH_TESTS` and `WITH_CATCH2`, and join Ceph's `tests` target.
Ceph registers only the Datum test groups supported by its configured toolchain:
the portable suites need C++23 language and library support, including constexpr
`std::string` construction, copying, and moving, and the native data suite
needs C++26 reflection. Unsupported toolchain facilities are reported without preventing
other Ceph tests from building; failures in Datum itself still fail the real target.
A standalone test build requires the core facilities. The combined native suite also needs
both fmt and standard formatting support. Set `DATUM_REQUIRE_REFLECTION_TESTS=ON`
to require that suite as well. Strong constexpr tests remain enabled by default;
passing runtime tests alone does not establish the constexpr contract on an older toolchain.
For example, Clang 19 with Ubuntu Jammy's libstdc++ 12.3 rejects a standard-only
constexpr string move. The toolchain probe reports that limitation; it does not remove
Datum's constexpr assertions or qualify that combination. Use a compatible standard
library for the full suite.

Build and run them in a configured Ceph tree:

```sh
cmake --build build --parallel 8 --target unittest_rgw_datum_core
ctest --test-dir build --output-on-failure -R '^unittest_rgw_datum_core$'
```

With a qualified C++26 reflection toolchain, also build and run
`unittest_rgw_datum_data` to cover the optional reflected facilities.

The pure target is also available by including `core.cmake` alone, without
reflection, codecs, formatting or test dependencies. Ceph owns fmt selection;
Datum does not discover a second package. The direct development build below
uses Ceph's bundled `src/fmt`, even if a system fmt package is installed. Set
`WITH_FMT_HEADER_ONLY=ON` to exercise Ceph's header-only formatting choice.

Use a C++23 compiler with the required library support for the portable targets. Native
reflection targets additionally use C++26. Reflection-dependent
targets currently use GCC's experimental reflection implementation; the tested
toolchain is GCC 17.0.0, snapshot 20260920. Supply that compiler's runtime libraries
when running its executables if they differ from the system toolchain.

```sh
cmake -S src/rgw/datum -B /tmp/datum-build -G Ninja \
  -DCMAKE_CXX_COMPILER=/path/to/g++ -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/datum-build --parallel 8
RC_PARAMS='seed=10620261009 max_success=1000 max_size=80' \
  ctest --test-dir /tmp/datum-build --output-on-failure
```

Tests use focused Catch2 executables, with pinned fallback dependencies:

- `unittest_rgw_datum_core` (`src/test/rgw/datum_core.cc`): reflection-free algebra,
  clipping and page traversal, including optional RapidCheck properties.
- `unittest_rgw_datum_data` (`src/test/rgw/datum_data.cc`): reflected records,
  ownership, byte mechanics, projected interval ranges, diagnostics, compile-time
  pipelines and optional codecs.
- `unittest_rgw_datum_render`: portable bounded rendering and standard vocabulary.
- `unittest_rgw_datum_display_profiles`: built-in profiles, checked declaration enrollment,
  confidentiality and borrowing; native builds also verify reflection correspondence.
- `unittest_rgw_datum_render_adapters` and `unittest_rgw_datum_std_format`: optional fmt
  and standard adapter contracts, including built-in profiles.

Additional portable groups cover ownership, ranges, record declarations, views, fields,
schemas and optional codecs. The original native data suite remains a separate backstop.

Catch2 tags select individual facilities. `DATUM_ENABLE_RAPIDCHECK=OFF` retains
all deterministic core tests. `DATUM_ENABLE_ZPP=OFF` omits only codec checks;
the remaining portable targets stay enabled, as do the native data tests when reflection is available. Set
`DATUM_BUILD_TESTING=OFF` in the focused development build to omit test dependencies;
Ceph test selection uses its standard switches above. The pure `core.cmake` import
needs neither fmt nor dependency provisioning.

`datum::fmt` is available in both Ceph and standalone builds. Its fixture checks
real formatter instantiation and agreement between fmt, standard formatting and
character sinks. Compile-time pipelines compose views, selection, clipping, byte
transfers, retained owners and budgets, including caught failure paths, and check
the same operations at runtime.

The deterministic suite checks all 38,416 endpoint-pair combinations against an
independent finite-set oracle. Generated tests check nested set operations and
canonical output. Cost checks count point copies and key comparisons and verify
workspace accounting without imposing timing thresholds.

Microbenchmarks and historical performance experiments are maintained outside
this branch in a standalone archive; Datum registers no benchmark targets or
options in Ceph or its standalone build. Stress studies with 128 intervals per
input do not establish the performance of common Ceph requests. Application
impact depends on actual range sizes, key storage and evaluation frequency;
further tuning should follow an identified consumer hotspot. Historical timings
describe local machinery, not backend throughput.
