# Datum

Datum is SALAD's reusable data core, in namespace `ceph::salad::datum`.
It is a Ceph component beside SALAD, with a focused development build. The interval
algebra is its mathematical foundation; data views, observation and codecs are
separate layers above it. Keep application records, native buffers, backend
policy and I/O implementations in consumers and adapters.

Read the [tutorial](tutorials/tutorial.md) for small, runnable examples, or use the
[documentation overview](../../../doc/dev/radosgw/datum/index.rst) to find the
tutorial, design, build instructions, and index together. All lessons are provisional.

## Architectural guidelines

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

| Layer | Public headers under `include/datum/` | CMake target |
| --- | --- | --- |
| Pure ordered-domain geometry | `interval/query.h`, `interval/clip.h`, `interval/metric.h` | `datum::core` |
| Generic data and traversal | `key.h`, `bytes.h`, `budget.h`, `plan/query.h`, `traversal.h` | `datum::core` |
| Reflected records, ownership and range projections | `record.h`, `ownership.h`, `interval/range.h`, `traversal/scan.h` | `datum::reflection` |
| Diagnostics and Datum-type renderers | `render/display.h`, `render/datum.h` | `datum::reflection` |
| Optional codec and fmt adapters | `codec/zpp.h`, `render/fmt.h` | `datum::zpp`, `datum::fmt` |

`datum::core` requires C++26 but no reflection, concrete codec or Ceph headers.
`datum::reflection` adds the GNU reflection opt-in when using GCC. Codec and
formatting targets add their own dependencies. `datum::fmt` uses Ceph's selected
`FMT_LIB`, including its system/bundled and header-only choices. Header inclusion and
record enrollment remain explicit; there is no umbrella that imports all layers.

Native integration belongs in separate consumer adapters. This library does not
provide Ceph buffer framing, subsystem records, or production consumer migrations.

The public surface has 16 entry headers. Shared reflection metadata lives in
`detail/record.h`; consumers do not include it directly. Record views, ordered
fields and wire descriptors share `record.h`, while `ownership.h` supplies payload
ownership and retained projections. `render/display.h` does not import geometry,
plans or page traversal; include `render/datum.h` for their diagnostic profiles.

## Geometry and data views

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
the binding decides whether to copy or retain the blocks. Cursor bookmarks and capacity preflight
still require stable, multipass fragments. This does not add fragment-native behavior to a
contiguous-only codec. Bulk gathering and incremental `read_into(span)` share byte-copy primitives;
the bulk operation needs no cursor state.

`retained_view` in `ownership.h` stores an audited payload owner and a pure lens.
Construction consumes an rvalue owner; access recomputes the projection after
moves instead of retaining self-pointers. `byte_slice{offset, length}` is a checked
contiguous projection. The lens must derive its borrow from the retained owner.
Backing lifetime does not establish immutability or a transaction lease; extracting
a projection directly from a temporary wrapper is rejected.

An ordered domain supplies a copy-constructible point type and a strong-order
comparison. Point equality operators, default construction, assignment and
arithmetic are unnecessary. Domain ordering and lens purity are semantic laws;
concepts check the corresponding expressions and types. A boundary's kind must
agree with its finite/infinity predicates; its key is available only when finite.

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

namespace iv = ::ceph::salad::datum::interval;

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
`compile_intervals()` materializes that output in a vector or an explicitly
bounded `static_interval_set`.

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

struct segment
{
    int begin;
    int end;
};
const std::array records {segment {0, 10}, segment {20, 30}};
iv::intervals_view geometry {
    records, [](const segment& value) { return Q::closed_open(value.begin, value.end); }};
static_assert(iv::interval_range_for<decltype(geometry), positions_domain>);

iv::interval_batch<positions_domain, 8> owned {geometry};
const auto progress = iv::visit_intervals(owned, [](const Q& value) {
    return iv::contains(value, 5) ? iv::acceptance::accept : iv::acceptance::reject_and_stop;
});
```

The view uses standard `all`/`transform` storage: lvalue ranges are borrowed and
movable rvalue ranges are owned. A const lens may return borrowed geometry;
references are read-only and owners must survive. Temporary range elements
require owning point values and materialization before the element dies.
The view does not opt into `borrowed_range`. Const iteration must keep the same
domain. The fixtures cover constructor lenses, proxies, move-only views and
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
for (const auto& cut : iv::clip_view {segments, selected, geometry}) {
    consume_slice(cut.source(), cut.bounds());
}
```

`cut_view` models the interval protocol directly: use `contains(view, point)` and
the `lower()`/`upper()` member views. Explicit `cut{view}` retains endpoint values,
while `source()` still borrows the original record. A point such as `string_view`
retains its normal backing-storage lifetime. `interval_cut` remains an alias for
the borrowed view, and `bounds()` preserves existing scoped call sites.

```cpp
const iv::cut saved {view};
auto retained = cuts | iv::views::cut | std::ranges::to<std::vector>();
```

Direct retention from native borrowed bounds copies each finite endpoint once. Standard
collection may move elements; copy-only points implement those moves by copying.
Reserve and `emplace_back(view)` when direct construction matters. Vector growth
can add further element moves or copies.

A geometry lens can return a small view whose accessors express fixed endpoint
semantics. Keeping those facts in the view type avoids caching runtime endpoint
flags. The record and its point storage still belong to the caller:

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
const iv::canonical_set prepared_selection {selected};
const iv::clip_view cuts {
    records, prepared_selection, [](const segment& row) { return extent_view {row}; }};
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
point assignment nor built-in arithmetic is required. `unsigned_metric<T>` provides checked
unsigned arithmetic.
`measure(coverage, metric)` and `take(coverage, quota, metric)` support
finite half-open canonical coverage. Holes consume no quota. Other inclusion
conventions and infinities throw explicitly; discrete stepping and storage
alignment remain external. Taking a quota returns an owning canonical view
without sorting or copying endpoints during ownership transfer. Constructing
output points and growing its vector can still copy and allocate.

`interval_batch` adds point ownership to the one `std::inplace_vector` container.
Its explicit constructor preserves source order, duplicates and empty entries.
Capacity limits entries, not point allocations, source prefetch or algebra
scratch. Ownership follows payloads recursively and does not imply immutability,
snapshot lifetime or wire enrollment. Opaque types need audited consumer profiles.

`visit_range()` reports acceptance and stopping for an input range. An accepted
stop counts the entry; a rejected stop does not. Neither increments again.
`offered`, `consumed` and `accepted` are distinct: `skip` consumes without
accepting, and `reject_and_stop` leaves the offered row unconsumed.
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

`canonical_set{expression}` retains normalized point values. Its range constructor
normalizes unordered input, removes empties and coalesces touching intervals when
at least one side includes their shared endpoint. `canonical_view{range}` validates
that invariant and observes an existing forward range with stable lvalue geometry.
It uses standard `views::all` ownership for its range; payload lifetime remains a
separate property. Mutation must preserve normalization during observation.

```cpp
const iv::canonical_set prepared {expression};
const iv::canonical_view borrowed {prepared};
iv::for_each_interval(iv::difference(borrowed, exclusions), sink);

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
of two individual interval views is evaluated directly through a separate overload.

Workspace evaluation prepares the whole result before publishing. A budget
failure clears scratch and invokes no output consumer. Consumer exceptions may
leave consumer effects. The result is a read-only span into scratch, so ordinary
`auto` preserves observation without copying the result vector. Reset/re-evaluation
invalidates it; inputs must not borrow results from that same workspace.
Work units count entries, sorting comparisons and sweep steps, not every CPU operation;
intermediate limits count retained intervals, not endpoint bytes, vector capacity
or application authority. Buffer capacity is reused; endpoint-owned allocations
can recur. Output capacity alone never limits intermediate work.

## Observation and wire schemas

Diagnostic schemas, enum schemas, projections and redaction fields are enrolled
in `datum::render`: specialize `display_schema`, `enum_schema` or
`display_projection`, with `reflected_display_schema` and `reflected_enum_schema`
for reviewed reflected descriptions. Standard formatting and the optional fmt
adapter share the same bounded character-sink implementation.

Explicit diagnostic tuples select each displayed or redacted member. Record and
member labels are inferred when omitted:

```cpp
namespace ceph::salad::datum::render {

template <>
struct display_schema<Entry>
{
    static constexpr auto fields =
        std::tuple {field<&Entry::key>(), field<&Entry::version>(),
                    display_field<&Entry::value, display_field_policy::redacted>()};
};
} // ceph::salad::datum::render
```

Use `field<&Entry::key>("object_key")` for a different field label, or initialize a
redacted descriptor with its explicit label. A schema's optional `name` overrides
the inferred record label. Inferred type labels use the underlying type rather
than a caller's alias spelling; specializations use their template's name.
Unnamed record types need an explicit schema name; callable accessors need explicit
field labels. Additional record members remain
undisplayed until selected; this tuple does not enroll the record for binary I/O.

Structural and wire schemas live directly in `datum`. Start with
`binary_schema_t<Record>`: it selects direct public value members in declaration
order, so the wire layout follows the record declaration. The schema is a type
descriptor; the source remains an ordinary record. No schema specialization or
record-local codec hooks are required to use it:

```cpp
using wire = datum::binary_schema_t<Entry>;
auto bytes = datum::render::to_bytes<wire>(entry);
auto restored = datum::render::from_bytes<wire>(bytes);
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

`render::to_bytes()` and `from_bytes()` retain the existing contiguous-buffer
convenience API, including aggregate envelopes and explicit codecs. Decode errors
retain `std::system_error` and its error code; trailing bytes report `std::errc::bad_message`.
These exceptions remain catchable as `std::runtime_error`. Callers that inspect codec status
directly can use the existing archive/`serialize_schema()` hooks without an exception conversion.
Decoding establishes representation,
not semantic admission; decoding into an existing value can partly mutate it on
failure. Fixed-span output returns the written subspan and avoids resizing.
Fragmented byte ranges use explicit caller-provided contiguous staging for zpp;
`scatter_bytes` and `gather_bytes` preflight capacity before writing. Fixed codec
overflow may leave a written prefix. Sources and destinations must not overlap.
`decode_bounded<T, MaximumContainerBytes>(bytes, maximum_input_bytes)` returns a
candidate only after full decoding; the codec allocation limit is per container,
not an aggregate memory/admission budget. Its wire enrollment/order is unchanged.
An exceeded input-byte limit still throws `std::length_error`. A malformed representation is
an error even when checking candidate input is routine; the binding chooses how to expose
that failure. Missing records, rejected admission, and end-of-traversal are separate outcomes.

`view_of<Record>` uses C++26 aggregate generation for named member projections and
an ordinary lvalue-only constructor. The default member profile is `const T&`;
consumers can specialize `member_view<T>` for other representations. Views are
shallow observations, not ownership guarantees. A derived consumer view can
inherit that constructor:

```cpp
struct listing_view : ceph::salad::datum::view_of<listing_record>
{
    using view_of::view_of;
};
```

Source records may have constructors; only their public nonstatic data members
are observed. Private state is not exposed. Records with base classes, unions,
anonymous members or bit fields require explicit projections. Viewing a native
record does not register a binary schema or infer its wire field order.

`member_lens<&Record::field>` observes that source member on the owner or its
matching `view_of<Record>`, including a named derived view. References and
consumer-selected member representations are preserved. Reflection maps the
approved source member to the generated member during constant evaluation;
unrelated records with matching field names and temporary sources are rejected.
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
    const auto bytes = ceph::salad::datum::render::to_bytes(std::uint64_t {41});

    return ceph::salad::datum::render::from_bytes<std::uint64_t>(bytes) == 41;
}());
```

SALAD's domain keyspaces, validation/authority carriers, wire profiles and backend
adapters belong in SALAD. This foundation branch contains Datum alone; consumer
integration and migration are separately scoped work.
The generic libfdb predicate implementations have been folded into Datum and
modernized here; libfdb itself has not been changed or redirected to Datum.

## Focused development

Datum is part of normal RGW builds: Ceph registers its targets whenever
`WITH_RADOSGW` is enabled, and the generated configuration header defines the
presence macro `HAVE_DATUM`. There is no separate Datum build switch. Its C++26
usage requirements are scoped to Datum targets; unrelated Ceph targets retain
their existing language standard. Tests belong to Ceph's RGW test suite under
`src/test/rgw`, follow `WITH_TESTS` and `WITH_CATCH2`, and join Ceph's `tests` target.
Ceph registers only the Datum test groups supported by its configured toolchain:
the core suite needs C++26 language and library support, and the data suite also
needs reflection. Unsupported groups are reported and skipped without preventing
other Ceph tests from building. An explicitly requested standalone Datum test
build instead reports a configuration error when either requirement is missing.

Build and run them in a configured Ceph tree:

```sh
cmake --build build --parallel 8 \
  --target unittest_rgw_datum_core unittest_rgw_datum_data
ctest --test-dir build --output-on-failure -R '^unittest_rgw_datum_(core|data)$'
```

The pure target is also available by including `core.cmake` alone, without
reflection, codecs, formatting or test dependencies. Ceph owns fmt selection;
Datum does not discover a second package. The direct development build below
uses Ceph's bundled `src/fmt`, even if a system fmt package is installed. Set
`WITH_FMT_HEADER_ONLY=ON` to exercise Ceph's header-only formatting choice.

Use a C++26 compiler with the required library support. Reflection-dependent
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

Tests use two C++ sources and two Catch2 executables, with pinned fallback
dependencies:

- `unittest_rgw_datum_core` (`src/test/rgw/datum_core.cc`): reflection-free algebra,
  clipping and page traversal, including optional RapidCheck properties.
- `unittest_rgw_datum_data` (`src/test/rgw/datum_data.cc`): reflected records,
  ownership, byte mechanics, projected interval ranges, diagnostics, compile-time
  pipelines and optional codecs.

Catch2 tags select individual facilities. `DATUM_ENABLE_RAPIDCHECK=OFF` retains
all deterministic core tests. `DATUM_ENABLE_ZPP=OFF` omits only codec checks;
both executables and the remaining data tests still build. Set
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
