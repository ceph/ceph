# Index

Find a task below, or look up a concept or API name alphabetically.
Each entry links to its teaching lesson and, where useful, a later application.
The [first lesson](../tutorial.md#1-hello-world--in-pieces) begins the reading
sequence; the [build appendix](../tutorial.md#appendix-a-building-and-running)
gives the setup commands.

## Find by task

| I want to… | Start here |
| --- | --- |
| Copy a byte range into ordinary storage | [Fragmented byte copying](../tutorial.md#1-hello-world--in-pieces) |
| Append a range or join fragments into a string | [First lesson variation](../tutorial.md#1-hello-world--in-pieces) |
| Compose a path supplied as a range of segments | [Key composition variation](../tutorial.md#4-put-2-before-10) |
| Read data split across buffers | [hello, world! — in pieces](../tutorial.md#1-hello-world--in-pieces) |
| Combine or subtract ranges | [Seat selection](../tutorial.md#2-seat-3-is-reserved) and [interval algebra](../tutorial.md#2½-an-interval-interlude) |
| Put overlapping ranges in order | [Untangle the ranges](../tutorial.md#3-untangle-the-ranges) |
| Make key order match numeric order | [Put 2 before 10](../tutorial.md#4-put-2-before-10) |
| Sort records by several keys | [Comparison composition](design.md#native-vocabulary-and-comparison-composition) |
| Select missing byte ranges using built-in vocabulary | [Byte geometry and queries](design.md#bytes-geometry-and-standard-ranges) |
| Plan reads around cached data with a per-batch size limit | [Interactive read planner](index.md#datum-read-plan) |
| Select a piece and keep its source label | [Keep the label on the slice](../tutorial.md#5-keep-the-label-on-the-slice) |
| Take a quota without counting gaps | [Five bytes, please](../tutorial.md#6-five-bytes-please) |
| Keep backing data with a slice | [Move it and keep reading](../tutorial.md#7-move-it-and-keep-reading) |
| Read or select record fields | [Named C++23 views](../tutorial.md#8-a-window-into-misos-toy-basket) and [selected field profiles](../tutorial.md#9-pick-the-basket-fields) |
| Stop a scan and continue later | [Keep your place](../tutorial.md#10-keep-your-place); [Turn the page](../tutorial.md#14-turn-the-page) for pages |
| Redact logs or select transmitted fields | [The discreet log](../tutorial.md#11-the-discreet-log) and [Send a postcard](../tutorial.md#12-send-a-postcard) |
| Refuse work when its allowance runs out | [Three tickets](../tutorial.md#13-three-tickets) |
| Store a selection with its quota | [Write the order ticket](../tutorial.md#15-write-the-order-ticket) |
| Copy or retain Ceph buffer bytes | [Bytes with a Ceph accent](../tutorial.md#18-bytes-with-a-ceph-accent) |
| Read RGW bucket-index pages | [Read an RGW page](../tutorial.md#16-read-an-rgw-page), then [One shard, two pages](../tutorial.md#17-one-shard-two-pages); review [page traversal](../tutorial.md#14-turn-the-page) first |

## Topics and API names

### A–C

**Acceptance.** A scan callback's decision controls whether an offered entry is
consumed and whether traversal stops. [Lesson 10](../tutorial.md#10-keep-your-place).

**`append_range`.** Append an input range to a growable sequence, with native
C++23 dispatch and a portable fallback. Use `views::join` to append characters
across fragments. [Lesson 1 variation](../tutorial.md#1-hello-world--in-pieces).

**`binary_fields`.** Select record members and their wire order; the member
pointers identify the record type. [Lesson 12](../tutorial.md#12-send-a-postcard).

**`bytes`, coverage metric.** Datum's supplied checked metric for counting and
limiting covered byte positions; gaps do not contribute. [Lesson 6](../tutorial.md#6-five-bytes-please).

**Borrowed range; `std::ranges::borrowed_range`.** A range whose iterators can
remain valid after the range object is destroyed, provided the backing storage
remains valid. A `std::span` has this property; Datum's Ceph fragment view does not,
because its iterators depend on the view object. This is distinct from borrowing
the bytes. [Ceph fragments](../tutorial.md#18-bytes-with-a-ceph-accent).

**Borrowing.** Observe existing data without keeping its backing storage alive.
[Fragmented bytes](../tutorial.md#1-hello-world--in-pieces),
[retained-owner projections](../tutorial.md#7-move-it-and-keep-reading), and
[record views](../tutorial.md#8-a-window-into-misos-toy-basket).

**Boundary.** An interval's endpoint may be included or excluded; an unbounded
end has no finite endpoint. [Chapter 2½](../tutorial.md#2½-an-interval-interlude).

**Boundary predicates; `is_finite`; `is_below_all`; `is_above_all`.** Shared checks
derive from a boundary's `kind()`. Providers supply domain identity, `kind()`,
`key()`, and `inclusive()`.
[Boundary protocol](design.md#geometry-and-data-views).

**Budget.** An allowance checked by cooperating operations before they perform
work. See `resource_budget` in [Three tickets](../tutorial.md#13-three-tickets).

**Bucket index.** RGW's index supplies the entries and marker used by the native
listing examples. [Lesson 16](../tutorial.md#16-read-an-rgw-page), then
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**`byte_extent`; `byte_mapping`.** Plain byte geometry and logical-to-backing
mapping, with checked `end_offset`, `subextent`, and `clip` operations.
See [byte clipping](../tutorial.md#5-keep-the-label-on-the-slice) and
[Ceph vocabulary](../tutorial.md#18-bytes-with-a-ceph-accent).

**`byte_query`.** Use `interval::byte_query` for numeric byte positions and
`key::byte_query` for byte-string keys and prefixes. The supplied vocabularies cover
these common domains. [Byte geometry](design.md#bytes-geometry-and-standard-ranges)
and [key ranges](design.md#geometry-and-data-views).

**`compose_range`.** Build an owned key from an input range of string-like segments.
[Lesson 4 variation](../tutorial.md#4-put-2-before-10).

**`copy`.** Transfer bytes into a supplied destination and return a span borrowing
the written storage. `copy(source, output)` copies the complete source and preserves
the output's unused tail. For a Ceph source with an extent argument, return a new
`ceph::buffer::list` with independent payload bytes.
[Lesson 1 variation](../tutorial.md#1-hello-world--in-pieces) and
[Ceph copying](../tutorial.md#18-bytes-with-a-ceph-accent).

**`byte_cursor`.** Read a bounded sequence across fragments without gathering
it into one buffer. [Lesson 1](../tutorial.md#1-hello-world--in-pieces).

**`byte_slice`.** An offset-and-length lens returning a borrowed span into
contiguous byte storage.
[Lesson 7](../tutorial.md#7-move-it-and-keep-reading).

**Canonical coverage.** Ordered, nonempty intervals with overlaps and joinable
touching bounds merged. A canonical set can also be empty.
[Normalization](../tutorial.md#3-untangle-the-ranges),
[clipping](../tutorial.md#5-keep-the-label-on-the-slice), and
[measurement](../tutorial.md#6-five-bytes-please).

**`canonical_range`; `compile_intervals`.** Compile expressions or normalize raw
interval ranges into certified coverage. Dynamic and bounded results own interval
values; workspace results borrow them. Checked range admission follows standard
`views::all` ownership.
[Canonical operands and evaluation](design.md#canonical-operands-and-evaluation).

**Ceph vocabulary; `ceph-datum.h`.** Shared operations for native `buffer::list`
storage: copy into an ordinary destination, retain a slice, create an independent
native copy, or expose fragments as standard spans.
[Lesson 18](../tutorial.md#18-bytes-with-a-ceph-accent).

**`clip_view`; cuts.** Intersect each source record's extent with selected coverage
while retaining the source association. [Lesson 5](../tutorial.md#5-keep-the-label-on-the-slice).

**`comparison_domain`; domain.** A domain identifies the point type and ordering.
The helper supplies a comparison policy; a named derived type preserves domain
identity. [Lesson 2](../tutorial.md#2-seat-3-is-reserved).

**Comparisons.** Use `order_by(ascending(...), descending(...))` for mixed-direction
sorting, or `nth_key` to project a tuple or pair element in a standard algorithm.
`lexicographic` retains a three-way result; `order_by(policy)` adapts an existing ordering policy.
[Comparison composition](design.md#native-vocabulary-and-comparison-composition).

**`contains`; membership.** Ask whether a point belongs to a selection.
[Lesson 2](../tutorial.md#2-seat-3-is-reserved), then
[chapter 2½](../tutorial.md#2½-an-interval-interlude).

**Controls.** Values stored with a query description; a consumer gives them
meaning and applies them. [Lesson 15](../tutorial.md#15-write-the-order-ticket).

**Covered byte.** A byte whose position lies in the input coverage. Positions in
gaps contribute nothing to the quota. [Lesson 6](../tutorial.md#6-five-bytes-please).

### D–F

**`DATUM_RECORD`; `DATUM_LOCAL_RECORD`.** Declare a record and its complete field
metadata from one field list. The local form supports records inside a function.
[Named views](../tutorial.md#8-a-window-into-misos-toy-basket) and
[wire schemas](../tutorial.md#12-send-a-postcard).

**`decode_reply`; `encode_request`.** RGW consumer bindings for native bucket-list
frames, supplied by the separate integration. [Lesson 16](../tutorial.md#16-read-an-rgw-page), used again in
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**Diagnostics; `display_schema`.** Explicitly approve record fields for readable
output, including redaction or omission. [Lesson 11](../tutorial.md#11-the-discreet-log).

**Difference; subtraction; `without`.** Keep selected points while excluding
another selection or a single point. [Lesson 2](../tutorial.md#2-seat-3-is-reserved)
and [chapter 2½](../tutorial.md#2½-an-interval-interlude).

**`evaluate`; workspace.** Prepare a canonical result within explicit work and
intermediate-entry limits. The returned range borrows the workspace until its next
reset or evaluation. [Evaluation](design.md#canonical-operands-and-evaluation).

**Fields; field profile; `member_fields`.** An ordered list of record members
for a reusable consumer to observe directly on an ordinary C++23 record. [Lesson 9](../tutorial.md#9-pick-the-basket-fields).

**`for_each_page`.** Drive a page loop with a reader, next-position callback,
and visitor. [Lesson 14](../tutorial.md#14-turn-the-page), applied to RGW in
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**`fragments`.** Adapt contiguous storage or a native Ceph list to a range of
standard spans. The spans borrow the backing bytes; iterators also depend on the
fragment view. [Byte sources and sinks](design.md#byte-sources-and-sinks) and
[native fragments](../tutorial.md#18-bytes-with-a-ceph-accent).

**`from_bytes`; `to_bytes`.** Decode and encode an explicitly selected wire
representation. [Lesson 12](../tutorial.md#12-send-a-postcard).

### G–M

**Gaps.** Unselected positions between pieces of coverage.
[Chapter 2½](../tutorial.md#2½-an-interval-interlude) and
[covered-byte quotas](../tutorial.md#6-five-bytes-please).

**Intersection.** Select points that belong to both operands.
[Chapter 2½](../tutorial.md#2½-an-interval-interlude).

**`interval_batch::append_range`.** Add interval entries from a range without
normalizing or exceeding inline capacity. Already appended entries survive a later
failure. [Lesson 3 variation](../tutorial.md#3-untangle-the-ranges).

**Keys.** Encode integer segments so byte ordering gives the intended numeric
ordering. [Lesson 4](../tutorial.md#4-put-2-before-10).

**Lens.** A description of how to observe part of an owner or source. The retained
slice example uses an offset and length. [Lesson 7](../tutorial.md#7-move-it-and-keep-reading).

**Marker, RGW.** A native object key used to position a bucket-list request. Keep
the complete server-supplied key for continuation.
[Lesson 16](../tutorial.md#16-read-an-rgw-page) and
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**`measure`; metric.** A metric supplies the distance and arithmetic needed to
measure coverage and advance positions. [Lesson 6](../tutorial.md#6-five-bytes-please).

### N–R

**`nth_key<N>`.** Project tuple or pair element `N` for a standard algorithm's
comparison. [Key sorting](../tutorial.md#4-put-2-before-10).

**Normalization.** Prepare ordered, merged coverage from unordered or overlapping
intervals. [Lesson 3](../tutorial.md#3-untangle-the-ranges).

**`observe`.** A field profile calls a consumer for each selected value, in profile
order. [Lesson 9](../tutorial.md#9-pick-the-basket-fields).

**`order_by`; `ascending`; `descending`.** Supply a sorting predicate from field
projections with explicit directions. These share the comparison composition engine.
[Comparison composition](design.md#native-vocabulary-and-comparison-composition).

**Owner; ownership.** An owner keeps backing storage available for later
observation. Retaining it does not by itself create a snapshot.
[Lesson 7](../tutorial.md#7-move-it-and-keep-reading).

**Position and next.** Position-profile operations used by Datum's scan. The
example implements them for array indices; they are not free Datum functions.
[Lesson 10](../tutorial.md#10-keep-your-place).

**Projection.** An observation computed from a source, such as a borrowed slice
returned by a retained view's `get()`.
[Lesson 7](../tutorial.md#7-move-it-and-keep-reading).

**Query description; `make_query`; `with_control`.** Keep a selection with typed
controls for a consumer to interpret. Plain records satisfy `plan::query` through
their interval and annotation members.
[Lesson 15](../tutorial.md#15-write-the-order-ticket) and
[query descriptions](design.md#query-descriptions).

**Quota.** An upper limit on the amount selected from covered intervals.
[Lesson 6](../tutorial.md#6-five-bytes-please), stored with a query in
[lesson 15](../tutorial.md#15-write-the-order-ticket).

**Record view; `view_of`.** Observe the source record through named field projections.
The default projections borrow its current fields; they do not capture a snapshot.
[Lesson 8](../tutorial.md#8-a-window-into-misos-toy-basket).

**Redaction.** Replace an approved diagnostic field's contents while leaving the
field identifiable. [Lesson 11](../tutorial.md#11-the-discreet-log).

**`resource_budget`; `try_charge`.** Charge a work unit before an effect and use
the Boolean result to handle ordinary refusal.
[Lesson 13](../tutorial.md#13-three-tickets).

**Resume; continuation.** Owned position state returned for a later traversal.
[Entry positions](../tutorial.md#10-keep-your-place),
[page positions](../tutorial.md#14-turn-the-page), and
[RGW markers](../tutorial.md#17-one-shard-two-pages).

**`retained_view`.** Keep an owner and lens together; obtain a fresh projection
from the destination after a move. [Lesson 7](../tutorial.md#7-move-it-and-keep-reading).

### S–Z

**`scan_range`.** Scan entries while recording progress and a resume position
when the callback stops. [Lesson 10](../tutorial.md#10-keep-your-place).

**`set_union`; union.** Select points that belong to either operand.
[Chapter 2½](../tutorial.md#2½-an-interval-interlude).

**`slice`, Ceph bytes.** Return another `ceph::buffer::list` whose segment handles
share the selected payloads. The result retains Ceph-managed storage; it does not
create independent bytes or return a standard Ranges view.
[Lesson 18](../tutorial.md#18-bytes-with-a-ceph-accent).

**`take()`, fragmented bytes.** `byte_cursor::take()` returns a bounded slice
across fragments. [Lesson 1](../tutorial.md#1-hello-world--in-pieces).

**`take()`, covered quota.** `interval::take()` selects up to the requested measure
of covered intervals. [Lesson 6](../tutorial.md#6-five-bytes-please).

**View.** A way to observe data. C++ Ranges' views model `std::ranges::view` and can
borrow or own storage. Datum also uses the name for projections: a record view
exposes named fields, while `retained_view` keeps an owner and lens together.
Those names alone establish neither iteration nor ownership. Check each interface's
ownership and backing lifetime.
[Lessons 7](../tutorial.md#7-move-it-and-keep-reading) and
[8](../tutorial.md#8-a-window-into-misos-toy-basket).

**`visit_result`; `consumed()`; `stopped()`.** Scan progress stores `offered` and
`accepted` counts and an optional `stop` decision. The two accessors derive
consumption and stopping; `skip` consumes an entry without accepting it.
[Lesson 10](../tutorial.md#10-keep-your-place) and
[progress contract](design.md#geometry-and-data-views).

**Visitor.** A callback that processes a page and returns a traversal decision.
[Lesson 14](../tutorial.md#14-turn-the-page). The entry-level callback in
[lesson 10](../tutorial.md#10-keep-your-place) makes acceptance decisions.

**Wire schema.** The selected fields and order of an encoded representation.
[Lesson 12](../tutorial.md#12-send-a-postcard). Native RGW frames are introduced in
[lesson 16](../tutorial.md#16-read-an-rgw-page).

## Further reading

The [bibliography](../tutorial.md#bibliography) collects the qualified API sources,
the greeting's history, and a short annotated background reading list.
