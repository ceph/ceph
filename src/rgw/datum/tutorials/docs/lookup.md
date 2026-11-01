# Index

Find a task below, or look up a concept or API name alphabetically.
Each entry links to its teaching lesson and, where useful, a later application.
The [first lesson](../tutorial.md#1-hello-world--in-pieces) begins the reading
sequence; the [build appendix](../tutorial.md#appendix-a-building-and-running)
gives the setup commands.

## Find by task

| I want to… | Start here |
| --- | --- |
| Read data split across buffers | [hello, world! — in pieces](../tutorial.md#1-hello-world--in-pieces) |
| Combine or subtract ranges | [Seat selection](../tutorial.md#2-seat-3-is-reserved) and [interval algebra](../tutorial.md#2½-an-interval-interlude) |
| Put overlapping ranges in order | [Untangle the ranges](../tutorial.md#3-untangle-the-ranges) |
| Make key order match numeric order | [Put 2 before 10](../tutorial.md#4-put-2-before-10) |
| Select a piece and keep its source label | [Keep the label on the slice](../tutorial.md#5-keep-the-label-on-the-slice) |
| Take a quota without counting gaps | [Five bytes, please](../tutorial.md#6-five-bytes-please) |
| Keep backing data with a slice | [Move it and keep reading](../tutorial.md#7-move-it-and-keep-reading) |
| Read or select record fields | [Record views](../tutorial.md#8-a-window-into-misos-toy-basket) and [field profiles](../tutorial.md#9-pick-the-basket-fields) |
| Stop a scan and continue later | [Keep your place](../tutorial.md#10-keep-your-place); [Turn the page](../tutorial.md#14-turn-the-page) for pages |
| Redact logs or select transmitted fields | [The discreet log](../tutorial.md#11-the-discreet-log) and [Send a postcard](../tutorial.md#12-send-a-postcard) |
| Refuse work when its allowance runs out | [Three tickets](../tutorial.md#13-three-tickets) |
| Store a selection with its quota | [Write the order ticket](../tutorial.md#15-write-the-order-ticket) |
| Read RGW bucket-index pages | [Read an RGW page](../tutorial.md#16-read-an-rgw-page), then [One shard, two pages](../tutorial.md#17-one-shard-two-pages); review [page traversal](../tutorial.md#14-turn-the-page) first |

## Topics and API names

### A–C

**Acceptance.** A scan callback's decision controls whether an offered entry is
consumed and whether traversal stops. [Lesson 10](../tutorial.md#10-keep-your-place).

**`binary_fields`.** Select record members and their wire order; the member
pointers identify the record type. [Lesson 12](../tutorial.md#12-send-a-postcard).

**Borrowing.** Observe existing data while its backing objects remain valid.
[Fragmented bytes](../tutorial.md#1-hello-world--in-pieces),
[retained-owner projections](../tutorial.md#7-move-it-and-keep-reading), and
[record views](../tutorial.md#8-a-window-into-misos-toy-basket).

**Boundary.** An interval's endpoint may be included or excluded; an unbounded
end has no finite endpoint. [Chapter 2½](../tutorial.md#2½-an-interval-interlude).

**Budget.** An allowance checked by cooperating operations before they perform
work. See `resource_budget` in [Three tickets](../tutorial.md#13-three-tickets).

**Bucket index.** RGW's index supplies the entries and marker used by the native
listing examples. [Lesson 16](../tutorial.md#16-read-an-rgw-page), then
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**`byte_cursor`.** Read a bounded sequence across fragments without gathering
it into one buffer. [Lesson 1](../tutorial.md#1-hello-world--in-pieces).

**`byte_slice`.** An offset-and-length lens applied to retained byte storage.
[Lesson 7](../tutorial.md#7-move-it-and-keep-reading).

**Canonical coverage.** Ordered, nonempty intervals with overlaps and joinable
touching bounds merged. A canonical set can also be empty.
[Normalization](../tutorial.md#3-untangle-the-ranges),
[clipping](../tutorial.md#5-keep-the-label-on-the-slice), and
[measurement](../tutorial.md#6-five-bytes-please).

**`clip_view`; cuts.** Intersect each source record's extent with selected coverage
while retaining the source association. [Lesson 5](../tutorial.md#5-keep-the-label-on-the-slice).

**`comparison_domain`; domain.** A domain identifies the point type and ordering.
The helper supplies a comparison policy; a named derived type preserves domain
identity. [Lesson 2](../tutorial.md#2-seat-3-is-reserved).

**`contains`; membership.** Ask whether a point belongs to a selection.
[Lesson 2](../tutorial.md#2-seat-3-is-reserved), then
[chapter 2½](../tutorial.md#2½-an-interval-interlude).

**Controls.** Values stored with a query description; a consumer gives them
meaning and applies them. [Lesson 15](../tutorial.md#15-write-the-order-ticket).

**Covered byte.** A byte whose position lies in the input coverage. Positions in
gaps contribute nothing to the quota. [Lesson 6](../tutorial.md#6-five-bytes-please).

### D–F

**`decode_reply`; `encode_request`.** RGW consumer bindings for native bucket-list
frames, supplied by the separate integration. [Lesson 16](../tutorial.md#16-read-an-rgw-page), used again in
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**Diagnostics; `display_schema`.** Explicitly approve record fields for readable
output, including redaction or omission. [Lesson 11](../tutorial.md#11-the-discreet-log).

**Difference; subtraction; `without`.** Keep selected points while excluding
another selection or a single point. [Lesson 2](../tutorial.md#2-seat-3-is-reserved)
and [chapter 2½](../tutorial.md#2½-an-interval-interlude).

**Fields; field profile; `member_fields`.** An ordered list of record members
for a reusable consumer to observe. [Lesson 9](../tutorial.md#9-pick-the-basket-fields).

**`for_each_page`.** Drive a page loop with a reader, next-position callback,
and visitor. [Lesson 14](../tutorial.md#14-turn-the-page), applied to RGW in
[lesson 17](../tutorial.md#17-one-shard-two-pages).

**`from_bytes`; `to_bytes`.** Decode and encode an explicitly selected wire
representation. [Lesson 12](../tutorial.md#12-send-a-postcard).

### G–M

**Gaps.** Unselected positions between pieces of coverage.
[Chapter 2½](../tutorial.md#2½-an-interval-interlude) and
[covered-byte quotas](../tutorial.md#6-five-bytes-please).

**Intersection.** Select points that belong to both operands.
[Chapter 2½](../tutorial.md#2½-an-interval-interlude).

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

**Normalization.** Prepare ordered, merged coverage from unordered or overlapping
intervals. [Lesson 3](../tutorial.md#3-untangle-the-ranges).

**`observe`.** A field profile calls a consumer for each selected value, in profile
order. [Lesson 9](../tutorial.md#9-pick-the-basket-fields).

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
controls for a consumer to interpret. [Lesson 15](../tutorial.md#15-write-the-order-ticket).

**Quota.** An upper limit on the amount selected from covered intervals.
[Lesson 6](../tutorial.md#6-five-bytes-please), stored with a query in
[lesson 15](../tutorial.md#15-write-the-order-ticket).

**Record view; `view_of`.** Observe the source record's current fields through a
borrowed view. [Lesson 8](../tutorial.md#8-a-window-into-misos-toy-basket).

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

**`take()`, fragmented bytes.** `byte_cursor::take()` returns a bounded slice
across fragments. [Lesson 1](../tutorial.md#1-hello-world--in-pieces).

**`take()`, covered quota.** `interval::take()` selects up to the requested measure
of covered intervals. [Lesson 6](../tutorial.md#6-five-bytes-please).

**View.** A way to observe data. Check the particular view's ownership and backing
lifetime: the record view borrows, while a retained view keeps its owner.
[Lessons 7](../tutorial.md#7-move-it-and-keep-reading) and
[8](../tutorial.md#8-a-window-into-misos-toy-basket).

**Visitor.** A callback that processes a page and returns a traversal decision.
[Lesson 14](../tutorial.md#14-turn-the-page). The entry-level callback in
[lesson 10](../tutorial.md#10-keep-your-place) makes acceptance decisions.

**Wire schema.** The selected fields and order of an encoded representation.
[Lesson 12](../tutorial.md#12-send-a-postcard). Native RGW frames are introduced in
[lesson 16](../tutorial.md#16-read-an-rgw-page).

## Further reading

The [bibliography](../tutorial.md#bibliography) collects the qualified API sources,
the greeting's history, and a short annotated background reading list.
