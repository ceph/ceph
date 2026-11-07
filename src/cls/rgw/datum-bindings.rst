Datum wire bindings for RGW CLS
==============================

The RGW-owned ``cls_rgw``, ``cls_rgw_gc`` and ``cls_user`` classes bind all
61 structured operations to independent client and module records. Seven
data-free entries (including an alias) and one encoded-stream entry need no
structured outer binding. The endpoints share explicit wire profiles, retaining
the existing Ceph encodings except for the negotiated BI-list reply below,
including versions, lengths, scalar widths, element encodings and map semantics.
A container category alone does not define that contract. Generic classes used
by RGW remain outside this migration.
The prepare, complete, OLH link and OLH trim builders borrow their arguments
during encoding, and their modules decode independent request records. Complete
decodes the client's removal-list encoding directly into a vector. Storage
algorithms and nested metadata, packed-version and zone codecs remain native.
OLH unlink, clear and refresh also use borrowed client encoders and independent
module records. Key-only and key/tag messages reuse the object-key profile;
unlink retains its tag and zone field-version gates and epoch receipt path.
Bucket-index log list and trim use independent requests. Log list encodes the
module's vector directly and decodes the same sequence into the public client
list, retaining native log-entry codecs and storage pagination semantics.
Bucket directory listing also uses independent records. Its associative profile
preserves sorted keys, last-wins duplicate decoding and the existing flat-map
storage. Header and directory-entry codecs, filtering and retry markers stay native.

Statistics updates borrow the public maps during encoding and decode ordered,
first-wins vector associations in the module. Index checks use independent reply
records with native header codecs; check/rebuild page sequences use vectors.
Tag-timeout requests also have independent bindings. Header mutation, accounting
and resharding rules retain their existing behavior.

Object removal, PG-version storage, attribute-prefix guards and modification-time
guards also bind independent requests. Removal decodes the public prefix-list
encoding into a vector; its client borrows the list. String arguments are borrowed
during encoding, and timestamps retain the native clock codec. Attribute storage,
missing-object behavior, precision and comparison rules remain native.

Usage add, read and trim use independent requests and read replies. Add borrows
its entry vector and skips the unused user string after validating its bounds.
Read replies decode first-wins associations into either a tree or flat map;
the public client moves its tree map and continuation string into caller outputs.
Nested usage-entry codecs, aggregation, key transition and scan policy stay native.

Reshard queue and bucket-control operations also use independent records.
List replies encode a module vector into the public list profile. Queue listing
decodes stored entries directly into final reply slots; get results move to their
caller. Borrowed builders retain full leaf encodings and the public set operation's
assert-exists ordering. Native limits, key checks and guard statuses stay unchanged.

Legacy GC operations in ``cls_rgw`` use borrowed builders and independent request
records. Listing moves decoded object chains directly into the module vector,
without the former append-only callback or chain copy. Public list replies retain
the native info/chain codecs, expired-only default, cutoff and continuation rules.
Fetched future rows do not cause eager reply allocation. Name/time-index storage
and cleanup remain native.

Lifecycle operations use borrowed builders and independent module records. The
client keeps its native compatibility decoders, including the unmarked version-1
get-reply forms. Old list replies retain ordered first-wins map encoding while
borrowing bucket keys as string views. The RGW-owned entry type has default move
operations as well as copy operations, so existing moves and sorting can transfer
bucket storage. Leaf encoding, request-version negotiation and storage rules stay
unchanged. Rebuild affected callers before measuring this header change.

Multipart update borrows the key and upload fields, retaining the native manifest
copy because its custom copy drops the encoded head placement rule. The module
uses an independent local request and decodes the native upload leaf in place:
copying or moving a decoded candidate would drop a received placement rule. The
cursor commits on success; a failed request may be partially decoded, is local,
and never reaches storage. No shared manifest implementation changes are needed.

The dedicated RGW GC queue wrapper reuses the legacy GC set/list/reply profiles
for enqueue, full-info defer, and listing. Only queue initialization and the
64-bit remove count need new scalar profiles. It borrows fetched queue records
and moves accepted object chains into vector replies. Shared queue operations,
storage helpers, urgent-data codecs, and public list outputs remain native.

User bucket and account operations also bind independent requests and replies.
Bulk bucket updates borrow the public list while encoding and decode directly
into a module vector. Bucket listing encodes its vector into the established
public list profile. Account listing retains its public vector interface. The
native bucket-header callback retains its existing partial-result behavior on decode errors; it does not acquire the BI/OLH
candidate-publication guarantee. Reset-statistics borrows the incoming buffer
and retains native handling of the accumulated header and update ordering.
Native leaf codecs, account key ordering and shared storage helpers stay unchanged.

Native callers remain C++23. Datum is registered with normal RGW builds;
RGW CLS requires its zpp adapter (``DATUM_ENABLE_ZPP``).
Three private object targets provide four
C++26 translation units: general RGW bindings and OLH log bindings in ``cls_rgw_datum``,
GC bindings in ``cls_rgw_datum_gc``, and user bindings in ``cls_user_datum``.
Their language requirements and Datum headers do not propagate through the
client libraries' public interfaces. Datum supplies the RGW-owned CLS wire
bindings; there is no alternate native implementation switch.
Native integration is grouped under ``rgw/datum/adapters/``. Ceph framing,
metadata I/O, bounds checks, and candidate publication live in ``ceph/wire.h``;
this consumer adapter uses ``datum::zpp`` and keeps native buffers outside
Datum's generic headers. The object-index key profile lives in ``rgw/key.h``,
and class records, wire profiles, and implementations live in ``cls/``.
The C++23 ``ceph/buffer_cursor.h`` helper is shared with gateway deduplication.
The general implementation is ``cls/rgw.cc``, retaining its per-family namespace
sections in one translation unit.

The C++23 adapter declarations provide selected compiled record and container
bindings. Their constraints list the available container types; unsupported
instantiations are rejected during compilation. BI list results provide
native-entry list and vector bindings, while
the lower-level wire profiles also accept independently selected record fields.

Independent client results
--------------------------

The established BI list API remains available. A caller can instead receive a
vector directly::

    std::vector<rgw_cls_bi_entry> entries;
    bool truncated = false;
    const int status = cls_rgw_bi_list(io, oid, filter, marker, maximum,
                                     entries, truncated);

An OLH caller can use the native map result or independent client records::

    cls::rgw::olh::reply page;
    const int status = cls_rgw_get_olh_log(io, oid, key, marker, tag, page);
    for (const auto& [epoch, events] : page.epochs) {
        for (const auto& event : events) {
            // event.object, event.tag, event.epoch, event.operation,
            // event.delete_marker belong to the client's representation.
        }
    }

The OLH compound-operation overload takes the same result and a completion
status reference. Keep those outputs alive until the operation completes.
These BI and OLH interfaces decode complete candidates before publishing
outputs; decode failures preserve the caller's previous result. Completion errors
map allocation failures to ``-ENOMEM`` and malformed responses to ``-EIO``.

RGW's OLH read and replay path carries the independent page directly. The read
helper publishes the page and its truncation flag together after successful
completion; replay borrows its ordered epoch groups and converts keys only at
native storage-effect boundaries. The admin log command renders the same page
using the established field names and action strings, without rebuilding a map.

Compact bucket-index list replies
---------------------------------

The BI-list client sends request version 3 with compatibility version 1 and the
same fields as version 2. Version 3 advertises support for compact replies. An
older module decodes those fields and returns its established reply; requests
through version 2 still receive that reply from a new module. No discovery
operation or retry is needed. The existing client signatures do not change.

A nonempty compact reply has outer version 2 and compatibility version 2.
Its body follows the ordinary zpp sequence profile: a little-endian uint32 count,
then each entry's uint8 kind, uint32-counted key and uint32-counted opaque payload,
followed by the existing byte flag. Datum selects the metadata fields; native
buffer operations retain fragmented payload owners. There is no intermediate
owning record or flattened payload. Removing each entry's six-byte frame saves
exactly six bytes per entry. Empty replies retain the existing eleven-byte form.

The decoder selects the entry profile using the compatibility byte, preserving
compatible future extensions of the original framed reply. It accepts both
reply forms into either public list or vector outputs. Counts and field lengths
are checked against the outer frame before storage is reserved or copied;
malformed responses preserve caller outputs. Future changes to compact entry
layout must establish a new wire profile and compatibility version, since entries have no individual
extension boundary. Stored records, batch puts and single-entry operations
retain their established encodings.

Single-entry reads
------------------

``cls_rgw_bi_get`` keeps its established native result interface. The Datum
request borrows the object's name and instance during encoding, and the module
decodes an independent operation record. BI get and OLH share the object-key
profile; BI get replies and single puts share the entry-message profile.
Successful client decoding moves the owned result to the caller, preserving
opaque payload owners without copying their chain. Transport errors pass
through, malformed replies return ``-EIO``, and allocation failures retain the
synchronous API's existing exception behavior.

The module performs one exact lookup after selecting the plain, instance or
OLH namespace. In the instance namespace, an empty instance selects the plain
name; the string ``null`` remains an instance. Invalid kinds return ``-EINVAL`` before storage access.
Storage errors pass through and payload bytes remain uninterpreted here; their
consumers decide which native stored record to decode.

Single-entry writes
-------------------

Both ``cls_rgw_bi_put`` overloads share one builder. The Datum binding borrows
the native entry during encoding and the module receives an independent owning
record. A single-entry message wraps the existing entry frame in an outer frame.
It performs no opaque payload interpretation or accounting. Native behavior is
preserved: reshard deletions ignore their payload, and set/remove errors are
logged while the operation returns success. Structural request errors still return
``-EINVAL`` before storage effects.

Borrowed index and OLH builders
------------------------------

Prepare and complete encode their key, tag, locator and metadata without creating
an owning request copy. Complete borrows removal lists and zone sets; link borrows
its optional metadata and zone set. A fragmented OLH tag is copied directly into
the encoded string field, without constructing a temporary string. Trim borrows
its key and tag. No input view escapes the synchronous builder.

The outer request versions and compatibility bytes remain prepare7/5,
complete9/7, link5/1 and trim1/1. The existing metadata, packed-version, removal
and zone codecs remain nested wire contracts. In particular, borrowing does not
change packed-integer boundary behavior. Optional arguments preserve their native
defaults, and link retains its existing zero bilog flags and epoch-output path.
The public interfaces remain C++23; the C++26 implementations remain private.

The private complete and link encoders accept borrowed views with named fields.
The client adapts its existing arguments directly, without constructing an owning
operation record::

    auto in = write_datum::encode(write_datum::complete_view {
      .operation = op, .operation_tag = tag, .locator = obj_locator,
      .key = key, .version = ver, .metadata = dir_meta,
      .removed_keys = remove_objs, .zones_trace = zones_trace,
      .log = log_op, .bilog_flags = bilog_flags});

The views retain references, string views, and optional pointers only during
encoding. Required objects remain references; absent optional inputs retain the
native defaults. Named fields distinguish the tags, object state, and logging
controls without changing the public client signatures.

The asynchronous directory-header helper uses the same borrowed bucket-list
request encoder with a zero entry limit. Its callback retains native decoding,
including a header decoded before a later response error.

The module profiles preserve the older single-version-byte prepare/complete
envelopes and every subsequent field-version gate. Complete reads old name-only
removal sequences directly into key candidates as well as current key frames.
Link retains the version-2 seconds-only timestamp and the later precise timestamp
rules. Compatible future tails and nonzero booleans keep their native meaning.
A shared RGW adapter publishes each decoder's candidate and cursor only after the
envelope succeeds, with no-throw move assignment required for publication.
Its ``decode_record`` overloads accept either a complete wire decoder or framed
field decoding, sharing the same publication boundary. The candidate is filled
in place; this requires neither move construction of the record nor a copy of
the decoding callable. BI get, entry and batch decoders use this same boundary.
No temporary native operation record or removal-list nodes are required.

Batch writes
------------

The existing ``cls_rgw_bi_put_entries`` API still consumes a vector by value.
The Datum encoder borrows those records only while constructing the request;
the operation retains its encoded bytes before the builder returns. Opaque
native payloads retain their backing buffers. The module decodes its own
``put_request`` and vector of independent entries, without a shared op class.

Batch requests reuse the same counted-entry wire profile as list replies.
The final flag means check-existing for a batch and truncated for a list.
Application semantics remain in RGW: existing keys are accounted once, every
incoming duplicate contributes statistics, and storage keeps the first incoming
value for a duplicate key. Reshard deletions run before the accumulated values
are written, and deletion errors remain nonfatal. Empty batches still perform
the native header and storage operations. The configured batch limit is checked
after structural decoding and before reading the header.

Malformed incoming opaque payloads return ``-EINVAL``; corrupt existing payloads
return ``-EIO``. RGW catches those decoding errors before they escape the operation.
OSD transaction handling rolls back staged changes on a failed operation.
Native stored-payload interpretation is shared without constructing an owning
native entry just to interpret an independent incoming record.

Wire contracts
--------------

Framed records have a version byte, compatibility byte and little-endian
uint32 body length. The legacy prepare/complete forms described above use their
older single-version-byte envelopes. Counted strings and containers use
little-endian uint32 lengths. Strings preserve arbitrary bytes. Decoders skip compatible frame
extensions and retain native nonzero-byte boolean semantics.

* BI list request v2/compat1: maximum uint32, filter string, marker string, reshard
  byte. A v1 request defaults reshard to false.
* BI list reply v1/compat1: counted entries, truncated byte. Each entry is a
  v1/compat1 frame containing kind byte, key string and counted opaque bytes.
* BI batch request v1/compat1: the same counted entries, check-existing byte.
* BI get request v1/compat1: object-key frame, kind byte.
* BI get reply and single put request v1/compat1: one entry frame, without a count or flag.
* OLH request v2/compat1: key frame, epoch marker uint64, tag string, get-stales
  byte. A v1 request defaults get-stales to false.
* Shared object key v1/compat1: name string, instance string.
* OLH reply v1/compat1: counted epoch groups, truncated byte. Each group has an
  epoch uint64 and counted events.
* OLH event v1/compat1: epoch uint64, action byte, tag string, key frame and
  delete-marker byte. The event epoch is independent of its group's key.

Unknown action bytes survive decoding. New OLH vectors have the native map's
numeric ordering and first-wins duplicate-key behavior. Compatible but unsorted
responses are sorted stably before removing duplicate groups; ordinary ordered
responses avoid sorting. Duplicate groups are fully decoded before discarding
their values, so a malformed duplicate does not silently bypass validation.

The module selects at most 1000 epochs strictly after the marker, then filters
stale events if requested. Filtering can produce an empty truncated page. Empty
groups survive only when get-stales is true. These are protocol semantics,
independent of the chosen local associative container.

Existing extension points and native buffers
--------------------------------------------

The private wire profiles select local fields with Datum ``binary_fields`` and
``member_lens``. ``serialize_schema`` delegates metadata encoding to the existing
Datum field hooks and zpp archive. For example, the OLH module binds its native
event fields in wire order::

    datum::binary_fields<&entry::epoch, &entry::op, &entry::op_tag,
                         &entry::key, &entry::delete_marker> {}

The independent client records have different field names and declaration order.
BI entries and OLH events use selected writable members to decode directly into
unpublished candidate storage. Request factories construct the remaining owning
request records; sequence storage and associative insertion remain separate
bindings. No native record or buffer type is globally enrolled in Datum and no
Datum core customization is required.

Metadata writers can derive their size from those same selected fields::

    archive.fields(datum::binary_fields<&prefix::maximum, &prefix::filter,
                                       &prefix::marker, &prefix::reshard> {},
                   prefix {maximum, filter, marker, reshard});

The RGW-local size observer invokes the existing Datum field codecs. Fixed
unsigned scalar widths compile to constants; strings contribute their runtime
lengths and four-byte prefixes. No payload is encoded or copied while sizing.
The writer checks the complete metadata size against its remaining message
capacity before allocating append space.
The automatic form supports unsigned scalars, ``std::string`` and
``std::string_view``, including field codecs that purely convert to those
representations. Such codecs run during both sizing and encoding. Other codecs,
or codecs for which a separate size observation is undesirable, retain the
explicit ``archive.fields(selected, value, size)`` form. The wire order is
specified once; no Datum or zpp core extension is needed. Fixed reader gather
bounds and frame-version requirements remain explicit protocol constraints.

Adapter interface conventions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use overloads for the same behavior when the existing argument types distinguish
the calls. For example, lifecycle callers use ``lc_datum::encode(entry)``,
``lc_datum::encode(head)`` and ``lc_datum::encode(marker, maximum)``. Distinct wire
operations with otherwise identical argument types retain descriptive names.

Private wire helpers take field-selection descriptors first, archive or input
handles next, then data and associated options. Automatic and explicit-size
metadata writes share the leading arguments: ``archive.fields(selected, value)``
and ``archive.fields(selected, value, size)``. Keep long calls grouped by role;
avoid introducing wrapper records or tags merely to shorten a function name.
Caller argument order is independent of encoded field order. Required Ceph
``encode(value, output)`` and ``decode(value, cursor)`` customization signatures
and established public client interfaces retain their conventions.

Native buffer handling
~~~~~~~~~~~~~~~~~~~~~~

zpp archives require contiguous storage. RGW's shared adapter writes metadata
directly into Ceph append space, retains opaque BI payload fragments and gathers
only fixed metadata when an input field crosses fragments. It does not flatten
an entire native message. When the stored OLH record is decoded, reply encoding
borrows the selected range of decoded epochs for the duration of the synchronous
encode, avoiding a second map of strings. The selected range carries its epoch
count; when ``get_stales`` is true, encoding uses that count and each event
vector's size without extra counting passes.

When ``get_stales`` is true, the module can also build the encoded reply directly
from canonical stored bytes. A bounded scan validates the complete record,
including events outside the selected page, and checks the tag before producing output.
It forwards the selected encoded range and rebuilds only the reply framing,
count and truncation flag. It does not construct a stored map or owning events.

This path requires canonical version/compatibility bytes, strictly ordered unique
epochs and normalized event booleans. Compatible extensions, noncanonical
booleans, duplicate or unsorted groups use the existing owning decoder and reply
encoder. When ``get_stales`` is false, replies use that filtering path. The
fallback decodes the already-fetched value without another OMAP lookup. Native
buffers and this encoded-field projection remain RGW adapter concerns; no
Datum-core facility or native type enrollment is added.

Records whose buffer-owner metadata exceeds their encoded size also use the
native fallback: its single pass handles dense fragment chains more efficiently.
Encoded ranges use existing reply append space when they fit. Larger ordinary
ranges retain their owners. Those underlying allocations remain alive until the
reply is released and may contain additional stored events; avoiding
materialization does not imply minimum retained memory.

Adapter-decoded nested frames have their own read boundaries. The RGW reader
shares one counted-sequence loop between native leaves and selected-field
decoders. Each binding supplies the minimum encoded element size and any
required trailing bytes; count checks precede storage reservation and in-place
element decoding. Vector and list bindings use the same loop.
Frame decoding also has one implementation for callbacks that inspect version
alone or both version and compatibility. Owning text and buffer fields share
their length-prefix checks and read operation; the destination type retains its
existing copy and ownership behavior.
These checks reject impossible outer allocations; they are not a total work or
memory quota for an arbitrarily large valid message. Native stored-object codecs and their resource policy remain
separate from the request/reply adapters.

Validation
----------

Focused Catch2 targets cover native C++23 encoding compatibility, independently
bound records, fragmented input, old/future frames, unknown bytes, truncation,
map order/duplicates and candidate publication. Client tests execute actual
client code with controlled transport/completion boundaries. Linux OLH, BI get and BI
put module targets share a plugin loader and supply controlled OMAP effects.
Batch cases cover duplicate accounting, overwrite checks, quota/error ordering,
reshard guards, empty batches and malformed opaque payloads. The controlled host
records staged effects; live OSD validation separately checks transaction rollback.
