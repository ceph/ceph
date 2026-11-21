Datum wire bindings for RGW CLS
===============================

The RGW-owned ``cls_rgw``, ``cls_rgw_gc`` and ``cls_user`` classes bind all
63 structured operations to independent client and module records. Seven
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
Bucket-index log list and trim use independent requests. ``cls::rgw::bilog::reply<EntriesT>``
in ``bilog_types.h`` holds an owning entry collection and truncation flag. Clients
use its default list; the module uses a vector. Log list encodes that vector
directly and decodes the same sequence into the client's final reply type. The
completion publishes the whole reply after validation, retaining operation statuses,
allocation/decode error mapping, and output lifetime. The service keeps the same
per-shard lists, merge order, and continuation markers. Native log-entry codecs,
dencoder records, and independent wire oracles remain; the public value header
requires no wire implementation headers. Storage pagination semantics are unchanged.
Bucket directory listing also uses independent records. Its associative profile
preserves sorted keys, last-wins duplicate decoding and the existing flat-map
storage. ``bucket_types.h`` declares the existing ``bucket_datum::list_reply<EntriesT>``
family and its flat-map ``reply`` specialization. The client, module, sharded
service, and RADOS listing callers share this owning page directly. It retains
the directory header, entries, continuation marker, truncation, and filtering state.
The completion validates retry progress before publishing the whole page with a
nothrow move. Submitted-marker ownership, transport/reply error precedence, and
unchanged outputs on failure retain their existing contracts. The value declarations
have the same names as before, and the runnable tutorials keep their existing calls.
Header and directory-entry codecs, filtering, retry markers, and listing algorithms
stay native. Native outer records remain dencoder inputs and independent wire oracles.

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
The module's usage implementation is isolated behind its four native CLS entry
points in ``cls_rgw_usage.h``. Its private traversal borrows typed consumers and
const decoded entries; registration continues to use the shared method descriptors.
Later OMAP reads in one CLS call need not observe its earlier staged writes.
Combine same-key usage increments before submitting a batch when they must be summed.

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

Lifecycle operations use borrowed builders and independent module records. Shared
client bindings retain the native compatibility rules, including the unmarked
version-1 get-reply forms. Old list replies retain ordered first-wins map encoding while
borrowing bucket keys as string views. The RGW-owned entry type has default move
operations as well as copy operations, so existing moves and sorting can transfer
bucket storage. Leaf encoding, request-version negotiation and storage rules stay
unchanged. Current list decoding bounds row counts before reserving storage. Old
associations keep native first-wins map decoding, then transfer their keys into a
vector reserved to the distinct-key count without another string allocation.
Strictly ordered client pages avoid another sorting pass. Duplicate or disordered
pages retain the existing public sort, including its equal-key behavior.

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
Removal keeps one lazily loaded xattr map for the operation and persists its
erasures with the queue head. The extra lookahead row supplies an exclusive
removal marker; its deferred metadata remains intact. Removal derives changed-map
counts from their sizes and reconciles the xattr count after loading. The removal
quota retains its native 64-bit width. Datum supplies fallible row visitation; queue markers,
deferred-time comparisons, xattr codecs, and transactional storage stay here.
Listing and defer reuse the same lazy xattr owner. Head matches avoid xattr reads;
listing still counts every nondeferred row against its quota even when
``expired_only`` excludes it from the reply. Defer reconciles loaded counts,
checks capacity without counter overflow, and reuses the encoded head when a new
entry fits. Storage failures retain the native operation's error and staging rules.

User bucket and account operations also bind independent requests and replies.
Bulk bucket updates borrow the public list while encoding and decode directly
into a module vector. ``bucket_list_reply<EntriesT>`` holds the bucket page's entries,
marker, and truncation flag. The module encodes a vector; the client obtains a list
directly from ``user_datum::decode_bucket_list_reply(bytes)``. Both use the established
wire profile. Account listing retains its public vector interface.
Account-resource clients decode through ``user_datum::decode_resource_reply(bytes)``
and ``decode_resource_list_reply(bytes)``. The first returns an owning native
resource; the second reuses ``resource_list_reply`` for the entries, truncation flag,
and continuation marker. Metadata remains opaque to these bindings. Both finish
decoding before returning, so the existing owned completion can publish its outputs
after success. Resource-list row counts must fit the enclosing frame and required
tail before vector storage is reserved. That completion retains operation statuses,
optional outputs, and ``-EIO`` translation for buffer errors; allocation and other
exceptions propagate.
The native bucket-header callback uses ``user_datum::decode_header_reply(bytes,
header)``. This binding decodes in place and retains native partial field updates
on buffer errors. The existing callback receives that header with ``-EIO``; its
external header output is published only on complete success. Negative operation
status still suppresses the callback, and reference ownership is unchanged.
Paged reset-statistics carries the request's
accumulated entry, byte, and rounded-byte totals into each reply and the final
header. Intermediate pages leave the header unchanged; a failed final write
still returns the complete candidate totals. Retrying the final page uses that
same request, without advancing its accumulator. This protocol has no snapshot generation;
concurrent bucket updates can still invalidate a multi-page calculation.
The RADOS caller builds each page request with ``user_datum::encode_reset(time,
marker, accumulated)`` and decodes an owning ``reset_reply`` with
``decode_reset_reply(bytes)``. It passes the preceding reply's marker and totals
into the next request and obtains a fresh timestamp for each iteration. Transport
failures return immediately. The caller decodes a returned page before testing
continuation and returning the final method status, preserving the final candidate
reply on method failure. Malformed replies retain the caller's ``-EINVAL`` mapping.
The yield context and ``OPERATION_RETURNVEC`` path stay with the existing RADOS call.
Native leaf codecs, account key ordering and shared storage helpers stay unchanged.

Native callers and Datum wire implementations use C++23. Datum is registered with normal RGW builds;
RGW CLS requires its zpp adapter (``DATUM_ENABLE_ZPP``).
Three private object targets provide four
C++23 translation units: general RGW bindings and OLH log bindings in ``cls_rgw_datum``,
GC bindings in ``cls_rgw_datum_gc``, and user bindings in ``cls_user_datum``.
Their language requirements and Datum headers do not propagate through the
client libraries' public interfaces. Datum supplies the RGW-owned CLS wire
bindings; there is no alternate native implementation switch.
Native integration is grouped under ``rgw/datum/adapters/``. Ceph framing,
metadata I/O, bounds checks, and candidate publication live in ``ceph/wire.h``;
this consumer adapter uses ``datum::zpp`` and keeps native buffers outside
Datum's generic headers. The object-index key profile lives in ``rgw/key.h``,
and class records, wire profiles, and implementations live in ``cls/``.
The shared ``ceph-datum.h`` vocabulary supplies contiguous reads and fragment views
to the wire adapter and gateway deduplication. These consumers use ``datum::ceph``
and the C++23 core; they do not require Datum's optional C++26 reflection target.
Its ``try_decode(value, input)`` also shares native malformed-buffer handling across
the three modules. It returns false only for ``ceph::buffer::error`` and retains
native in-place mutation and cursor progress; allocation and other exceptions
propagate. The bufferlist overload retains the native complete-consumption assertion;
cursor inputs support prefixes and record sequences. Callers retain their existing
errno, logging, compatibility fallback, and publication rules. Decoders whose
diagnostics use the exception remain explicit.

Synchronous bucket-list callers combine transport and completion statuses through
``cls_rgw_bucket_list_result(transport_status, reply_status)``. Successful transport
and advance-and-retry permit a reply status to take precedence, including decode
and allocation failures. Other transport errors take precedence over the reply.
Asynchronous readers retain their error-category and retry classification contracts.

Native OMAP reads retain the owning SDK map interface while passing typed arguments
and results through each OSD's normal execution wrapper. They avoid a local query/reply
encoding round trip without exporting storage visitor callbacks. Access enforcement,
suboperation identities, encoded-equivalent read accounting, native quotas, and journal
overlays remain at that boundary. SeaStore conflict replay resets partial collections.
This execution mechanism has no dependency on Datum or on enabling RGW.
These native codec and retry policies remain outside Datum core.
The module also shares native namespace-key and adjacent-fence construction across
index, log, trim, and reshard operations. Namespace classification borrows byte views.
The fences retain the established native ordering; they are not general prefix-successor
bounds. Each caller retains its own endpoint inclusion and continuation rules, including
bilog's inclusive read end and its one-past-end deletion key. This native key layout
does not enter Datum's pure byte-string or interval vocabulary.
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

Synchronous OLH reads retain the encoded reply in the completion and decode on
the calling thread after the operation returns. Compound-operation overloads
decode in their completion, as their output must be ready at that boundary.

RGW's OLH read and replay path carries the independent page directly. The read
helper retains the encoded reply until the asynchronous read returns, then
decodes on the consuming coroutine and publishes the page and its truncation
flag together. Replay borrows its ordered epoch groups and converts keys only at
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

Entry codec template arguments select framed version 1 or compact version 2;
other entry profiles are rejected during compilation. Received versions and
compatibility bytes retain the runtime checks above.

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
-------------------------------

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
The public interfaces and private implementations both use C++23.

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
request encoder with a zero entry limit. Its ``bucket_datum::decode_header_reply(bytes,
header)`` binding shares the listing reader and validates the full directory,
entries, truncation flag, and marker. Header fields update in place, retaining native
partial data if a later field or enclosing bound fails. The directory is read through
a separate cursor before checking its parent bound so that such an error retains
the native header progress; ordinary page decoding keeps its earlier bound checks.
The callback receives decoded results and buffer errors even on negative operation
status. Malformed replies replace that status with ``-EIO``; a valid reply retains it.
Callback ownership, submission-failure handling, and propagation of other exceptions
are unchanged.

Bucket-list and bilog compound-operation builders require both a result reference
and a status reference, as the OLH builder does. Keep both alive until completion.
Their callbacks report malformed replies as ``-EIO`` and allocation failure as
``-ENOMEM`` while preserving the result. Callers retain transport-error precedence;
an advance-and-retry reply must decode and validate before its marker is used.
Such a reply must be empty and truncated, with a nonempty marker different from
the submitted marker. The completion owns that submitted marker until it runs,
compares both name and instance, and rejects inconsistent replies as ``-EIO``
before publishing outputs. Equality is the progress check: instance ordering
does not establish the native version-list order. Successful ordinary replies
retain their existing marker behavior.
Sharded readers incorporate the completion status into their error code before
choosing success, retry, or failure, so decode errors reach the listing caller.
Before reading a directory page, the module resolves any versioned starting marker.
Storage and decoding failures from that lookup return immediately with no reply;
they cannot restart traversal from an empty marker. An absent instance retains
the native position after that object's versions, including null-version handling.
Header-only requests do not resolve markers.
The module uses one continuation flag for backend availability and observed page
progress. An empty fetched page ends traversal even if the backend reports more,
retaining the existing rule without a parallel completion flag.

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

Native category-counter arithmetic lives in ``rgw_bucket_category_stats``.
Explicit construction from ``rgw_bucket_dir_entry_meta`` describes one entry:
accounted bytes, their rounded size, an entry count of one, and actual bytes.
Callers retain category selection and entry-eligibility rules. The ``+=`` and
``-=`` operators combine category totals, preserving unsigned 64-bit wraparound
and the existing 4096-byte rounding, including rounding overflow. The encoded
record and its version gates retain their existing form.

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

The native and client OLH action tags and the bucket-index namespace ordering
are checked at compile time.

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

Lifecycle client decoders keep their existing native value and status interfaces.
The shared bindings return owning values through ``decode_head_reply(bytes)``,
``decode_entry_reply(bytes)``, ``decode_next_entry_reply(bytes)``, and
``decode_list_reply(bytes)`` in ``lc_datum``. Head and entry field readers fill the
existing unpublished candidate, shared by client values and module messages without
an intermediate value transfer. The two entry reply functions preserve their distinct
historical v0/1 forms: ``get_entry`` tries a native entry before the old bucket/status
pair, while ``get_next_entry`` accepts the pair form. List replies preserve old map
normalization and current vector order; sorting remains in the public client layer.
These bindings finish decoding before returning a value. Public clients translate
buffer errors to ``-EIO`` and publish their outputs only after successful decoding
and any list ordering. Allocation and other exceptions retain their existing behavior.

The synchronous lifecycle and reshard-get clients share one private publication
policy: decode a complete value, assign it without throwing, and translate only
buffer errors to ``-EIO``. The decoder is borrowed for that synchronous call; it
needs no copy or default construction. I/O remains outside that boundary, and
operation-specific mappings and asynchronous callback policies remain separate.

Private wire helpers take field-selection descriptors first, archive or input
handles next, then data and associated options. Automatic and explicit-size
metadata writes share the leading arguments: ``archive.fields(selected, value)``
and ``archive.fields(selected, value, size)``. Keep long calls grouped by role;
avoid introducing wrapper records or tags merely to shorten a function name.
Readers use ``archive.payload(value)`` for a length-prefixed field and
``archive.payload(value, size)`` when preceding metadata already supplied its
length. Both validate the field against the current frame before replacing the
destination. BI entry readers group the adjacent kind and key-length fields in
one metadata read, then retain the usual owning key and payload representations.
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

Decoded index scans
-------------------

Plain and special index scans decode each selected stored record once. They borrow
that record, its storage key, and its encoded payload into a synchronous consumer.
The listing consumer retains encoded rows for the established reply format. Index
checking consumes the validated record directly, without an intermediate encoded
entry vector or a second decode. Consumers need only an invocation operation;
encoded collection additionally exposes optional reservation.

Datum's pure visitation machinery carries handled decode errors and progress with
``std::expected``. The native binding supplies Ceph decoders, translates malformed
records to ``-EIO``, and preserves key boundaries, exact-key inclusion, quota arithmetic,
and native markers. Statistics accumulated before a later failure remain local:
the original full-call check/rebuild publish no reply or header until the full scan
succeeds. Their wire format and concurrency contract remain available.

The additional ``bucket_check_index_page`` and ``bucket_rebuild_index_commit``
methods provide bounded maintenance with native object identity/version cursors.
Independent version-1 bindings encode the cursor, limits, page statistics, and final
totals. The client stages each page's totals before publishing its cursor, so decode
or allocation failures leave both unchanged. The server caps stored-row decoding at
1,000 rows and 4 MiB per call, and checks the generation before each page and the
final publication. Each new method must be the sole operation in its RADOS request.
The service falls back to the historical full-call protocol on older modules.
See `Bounded bucket-index maintenance
<../../../doc/dev/radosgw/cls-index-maintenance.rst>`_ for the exact budgets,
concurrency and retry rules, status codes, and a downloadable runnable client.

The same Datum acceptance and progress vocabulary serves SALAD listing consumers.
SALAD keeps object/prefix quotas, native row normalization and transaction policy
in its listing layer; CLS keeps stored-record validation and errno translation
here. Neither requires a second traversal implementation or a core record codec.
A shared decoded-row carrier would need matching ownership and lifetime contracts
in several native consumers before it earns a place in the Ceph vocabulary.

Paired instance/listing writes encode the unchanged record once and borrow those
bytes into the common native writer. Each index key still receives its own OMAP
write, reshard log record, and counter update in the established order. The writer
accepts either a native record or its encoded bytes; codec and reshard policy
remain at this boundary.

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
