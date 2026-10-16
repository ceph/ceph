Datum wire bindings for RGW CLS
==============================

The ``bi_get``, ``bi_list``, ``bi_put``, ``bi_put_entries`` and ``bucket_read_olh_log`` bindings
let the client and module use different owning records and containers. They
share the existing Ceph encoding, including versions, lengths, scalar widths,
element encodings and map semantics. A container category alone does not define
that contract.
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
Other RGW CLS operations continue to use their existing codecs.

Native callers remain C++23. With ``WITH_DATUM``, ``DATUM_ENABLE_ZPP`` and
``WITH_RGW_CLS_DATUM`` enabled, the private ``cls_rgw_datum`` object target
provides the C++26 implementation. Its language requirements and Datum headers
do not propagate through the client library's public interface. Disabling the
RGW option retains both client result interfaces using native codecs.

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
Successful decoding produces complete candidates before publishing outputs;
decode failures preserve the caller's previous result. Completion errors map
allocation failures to ``-ENOMEM`` and malformed responses to ``-EIO``.

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
The public interfaces remain C++23 and the private implementation still has two
C++26 translation units.

The module profiles preserve the older single-version-byte prepare/complete
envelopes and every subsequent field-version gate. Complete reads old name-only
removal sequences directly into key candidates as well as current key frames.
Link retains the version-2 seconds-only timestamp and the later precise timestamp
rules. Compatible future tails and nonzero booleans keep their native meaning.
Each decoder publishes its candidate and cursor only after the envelope succeeds.
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

All frame headers are version byte, compatibility byte and little-endian
uint32 body length. Counted strings and containers use little-endian uint32
lengths. Strings preserve arbitrary bytes. Decoders skip compatible frame
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

zpp archives require contiguous storage. RGW's shared adapter writes metadata
directly into Ceph append space, retains opaque BI payload fragments and gathers
only fixed metadata when an input field crosses fragments. It does not flatten
an entire native message. OLH encoding borrows the selected stored epoch range
for the duration of the synchronous encode, avoiding a second map of strings.
The selected range carries its epoch count; when get-stales is true, encoding
uses that count and each event vector's size without extra counting passes.

For true-stales reads, the module can project a canonical stored OLH log directly
from its encoded field. A bounded scan validates the complete record, including
events outside the selected page, and checks the tag before producing output.
It forwards the selected encoded range and rebuilds only the reply framing,
count and truncation flag. It does not construct a stored map or owning events.

This path requires canonical version/compatibility bytes, strictly ordered unique
epochs and normalized event booleans. Compatible extensions, noncanonical
booleans, duplicate or unsorted groups use the existing owning decoder and reply
encoder. False-stales reads retain that filtering path. The fallback decodes the
already-fetched value, so there is still one exact OMAP lookup. Native buffers
and this encoded-field projection remain RGW adapter concerns; no Datum-core
facility or native type enrollment is added.

Records whose buffer-owner metadata exceeds their encoded size also use the
native fallback: its single pass handles dense fragment chains more efficiently.
Encoded ranges use existing reply append space when they fit. Larger ordinary
ranges retain their owners. Those underlying allocations remain alive until the
reply is released and may contain additional stored events; avoiding
materialization does not imply minimum retained memory.

Every nested frame has its own read boundary. Counts are checked against minimum
encoded element sizes before reserving storage. These checks prevent impossible
allocations; they are not a total work or memory quota for an arbitrarily large
valid message. Native stored-object codecs and their resource policy remain
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
