Datum wire bindings for RGW CLS
==============================

The ``bi_list`` and ``bucket_read_olh_log`` bindings let the client and module
use different owning records and containers. They share the existing Ceph
encoding, including versions, lengths, scalar widths, element encodings and
map semantics. A container category alone does not define that contract.
Other RGW CLS methods continue to use their existing codecs.

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

Wire contracts
--------------

All frame headers are version byte, compatibility byte and little-endian
uint32 body length. Counted strings and containers use little-endian uint32
lengths. Strings preserve arbitrary bytes. Decoders skip compatible frame
extensions and retain native nonzero-byte boolean semantics.

* BI request v2/compat1: maximum uint32, filter string, marker string, reshard
  byte. A v1 request defaults reshard to false.
* BI reply v1/compat1: counted entries, truncated byte. Each entry is a
  v1/compat1 frame containing kind byte, key string and counted opaque bytes.
* OLH request v2/compat1: key frame, epoch marker uint64, tag string, get-stales
  byte. A v1 request defaults get-stales to false.
* OLH key v1/compat1: name string, instance string.
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

The independent client event has different field names and declaration order.
Owning decode factories select the local record, while sequence storage and
associative insertion are separate bindings. No native record or buffer type is
globally enrolled in Datum and no Datum core customization is required.

zpp archives require contiguous storage. RGW's shared adapter writes metadata
directly into Ceph append space, retains opaque BI payload fragments and gathers
only fixed metadata when an input field crosses fragments. It does not flatten
an entire native message. OLH encoding borrows the selected stored epoch range
for the duration of the synchronous encode, avoiding a second map of strings.

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
client code with controlled transport/completion boundaries. The Linux OLH
module target loads the real plugin and supplies controlled OMAP reads. Those
tests complement live OSD validation; the controlled host is not an OSD.
