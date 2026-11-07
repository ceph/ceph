// GC wire implementation shared only by the two RGW-owned CLS classes.
#include "gc.h"
#include "rgw/datum/adapters/ceph/wire.h"
#include "queued_gc.h"

namespace cls::rgw::gc_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::reader;
using ::ceph::datum_wire::encode_frame;

::ceph::buffer::list encode_set(const std::uint32_t expiration, const ::cls_rgw_gc_obj_info& info)
{
    return encode_frame(1, [&](auto& archive) {
        archive.scalar(expiration);
        archive.native(info);
    });
}

::ceph::buffer::list encode_defer(const std::uint32_t expiration, const std::string_view tag)
{
    return encode_frame(1, [&](auto& archive) {
        archive.scalar(expiration);
        archive.text(tag);
    });
}

::ceph::buffer::list encode_list(const std::string_view marker, const std::uint32_t maximum,
                                 const bool expired)
{
    return encode_frame(2, [&](auto& archive) {
        archive.text(marker);
        archive.scalar(maximum);
        archive.scalar(static_cast<std::uint8_t>(expired));
    });
}

::ceph::buffer::list encode_remove(const std::vector<std::string>& tags)
{
    return encode_frame(1, [&](auto& archive) { archive.native(tags); });
}

void decode(set_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.expiration_secs = fields.template scalar<std::uint32_t>();
        fields.native(candidate.info);
    });
}

void decode(defer_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.expiration_secs = fields.template scalar<std::uint32_t>();
        fields.text(candidate.tag);
    });
}

void decode(list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        fields.text(candidate.marker);
        candidate.max = fields.template scalar<std::uint32_t>();
        if (version >= 2) {
            candidate.expired_only = 0 != fields.template scalar<std::uint8_t>();
        }
    });
}

void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.tags = fields.template sequence<4, decltype(candidate.tags)>(
            [](auto& fields, auto& tag) { fields.text(tag); });
    });
}

void encode(const reply& value, ::ceph::buffer::list& output)
{
    auto candidate = encode_frame(2, [&](auto& archive) {
        archive.native(value.entries);
        archive.text(value.next_marker);
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
    });
    output.claim_append(candidate);
}

template <typename EntriesT>
requires ::ceph::concepts::same_as_any<EntriesT,
                                    std::vector<::cls_rgw_gc_obj_info>, std::list<::cls_rgw_gc_obj_info>>
list_reply<EntriesT> decode_reply(const ::ceph::buffer::list& input)
{
    auto cursor = input.cbegin();
    list_reply<EntriesT> candidate;
    reader archive {cursor};
    archive.frame(2, [&](auto& fields, const std::uint8_t version) {
        // Frames wrap info and its chain; uint32 fields hold tag length and chain object count.
        constexpr auto minimum_info_size = 2 * ::ceph::encoding_detail::struct_header_len() +
            2 * sizeof(std::uint32_t) + ::ceph::datum_wire::encoded_time_size;
        // The tail holds next_marker (v2+) and the final truncated flag.
        const auto tail_size = sizeof(std::uint8_t) + (version >= 2 ? sizeof(std::uint32_t) : 0);
        candidate.entries = fields.template native<minimum_info_size, EntriesT>(tail_size);
        if (version >= 2) {
            fields.text(candidate.next_marker);
        }
        candidate.truncated = 0 != fields.template scalar<std::uint8_t>();
    });

    return candidate;
}

template list_reply<std::vector<::cls_rgw_gc_obj_info>> decode_reply(const ::ceph::buffer::list& input);
template list_reply<std::list<::cls_rgw_gc_obj_info>> decode_reply(const ::ceph::buffer::list& input);

} // cls::rgw::gc_datum

namespace cls::rgw::queued_gc_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::encode_frame;
using ::ceph::datum_wire::writer;

::ceph::buffer::list encode_init(const std::uint64_t size, const std::uint64_t deferred)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(::ceph::salad::datum::binary_fields<&init_request::size, &init_request::num_deferred_entries> {},
                  1, init_request {size, deferred});

    return output;
}

::ceph::buffer::list encode_remove(const std::uint64_t count)
{
    return encode_frame(1, [&](auto& archive) { archive.scalar(count); });
}

void decode(init_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.size = fields.template scalar<std::uint64_t>();
        candidate.num_deferred_entries = fields.template scalar<std::uint64_t>();
    });
}

void decode(remove_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        candidate.num_entries = fields.template scalar<std::uint64_t>();
    });
}

} // cls::rgw::queued_gc_datum
