// RGW user CLS wire profiles over the existing Datum conversion hooks.
#include "user.h"
#include "rgw/datum/adapters/ceph/wire.h"

namespace cls::rgw::user_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::encode_frame;
using ::ceph::datum_wire::writer;

::ceph::buffer::list encode_bulk(const std::list<::cls_user_bucket_entry>& entries,
                                 const bool add, const ::ceph::real_time time)
{
    return encode_frame(1, [&](auto& archive) {
        archive.native(entries);
        archive.scalar(static_cast<std::uint8_t>(add));
        archive.native(time);
    });
}

::ceph::buffer::list encode_bucket(const ::cls_user_bucket& bucket)
{
    return encode_frame(1, [&](auto& archive) { archive.native(bucket); });
}

::ceph::buffer::list encode_bucket_list(const std::string_view marker,
                                        const std::string_view end_marker, const int maximum)
{
    return encode_frame(2, [&](auto& archive) {
        archive.text(marker);
        archive.native(maximum);
        archive.text(end_marker);
    });
}

::ceph::buffer::list encode_get_header()
{
    return encode_frame(1, [](auto&) {});
}

::ceph::buffer::list encode_time(const ::ceph::real_time time)
{
    return encode_frame(1, [&](auto& archive) { archive.native(time); });
}

::ceph::buffer::list encode_resource_add(const ::cls_user_account_resource& entry,
                                         const bool exclusive, const std::uint32_t limit)
{
    return encode_frame(1, [&](auto& archive) {
        archive.native(entry);
        archive.scalar(static_cast<std::uint8_t>(exclusive));
        archive.scalar(limit);
    });
}

::ceph::buffer::list encode_name(const std::string_view name)
{
    return encode_frame(1, [&](auto& archive) { archive.text(name); });
}

::ceph::buffer::list encode_resource_list(const std::string_view marker,
                                          const std::string_view path_prefix,
                                          const std::uint32_t maximum)
{
    return encode_frame(1, [&](auto& archive) {
        archive.text(marker);
        archive.text(path_prefix);
        archive.scalar(maximum);
    });
}

void decode(bulk_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        // Bucket-entry v1 predates framing and carries only name, size, and mtime.
        constexpr auto minimum_entry_size = sizeof(std::uint8_t) + sizeof(std::uint32_t) +
            sizeof(std::uint64_t) + sizeof(std::uint32_t);
        constexpr auto tail_size = sizeof(std::uint8_t) + ::ceph::datum_wire::encoded_time_size;
        candidate.entries = fields.template native<
            minimum_entry_size, decltype(candidate.entries)>(tail_size);
        candidate.add = 0 != fields.template scalar<std::uint8_t>();
        fields.native(candidate.time);
    });
}

void decode(bucket_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.bucket);
    });
}

void decode(bucket_list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<2>(value, input, [](auto& fields, auto& candidate, const std::uint8_t version) {
        fields.text(candidate.marker);
        fields.native(candidate.max_entries);
        if (version >= 2) {
            fields.text(candidate.end_marker);
        }
    });
}

void decode(empty_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto&, auto&, std::uint8_t) {});
}

void decode(time_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.time);
    });
}

void decode(reset_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.time);
        fields.text(candidate.marker);
        fields.native(candidate.acc_stats);
    });
}

void decode(resource_add_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.native(candidate.entry);
        candidate.exclusive = 0 != fields.template scalar<std::uint8_t>();
        candidate.limit = fields.template scalar<std::uint32_t>();
    });
}

void decode(name_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.name);
    });
}

void decode(resource_list_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        fields.text(candidate.marker);
        fields.text(candidate.path_prefix);
        candidate.max_entries = fields.template scalar<std::uint32_t>();
    });
}

void encode(const bucket_list_reply& value, ::ceph::buffer::list& output)
{
    writer archive {output};
    archive.frame(1, [&] {
        archive.native(value.entries);
        archive.text(value.marker);
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
    });
}

void encode(const header_reply& value, ::ceph::buffer::list& output)
{
    writer archive {output};
    archive.frame(1, [&] { archive.native(value.header); });
}

void encode(const reset_reply& value, ::ceph::buffer::list& output)
{
    writer archive {output};
    archive.frame(1, [&] {
        archive.text(value.marker);
        archive.native(value.acc_stats);
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
    });
}

void encode(const resource_reply& value, ::ceph::buffer::list& output)
{
    writer archive {output};
    archive.frame(1, [&] { archive.native(value.entry); });
}

void encode(const resource_list_reply& value, ::ceph::buffer::list& output)
{
    writer archive {output};
    archive.frame(1, [&] {
        archive.native(value.entries);
        archive.scalar(static_cast<std::uint8_t>(value.truncated));
        archive.text(value.marker);
    });
}

} // cls::rgw::user_datum
