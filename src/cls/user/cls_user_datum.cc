// RGW user CLS wire profiles over the existing Datum conversion hooks.
#include "cls_user_datum.h"
#include "common/datum_wire.h"

namespace cls::rgw::user_datum {

using ::ceph::datum_wire::decode_record;
using ::ceph::datum_wire::writer;

::ceph::buffer::list encode_bulk(const std::list<::cls_user_bucket_entry>& entries,
                                 const bool add, const ::ceph::real_time time)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        archive.native(entries);
        archive.scalar(static_cast<std::uint8_t>(add));
        archive.native(time);
    });

    return output;
}

::ceph::buffer::list encode_bucket(const ::cls_user_bucket& bucket)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] { archive.native(bucket); });

    return output;
}

::ceph::buffer::list encode_bucket_list(const std::string_view marker,
                                        const std::string_view end_marker, const int maximum)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(2, [&] {
        archive.text(marker);
        archive.native(maximum);
        archive.text(end_marker);
    });

    return output;
}

::ceph::buffer::list encode_get_header()
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [] {});

    return output;
}

::ceph::buffer::list encode_time(const ::ceph::real_time time)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] { archive.native(time); });

    return output;
}

::ceph::buffer::list encode_resource_add(const ::cls_user_account_resource& entry,
                                         const bool exclusive, const std::uint32_t limit)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        archive.native(entry);
        archive.scalar(static_cast<std::uint8_t>(exclusive));
        archive.scalar(limit);
    });

    return output;
}

::ceph::buffer::list encode_name(const std::string_view name)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] { archive.text(name); });

    return output;
}

::ceph::buffer::list encode_resource_list(const std::string_view marker,
                                          const std::string_view path_prefix,
                                          const std::uint32_t maximum)
{
    ::ceph::buffer::list output;
    writer archive {output};
    archive.frame(1, [&] {
        archive.text(marker);
        archive.text(path_prefix);
        archive.scalar(maximum);
    });

    return output;
}

void decode(bulk_request& value, ::ceph::buffer::list::const_iterator& input)
{
    decode_record<1>(value, input, [](auto& fields, auto& candidate, std::uint8_t) {
        // The oldest bucket entry has a one-byte version, empty name, size and mtime.
        // Reserve only after proving the count fits, including the bool/time tail.
        candidate.entries = fields.template native<17, decltype(candidate.entries)>(9);
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
