// RGW CLS: Use the established op/res codecs as an independent C++23 oracle.
#include "bi_native_oracle.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <utility>

namespace rgw_bi_oracle {

namespace {

void copy_rows(const std::vector<row>& rows, auto& destination)
{
    for (const auto& source : rows) {
        destination.push_back({static_cast<::BIIndexType>(source.kind), source.key, source.payload});
    }
}

std::vector<row> take_rows(auto& entries)
{
    std::vector<row> rows;
    rows.reserve(entries.size());
    for (auto& entry : entries) {
        rows.emplace_back(static_cast<std::uint8_t>(entry.type), std::move(entry.idx), std::move(entry.data));
    }

    return rows;
}

} // anonymous namespace

::ceph::buffer::list encode_get_request(const get_request& value, const std::uint8_t version)
{
    ::ceph::buffer::list bytes;
    if (version == 1) {
        ::rgw_cls_bi_get_op request;
        request.key = {value.name, value.instance};
        request.type = static_cast<::BIIndexType>(value.kind);
        ::encode(request, bytes);

        return bytes;
    }
    ENCODE_START(version, 1, bytes);
    {
        ENCODE_START(version, 1, bytes);
        ::ceph::encode(value.name, bytes);
        ::ceph::encode(value.instance, bytes);
        ::ceph::encode(std::uint64_t {0x11223344}, bytes);
        ENCODE_FINISH(bytes);
    }
    ::ceph::encode(value.kind, bytes);
    ::ceph::encode(std::uint64_t {0x55667788}, bytes);
    ENCODE_FINISH(bytes);

    return bytes;
}

get_request decode_get_request(const ::ceph::buffer::list& bytes)
{
    ::rgw_cls_bi_get_op value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);

    return {std::move(value.key.name), std::move(value.key.instance), static_cast<std::uint8_t>(value.type)};
}

::ceph::buffer::list encode_get_reply(const row& value)
{
    ::rgw_cls_bi_get_ret reply;
    reply.entry = {static_cast<::BIIndexType>(value.kind), value.key, value.payload};
    ::ceph::buffer::list bytes;
    ::encode(reply, bytes);

    return bytes;
}

row decode_get_reply(const ::ceph::buffer::list& bytes)
{
    ::rgw_cls_bi_get_ret value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);

    return {static_cast<std::uint8_t>(value.entry.type), std::move(value.entry.idx), std::move(value.entry.data)};
}

::ceph::buffer::list encode_request(const request& value, const std::uint8_t version)
{
    ::ceph::buffer::list bytes;
    ENCODE_START(version, 1, bytes);
    ::ceph::encode(value.maximum, bytes);
    ::ceph::encode(value.filter, bytes);
    ::ceph::encode(value.marker, bytes);
    if (version >= 2) {
        ::ceph::encode(value.reshard, bytes);
    }
    if (version > 2) {
        ::ceph::encode(std::uint64_t {0x12345678}, bytes);
    }
    ENCODE_FINISH(bytes);

    return bytes;
}

request decode_request(const ::ceph::buffer::list& bytes)
{
    ::rgw_cls_bi_list_op value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);

    return request {value.max, std::move(value.name_filter), std::move(value.marker), value.reshardlog};
}

::ceph::buffer::list encode_reply(const std::vector<row>& rows, const bool truncated)
{
    ::rgw_cls_bi_list_ret value;
    value.is_truncated = truncated;
    copy_rows(rows, value.entries);
    ::ceph::buffer::list bytes;
    ::encode(value, bytes);

    return bytes;
}

std::vector<row> decode_reply(const ::ceph::buffer::list& bytes, bool& truncated)
{
    ::rgw_cls_bi_list_ret value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);
    auto rows = take_rows(value.entries);
    truncated = value.is_truncated;

    return rows;
}

::ceph::buffer::list encode_put_request(const std::vector<row>& rows, const bool check_existing)
{
    ::rgw_cls_bi_put_entries_op value;
    value.check_existing = check_existing;
    copy_rows(rows, value.entries);
    ::ceph::buffer::list bytes;
    ::encode(value, bytes);

    return bytes;
}

std::vector<row> decode_put_request(const ::ceph::buffer::list& bytes, bool& check_existing)
{
    ::rgw_cls_bi_put_entries_op value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);
    auto rows = take_rows(value.entries);
    check_existing = value.check_existing;

    return rows;
}

::ceph::buffer::list encode_put_entry(const row& value)
{
    ::rgw_cls_bi_put_op request;
    request.entry = {static_cast<::BIIndexType>(value.kind), value.key, value.payload};
    ::ceph::buffer::list bytes;
    ::encode(request, bytes);

    return bytes;
}

row decode_put_entry(const ::ceph::buffer::list& bytes)
{
    ::rgw_cls_bi_put_op value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);

    return {static_cast<std::uint8_t>(value.entry.type), std::move(value.entry.idx), std::move(value.entry.data)};
}

::ceph::buffer::list extended_put_entry(const row& value)
{
    ::ceph::buffer::list bytes;
    ENCODE_START(2, 1, bytes);
    {
        ENCODE_START(3, 1, bytes);
        ::ceph::encode(value.kind, bytes);
        ::ceph::encode(value.key, bytes);
        ::ceph::encode(value.payload, bytes);
        ::ceph::encode(std::uint64_t {0xFFEEDDCC}, bytes);
        ENCODE_FINISH(bytes);
    }
    ::ceph::encode(std::uint64_t {0xAABBCCDD}, bytes);
    ENCODE_FINISH(bytes);

    return bytes;
}

::ceph::buffer::list extended_reply(const std::vector<row>& rows)
{
    ::ceph::buffer::list bytes;
    ENCODE_START(2, 1, bytes);
    ::ceph::encode(static_cast<std::uint32_t>(rows.size()), bytes);
    for (const auto& entry : rows) {
        ENCODE_START(3, 1, bytes);
        ::ceph::encode(entry.kind, bytes);
        ::ceph::encode(entry.key, bytes);
        ::ceph::encode(entry.payload, bytes);
        ::ceph::encode(std::uint64_t {0xFFEEDDCC}, bytes);
        ENCODE_FINISH(bytes);
    }
    ::ceph::encode(std::uint8_t {0x7F}, bytes);
    ::ceph::encode(std::uint64_t {0xAABBCCDD}, bytes);
    ENCODE_FINISH(bytes);

    return bytes;
}

::ceph::buffer::list encode_groups(const epoch_groups& groups)
{
    ::ceph::buffer::list bytes;
    ::ceph::encode(groups, bytes);

    return bytes;
}

epoch_groups decode_groups(const ::ceph::buffer::list& bytes)
{
    epoch_groups groups;
    auto cursor = bytes.cbegin();
    ::ceph::decode(groups, cursor);

    return groups;
}

} // rgw_bi_oracle
