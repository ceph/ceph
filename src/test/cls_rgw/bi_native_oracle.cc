// RGW CLS: Use the established op/res codecs as an independent C++23 oracle.
#include "bi_native_oracle.h"
#include "cls/rgw/cls_rgw_ops.h"

#include <utility>

namespace rgw_bi_oracle
{
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
    for (const auto& source : rows) {
        ::rgw_cls_bi_entry entry;
        entry.type = static_cast<::BIIndexType>(source.kind);
        entry.idx = source.key;
        entry.data = source.payload;
        value.entries.emplace_back(std::move(entry));
    }
    ::ceph::buffer::list bytes;
    ::encode(value, bytes);

    return bytes;
}

std::vector<row> decode_reply(const ::ceph::buffer::list& bytes, bool& truncated)
{
    ::rgw_cls_bi_list_ret value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);
    std::vector<row> rows;
    rows.reserve(value.entries.size());
    for (auto& entry : value.entries) {
        rows.emplace_back(static_cast<std::uint8_t>(entry.type), std::move(entry.idx), std::move(entry.data));
    }
    truncated = value.is_truncated;

    return rows;
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
