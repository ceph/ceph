// RGW CLS: Native encoders/decoders, compiled in the ordinary C++23 mode.
#include "olh_native_oracle.h"
#include "cls/rgw/cls_rgw_ops.h"

namespace rgw_olh_oracle {

namespace {

using byte_buffer = ::ceph::buffer::list;

void encode_key(const ::cls::rgw::olh::object_key& key, byte_buffer& bytes, const bool extensions)
{
    ENCODE_START(extensions ? 2 : 1, 1, bytes);
    ::ceph::encode(key.name, bytes);
    ::ceph::encode(key.instance, bytes);
    if (extensions) {
        ::ceph::encode(std::uint32_t {0xDEADBEEF}, bytes);
    }
    ENCODE_FINISH(bytes);
}

} // anonymous namespace

byte_buffer encode_request(const request& value, const std::uint8_t version, const bool extensions)
{
    byte_buffer bytes;
    ENCODE_START(version, 1, bytes);
    encode_key(value.key, bytes, extensions);
    ::ceph::encode(value.marker, bytes);
    ::ceph::encode(value.tag, bytes);
    if (version >= 2) {
        ::ceph::encode(value.stales, bytes);
    }
    if (extensions) {
        ::ceph::encode(std::uint32_t {0xC001CAFE}, bytes);
    }
    ENCODE_FINISH(bytes);

    return bytes;
}

request decode_request(const byte_buffer& bytes)
{
    ::rgw_cls_read_olh_log_op value;
    auto cursor = bytes.cbegin();
    ::decode(value, cursor);

    return {{std::move(value.olh.name), std::move(value.olh.instance)}, std::move(value.olh_tag),
            value.ver_marker, value.get_stales};
}

byte_buffer encode_reply(const ::cls::rgw::olh::reply& value, const bool extensions)
{
    byte_buffer bytes;
    ENCODE_START(extensions ? 2 : 1, 1, bytes);
    ::ceph::encode(static_cast<std::uint32_t>(value.epochs.size()), bytes);
    for (const auto& [epoch, entries] : value.epochs) {
        ::ceph::encode(epoch, bytes);
        ::ceph::encode(static_cast<std::uint32_t>(entries.size()), bytes);
        for (const auto& entry : entries) {
            ENCODE_START(extensions ? 2 : 1, 1, bytes);
            ::ceph::encode(entry.epoch, bytes);
            ::ceph::encode(static_cast<std::uint8_t>(entry.operation), bytes);
            ::ceph::encode(entry.tag, bytes);
            encode_key(entry.object, bytes, extensions);
            ::ceph::encode(entry.delete_marker, bytes);
            if (extensions) {
                ::ceph::encode(std::uint32_t {0xBADF00D}, bytes);
            }
            ENCODE_FINISH(bytes);
        }
    }
    ::ceph::encode(value.truncated, bytes);
    if (extensions) {
        ::ceph::encode(std::uint32_t {0x12345678}, bytes);
    }
    ENCODE_FINISH(bytes);

    return bytes;
}

::cls::rgw::olh::reply decode_reply(const byte_buffer& bytes)
{
    ::rgw_cls_read_olh_log_ret native;
    auto cursor = bytes.cbegin();
    ::decode(native, cursor);
    ::cls::rgw::olh::reply result;
    for (auto& [epoch, entries] : native.log) {
        auto& rows = result.epochs.emplace_back(epoch, std::vector<::cls::rgw::olh::event> {}).second;
        for (auto& entry : entries) {
            rows.push_back({{std::move(entry.key.name), std::move(entry.key.instance)}, std::move(entry.op_tag),
                           entry.epoch, static_cast<::cls::rgw::olh::action>(entry.op), entry.delete_marker});
        }
    }
    result.truncated = native.is_truncated;

    return result;
}

} // rgw_olh_oracle
