// RGW object-index key wire profile over the shared Ceph/Datum adapter.
#ifndef CEPH_RGW_DATUM_WIRE_H
 #define CEPH_RGW_DATUM_WIRE_H

#include "rgw/datum/adapters/ceph/wire.h"

#include <cstdint>

#include <string>
#include <string_view>
#include <utility>

namespace rgw::datum_wire {

namespace datum = ::ceph::salad::datum;

// An object key frame contains name and instance strings, each with a length prefix.
inline constexpr auto minimum_key_size =
    ::ceph::encoding_detail::struct_header_len() + 2 * sizeof(std::uint32_t);

struct key_fields final
{
    std::string_view name;
    std::string_view instance;
};

inline void encode_key(::ceph::datum_wire::writer& archive,
                       const std::string_view name, const std::string_view instance)
{
    archive.frame(1, [&] {
        archive.fields(datum::binary_fields<&key_fields::name, &key_fields::instance> {}, key_fields {name, instance});
    });
}

template <typename KeyT, auto Name, auto Instance>
void decode_key(datum::binary_fields<Name, Instance>,
                ::ceph::datum_wire::reader& archive, KeyT& value)
{
    archive.frame(1, [&](auto& fields, std::uint8_t) {
        fields.text(value.*Name);
        fields.text(value.*Instance);
    });
}

inline auto decode_key(::ceph::datum_wire::reader& archive)
{
    using key = std::pair<std::string, std::string>;

    key value;
    decode_key(datum::binary_fields<&key::first, &key::second> {}, archive, value);

    return value;
}

} // rgw::datum_wire

#endif // CEPH_RGW_DATUM_WIRE_H
