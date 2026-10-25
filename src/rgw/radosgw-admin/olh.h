// RGW admin: retain the native OLH diagnostic layout for independent log pages.
#ifndef CEPH_RGW_ADMIN_OLH_H
 #define CEPH_RGW_ADMIN_OLH_H

#include "common/ceph_json.h"
#include "rgw/datum/adapters/cls/olh_types.h"

#include <array>
#include <cstdint>

namespace rgw::admin {

inline void dump_olh_log(const ::cls::rgw::olh::reply& page, ::ceph::Formatter& formatter)
{
    constexpr std::array operation_names {
        "unknown", "link_olh", "unlink_olh", "remove_instance", "stale_olh_op"
    };

    formatter.open_object_section("result");
    ::encode_json("is_truncated", page.truncated, &formatter);
    formatter.open_array_section("log");
    for (const auto& [epoch, entries] : page.epochs) {
        formatter.open_object_section("entry");
        ::encode_json("key", epoch, &formatter);
        formatter.open_array_section("val");
        for (const auto& entry : entries) {
            formatter.open_object_section("obj");
            ::encode_json("epoch", entry.epoch, &formatter);
            const auto operation = static_cast<std::uint8_t>(entry.operation);
            const auto name = operation < operation_names.size() ? operation_names[operation] : "unknown";
            ::encode_json("op", name, &formatter);
            ::encode_json("op_tag", entry.tag, &formatter);
            formatter.open_object_section("key");
            ::encode_json("name", entry.object.name, &formatter);
            ::encode_json("instance", entry.object.instance, &formatter);
            formatter.close_section();
            ::encode_json("delete_marker", entry.delete_marker, &formatter);
            formatter.close_section();
        }
        formatter.close_section();
        formatter.close_section();
    }
    formatter.close_section();
    formatter.close_section();
}

} // rgw::admin

#endif // CEPH_RGW_ADMIN_OLH_H
