// Datum: Cached public member facts for reflected records.
// Shared by ownership, views and explicitly enrolled schemas above the core.
//
// Please keep record policy and native type recognition in consumer profiles.
// Reflection describes structure; it does not establish a record's meaning.

#ifndef CEPH_RGW_DATUM_DETAIL_RECORD_H
 #define CEPH_RGW_DATUM_DETAIL_RECORD_H

#include <meta>
#include <type_traits>

namespace ceph::salad::datum::detail {

template <typename RecordT>
inline constexpr auto record_members = std::define_static_array(
    std::meta::nonstatic_data_members_of(^^RecordT, std::meta::access_context::unprivileged()));

template <auto Member>
requires std::is_member_object_pointer_v<decltype(Member)> && (nullptr != Member)
consteval std::meta::info record_member()
{
    constexpr auto selected =
        []<typename RecordT, typename FieldT>(FieldT RecordT::*) -> std::meta::info {
        template for (constexpr auto field : record_members<RecordT>) {
            using field_type = [:std::meta::type_of(field):];

            if constexpr (!std::meta::is_bit_field(field) && !std::is_reference_v<field_type>) {
                if constexpr (std::meta::reflect_constant(Member) ==
                              std::meta::reflect_constant(&[:field:])) {
                    return field;
                }
            }
        }

        return {};
    }(Member);
    static_assert(selected != std::meta::info {},
                  "A field must select a direct public source member");

    return selected;
}

} // ceph::salad::datum::detail

#endif // CEPH_RGW_DATUM_DETAIL_RECORD_H
