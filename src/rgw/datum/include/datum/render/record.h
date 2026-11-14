// Datum: Checked diagnostic enrollment over declared record members.
#ifndef CEPH_RGW_DATUM_RENDER_RECORD_H
 #define CEPH_RGW_DATUM_RENDER_RECORD_H

#include "datum/record_definition.h"
#include "datum/render/detail/schema.h"

namespace ceph::salad::datum::compat::detail {

template <typename RecordT, typename FieldT, auto ...Redacted>
consteval auto declared_display_field()
{
    constexpr auto accessor = [](const RecordT& value) noexcept -> decltype(auto) {
        return FieldT::get(value);
    };
    constexpr auto policy = (matches_member<Redacted, FieldT>() || ...)
        ? render::display_field_policy::redacted : render::display_field_policy::value;

    return render::display_field<accessor, policy> {FieldT::name()};
}

} // ceph::salad::datum::compat::detail

namespace ceph::salad::datum::render {

// A schema supplies its record label explicitly; member labels come from the declaration.
// Redaction selects direct member pointers. Reference-member policies use an explicit schema.
// Native C++26 uses reflected_display_schema to derive member identities.
template <compat::detail::declared_record RecordT, std::size_t ApprovedMembers, auto ...Redacted>
requires (std::is_member_object_pointer_v<decltype(Redacted)> and ...) and
         ((Redacted != nullptr) and ...)
struct declared_display_schema
{
    static_assert(ApprovedMembers == compat::detail::record_size<RecordT>,
                  "Review new diagnostic members and their redaction before enrollment");
    static_assert(((compat::detail::member_index<RecordT, Redacted>() != compat::detail::record_size<RecordT>) and ...),
                  "Redaction must name a member of the displayed record");

    static constexpr auto fields = compat::detail::with_field_types<RecordT>([]<typename ...FieldT>() {
        return std::tuple {compat::detail::declared_display_field<RecordT, FieldT, Redacted...>()...};
    });
};

} // ceph::salad::datum::render

#endif // CEPH_RGW_DATUM_RENDER_RECORD_H
