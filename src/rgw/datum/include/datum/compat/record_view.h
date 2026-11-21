// Datum: Temporary C++23 named-view type production over complete record facts.
#ifndef CEPH_RGW_DATUM_RECORD_VIEW_H
 #define CEPH_RGW_DATUM_RECORD_VIEW_H

#include "datum/record_definition.h"
#include "datum/detail/field.h"

namespace ceph::salad::datum::compat::detail {

template <typename FieldT>
concept projected_field = requires(const std::remove_cvref_t<typename FieldT::value_type>& source) {
    { member_view<std::remove_cvref_t<typename FieldT::value_type>>::get(source) } ->
        std::same_as<typename member_view<std::remove_cvref_t<typename FieldT::value_type>>::type>;
};

template <typename RecordT>
struct view_members
{
    static_assert(declared_record<RecordT>,
        "view_of needs complete C++23 record metadata; declare the record with DATUM_RECORD "
        "from <datum/record_definition.h>, or use member_fields to observe an existing record");

    // Keep metadata-dependent expressions out of the missing-metadata diagnostic.
    static consteval auto describe()
    {
        if constexpr (declared_record<RecordT>) {
            static_assert(with_field_types<RecordT>([]<typename ...FieldT>() {
                return (projected_field<FieldT> and ...);
            }), "A member projection must return its declared view type");

            // The factory declares types only; projection policies remain delayed.
            return RecordT::datum_cpp23_definition::view_builder().template operator()<RecordT>();
        }
    }

    using type = typename decltype(describe())::type;
};

} // ceph::salad::datum::compat::detail

#endif // CEPH_RGW_DATUM_RECORD_VIEW_H
