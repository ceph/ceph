// Datum: Preferred native C++26 enrollment for built-in diagnostic profiles.
#ifndef CEPH_RGW_DATUM_RENDER_DETAIL_DATUM_SCHEMA_H
 #define CEPH_RGW_DATUM_RENDER_DETAIL_DATUM_SCHEMA_H

#include "datum/render/detail/datum_types.h"

namespace ceph::salad::datum::render {

template <interval::boundary_view BoundaryT>
requires render_detail::interval_value_displayable<typename interval::boundary_domain_t<BoundaryT>::value_type>
struct display_schema<render_detail::interval_boundary_display<BoundaryT>> final
    : reflected_display_schema<^^render_detail::interval_boundary_display<BoundaryT>, 3>
{
    static constexpr std::string_view name = "interval_boundary";
};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct display_schema<interval::lower_endpoint<DomainT>> final
    : reflected_display_schema<^^interval::lower_endpoint<DomainT>, 2>
{};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct display_schema<interval::upper_endpoint<DomainT>> final
    : reflected_display_schema<^^interval::upper_endpoint<DomainT>, 2>
{};

template <>
struct enum_schema<interval::boundary_kind> final : reflected_enum_schema<interval::boundary_kind>
{};

template <>
struct enum_schema<interval::endpoint_inclusion> final
    : reflected_enum_schema<interval::endpoint_inclusion>
{};

template <render_detail::page_traversal_display_value StateT>
struct display_schema<traversal::page_traversal_result<StateT>> final
    : reflected_display_schema<^^traversal::page_traversal_result<StateT>, 3>
{};

template <render_detail::page_traversal_display_value StateT,
          render_detail::page_traversal_display_value ErrorT>
struct display_schema<traversal::page_traversal_failure<StateT, ErrorT>> final
    : reflected_display_schema<^^traversal::page_traversal_failure<StateT, ErrorT>, 3>
{};

template <>
struct enum_schema<traversal::page_action> final : reflected_enum_schema<traversal::page_action>
{};

template <>
struct enum_schema<traversal::page_traversal_outcome> final
    : reflected_enum_schema<traversal::page_traversal_outcome>
{};

} // ceph::salad::datum::render

#endif // CEPH_RGW_DATUM_RENDER_DETAIL_DATUM_SCHEMA_H
