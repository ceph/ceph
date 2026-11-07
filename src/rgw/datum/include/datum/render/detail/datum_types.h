// Datum: Shared borrowing vocabulary for built-in diagnostic profiles.
#ifndef CEPH_RGW_DATUM_RENDER_DETAIL_DATUM_TYPES_H
 #define CEPH_RGW_DATUM_RENDER_DETAIL_DATUM_TYPES_H

#include "datum/interval/query.h"
#include "datum/plan/query.h"
#include "datum/render/display.h"
#include "datum/traversal.h"

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <optional>

#include <functional>

namespace ceph::salad::datum::render_detail {

template <typename T>
concept interval_value_displayable =
    render::displayable<T> || text_value<T> || std::integral<std::remove_cvref_t<T>>;

template <typename ValueT>
struct borrowed_interval_value final
{
    std::reference_wrapper<const ValueT> value;
};

template <interval::boundary_view BoundaryT>
requires interval_value_displayable<typename interval::boundary_domain_t<BoundaryT>::value_type>
struct interval_boundary_display final
{
    interval::boundary_kind kind;
    std::optional<borrowed_interval_value<typename interval::boundary_domain_t<BoundaryT>::value_type>> key;
    bool inclusive;
};

template <interval::ordered_domain DomainT>
requires interval_value_displayable<typename DomainT::value_type>
struct interval_display final
{
    const interval::query<DomainT>& value;
};

template <typename RangeT>
struct interval_set_display final
{
    const RangeT& intervals;
};

template <plan::query QueryT>
requires render::displayable<decltype(std::declval<const QueryT&>().intervals)>
struct query_expression_display final
{
    const QueryT& query;
    std::size_t requirements;
    std::size_t residuals;
    std::size_t controls;
};

template <typename T>
concept page_traversal_display_value =
    std::integral<std::remove_cvref_t<T>> || text_value<T> || display_record<T> ||
    display_enum<T> || projected_display_value<T>;

} // ceph::salad::datum::render_detail

#endif // CEPH_RGW_DATUM_RENDER_DETAIL_DATUM_TYPES_H
