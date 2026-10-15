// Datum: Diagnostic profiles for interval geometry, query plans and page progress.
// An optional observation layer above the pure interval kernel.
//
// Please keep this layer independent of application records and backend or wire semantics.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_RENDER_DATUM_H
 #define CEPH_RGW_DATUM_RENDER_DATUM_H

#include "datum/interval/query.h"
#include "datum/plan/query.h"
#include "datum/render/display.h"
#include "datum/traversal.h"

#include <concepts>
#include <cstddef>
#include <type_traits>

#include <optional>
#include <tuple>

#include <functional>
#include <ranges>

#include <string_view>

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
requires interval_value_displayable<typename BoundaryT::value_type>
struct interval_boundary_display final
{
    interval::boundary_kind kind;
    std::optional<borrowed_interval_value<typename BoundaryT::value_type>> key;
    bool inclusive;
};

template <interval::ordered_domain DomainT>
requires interval_value_displayable<typename DomainT::value_type>
struct interval_display final
{
    const interval::query<DomainT>& value;
};

template <interval::ordered_domain DomainT, std::size_t Capacity>
requires render::displayable<interval::query<DomainT>>
struct interval_set_display final
{
    const interval::static_interval_set<DomainT, Capacity>& intervals;
};

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

template <render_detail::interval_value_displayable ValueT>
struct display_projection<render_detail::borrowed_interval_value<ValueT>>
{
    static constexpr const ValueT&
    get(const render_detail::borrowed_interval_value<ValueT> value) noexcept
    {
        return value.value;
    }

};

template <interval::boundary_view BoundaryT>
requires render_detail::interval_value_displayable<typename BoundaryT::value_type>
struct display_schema<render_detail::interval_boundary_display<BoundaryT>> final
    : reflected_display_schema<^^render_detail::interval_boundary_display<BoundaryT>, 3>
{
    static constexpr std::string_view name = "interval_boundary";
};

template <interval::boundary_view BoundaryT>
requires render_detail::interval_value_displayable<typename BoundaryT::value_type>
struct display_projection<BoundaryT>
{
    static constexpr render_detail::interval_boundary_display<BoundaryT>

    get(const BoundaryT& value) noexcept
    {
        using key_view = render_detail::borrowed_interval_value<typename BoundaryT::value_type>;

        auto key = std::optional<key_view> {};

        if (value.finite()) {
            key.emplace(value.key());
        }

        return {.kind = value.kind(), .key = key, .inclusive = value.inclusive()};
    }

};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct display_schema<render_detail::interval_display<DomainT>>
{
    static constexpr std::string_view name = "interval";
    static constexpr auto fields =
        std::tuple {field<[](const render_detail::interval_display<DomainT>& view) -> const auto& {
                        return view.value.lower();
                    }>("lower"),
                    field<[](const render_detail::interval_display<DomainT>& view) -> const auto& {
                        return view.value.upper();
                    }>("upper"),
                    field<[](const render_detail::interval_display<DomainT>& view) {
                        return interval::is_empty(view.value);
                    }>("empty")};
};

template <interval::ordered_domain DomainT>
requires render_detail::interval_value_displayable<typename DomainT::value_type>
struct display_projection<interval::query<DomainT>>
{
    static constexpr render_detail::interval_display<DomainT>

    get(const interval::query<DomainT>& value) noexcept
    {
        return {value};
    }

};

template <interval::ordered_domain DomainT, std::size_t Capacity>
requires render::displayable<interval::query<DomainT>>
struct display_schema<render_detail::interval_set_display<DomainT, Capacity>>
{
    static constexpr std::string_view name = "interval_set";
    static constexpr auto fields =
        std::tuple {field<[](const render_detail::interval_set_display<DomainT, Capacity>& value) {
            return std::ranges::subrange(std::begin(value.intervals), std::end(value.intervals));
        }>("intervals")};
};

template <interval::ordered_domain DomainT, std::size_t Capacity>
requires render::displayable<interval::query<DomainT>>
struct display_projection<interval::static_interval_set<DomainT, Capacity>>
{
    static constexpr render_detail::interval_set_display<DomainT, Capacity>

    get(const interval::static_interval_set<DomainT, Capacity>& value) noexcept
    {
        return {value};
    }

};

template <interval::expression LHS_T, interval::expression RHS_T>
requires render::displayable<LHS_T> && render::displayable<RHS_T>
struct display_schema<interval::detail::difference_expr<LHS_T, RHS_T>>
{
    using expression_type = interval::detail::difference_expr<LHS_T, RHS_T>;

    static constexpr std::string_view name = "interval_difference";
    static constexpr auto fields =
        std::tuple {field<&expression_type::lhs>("lhs"), field<&expression_type::rhs>("rhs")};
};

template <interval::expression LHS_T, interval::expression RHS_T>
requires render::displayable<LHS_T> && render::displayable<RHS_T>
struct display_schema<interval::detail::intersection_expr<LHS_T, RHS_T>>
{
    using expression_type = interval::detail::intersection_expr<LHS_T, RHS_T>;

    static constexpr std::string_view name = "interval_intersection";
    static constexpr auto fields =
        std::tuple {field<&expression_type::lhs>("lhs"), field<&expression_type::rhs>("rhs")};
};

template <interval::expression LHS_T, interval::expression RHS_T>
requires render::displayable<LHS_T> && render::displayable<RHS_T>
struct display_schema<interval::detail::set_union_expr<LHS_T, RHS_T>>
{
    using expression_type = interval::detail::set_union_expr<LHS_T, RHS_T>;

    static constexpr std::string_view name = "interval_union";
    static constexpr auto fields =
        std::tuple {field<&expression_type::lhs>("lhs"), field<&expression_type::rhs>("rhs")};
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

} // ceph::salad::datum::render

// Datum: Diagnostic projections for Datum query descriptions.
// An optional observation layer above generic plan composition.
//
// Please keep this layer independent of application predicates, execution and backend
// policy.
// Put domain-specific behavior in consumer profiles or adapters.

namespace ceph::salad::datum::render_detail {

template <plan::query QueryT>
requires render::displayable<typename QueryT::interval_expression_type>
struct query_expression_display final
{
    const QueryT& query;
    std::size_t requirements;
    std::size_t residuals;
    std::size_t controls;
};

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

template <plan::query QueryT>
requires render::displayable<typename QueryT::interval_expression_type>
struct display_schema<render_detail::query_expression_display<QueryT>>
{
    static constexpr std::string_view name = "query_expression";
    static constexpr auto fields = std::tuple {
        field<[](const render_detail::query_expression_display<QueryT>& value)
                  -> const typename QueryT::interval_expression_type& {
            return value.query.intervals;
        }>("intervals"),
        field<&render_detail::query_expression_display<QueryT>::requirements>("requirements"),
        field<&render_detail::query_expression_display<QueryT>::residuals>("residuals"),
        field<&render_detail::query_expression_display<QueryT>::controls>("controls")};
};

template <interval::expression IntervalExprT, plan::detail::tuple RequirementTupleT,
          plan::detail::tuple ResidualTupleT, plan::detail::tuple ControlTupleT>
requires render::displayable<IntervalExprT>
struct display_projection<
    plan::query_expression<IntervalExprT, RequirementTupleT, ResidualTupleT, ControlTupleT>>
{
    using query_type =
        plan::query_expression<IntervalExprT, RequirementTupleT, ResidualTupleT, ControlTupleT>;

public:
    static constexpr render_detail::query_expression_display<query_type>

    get(const query_type& value) noexcept
    {
        return {.query = value,
                .requirements = plan::requirement_count(value),
                .residuals = plan::residual_count(value),
                .controls = plan::control_count(value)};
    }

};

} // ceph::salad::datum::render

// Datum: Diagnostic enrollment for generic page progress and cancellation.
// An optional observation layer above explicit page traversal.
//
// Please keep this layer independent of native cursors, I/O and wire policy.
// Put domain-specific behavior in consumer profiles or adapters.

namespace ceph::salad::datum::render_detail {

template <typename T>
concept page_traversal_display_value =
    std::integral<std::remove_cvref_t<T>> || text_value<T> || display_record<T> ||
    display_enum<T> || projected_display_value<T>;

} // ceph::salad::datum::render_detail

namespace ceph::salad::datum::render {

template <>
struct display_schema<traversal::page_traversal_options>
{
    static constexpr std::string_view name = "page_traversal_options";
    static constexpr auto fields =
        std::tuple {field<[](const traversal::page_traversal_options& options) {
                        return options.cancellation.stop_requested();
                    }>("cancellation_requested"),
                    field<&traversal::page_traversal_options::page_limit>("page_limit")};
};

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

#endif // CEPH_RGW_DATUM_RENDER_DATUM_H
