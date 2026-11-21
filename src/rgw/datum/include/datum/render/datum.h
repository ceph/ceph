// Datum: Diagnostic profiles for interval geometry, query plans and page progress.
// An optional observation layer above the pure interval kernel.
//
// Please keep this layer independent of application records and backend or wire semantics.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_RENDER_DATUM_H
 #define CEPH_RGW_DATUM_RENDER_DATUM_H

#include "datum/render/detail/datum_types.h"

#include <cstddef>

#include <optional>
#include <tuple>

#include <iterator>
#include <ranges>

#include <string_view>

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
requires render_detail::interval_value_displayable<typename interval::boundary_domain_t<BoundaryT>::value_type>
struct display_projection<BoundaryT>
{
    static constexpr render_detail::interval_boundary_display<BoundaryT>

    get(const BoundaryT& value)
    {
        using key_view = render_detail::borrowed_interval_value<
            typename interval::boundary_domain_t<BoundaryT>::value_type>;

        auto key = std::optional<key_view> {};

        if (interval::is_finite(value)) {
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

template <typename RangeT>
requires std::ranges::input_range<const RangeT> and
         render::displayable<std::ranges::range_reference_t<const RangeT>>
struct display_schema<render_detail::interval_set_display<RangeT>>
{
    static constexpr std::string_view name = "interval_set";
    static constexpr auto fields =
        std::tuple {field<[](const render_detail::interval_set_display<RangeT>& value) {
            return std::ranges::subrange(std::begin(value.intervals), std::end(value.intervals));
        }>("intervals")};
};

template <interval::ordered_domain DomainT, std::size_t Capacity>
requires render::displayable<interval::query<DomainT>>
struct display_projection<interval::static_interval_set<DomainT, Capacity>>
{
    using range_type = interval::static_interval_set<DomainT, Capacity>;

    static constexpr render_detail::interval_set_display<range_type> get(const range_type& value) noexcept
    {
        return {value};
    }
};

template <typename StorageT>
requires render::displayable<std::ranges::range_reference_t<const StorageT>>
struct display_projection<interval::canonical_range<StorageT>>
{
    using range_type = interval::canonical_range<StorageT>;

    static constexpr render_detail::interval_set_display<range_type> get(const range_type& value) noexcept
    {
        return {value};
    }
};

template <interval::detail::set_operation Operation, interval::expression LHS_T,
          interval::expression RHS_T>
requires render::displayable<LHS_T> and render::displayable<RHS_T>
struct display_schema<interval::detail::binary_expression<Operation, LHS_T, RHS_T>>
{
    using expression_type = interval::detail::binary_expression<Operation, LHS_T, RHS_T>;

    static constexpr std::string_view name = [] {
        if constexpr (interval::detail::set_operation::unite == Operation) {
            return "interval_union";
        }

        if constexpr (interval::detail::set_operation::intersect == Operation) {
            return "interval_intersection";
        }

        return "interval_difference";
    }();
    static constexpr auto fields =
        std::tuple {field<&expression_type::lhs>("lhs"), field<&expression_type::rhs>("rhs")};
};

} // ceph::salad::datum::render

// Datum: Diagnostic projections for Datum query descriptions.
// An optional observation layer above generic plan composition.
//
// Please keep this layer independent of application predicates, execution and backend
// policy.
// Put domain-specific behavior in consumer profiles or adapters.

namespace ceph::salad::datum::render {

template <plan::query QueryT>
requires render::displayable<decltype(std::declval<const QueryT&>().intervals)>
struct display_schema<render_detail::query_expression_display<QueryT>>
{
    static constexpr std::string_view name = "query_expression";
    static constexpr auto fields = std::tuple {
        field<[](const render_detail::query_expression_display<QueryT>& value)
                  -> decltype(auto) {
            return (value.query.intervals);
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

} // ceph::salad::datum::render

#if defined(__cpp_lib_reflection)
 #include "datum/render/detail/datum_schema.h"
#else
 #include "datum/compat/detail/datum_render.h"
#endif

#endif // CEPH_RGW_DATUM_RENDER_DATUM_H
