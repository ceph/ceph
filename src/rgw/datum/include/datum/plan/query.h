// Datum: Typed composition of geometry, requirements, residuals and controls.
// Query descriptions above the pure algebra; execution belongs to consumers.
//
// Please keep this layer independent of backend capabilities, application predicates and
// execution effects.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_PLAN_QUERY_H
 #define CEPH_RGW_DATUM_PLAN_QUERY_H

#include "datum/interval/query.h"

#include <cstddef>
#include <type_traits>

#include <tuple>
#include <utility>

namespace ceph::salad::datum::plan {

namespace detail {

template <typename T>
concept tuple = requires { typename std::tuple_size<std::remove_cvref_t<T>>::type; };

template <tuple TupleT, typename ValueT>
constexpr auto append(TupleT&& tuple, ValueT&& value)
{
    return std::tuple_cat(std::forward<TupleT>(tuple),
                          std::tuple<std::remove_cvref_t<ValueT>> {std::forward<ValueT>(value)});
}

} // detail

template <interval::expression IntervalExprT, detail::tuple RequirementTupleT = std::tuple<>,
          detail::tuple ResidualTupleT = std::tuple<>, detail::tuple ControlTupleT = std::tuple<>>
struct query_expression final
{
    using interval_expression_type = IntervalExprT;
    using domain_type = typename IntervalExprT::domain_type;
    using requirement_tuple_type = RequirementTupleT;
    using residual_tuple_type = ResidualTupleT;
    using control_tuple_type = ControlTupleT;

    IntervalExprT intervals;
    RequirementTupleT requirements {};
    ResidualTupleT residuals {};
    ControlTupleT controls {};
};

namespace detail {

template <typename QueryT, typename IntervalExprT, typename RequirementTupleT,
          typename ResidualTupleT, typename ControlTupleT>
constexpr auto rebuild_query(IntervalExprT&& intervals, RequirementTupleT&& requirements,
                             ResidualTupleT&& residuals, ControlTupleT&& controls)
{
    return query_expression<typename std::remove_cvref_t<QueryT>::interval_expression_type,
                            std::remove_cvref_t<RequirementTupleT>,
                            std::remove_cvref_t<ResidualTupleT>,
                            std::remove_cvref_t<ControlTupleT>> {
        .intervals = std::forward<IntervalExprT>(intervals),
        .requirements = std::forward<RequirementTupleT>(requirements),
        .residuals = std::forward<ResidualTupleT>(residuals),
        .controls = std::forward<ControlTupleT>(controls)};
}

} // detail

template <typename T>
concept query =
    requires {
        typename std::remove_cvref_t<T>::interval_expression_type;
        typename std::remove_cvref_t<T>::requirement_tuple_type;
        typename std::remove_cvref_t<T>::residual_tuple_type;
        typename std::remove_cvref_t<T>::control_tuple_type;
    } && interval::expression<typename std::remove_cvref_t<T>::interval_expression_type> &&
    detail::tuple<typename std::remove_cvref_t<T>::requirement_tuple_type> &&
    detail::tuple<typename std::remove_cvref_t<T>::residual_tuple_type> &&
    detail::tuple<typename std::remove_cvref_t<T>::control_tuple_type>;

template <query QueryT>
inline constexpr auto requirement_count_v =
    std::tuple_size_v<typename std::remove_cvref_t<QueryT>::requirement_tuple_type>;

template <query QueryT>
inline constexpr auto residual_count_v =
    std::tuple_size_v<typename std::remove_cvref_t<QueryT>::residual_tuple_type>;

template <query QueryT>
inline constexpr auto control_count_v =
    std::tuple_size_v<typename std::remove_cvref_t<QueryT>::control_tuple_type>;

template <typename T>
concept resolved_query = query<T> && 0 == requirement_count_v<T>;

template <interval::expression IntervalExprT>
constexpr auto make_query(IntervalExprT&& intervals)
{
    using interval_expression_type = std::remove_cvref_t<IntervalExprT>;

    return query_expression<interval_expression_type> {.intervals =
                                                           std::forward<IntervalExprT>(intervals)};
}

template <query QueryT, typename RequirementT>
constexpr auto with_requirement(QueryT query, RequirementT&& requirement)
{
    return detail::rebuild_query<QueryT>(
        std::move(query.intervals),
        detail::append(std::move(query.requirements), std::forward<RequirementT>(requirement)),
        std::move(query.residuals), std::move(query.controls));
}

template <query QueryT, typename ResidualT>
constexpr auto with_residual(QueryT query, ResidualT&& residual)
{
    return detail::rebuild_query<QueryT>(
        std::move(query.intervals), std::move(query.requirements),
        detail::append(std::move(query.residuals), std::forward<ResidualT>(residual)),
        std::move(query.controls));
}

template <query QueryT, typename ControlT>
constexpr auto with_control(QueryT query, ControlT&& control)
{
    return detail::rebuild_query<QueryT>(
        std::move(query.intervals), std::move(query.requirements), std::move(query.residuals),
        detail::append(std::move(query.controls), std::forward<ControlT>(control)));
}

template <query QueryT>
constexpr bool is_resolved(const QueryT&) noexcept
{
    return 0 == requirement_count_v<QueryT>;
}

template <query QueryT>
constexpr std::size_t requirement_count(const QueryT&) noexcept
{
    return requirement_count_v<QueryT>;
}

template <query QueryT>
constexpr std::size_t residual_count(const QueryT&) noexcept
{
    return residual_count_v<QueryT>;
}

template <query QueryT>
constexpr std::size_t control_count(const QueryT&) noexcept
{
    return control_count_v<QueryT>;
}

template <typename RequirementT, query QueryT>
constexpr const RequirementT& requirement(const QueryT& query) noexcept
{
    return std::get<RequirementT>(query.requirements);
}

template <typename ResidualT, query QueryT>
constexpr const ResidualT& residual(const QueryT& query) noexcept
{
    return std::get<ResidualT>(query.residuals);
}

template <typename ControlT, query QueryT>
constexpr const ControlT& control(const QueryT& query) noexcept
{
    return std::get<ControlT>(query.controls);
}

template <resolved_query QueryT>
constexpr auto compile_intervals(const QueryT& query)
{
    return interval::compile_intervals(query.intervals);
}

template <std::size_t Capacity, resolved_query QueryT>
constexpr auto compile_intervals(const QueryT& query)
{
    return interval::compile_intervals<Capacity>(query.intervals);
}

} // ceph::salad::datum::plan

#endif // CEPH_RGW_DATUM_PLAN_QUERY_H
