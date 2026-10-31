// Datum: Read-only interval projections, owning batches and scoped visitation.
// Connects standard ranges and lenses to the pure interval kernel.
//
// Please keep this layer independent of native record types, storage lifetimes and backend
// commit policy.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

#ifndef CEPH_RGW_DATUM_INTERVAL_RANGE_H
 #define CEPH_RGW_DATUM_INTERVAL_RANGE_H

#include "datum/interval/query.h"
#include "datum/ownership.h"
#include "datum/traversal.h"

#include <concepts>
#include <type_traits>

#include <utility>

#include <functional>
#include <ranges>

namespace ceph::salad::datum::interval {

template <typename LensT, typename ArgT, typename DomainT>
concept interval_projection_for = std::regular_invocable<const LensT&, ArgT> &&
                                  interval_for<std::invoke_result_t<const LensT&, ArgT>, DomainT>;

namespace detail {

template <typename LensT, typename ArgT>
using projection_result_t = std::invoke_result_t<const LensT&, ArgT>;

template <typename LensT, typename ArgT>
using projection_domain_t = expression_domain_t<projection_result_t<LensT, ArgT>>;

// A temporary element dies during dereference. Its geometry must retain its
// point storage before escaping; lvalue observations may continue to borrow.
template <typename LensT, typename ArgT, typename DomainT = projection_domain_t<LensT, ArgT>>
concept safe_interval_projection_for =
    interval_projection_for<LensT, ArgT, DomainT> &&
    (std::is_lvalue_reference_v<ArgT> ||
     ::ceph::salad::datum::owning_value<typename DomainT::value_type>);

// transform_view invokes F&. Force the const lens overload that was checked,
// and bind both const/nonconst iteration to the same domain.
template <ordered_domain DomainT, typename LensT>
class const_projection final
{
    [[no_unique_address]] LensT lens;

public:
    constexpr const_projection()
    requires std::default_initializable<LensT>
    = default;

    constexpr explicit const_projection(LensT value) : lens(std::move(value))
    {}

public:
    template <typename ArgT>
    requires safe_interval_projection_for<LensT, ArgT&&, DomainT>
    constexpr decltype(auto) operator()(ArgT&& source) const
    {
        using result_type = projection_result_t<LensT, ArgT&&>;

        constexpr bool borrows =
            std::is_lvalue_reference_v<ArgT&&> && std::is_reference_v<result_type>;
        if constexpr (borrows) {
            return static_cast<const std::remove_reference_t<result_type>&>(
                std::invoke(std::as_const(lens), std::forward<ArgT>(source)));
        }

        if constexpr (!borrows) {
            constexpr bool copies =
                !std::is_lvalue_reference_v<ArgT&&> &&
                (std::is_reference_v<result_type> || !canonical_interval<result_type>);
            if constexpr (copies) {
                // Copy borrowed geometry while its temporary source still lives.
                return copy_interval(std::invoke(std::as_const(lens), std::forward<ArgT>(source)));
            }

            if constexpr (!copies) {
                // An already canonical value needs no second endpoint copy.
                return std::invoke(std::as_const(lens), std::forward<ArgT>(source));
            }
        }
    }

};

} // detail

// Ordinary constructor: lvalue containers are borrowed; movable rvalues owned.
// Iterators depend on the stored lens, so this is not a borrowed_range.
template <std::ranges::view ViewT, std::move_constructible LensT>
requires std::ranges::input_range<ViewT> &&
         detail::safe_interval_projection_for<LensT, std::ranges::range_reference_t<ViewT>>
class intervals_view final : public std::ranges::view_interface<intervals_view<ViewT, LensT>>
{
    using projected_domain =
        detail::projection_domain_t<LensT, std::ranges::range_reference_t<ViewT>>;
    using projection_type =
        std::ranges::transform_view<ViewT, detail::const_projection<projected_domain, LensT>>;

    projection_type projection;

public:
    using domain_type = projected_domain;

public:
    constexpr intervals_view()
    requires std::default_initializable<ViewT> && std::default_initializable<LensT>
    = default;

    template <std::ranges::viewable_range RangeT, typename ProjectionT>
    requires std::same_as<std::views::all_t<RangeT>, ViewT> &&
             std::constructible_from<LensT, ProjectionT>
    constexpr explicit intervals_view(RangeT&& source, ProjectionT&& lens)
        : projection(std::views::all(std::forward<RangeT>(source)),
                     detail::const_projection<projected_domain, LensT> {
                         LensT(std::forward<ProjectionT>(lens))})
    {}

public:
    constexpr auto begin(this auto& self)
    requires std::ranges::range<decltype((self.projection))>
    {
        return std::ranges::begin(self.projection);
    }

    constexpr auto end(this auto& self)
    requires std::ranges::range<decltype((self.projection))>
    {
        return std::ranges::end(self.projection);
    }

public:
    constexpr auto size(this auto& self)
    requires std::ranges::sized_range<decltype((self.projection))>
    {
        return std::ranges::size(self.projection);
    }

};

template <std::ranges::viewable_range RangeT, typename LensT>
intervals_view(RangeT&&, LensT&&) -> intervals_view<std::views::all_t<RangeT>, std::decay_t<LensT>>;

// Own the points without introducing a second container or iterator protocol.
// Capacity bounds entries, not allocations made by points or source storage.
template <ordered_domain DomainT, std::size_t Capacity>
requires ::ceph::salad::datum::owning_value<typename DomainT::value_type>
using interval_batch = static_interval_set<DomainT, Capacity>;

using ::ceph::salad::datum::acceptance;
using ::ceph::salad::datum::visit_result;

// Canonical values are scoped to each call; borrowed point domains remain
// usable with lvalue elements. The standard views handle projection, while
// visit_range alone governs consumer effects and stopping.
template <ordered_domain DomainT, interval_range_for<DomainT> RangeT, typename ConsumerT>
requires detail::safe_interval_projection_for<std::identity,
                                              std::ranges::range_reference_t<RangeT>, DomainT> &&
         std::invocable<ConsumerT&, const query<DomainT>&> &&
         std::same_as<std::invoke_result_t<ConsumerT&, const query<DomainT>&>, acceptance>
[[nodiscard]] constexpr visit_result visit_intervals(RangeT&& source, ConsumerT&& consumer)
{
    using reference = std::ranges::range_reference_t<RangeT>;

    if constexpr (canonical_interval<reference> && std::is_lvalue_reference_v<reference>) {
        return ::ceph::salad::datum::visit_range(
                  source, [&consumer](const query<DomainT>& value) { return std::invoke(consumer, value); });
    }

    if constexpr (!(canonical_interval<reference> && std::is_lvalue_reference_v<reference>)) {
        auto values = std::ranges::transform_view {std::ranges::ref_view {source},
            [](const auto& value) { return copy_interval(value); }};

        return ::ceph::salad::datum::visit_range(
                  values, [&consumer](const query<DomainT>& value) { return std::invoke(consumer, value); });
    }
}

// The interval range already carries its domain; keep explicit selection available.
template <std::ranges::input_range RangeT, typename ConsumerT>
requires requires(RangeT&& source, ConsumerT&& consumer) {
    interval::visit_intervals<expression_domain_t<std::ranges::range_reference_t<RangeT>>>(
        std::forward<RangeT>(source), std::forward<ConsumerT>(consumer));
}
[[nodiscard]] constexpr visit_result visit_intervals(RangeT&& source, ConsumerT&& consumer)
{
    return interval::visit_intervals<expression_domain_t<std::ranges::range_reference_t<RangeT>>>(
        std::forward<RangeT>(source), std::forward<ConsumerT>(consumer));
}

} // ceph::salad::datum::interval

namespace ceph::salad::datum {

// Canonical geometry owns exactly what its point values own. Views are not
// enrolled: retaining a range or lens does not prove ownership of its payload.
template <interval::ordered_domain DomainT>
struct ownership_traits<interval::boundary<DomainT>>
    : std::bool_constant<owning_value<typename DomainT::value_type>>
{};

template <interval::ordered_domain DomainT>
struct ownership_traits<interval::query<DomainT>>
    : std::bool_constant<owning_value<typename DomainT::value_type>>
{};

template <interval::ordered_domain DomainT>
struct ownership_traits<interval::canonical_set<DomainT>>
    : std::bool_constant<owning_value<interval::query<DomainT>>>
{};

template <interval::ordered_domain DomainT, std::size_t Capacity>
struct ownership_traits<interval::static_interval_set<DomainT, Capacity>>
    : std::bool_constant<owning_value<interval::query<DomainT>>>
{};

} // ceph::salad::datum

#endif // CEPH_RGW_DATUM_INTERVAL_RANGE_H
