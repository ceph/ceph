// Datum: Ordered-domain algebra, canonical sweeps and typed endpoint helpers.
// The mathematical foundation for Datum data views and query plans.
//
// Please keep this layer independent of application records, wire formats, storage backends
// and I/O.
// Put domain-specific behavior in consumer profiles or adapters.

// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
// vim: ts=8 sw=4 sts=4 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_DATUM_INTERVAL_QUERY_H
 #define CEPH_RGW_DATUM_INTERVAL_QUERY_H

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <optional>
#include <stdexcept>
#include <utility>

#include <array>
#include <inplace_vector>
#include <span>
#include <vector>

#include <algorithm>
#include <functional>
#include <iterator>
#include <ranges>

namespace ceph::salad::datum::interval {

template <typename DomainT>
concept ordered_domain =
    requires (const typename DomainT::value_type& lhs, const typename DomainT::value_type& rhs) {
        typename DomainT::value_type;
        requires std::copy_constructible<typename DomainT::value_type>;
        { DomainT::compare(lhs, rhs) } -> std::same_as<std::strong_ordering>;
    };

template <typename DomainT>
concept successor_domain =
    ordered_domain<DomainT> && requires(const typename DomainT::value_type& value) {
        { DomainT::successor(value) } -> std::same_as<std::optional<typename DomainT::value_type>>;
    };

namespace detail {

struct expression_tag
{};
struct canonical_tag : expression_tag
{};

template <typename DomainT, typename ValueT>
concept comparable_value = ordered_domain<DomainT> &&
                           requires (const typename DomainT::value_type& key, const ValueT& value) {
                               {
                                   DomainT::compare(key, value)
                               } -> std::same_as<std::strong_ordering>;
                               {
                                   DomainT::compare(value, key)
                               } -> std::same_as<std::strong_ordering>;
                           };

} // detail

enum struct boundary_kind : std::uint8_t { negative_infinity, finite, positive_infinity };

template <ordered_domain DomainT>
class boundary_ref;
template <ordered_domain DomainT>
class query;
template <typename RowT, ordered_domain DomainT>
class cut;
namespace detail {

struct boundary_access;
} // detail

template <ordered_domain DomainT>
class boundary final
{

public:
    using domain_type = DomainT;
    using value_type = typename DomainT::value_type;

public:
    static constexpr boundary negative_infinity() noexcept
    {
        return boundary(boundary_kind::negative_infinity, std::nullopt, false);
    }

public:
    static constexpr boundary positive_infinity() noexcept
    {
        return boundary(boundary_kind::positive_infinity, std::nullopt, false);
    }

public:
    static constexpr boundary closed(value_type key)
    {
        return boundary(boundary_kind::finite, std::move(key), true);
    }

public:
    static constexpr boundary open(value_type key)
    {
        return boundary(boundary_kind::finite, std::move(key), false);
    }

public:
    constexpr boundary_kind kind() const noexcept { return position_kind; }

public:
    constexpr const value_type& key() const noexcept { return *point; }

public:
    constexpr bool finite() const noexcept { return boundary_kind::finite == position_kind; }

public:
    constexpr bool inclusive() const noexcept { return included; }

public:
    constexpr bool below_all() const noexcept
    {
        return boundary_kind::negative_infinity == position_kind;
    }

public:
    constexpr bool above_all() const noexcept
    {
        return boundary_kind::positive_infinity == position_kind;
    }

public:
    constexpr boundary with_inclusive(const bool inclusive) const&
    {
        auto out = *this;
        out.included = finite() && inclusive;

        return out;
    }

    constexpr boundary with_inclusive(const bool inclusive) &&
    {
        included = finite() && inclusive;

        return std::move(*this);
    }

public:
    constexpr bool operator==(const boundary& rhs) const
    {
        return position_kind == rhs.position_kind && included == rhs.included &&
               (!finite() || std::strong_ordering::equal == DomainT::compare(key(), rhs.key()));
    }

private:
    friend struct detail::boundary_access;
    template <ordered_domain OtherDomainT>
    friend class query;
    template <typename RowT, ordered_domain OtherDomainT>
    friend class cut;

private:
    constexpr boundary(value_type&& key, const bool inclusive)
        : point(std::in_place, std::move(key)), included(inclusive)
    {}

    constexpr boundary(const boundary_ref<DomainT>& source, const bool inclusive)
        : point(source.finite() ? std::optional<value_type> {std::in_place, source.key()}
                                : std::optional<value_type> {}),
          position_kind(source.kind()), included(source.finite() && inclusive)
    {}

    constexpr boundary(const boundary_kind kind, std::optional<value_type> key,
                       const bool inclusive)
        : point(std::move(key)), position_kind(kind), included(inclusive)
    {}

    std::optional<value_type> point;
    boundary_kind position_kind = boundary_kind::finite;
    bool included = false;

};

template <ordered_domain DomainT>
class boundary_ref final
{
    // The kind proves whether the borrowed key is present. Keep flags beside
    // the pointer to avoid padding between the two.

public:
    using domain_type = DomainT;
    using value_type = typename DomainT::value_type;

public:
    static constexpr boundary_ref negative_infinity() noexcept
    {
        return boundary_ref(boundary_kind::negative_infinity, nullptr, false);
    }

public:
    static constexpr boundary_ref positive_infinity() noexcept
    {
        return boundary_ref(boundary_kind::positive_infinity, nullptr, false);
    }

public:
    static constexpr boundary_ref closed(const value_type& key) noexcept
    {
        return boundary_ref(boundary_kind::finite, std::addressof(key), true);
    }

public:
    static constexpr boundary_ref open(const value_type& key) noexcept
    {
        return boundary_ref(boundary_kind::finite, std::addressof(key), false);
    }

public:
    constexpr boundary_kind kind() const noexcept { return position_kind; }

public:
    constexpr const value_type& key() const noexcept { return *point; }

public:
    constexpr bool finite() const noexcept { return boundary_kind::finite == position_kind; }

public:
    constexpr bool inclusive() const noexcept { return included; }

public:
    constexpr bool below_all() const noexcept
    {
        return boundary_kind::negative_infinity == position_kind;
    }

public:
    constexpr bool above_all() const noexcept
    {
        return boundary_kind::positive_infinity == position_kind;
    }

public:
    static constexpr boundary_ref closed(value_type&&) = delete;

public:
    static constexpr boundary_ref open(value_type&&) = delete;

public:
    static constexpr boundary_ref closed(const value_type&&) = delete;

public:
    static constexpr boundary_ref open(const value_type&&) = delete;

public:
    constexpr boundary_ref with_inclusive(const bool inclusive) const noexcept
    {
        return boundary_ref(position_kind, point, inclusive);
    }

private:
    constexpr boundary_ref(const boundary_kind kind, const value_type *key,
                           const bool inclusive) noexcept
        : point(key), position_kind(kind), included(boundary_kind::finite == kind && inclusive)
    {}

    const value_type *point = nullptr;
    boundary_kind position_kind = boundary_kind::finite;
    bool included = false;

};

namespace detail {

struct boundary_access final
{
    template <ordered_domain DomainT>
    static constexpr auto retain(const boundary_ref<DomainT>& source, const bool inclusive)
    {
        return boundary<DomainT> {source, inclusive};
    }

};
} // detail

// A small geometry value whose endpoints borrow independently owned points.
// Reused by canonical sweeps and origin-preserving clipping above the algebra.
template <ordered_domain DomainT>
struct borrowed_interval final
{
    using domain_type = DomainT;

    boundary_ref<DomainT> first;
    boundary_ref<DomainT> last;

public:
    constexpr const auto& lower() const noexcept { return first; }
    constexpr const auto& upper() const noexcept { return last; }

};

template <typename BoundT, typename DomainT>
concept boundary_view_for = requires(const std::remove_reference_t<BoundT>& bound) {
    typename std::remove_cvref_t<BoundT>::domain_type;
    requires std::same_as<typename std::remove_cvref_t<BoundT>::domain_type, DomainT>;
    { bound.kind() } -> std::same_as<boundary_kind>;
    { bound.key() } -> std::same_as<const typename DomainT::value_type&>;
    { bound.finite() } -> std::same_as<bool>;
    { bound.inclusive() } -> std::same_as<bool>;
    { bound.below_all() } -> std::same_as<bool>;
    { bound.above_all() } -> std::same_as<bool>;
};

template <typename BoundT>
concept boundary_view = requires {
    typename std::remove_cvref_t<BoundT>::domain_type;
    requires ordered_domain<typename std::remove_cvref_t<BoundT>::domain_type>;
    requires boundary_view_for<BoundT, typename std::remove_cvref_t<BoundT>::domain_type>;
};

template <boundary_view BoundT>
using boundary_domain_t = typename std::remove_cvref_t<BoundT>::domain_type;

template <boundary_view BoundT, typename ValueT>
requires detail::comparable_value<boundary_domain_t<BoundT>, ValueT>
constexpr bool allows_after(const BoundT& bound, const ValueT& value)
{
    if (!bound.finite()) {
        return bound.below_all();
    }

    const auto order = boundary_domain_t<BoundT>::compare(bound.key(), value);

    return std::strong_ordering::less == order ||
           (std::strong_ordering::equal == order && bound.inclusive());
}

template <boundary_view BoundT, typename ValueT>
requires detail::comparable_value<boundary_domain_t<BoundT>, ValueT>
constexpr bool allows_before(const BoundT& bound, const ValueT& value)
{
    if (!bound.finite()) {
        return bound.above_all();
    }

    const auto order = boundary_domain_t<BoundT>::compare(value, bound.key());

    return std::strong_ordering::less == order ||
           (std::strong_ordering::equal == order && bound.inclusive());
}

template <boundary_view BoundT>
constexpr auto materialize_boundary(const BoundT& bound)
{
    using domain_type = boundary_domain_t<BoundT>;

    if constexpr (std::same_as<BoundT, boundary_ref<domain_type>>) {
        return detail::boundary_access::retain(bound, bound.inclusive());
    }

    if (bound.below_all()) {
        return boundary<domain_type>::negative_infinity();
    }

    if (bound.above_all()) {
        return boundary<domain_type>::positive_infinity();
    }

    if (bound.inclusive()) {
        return boundary<domain_type>::closed(bound.key());
    }

    return boundary<domain_type>::open(bound.key());
}

template <boundary_view BoundT>
constexpr auto materialize_boundary(const BoundT& bound, const bool inclusive)
{
    using domain_type = boundary_domain_t<BoundT>;

    if constexpr (std::same_as<BoundT, boundary_ref<domain_type>>) {
        return detail::boundary_access::retain(bound, inclusive);
    }

    if (bound.below_all()) {
        return boundary<domain_type>::negative_infinity();
    }

    if (bound.above_all()) {
        return boundary<domain_type>::positive_infinity();
    }

    if (inclusive) {
        return boundary<domain_type>::closed(bound.key());
    }

    return boundary<domain_type>::open(bound.key());
}

template <ordered_domain DomainT>
constexpr auto closed_bound(typename DomainT::value_type key)
{
    return boundary<DomainT>::closed(std::move(key));
}

template <ordered_domain DomainT>
constexpr auto open_bound(typename DomainT::value_type key)
{
    return boundary<DomainT>::open(std::move(key));
}

template <ordered_domain DomainT>
class query final : public detail::expression_tag
{

public:
    using domain_type = DomainT;
    using value_type = typename DomainT::value_type;
    using boundary_type = boundary<DomainT>;

public:
    constexpr query() = default;

    constexpr query(boundary_type begin, boundary_type end)
        : lower_boundary(std::move(begin)), upper_boundary(std::move(end))
    {}

public:
    static constexpr query empty() noexcept
    {
        query out;
        out.forced_empty = true;

        return out;
    }

public:
    static constexpr query universal() noexcept { return query {}; }

public:
    static constexpr query singleton(value_type key)
    {
        auto upper_key = key;

        return query(std::move(key), std::move(upper_key), true, true);
    }

public:
    static constexpr query closed(value_type begin_key, value_type end_key)
    {
        return query(std::move(begin_key), std::move(end_key), true, true);
    }

public:
    static constexpr query open(value_type begin_key, value_type end_key)
    {
        return query(std::move(begin_key), std::move(end_key), false, false);
    }

public:
    static constexpr query closed_open(value_type begin_key, value_type end_key)
    {
        return query(std::move(begin_key), std::move(end_key), true, false);
    }

public:
    static constexpr query between(boundary_type begin, boundary_type end)
    {
        return query(std::move(begin), std::move(end));
    }

    template <boundary_view_for<DomainT> LowerT, boundary_view_for<DomainT> UpperT>
    static constexpr query between(const LowerT& begin, const UpperT& end)
    {
        if constexpr (std::same_as<std::remove_cvref_t<LowerT>, boundary_ref<DomainT>> &&
                      std::same_as<std::remove_cvref_t<UpperT>, boundary_ref<DomainT>>) {
            return query(begin, end);
        }

        return query(materialize_boundary(begin), materialize_boundary(end));
    }

public:
    static constexpr query prefix(value_type prefix)
    requires successor_domain<DomainT>
    {
        if (auto successor = DomainT::successor(prefix)) {
            return closed_open(std::move(prefix), std::move(*successor));
        }

        return query(boundary_type::closed(std::move(prefix)), boundary_type::positive_infinity());
    }

public:
    constexpr const boundary_type& lower() const noexcept { return lower_boundary; }

    constexpr const boundary_type& upper() const noexcept { return upper_boundary; }

public:
    constexpr bool explicitly_empty() const noexcept { return forced_empty; }

public:
    constexpr bool operator==(const query& rhs) const
    {
        return lower_boundary == rhs.lower_boundary && upper_boundary == rhs.upper_boundary &&
               forced_empty == rhs.forced_empty;
    }

private:
    constexpr query(value_type&& begin_key, value_type&& end_key, const bool lower_inclusive,
                    const bool upper_inclusive)
        : lower_boundary(std::move(begin_key), lower_inclusive),
          upper_boundary(std::move(end_key), upper_inclusive)
    {}

    constexpr query(const boundary_ref<DomainT>& begin, const boundary_ref<DomainT>& end)
        : lower_boundary(begin, begin.inclusive()), upper_boundary(end, end.inclusive())
    {}

    boundary_type lower_boundary = boundary_type::negative_infinity();
    boundary_type upper_boundary = boundary_type::positive_infinity();
    bool forced_empty = false;

};

namespace detail {

template <typename T>
concept has_explicit_empty = requires(const std::remove_reference_t<T>& x) {
    { x.explicitly_empty() } -> std::same_as<bool>;
};

template <typename BoundT>
using boundary_storage =
    std::conditional_t<std::is_lvalue_reference_v<BoundT>, BoundT, std::remove_cvref_t<BoundT>>;

template <typename T>
constexpr bool explicitly_empty(const T& x)
{
    if constexpr (has_explicit_empty<T>) {
        return x.explicitly_empty();
    }

    return false;
}

template <typename IntervalT>
struct interval_bounds final
{
    using domain_type = typename std::remove_cvref_t<IntervalT>::domain_type;
    using lower_result = decltype(std::declval<const IntervalT&>().lower());
    using upper_result = decltype(std::declval<const IntervalT&>().upper());

public:
    constexpr explicit interval_bounds(const IntervalT& interval)
        : first(interval.lower()), last(interval.upper())
    {}

public:
    constexpr const auto& lower() const noexcept { return first; }

    constexpr const auto& upper() const noexcept { return last; }

private:
    boundary_storage<lower_result> first;
    boundary_storage<upper_result> last;

};

template <typename IntervalT>
constexpr auto bounds_of(const IntervalT& x)
{
    return interval_bounds<IntervalT> {x};
}

} // detail

template <typename T>
concept interval_view = requires(const std::remove_reference_t<T>& x) {
    typename std::remove_cvref_t<T>::domain_type;
    requires ordered_domain<typename std::remove_cvref_t<T>::domain_type>;
    { x.lower() } -> boundary_view_for<typename std::remove_cvref_t<T>::domain_type>;
    { x.upper() } -> boundary_view_for<typename std::remove_cvref_t<T>::domain_type>;
};

template <typename IntervalT, typename DomainT>
concept interval_for = interval_view<IntervalT> &&
                       std::same_as<typename std::remove_cvref_t<IntervalT>::domain_type, DomainT>;

template <typename RangeT, typename DomainT>
concept interval_range_for = std::ranges::input_range<RangeT> &&
                             interval_for<std::ranges::range_reference_t<RangeT>, DomainT>;

template <typename T>
concept canonical_interval =
    interval_view<T> &&
    std::same_as<std::remove_cvref_t<T>, query<typename std::remove_cvref_t<T>::domain_type>>;

template <typename T>
concept expression =
    std::derived_from<std::remove_cvref_t<T>, detail::expression_tag> || interval_view<T>;

template <typename T>
concept domain_expression =
    expression<T> && requires { typename std::remove_cvref_t<T>::domain_type; };

template <typename LhsT, typename RhsT>
concept same_expression_domain = domain_expression<LhsT> && domain_expression<RhsT> &&
                                 std::same_as<typename std::remove_cvref_t<LhsT>::domain_type,
                                              typename std::remove_cvref_t<RhsT>::domain_type>;

// Opt in only when a value boundary's keys survive destruction of that
// boundary. Foreign profiles can certify this without native type recognition.
template <typename BoundT>
inline constexpr bool enable_borrowed_boundary = false;

template <ordered_domain DomainT>
inline constexpr bool enable_borrowed_boundary<boundary_ref<DomainT>> = true;

template <typename T>
concept stable_interval =
    interval_view<T> &&
    (std::is_lvalue_reference_v<decltype(std::declval<const T&>().lower())> ||
     enable_borrowed_boundary<std::remove_cvref_t<decltype(std::declval<const T&>().lower())>>) &&
    (std::is_lvalue_reference_v<decltype(std::declval<const T&>().upper())> ||
     enable_borrowed_boundary<std::remove_cvref_t<decltype(std::declval<const T&>().upper())>>);

// Enrollment certifies nonempty, ordered intervals with no coalescible neighbors.
// Borrowed sources must preserve this invariant for the entire observation.
template <typename T>
concept canonical_sequence =
    std::derived_from<std::remove_cvref_t<T>, detail::canonical_tag> &&
    std::ranges::forward_range<const std::remove_reference_t<T>> &&
    interval_range_for<const std::remove_reference_t<T>,
                       typename std::remove_cvref_t<T>::domain_type> &&
    std::is_lvalue_reference_v<std::ranges::range_reference_t<const std::remove_reference_t<T>>> &&
    stable_interval<std::ranges::range_reference_t<const std::remove_reference_t<T>>>;

namespace detail {

template <typename T>
using operand_storage =
    std::conditional_t<canonical_sequence<T> && std::is_lvalue_reference_v<T>,
                       const std::remove_reference_t<T>&, std::remove_cvref_t<T>>;

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
struct difference_expr final : expression_tag
{
    using domain_type = typename std::remove_cvref_t<LhsT>::domain_type;

    LhsT lhs;
    RhsT rhs;
};

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
struct intersection_expr final : expression_tag
{
    using domain_type = typename std::remove_cvref_t<LhsT>::domain_type;

    LhsT lhs;
    RhsT rhs;
};

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
struct set_union_expr final : expression_tag
{
    using domain_type = typename std::remove_cvref_t<LhsT>::domain_type;

    LhsT lhs;
    RhsT rhs;
};

} // detail

template <ordered_domain DomainT, typename SinkT>
constexpr void for_each_interval(const query<DomainT>& x, SinkT&& sink);

template <interval_view IntervalT, typename SinkT>
requires (!canonical_interval<IntervalT>)
constexpr void for_each_interval(const IntervalT& x, SinkT&& sink);

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const detail::difference_expr<LhsT, RhsT>& expression,
                                 SinkT&& sink);

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const detail::intersection_expr<LhsT, RhsT>& expression,
                                 SinkT&& sink);

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const detail::set_union_expr<LhsT, RhsT>& expression,
                                 SinkT&& sink);

template <canonical_sequence SequenceT, typename SinkT>
constexpr void for_each_interval(const SequenceT& source, SinkT&& sink)
{
    for (const auto& value : source) {
        interval::for_each_interval(value, sink);
    }
}

namespace detail {

template <boundary_view BoundT>
constexpr auto borrow_bound(const BoundT& bound)
{
    using ref = boundary_ref<boundary_domain_t<BoundT>>;

    if constexpr (std::same_as<BoundT, ref>) {
        return bound;
    }
    if (bound.below_all()) {
        return ref::negative_infinity();
    }
    if (bound.above_all()) {
        return ref::positive_infinity();
    }

    return bound.inclusive() ? ref::closed(bound.key()) : ref::open(bound.key());
}

enum struct inclusivity_merge : std::uint8_t { all, any };

constexpr bool merge_inclusive(const bool lhs, const bool rhs,
                               const inclusivity_merge merge) noexcept
{
    if (inclusivity_merge::all == merge) {
        return lhs && rhs;
    }

    return lhs || rhs;
}

template <boundary_view LhsT, boundary_view RhsT>
requires std::same_as<boundary_domain_t<LhsT>, boundary_domain_t<RhsT>>
constexpr std::strong_ordering compare_position(const LhsT& lhs, const RhsT& rhs)
{
    if (lhs.kind() != rhs.kind()) {
        return lhs.kind() <=> rhs.kind();
    }

    if (lhs.below_all() || lhs.above_all()) {
        return std::strong_ordering::equal;
    }

    return boundary_domain_t<LhsT>::compare(lhs.key(), rhs.key());
}

template <boundary_view LowerT, boundary_view UpperT>
requires std::same_as<boundary_domain_t<LowerT>, boundary_domain_t<UpperT>>
constexpr bool empty_bounds(const LowerT& lower, const UpperT& upper)
{
    const auto order = compare_position(lower, upper);

    if (std::strong_ordering::less == order) {
        return false;
    }

    if (std::strong_ordering::greater == order) {
        return true;
    }

    return !(lower.inclusive() && upper.inclusive());
}

template <typename BoundsT>
constexpr bool empty_bounds(const BoundsT& bounds)
{
    return empty_bounds(bounds.lower(), bounds.upper());
}

template <typename IntervalT, typename BoundsT>
constexpr bool empty_interval(const IntervalT& interval, const BoundsT& bounds)
{
    return explicitly_empty(interval) || empty_bounds(bounds);
}

template <typename SinkT, typename LowerT, typename UpperT>
concept boundary_sink_for = requires(SinkT&& sink, const LowerT& lower, const UpperT& upper) {
    std::forward<SinkT>(sink).emit_interval(lower, upper);
};

// CheckEmpty may be false only when the internal sweep proves nonempty geometry.
template <bool CheckEmpty = true, typename SinkT, boundary_view LowerT, boundary_view UpperT>
requires std::same_as<boundary_domain_t<LowerT>, boundary_domain_t<UpperT>>
constexpr bool emit_bounds(SinkT&& sink, const LowerT& lower, const UpperT& upper)
{
    using domain_type = boundary_domain_t<LowerT>;

    if constexpr (CheckEmpty) {
        if (empty_bounds(lower, upper)) {
            return false;
        }
    }

    if constexpr (boundary_sink_for<SinkT, LowerT, UpperT>) {
        std::forward<SinkT>(sink).emit_interval(lower, upper);
    }

    if constexpr (!(boundary_sink_for<SinkT, LowerT, UpperT>)) {
        std::invoke(std::forward<SinkT>(sink), query<domain_type>::between(lower, upper));
    }

    return true;
}

template <boundary_view LhsT, boundary_view RhsT>
requires std::same_as<boundary_domain_t<LhsT>, boundary_domain_t<RhsT>>
constexpr auto min_bound(const LhsT& lhs, const RhsT& rhs, const inclusivity_merge merge)
{
    const auto order = compare_position(lhs, rhs);

    if (std::strong_ordering::less == order) {
        return borrow_bound(lhs);
    }

    if (std::strong_ordering::greater == order) {
        return borrow_bound(rhs);
    }

    return borrow_bound(lhs).with_inclusive(
        merge_inclusive(lhs.inclusive(), rhs.inclusive(), merge));
}

template <boundary_view LhsT, boundary_view RhsT>
requires std::same_as<boundary_domain_t<LhsT>, boundary_domain_t<RhsT>>
constexpr auto max_bound(const LhsT& lhs, const RhsT& rhs, const inclusivity_merge merge)
{
    const auto order = compare_position(lhs, rhs);

    if (std::strong_ordering::less == order) {
        return borrow_bound(rhs);
    }

    if (std::strong_ordering::greater == order) {
        return borrow_bound(lhs);
    }

    return borrow_bound(lhs).with_inclusive(
        merge_inclusive(lhs.inclusive(), rhs.inclusive(), merge));
}

} // detail

template <typename ExpressionT>
using expression_domain_t = typename std::remove_cvref_t<ExpressionT>::domain_type;

template <interval_view IntervalT>
constexpr bool is_empty(const IntervalT& x)
{
    return detail::empty_interval(x, detail::bounds_of(x));
}

// Copy point values, not necessarily their backing storage. Each endpoint is
// observed once; foreign views need only the public boundary protocol.
template <interval_view IntervalT>
constexpr auto copy_interval(const IntervalT& source)
{
    using domain_type = expression_domain_t<IntervalT>;

    const auto bounds = detail::bounds_of(source);
    if (detail::empty_interval(source, bounds)) {
        return query<domain_type>::empty();
    }

    if constexpr (std::same_as<std::remove_cvref_t<IntervalT>, query<domain_type>>) {
        return source;
    }

    return query<domain_type>::between(bounds.lower(), bounds.upper());
}

// Capacity bounds entry count only. Range construction preserves order,
// duplicates and empty entries; normalization belongs to compile_intervals().
template <ordered_domain DomainT, std::size_t Capacity>
class static_interval_set final
{
    std::inplace_vector<query<DomainT>, Capacity> intervals;

private:
    constexpr void ensure_capacity() const
    {
        if (Capacity == std::size(intervals)) {
            throw std::length_error {"static interval set capacity exceeded"};
        }
    }

public:
    using domain_type = DomainT;
    using value_type = query<DomainT>;
    using const_iterator = typename decltype(intervals)::const_iterator;

public:
    constexpr static_interval_set() = default;
    constexpr static_interval_set(const static_interval_set&) = default;
    constexpr static_interval_set(static_interval_set&&) = default;

public:
    constexpr static_interval_set& operator=(const static_interval_set&)
    requires std::is_copy_assignable_v<value_type>
    = default;
    constexpr static_interval_set& operator=(static_interval_set&&)
    requires std::is_move_assignable_v<value_type>
    = default;

public:
    template <interval_range_for<DomainT> RangeT>
    requires (!std::same_as<std::remove_cvref_t<RangeT>, static_interval_set>)
    constexpr explicit static_interval_set(RangeT&& source)
    {
        auto current = std::ranges::begin(source);

        const auto finish = std::ranges::end(source);
        for (; current != finish; ++current) {
            // Check before dereference/projection. begin/increment may still prefetch.
            ensure_capacity();
            intervals.unchecked_emplace_back(copy_interval(*current));
        }
    }

public:
    constexpr void push_back(value_type value)
    {
        ensure_capacity();
        intervals.unchecked_emplace_back(std::move(value));
    }

public:
    constexpr bool empty() const noexcept { return std::empty(intervals); }

public:
    constexpr std::size_t size() const noexcept { return std::size(intervals); }

public:
    constexpr const value_type& operator[](const std::size_t index) const noexcept
    {
        return intervals[index];
    }

public:
    constexpr const_iterator begin() const noexcept { return std::begin(intervals); }

    constexpr const_iterator end() const noexcept { return std::end(intervals); }

public:
    constexpr bool operator==(const static_interval_set& rhs) const
    {
        return std::ranges::equal(*this, rhs);
    }

};

template <interval_view IntervalT, typename ValueT>
requires detail::comparable_value<expression_domain_t<IntervalT>, ValueT>
constexpr bool contains(const IntervalT& x, const ValueT& value)
{
    const auto bounds = detail::bounds_of(x);

    if (detail::empty_interval(x, bounds)) {
        return false;
    }

    return allows_after(bounds.lower(), value) && allows_before(bounds.upper(), value);
}

template <expression LhsT, expression RhsT, typename ValueT>
requires detail::comparable_value<expression_domain_t<LhsT>, ValueT>
constexpr bool contains(const detail::difference_expr<LhsT, RhsT>& expression, const ValueT& value);

template <expression LhsT, expression RhsT, typename ValueT>
requires detail::comparable_value<expression_domain_t<LhsT>, ValueT>
constexpr bool contains(const detail::intersection_expr<LhsT, RhsT>& expression,
                        const ValueT& value);

template <expression LhsT, expression RhsT, typename ValueT>
requires detail::comparable_value<expression_domain_t<LhsT>, ValueT>
constexpr bool contains(const detail::set_union_expr<LhsT, RhsT>& expression, const ValueT& value);

template <expression ExprT, typename ValueT>
requires (!interval_view<ExprT>) && detail::comparable_value<expression_domain_t<ExprT>, ValueT>
constexpr bool contains(const ExprT& expression, const ValueT& value)
{
    bool found = false;

    interval::for_each_interval(expression, [&found, &value](const auto& x) {
        found = found || interval::contains(x, value);
    });

    return found;
}

template <expression LhsT, expression RhsT, typename ValueT>
requires detail::comparable_value<expression_domain_t<LhsT>, ValueT>
constexpr bool contains(const detail::difference_expr<LhsT, RhsT>& expression, const ValueT& value)
{
    return interval::contains(expression.lhs, value) && !interval::contains(expression.rhs, value);
}

template <expression LhsT, expression RhsT, typename ValueT>
requires detail::comparable_value<expression_domain_t<LhsT>, ValueT>
constexpr bool contains(const detail::intersection_expr<LhsT, RhsT>& expression,
                        const ValueT& value)
{
    return interval::contains(expression.lhs, value) && interval::contains(expression.rhs, value);
}

template <expression LhsT, expression RhsT, typename ValueT>
requires detail::comparable_value<expression_domain_t<LhsT>, ValueT>
constexpr bool contains(const detail::set_union_expr<LhsT, RhsT>& expression, const ValueT& value)
{
    return interval::contains(expression.lhs, value) || interval::contains(expression.rhs, value);
}

template <interval_view LhsT, interval_view RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr auto intersection(const LhsT& lhs, const RhsT& rhs)
{
    using domain_type = typename std::remove_cvref_t<LhsT>::domain_type;

    const auto lhs_bounds = detail::bounds_of(lhs);
    const auto rhs_bounds = detail::bounds_of(rhs);

    if (detail::empty_interval(lhs, lhs_bounds) || detail::empty_interval(rhs, rhs_bounds)) {
        return query<domain_type>::empty();
    }

    auto out = query<domain_type>::between(
        detail::max_bound(lhs_bounds.lower(), rhs_bounds.lower(), detail::inclusivity_merge::all),
        detail::min_bound(lhs_bounds.upper(), rhs_bounds.upper(), detail::inclusivity_merge::all));

    if (is_empty(out)) {
        return query<domain_type>::empty();
    }

    return out;
}

namespace detail {

template <interval_view LhsT, interval_view RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool definitely_before(const LhsT& lhs, const RhsT& rhs)
{
    const auto lhs_bounds = detail::bounds_of(lhs);
    const auto rhs_bounds = detail::bounds_of(rhs);
    const auto order = compare_position(lhs_bounds.upper(), rhs_bounds.lower());

    if (std::strong_ordering::less == order) {
        return true;
    }

    if (std::strong_ordering::greater == order) {
        return false;
    }

    return !lhs_bounds.upper().inclusive() && !rhs_bounds.lower().inclusive();
}

template <interval_view LhsT, interval_view RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool definitely_after(const LhsT& lhs, const RhsT& rhs)
{
    return definitely_before(rhs, lhs);
}

template <interval_view LhsT, interval_view RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool can_coalesce(const LhsT& lhs, const RhsT& rhs)
{
    return !definitely_before(lhs, rhs) && !definitely_after(lhs, rhs);
}

template <interval_view LhsT, interval_view RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr auto coalesce(const LhsT& lhs, const RhsT& rhs)
{
    using domain_type = typename std::remove_cvref_t<LhsT>::domain_type;

    const auto lhs_bounds = detail::bounds_of(lhs);
    const auto rhs_bounds = detail::bounds_of(rhs);
    const auto lhs_empty = detail::empty_interval(lhs, lhs_bounds);
    const auto rhs_empty = detail::empty_interval(rhs, rhs_bounds);

    if (lhs_empty && rhs_empty) {
        return query<domain_type>::empty();
    }

    if (lhs_empty) {
        return query<domain_type>::between(rhs_bounds.lower(), rhs_bounds.upper());
    }

    if (rhs_empty) {
        return query<domain_type>::between(lhs_bounds.lower(), lhs_bounds.upper());
    }

    return query<domain_type>::between(
        min_bound(lhs_bounds.lower(), rhs_bounds.lower(), inclusivity_merge::any),
        max_bound(lhs_bounds.upper(), rhs_bounds.upper(), inclusivity_merge::any));
}

template <boundary_view BoundT>
constexpr auto difference_end_before(const BoundT& removed_begin)
{
    return borrow_bound(removed_begin).with_inclusive(!removed_begin.inclusive());
}

template <boundary_view BoundT>
constexpr auto difference_begin_after(const BoundT& removed_end)
{
    return borrow_bound(removed_end).with_inclusive(!removed_end.inclusive());
}

template <interval_view LhsT, interval_view RhsT, typename SinkT>
requires same_expression_domain<LhsT, RhsT>
constexpr void emit_difference(const LhsT& lhs, const RhsT& rhs, SinkT&& sink)
{
    const auto lhs_bounds = bounds_of(lhs);
    const auto rhs_bounds = bounds_of(rhs);

    if (empty_interval(lhs, lhs_bounds)) {
        return;
    }

    if (empty_interval(rhs, rhs_bounds) || definitely_before(rhs, lhs) ||
        definitely_after(rhs, lhs)) {
        ::ceph::salad::datum::interval::for_each_interval(lhs, std::forward<SinkT>(sink));

        return;
    }

    emit_bounds(sink, lhs_bounds.lower(), difference_end_before(rhs_bounds.lower()));
    emit_bounds(std::forward<SinkT>(sink), difference_begin_after(rhs_bounds.upper()),
                lhs_bounds.upper());
}

} // detail

template <typename LhsT, typename RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr auto difference(LhsT&& lhs, RhsT&& rhs)
{
    using lhs_type = detail::operand_storage<LhsT&&>;
    using rhs_type = detail::operand_storage<RhsT&&>;

    return detail::difference_expr<lhs_type, rhs_type> {.lhs = std::forward<LhsT>(lhs),
                                                        .rhs = std::forward<RhsT>(rhs)};
}

template <typename LhsT, typename RhsT>
requires same_expression_domain<LhsT, RhsT> && (!interval_view<LhsT> || !interval_view<RhsT>)
constexpr auto intersection(LhsT&& lhs, RhsT&& rhs)
{
    using lhs_type = detail::operand_storage<LhsT&&>;
    using rhs_type = detail::operand_storage<RhsT&&>;

    return detail::intersection_expr<lhs_type, rhs_type> {.lhs = std::forward<LhsT>(lhs),
                                                          .rhs = std::forward<RhsT>(rhs)};
}

template <typename LhsT, typename RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr auto set_union(LhsT&& lhs, RhsT&& rhs)
{
    using lhs_type = detail::operand_storage<LhsT&&>;
    using rhs_type = detail::operand_storage<RhsT&&>;

    return detail::set_union_expr<lhs_type, rhs_type> {.lhs = std::forward<LhsT>(lhs),
                                                       .rhs = std::forward<RhsT>(rhs)};
}

template <expression ExprT>
constexpr auto complement(ExprT&& expression)
{
    using domain_type = typename std::remove_cvref_t<ExprT>::domain_type;

    return difference(query<domain_type>::universal(), std::forward<ExprT>(expression));
}

template <ordered_domain DomainT, typename SinkT>
constexpr void for_each_interval(const query<DomainT>& x, SinkT&& sink)
{
    if (is_empty(x)) {
        return;
    }

    if constexpr (detail::boundary_sink_for<SinkT, boundary<DomainT>, boundary<DomainT>>) {
        std::forward<SinkT>(sink).emit_interval(x.lower(), x.upper());
    }

    if constexpr (!(detail::boundary_sink_for<SinkT, boundary<DomainT>, boundary<DomainT>>)) {
        std::invoke(std::forward<SinkT>(sink), x);
    }
}

template <interval_view IntervalT, typename SinkT>
requires (!canonical_interval<IntervalT>)
constexpr void for_each_interval(const IntervalT& x, SinkT&& sink)
{
    const auto bounds = detail::bounds_of(x);

    if (detail::explicitly_empty(x)) {
        return;
    }

    detail::emit_bounds(std::forward<SinkT>(sink), bounds.lower(), bounds.upper());
}

// Limits describe retained intermediate entries and algorithm work units, not
// endpoint-owned bytes, vector capacity, or application admission policy.
struct evaluation_limits
{
    std::size_t intermediate_intervals = std::numeric_limits<std::size_t>::max();
    std::size_t work = std::numeric_limits<std::size_t>::max();
};

template <ordered_domain DomainT>
class workspace
{
    std::vector<std::vector<query<DomainT>>> buffers;

    evaluation_limits limits;

    std::size_t retained = 0;
    std::size_t performed = 0;

public:
    constexpr explicit workspace(const evaluation_limits budget = {}) : limits(budget)
    {}

public:
    constexpr void reset(std::size_t slots)
    {
        for (auto& buffer : buffers) {
            buffer.clear();
        }
        retained = performed = 0;
        buffers.resize(std::max(slots, std::size(buffers)));
    }

public:
    constexpr void tick()
    {
        if (performed == limits.work) {
            throw std::length_error {"interval evaluation work limit exceeded"};
        }
        ++performed;
    }

public:
    constexpr std::size_t work_performed() const noexcept { return performed; }

public:
    constexpr std::size_t retained_intervals() const noexcept { return retained; }

public:
    constexpr auto& clear(std::size_t slot)
    {
        auto& buffer = buffers.at(slot);
        retained -= std::size(buffer);
        buffer.clear();

        return buffer;
    }

public:
    constexpr void check_entries(std::size_t additional) const
    {
        if (additional > limits.intermediate_intervals - retained) {
            throw std::length_error {"interval evaluation intermediate limit exceeded"};
        }
    }

public:
    constexpr void replace(std::size_t slot, std::vector<query<DomainT>> values)
    {
        retained -= std::size(buffers.at(slot));
        retained += std::size(values);
        buffers.at(slot).swap(values);
    }

public:
    constexpr auto& get(std::size_t slot) { return buffers.at(slot); }
    constexpr const auto& get(std::size_t slot) const { return buffers.at(slot); }

public:
    template <boundary_view LowerT, boundary_view UpperT>

    constexpr void append(std::size_t slot, const LowerT& lower, const UpperT& upper)
    {
        tick();
        if (detail::empty_bounds(lower, upper)) {
            return;
        }
        check_entries(1);
        buffers.at(slot).push_back(query<DomainT>::between(lower, upper));
        ++retained;
    }

public:
    constexpr auto take(std::size_t slot)
    {
        retained -= std::size(buffers.at(slot));

        return std::move(buffers.at(slot));
    }

public:
    constexpr void shrink(std::size_t slot, std::size_t count)
    {
        auto& buffer = buffers.at(slot);
        retained -= std::size(buffer) - count;
        // pop_back only destroys values: points need not be default-constructible.
        while (std::size(buffer) > count) {
            buffer.pop_back();
        }
    }

};

namespace detail {

enum struct set_operation { unite, intersect, subtract };

template <typename T>
inline constexpr set_operation operation = set_operation::unite;
template <typename L, typename R>
inline constexpr set_operation operation<intersection_expr<L, R>> = set_operation::intersect;
template <typename L, typename R>
inline constexpr set_operation operation<difference_expr<L, R>> = set_operation::subtract;
template <typename T>
inline constexpr bool union_expression = false;
template <typename L, typename R>
inline constexpr bool union_expression<set_union_expr<L, R>> = true;

template <typename ExprT>
consteval std::size_t scratch_slots()
{
    if constexpr (!interval_view<ExprT> && !canonical_sequence<ExprT>) {
        return 1 + scratch_slots<decltype(std::declval<ExprT>().lhs)>() +
               scratch_slots<decltype(std::declval<ExprT>().rhs)>();
    }

    return 0;
}

template <interval_view LhsT, interval_view RhsT>
constexpr bool interval_less(const LhsT& lhs, const RhsT& rhs)
{
    const auto l = bounds_of(lhs);
    const auto r = bounds_of(rhs);
    if (const auto order = compare_position(l.lower(), r.lower()); order != 0) {
        return order < 0;
    }
    if (l.lower().inclusive() != r.lower().inclusive()) {
        return l.lower().inclusive();
    }
    if (const auto order = compare_position(l.upper(), r.upper()); order != 0) {
        return order < 0;
    }

    return l.upper().inclusive() && !r.upper().inclusive();
}

template <ordered_domain DomainT>
constexpr void normalize_buffer(workspace<DomainT>& scratch, std::size_t slot)
{
    constexpr bool assignable = std::is_move_assignable_v<query<DomainT>>;
    // Only this admission boundary sorts. All subsequent operations are sweeps.
    auto& values = scratch.get(slot);
    auto sort_if_needed = [&scratch](auto& intervals) {
        const auto less = [&scratch](const auto& lhs, const auto& rhs) {
            scratch.tick();

            return interval_less(lhs, rhs);
        };
        if (!std::ranges::is_sorted(intervals, less)) {
            std::ranges::sort(intervals, less);
        }
    };
    if constexpr (!assignable) {
        // Sort metadata rather than imposing assignment on the ordered point type.
        std::vector<borrowed_interval<DomainT>> ordered;
        ordered.reserve(std::size(values));
        for (const auto& value : values) {
            ordered.push_back({borrow_bound(value.lower()), borrow_bound(value.upper())});
        }
        sort_if_needed(ordered);
        std::size_t count = 0;
        for (const auto& next : ordered) {
            scratch.tick();
            if (count != 0 && can_coalesce(ordered[count - 1], next)) {
                auto& current = ordered[count - 1];
                current.first = min_bound(current.first, next.first, inclusivity_merge::any);
                current.last = max_bound(current.last, next.last, inclusivity_merge::any);
                continue;
            }
            ordered[count++] = next;
        }
        scratch.check_entries(count);
        std::vector<query<DomainT>> normalized;
        normalized.reserve(count);
        for (const auto& value : ordered | std::views::take(count)) {
            normalized.push_back(query<DomainT>::between(value.first, value.last));
        }
        scratch.replace(slot, std::move(normalized));
    }

    if constexpr (assignable) {
        sort_if_needed(values);
        std::size_t output = 0;
        for (std::size_t first = 0; first < std::size(values);) {
            auto lower = borrow_bound(values[first].lower());
            auto upper = borrow_bound(values[first].upper());
            auto next = first + 1;
            for (; next < std::size(values); ++next) {
                scratch.tick();
                const auto order = compare_position(upper, values[next].lower());
                if (order < 0 ||
                    (order == 0 && !upper.inclusive() && !values[next].lower().inclusive())) {
                    break;
                }
                lower = min_bound(lower, values[next].lower(), inclusivity_merge::any);
                upper = max_bound(upper, values[next].upper(), inclusivity_merge::any);
            }
            const bool merged = next != first + 1;
            if (merged) {
                values[output] = query<DomainT>::between(lower, upper);
            }
            if (!merged && output != first) {
                values[output] = std::move(values[first]);
            }
            ++output;
            first = next;
        }
        scratch.shrink(slot, output);
    }
}

template <ordered_domain DomainT>
struct workspace_sink
{
    workspace<DomainT>& scratch;
    std::size_t slot;

public:
    template <boundary_view L, boundary_view U>

    constexpr void emit_interval(const L& lower, const U& upper) const
    {
        scratch.append(slot, lower, upper);
    }

};

template <expression ExprT, typename SinkT>
constexpr void evaluate_to(const ExprT& source, workspace<expression_domain_t<ExprT>>& scratch,
                           std::size_t slot, SinkT& sink);

template <expression ExprT, typename FunctionT>
requires canonical_sequence<ExprT>
constexpr void with_canonical(const ExprT& source, workspace<expression_domain_t<ExprT>>&,
                              std::size_t, FunctionT&& function)
{
    std::invoke(function, source);
}

template <expression ExprT, typename FunctionT>
requires (!canonical_sequence<ExprT> && interval_view<ExprT>)
constexpr void with_canonical(const ExprT& source, workspace<expression_domain_t<ExprT>>&,
                              std::size_t, FunctionT&& function)
{
    const auto bounds = bounds_of(source);
    const std::array values {borrowed_interval<expression_domain_t<ExprT>> {
        borrow_bound(bounds.lower()), borrow_bound(bounds.upper())}};
    std::invoke(function, std::span {values}.first(empty_interval(source, bounds) ? 0 : 1));
}

template <expression ExprT, typename FunctionT>
requires (!canonical_sequence<ExprT> && !interval_view<ExprT>)
constexpr void with_canonical(const ExprT& source, workspace<expression_domain_t<ExprT>>& scratch,
                              std::size_t slot, FunctionT&& function)
{
    scratch.clear(slot);
    auto sink = workspace_sink {scratch, slot};
    evaluate_to(source, scratch, slot + 1, sink);
    std::invoke(function, scratch.get(slot));
}

template <expression ExprT, typename SinkT>
constexpr void collect_union(const ExprT& source, workspace<expression_domain_t<ExprT>>& scratch,
                             std::size_t slot, SinkT& sink)
{
    if constexpr (union_expression<std::remove_cvref_t<ExprT>>) {
        collect_union(source.lhs, scratch, slot, sink);
        collect_union(source.rhs, scratch, slot + scratch_slots<decltype(source.lhs)>(), sink);
    }

    if constexpr (!union_expression<std::remove_cvref_t<ExprT>>) {
        with_canonical(source, scratch, slot, [&sink](const auto& values) {
            for (const auto& value : values) {
                const auto bounds = bounds_of(value);
                emit_bounds(sink, bounds.lower(), bounds.upper());
            }
        });
    }
}

template <set_operation Operation, bool FirstOnly = false, std::ranges::input_range LhsT,
          std::ranges::input_range RhsT, typename SinkT, typename TickT>
requires std::ranges::input_range<const LhsT> && std::ranges::input_range<const RhsT> &&
         (!FirstOnly || Operation != set_operation::unite)
constexpr void sweep(const LhsT& lhs, const RhsT& rhs, SinkT& sink, TickT&& tick)
{
    auto left = std::ranges::begin(lhs);
    auto right = std::ranges::begin(rhs);

    const auto left_end = std::ranges::end(lhs);
    const auto right_end = std::ranges::end(rhs);
    const auto emit = [&sink]<bool CheckEmpty = true>(const auto& lower, const auto& upper) {
        const bool emitted = emit_bounds<CheckEmpty>(sink, lower, upper);

        return FirstOnly && emitted;
    };
    if constexpr (Operation == set_operation::unite) {
        using domain = expression_domain_t<std::ranges::range_value_t<LhsT>>;

        std::optional<borrowed_interval<domain>> current;
        auto take = [&tick, &current, &sink](const auto& value) {
            tick();
            const auto bounds = bounds_of(value);
            const auto lower = borrow_bound(bounds.lower());
            const auto upper = borrow_bound(bounds.upper());
            if (current) {
                const auto order = compare_position(current->last, lower);
                if (order > 0 || (order == 0 && (current->last.inclusive() || lower.inclusive()))) {
                    // Ordered admission already establishes the earliest lower bound.
                    current->last = max_bound(current->last, upper, inclusivity_merge::any);

                    return;
                }
                emit_bounds<false>(sink, current->first, current->last);
            }
            current = borrowed_interval<domain> {lower, upper};
        };
        while (left != left_end || right != right_end) {
            if (right == right_end || (left != left_end && interval_less(*left, *right))) {
                take(*left++);
                continue;
            }
            take(*right++);
        }
        if (current) {
            emit_bounds<false>(sink, current->first, current->last);
        }

        return;
    }

    if constexpr (Operation == set_operation::intersect) {
        while (left != left_end && right != right_end) {
            tick();
            const auto l = bounds_of(*left);
            const auto r = bounds_of(*right);
            const auto order = compare_position(l.upper(), r.upper());
            // Only the interval ending first can precede the other interval's start.
            if (order < 0 && empty_bounds(r.lower(), l.upper())) {
                ++left;
                continue;
            }
            if (order > 0 && empty_bounds(l.lower(), r.upper())) {
                ++right;
                continue;
            }
            auto upper = order > 0 ? borrow_bound(r.upper()) : borrow_bound(l.upper());
            if (order == 0) {
                upper = upper.with_inclusive(l.upper().inclusive() && r.upper().inclusive());
            }
            const auto lower = max_bound(l.lower(), r.lower(), inclusivity_merge::all);
            // Unequal ends and the separation guard already certify an overlap.
            if (order == 0 && empty_bounds(lower, upper)) {
                ++left;
                ++right;
                continue;
            }
            if (emit.template operator()<false>(lower, upper)) {
                return;
            }
            if (order <= 0) {
                ++left;
            }
            if (order >= 0) {
                ++right;
            }
        }

        return;
    }
    while (left != left_end) {
        tick();
        const auto l = bounds_of(*left++);
        auto cursor = borrow_bound(l.lower());
        while (right != right_end) {
            tick();
            const auto r = bounds_of(*right);
            const auto before = compare_position(r.upper(), cursor);
            if (before < 0 || (before == 0 && (!r.upper().inclusive() || !cursor.inclusive()))) {
                ++right;
                continue;
            }
            const auto after = compare_position(l.upper(), r.lower());
            if (after < 0 || (after == 0 && (!l.upper().inclusive() || !r.lower().inclusive()))) {
                break;
            }
            const auto begin_order = compare_position(cursor, r.lower());
            const auto end_order = compare_position(l.upper(), r.upper());
            const auto upper = end_order < 0 ? borrow_bound(l.upper())
                               : end_order > 0
                                   ? borrow_bound(r.upper())
                                   : borrow_bound(l.upper()).with_inclusive(l.upper().inclusive() &&
                                                                            r.upper().inclusive());
            if (begin_order < 0) {
                if (emit.template operator()<false>(cursor, difference_end_before(r.lower()))) {
                    return;
                }
            }
            if (begin_order == 0 && cursor.finite() && cursor.inclusive() &&
                !r.lower().inclusive()) {
                if (emit.template operator()<false>(cursor, cursor)) {
                    return;
                }
            }
            cursor = upper.with_inclusive(!upper.inclusive());
            if (end_order <= 0) {
                break;
            }
            ++right;
        }
        if (emit(cursor, l.upper())) {
            return;
        }
    }
}

template <expression ExprT, typename SinkT>
constexpr void evaluate_to(const ExprT& source, workspace<expression_domain_t<ExprT>>& scratch,
                           std::size_t slot, SinkT& sink)
{
    if constexpr (interval_view<ExprT> || canonical_sequence<ExprT>) {
        interval::for_each_interval(source, sink);
    }

    if constexpr (!interval_view<ExprT> && !canonical_sequence<ExprT>) {
        constexpr bool collect =
            union_expression<std::remove_cvref_t<ExprT>> &&
            !((interval_view<decltype(source.lhs)> || canonical_sequence<decltype(source.lhs)>) &&
              (interval_view<decltype(source.rhs)> || canonical_sequence<decltype(source.rhs)>));
        if constexpr (collect) {
            scratch.clear(slot);
            auto collector = workspace_sink {scratch, slot};
            collect_union(source, scratch, slot + 1, collector);
            normalize_buffer(scratch, slot);
            for (const auto& value : scratch.get(slot)) {
                emit_bounds(sink, value.lower(), value.upper());
            }
        }

        if constexpr (!collect) {
            with_canonical(source.lhs, scratch, slot, [&](const auto& lhs) {
                with_canonical(source.rhs, scratch, slot + scratch_slots<decltype(source.lhs)>(),
                               [&](const auto& rhs) {
                                   sweep<operation<std::remove_cvref_t<ExprT>>>(
                                       lhs, rhs, sink, [&scratch] { scratch.tick(); });
                               });
            });
        }
    }
}

// Only admitted sequences and native intervals certify stable endpoint storage.
// Foreign interval getters keep their existing observation/materialization path.
template <expression ExprT>
consteval bool streamable_expression()
{
    if constexpr (!canonical_sequence<ExprT> && !interval_view<ExprT>) {
        return streamable_expression<decltype(std::declval<ExprT>().lhs)>() &&
               streamable_expression<decltype(std::declval<ExprT>().rhs)>();
    }

    return canonical_sequence<ExprT> || canonical_interval<ExprT>;
}

// An admitted object is one canonical set, including noncontiguous ranges.
template <typename LhsT, typename RhsT>
constexpr bool same_canonical_operand(const LhsT& lhs, const RhsT& rhs) noexcept
{
    if constexpr (canonical_sequence<LhsT> && std::same_as<LhsT, RhsT>) {
        return std::addressof(lhs) == std::addressof(rhs);
    }

    return false;
}

template <canonical_sequence LhsT, canonical_sequence RhsT>
constexpr bool same_canonical_operand(const std::ranges::ref_view<LhsT>& lhs,
                                      const std::ranges::ref_view<RhsT>& rhs) noexcept
{
    return same_canonical_operand(lhs.base(), rhs.base());
}

struct no_remainder
{};

// Each cursor owns descriptors, never points. Advancing a child cannot invalidate
// a selected endpoint: all references lead back to the admitted leaf storage.
template <set_operation Operation, std::ranges::view LhsT, std::ranges::view RhsT>
requires std::ranges::input_range<const LhsT> && std::ranges::input_range<const RhsT> &&
         std::is_lvalue_reference_v<std::ranges::range_reference_t<const LhsT>> &&
         std::is_lvalue_reference_v<std::ranges::range_reference_t<const RhsT>> &&
         stable_interval<std::ranges::range_reference_t<const LhsT>> &&
         stable_interval<std::ranges::range_reference_t<const RhsT>> &&
         same_expression_domain<std::ranges::range_value_t<const LhsT>,
                                std::ranges::range_value_t<const RhsT>>
class canonical_merge_view final
    : public std::ranges::view_interface<canonical_merge_view<Operation, LhsT, RhsT>>
{
    LhsT lhs;
    RhsT rhs;

    using domain = expression_domain_t<std::ranges::range_value_t<const LhsT>>;
    using geometry = borrowed_interval<domain>;

public:
    constexpr canonical_merge_view(LhsT left, RhsT right)
        : lhs(std::move(left)), rhs(std::move(right))
    {}

    class iterator final
    {
        // Postincrement must snapshot descriptors before advancing child caches.
        // Value-returning getters keep bounds_of(*cursor++) independent of this proxy.
        struct post_increment final
        {
            using domain_type = domain;

            geometry value;

        public:
            constexpr auto lower() const noexcept { return value.first; }
            constexpr auto upper() const noexcept { return value.last; }

        public:
            constexpr const post_increment& operator*() const noexcept { return *this; }

        };

        std::ranges::iterator_t<const LhsT> left {};
        [[no_unique_address]] std::ranges::sentinel_t<const LhsT> left_end {};
        std::ranges::iterator_t<const RhsT> right {};
        [[no_unique_address]] std::ranges::sentinel_t<const RhsT> right_end {};

        std::optional<geometry> selected;
        [[no_unique_address]] std::conditional_t<Operation == set_operation::subtract,
                                                 std::optional<geometry>, no_remainder> remaining;

    private:
        static constexpr geometry bounds(const auto& value)
        {
            const auto observed = bounds_of(value);

            return {borrow_bound(observed.lower()), borrow_bound(observed.upper())};
        }

    private:
        constexpr bool take_left() const
        {
            return right == right_end || (left != left_end && interval_less(*left, *right));
        }

        constexpr geometry take_first()
        {
            if (take_left()) {
                const auto value = bounds(*left);
                ++left;

                return value;
            }
            const auto value = bounds(*right);
            ++right;

            return value;
        }

        constexpr void unite()
        {
            if (left == left_end && right == right_end) {
                return;
            }
            auto value = take_first();
            while (left != left_end || right != right_end) {
                const bool from_left = take_left();
                const auto next = from_left ? bounds(*left) : bounds(*right);
                const auto order = compare_position(value.last, next.first);
                if (order < 0 ||
                    (order == 0 && !value.last.inclusive() && !next.first.inclusive())) {
                    break;
                }
                value.last = max_bound(value.last, next.last, inclusivity_merge::any);
                if (from_left) {
                    ++left;
                    continue;
                }
                ++right;
            }
            selected.emplace(value);
        }

        constexpr void intersect()
        {
            while (left != left_end && right != right_end) {
                const auto l = bounds(*left);
                const auto r = bounds(*right);
                const geometry value {max_bound(l.first, r.first, inclusivity_merge::all),
                                      min_bound(l.last, r.last, inclusivity_merge::all)};
                const auto order = compare_position(l.last, r.last);
                if (order <= 0) {
                    ++left;
                }
                if (order >= 0) {
                    ++right;
                }
                if (!empty_bounds(value)) {
                    selected.emplace(value);

                    return;
                }
            }
        }

        constexpr void subtract()
        {
            while (remaining || left != left_end) {
                if (!remaining) {
                    remaining.emplace(bounds(*left));
                    ++left;
                }
                if (right == right_end) {
                    selected.emplace(*remaining);
                    remaining.reset();

                    return;
                }
                const auto removed = bounds(*right);
                const auto prefix_lower = remaining->first;
                const auto begin_order = compare_position(prefix_lower, removed.first);
                // Ordered lower bounds determine which separation test can matter.
                if (begin_order < 0) {
                    const auto after = compare_position(remaining->last, removed.first);
                    if (after < 0 || (after == 0 && (!remaining->last.inclusive() ||
                                                     !removed.first.inclusive()))) {
                        selected.emplace(*remaining);
                        remaining.reset();

                        return;
                    }
                }
                if (begin_order >= 0 &&
                    (begin_order > 0 || (prefix_lower.finite() && !prefix_lower.inclusive() &&
                                         removed.first.inclusive()))) {
                    // Equal starts can be separated only by an excluded closed singleton.
                    const auto before = compare_position(removed.last, prefix_lower);
                    if (before < 0 ||
                        (before == 0 && (!removed.last.inclusive() || !prefix_lower.inclusive()))) {
                        ++right;
                        continue;
                    }
                }
                const auto end_order = compare_position(removed.last, remaining->last);
                if (end_order > 0 || (end_order == 0 &&
                                      (removed.last.inclusive() || !remaining->last.inclusive()))) {
                    remaining.reset();
                    // An equal upper key cannot remove points from a later canonical interval.
                    if (end_order == 0 && left != left_end) {
                        ++right;
                    }
                }
                if (remaining) {
                    const auto upper =
                        end_order < 0
                            ? removed.last
                            : remaining->last.with_inclusive(remaining->last.inclusive() &&
                                                             removed.last.inclusive());
                    remaining->first = upper.with_inclusive(!upper.inclusive());
                    ++right;
                }
                if (begin_order < 0) {
                    selected.emplace(geometry {prefix_lower, difference_end_before(removed.first)});

                    return;
                }
                if (begin_order == 0 && prefix_lower.finite() && prefix_lower.inclusive() &&
                    !removed.first.inclusive()) {
                    selected.emplace(geometry {prefix_lower, prefix_lower});

                    return;
                }
            }
        }

        constexpr void seek()
        {
            selected.reset();
            if constexpr (Operation == set_operation::unite) {
                unite();
            }

            if constexpr (Operation == set_operation::intersect) {
                intersect();
            }

            if constexpr (Operation == set_operation::subtract) {
                subtract();
            }
        }

    public:
        using iterator_concept = std::input_iterator_tag;
        using value_type = geometry;
        using difference_type = std::ptrdiff_t;

    public:
        constexpr iterator() = default;
        constexpr explicit iterator(const canonical_merge_view& source)

            : left(std::ranges::begin(source.lhs)), left_end(std::ranges::end(source.lhs)),
              right(std::ranges::begin(source.rhs)), right_end(std::ranges::end(source.rhs))
        {
            constexpr bool contiguous_subtraction =
                Operation == set_operation::subtract && std::ranges::contiguous_range<const LhsT> &&
                std::ranges::contiguous_range<const RhsT> && std::ranges::sized_range<const LhsT> &&
                std::ranges::sized_range<const RhsT> &&
                std::same_as<std::ranges::range_value_t<const LhsT>,
                             std::ranges::range_value_t<const RhsT>>;
            if constexpr (contiguous_subtraction) {
                // Identical admitted storage denotes the same set; A minus A is empty.
                if (std::ranges::data(source.lhs) == std::ranges::data(source.rhs) &&
                    std::ranges::size(source.lhs) == std::ranges::size(source.rhs)) {
                    return;
                }
            }

            if constexpr (Operation == set_operation::subtract && !contiguous_subtraction) {
                if (same_canonical_operand(source.lhs, source.rhs)) {
                    return;
                }
            }
            seek();
        }

    public:
        constexpr const value_type& operator*() const noexcept { return *selected; }

        constexpr iterator& operator++()
        {
            seek();

            return *this;
        }

        constexpr auto operator++(int)
        {
            const post_increment value {*selected};
            ++*this;

            return value;
        }

        constexpr bool operator==(std::default_sentinel_t) const noexcept { return !selected; }

    };

public:
    constexpr iterator begin() const { return iterator {*this}; }

    constexpr std::default_sentinel_t end() const noexcept { return {}; }

};

template <expression ExprT>
requires canonical_sequence<ExprT>
constexpr auto canonical_stream(const ExprT& source)
{
    return std::ranges::ref_view {source};
}

template <expression ExprT>
requires (!canonical_sequence<ExprT> && canonical_interval<ExprT>)
constexpr auto canonical_stream(const ExprT& source)
{
    return std::span<const std::remove_cvref_t<ExprT>> {std::addressof(source),
                                                        is_empty(source) ? 0U : 1U};
}

template <expression ExprT>
requires (!canonical_sequence<ExprT> && !canonical_interval<ExprT> && streamable_expression<ExprT>())
constexpr auto canonical_stream(const ExprT& source)
{
    auto lhs = canonical_stream(source.lhs);
    auto rhs = canonical_stream(source.rhs);

    return canonical_merge_view<operation<std::remove_cvref_t<ExprT>>, decltype(lhs),
                                decltype(rhs)> {std::move(lhs), std::move(rhs)};
}

template <expression ExprT>
auto canonical_stream(const ExprT&&) = delete;

template <expression ExprT>
consteval bool prepared_operands()
{
    using lhs_type = decltype(std::declval<ExprT>().lhs);
    using rhs_type = decltype(std::declval<ExprT>().rhs);

    return (interval_view<lhs_type> || canonical_sequence<lhs_type>) &&
           (interval_view<rhs_type> || canonical_sequence<rhs_type>);
}

template <expression ExprT, typename SinkT>
constexpr void for_each_compound(const ExprT& source, SinkT& sink)
{
    using lhs_type = decltype(source.lhs);
    using rhs_type = decltype(source.rhs);

    constexpr bool direct_difference =
        operation<std::remove_cvref_t<ExprT>> == set_operation::subtract &&
        interval_view<lhs_type> && interval_view<rhs_type>;
    if constexpr (operation<std::remove_cvref_t<ExprT>> == set_operation::subtract) {
        if (same_canonical_operand(source.lhs, source.rhs)) {
            return;
        }
    }

    if constexpr (direct_difference) {
        emit_difference(source.lhs, source.rhs, sink);
    }

    if constexpr (!direct_difference) {
        if constexpr (streamable_expression<ExprT>()) {
            const auto lhs = canonical_stream(source.lhs);
            const auto rhs = canonical_stream(source.rhs);
            // Default traversal has no requested work budget. Explicit evaluate() counts work.
            sweep<operation<std::remove_cvref_t<ExprT>>>(lhs, rhs, sink, [] {});
        }

        if constexpr (!streamable_expression<ExprT>()) {
            workspace<expression_domain_t<ExprT>> scratch;
            scratch.reset(prepared_operands<ExprT>() ? 0 : 1 + scratch_slots<ExprT>());
            evaluate_to(source, scratch, 0, sink);
        }
    }
}

} // detail

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const detail::difference_expr<LhsT, RhsT>& source, SinkT&& sink)
{
    detail::for_each_compound(source, sink);
}

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const detail::intersection_expr<LhsT, RhsT>& source, SinkT&& sink)
{
    detail::for_each_compound(source, sink);
}

template <expression LhsT, expression RhsT, typename SinkT>
constexpr void for_each_interval(const detail::set_union_expr<LhsT, RhsT>& source, SinkT&& sink)
{
    detail::for_each_compound(source, sink);
}

template <expression ExprT>
constexpr auto compile_intervals(const ExprT& expression)
{
    using domain_type = typename std::remove_cvref_t<ExprT>::domain_type;

    std::vector<query<domain_type>> out;
    out.reserve(interval_view<ExprT> ? 1 : 4);

    ::ceph::salad::datum::interval::for_each_interval(expression, [&out](auto&& interval) {
        out.push_back(std::forward<decltype(interval)>(interval));
    });

    return out;
}

template <std::size_t Capacity, expression ExprT>
constexpr auto compile_intervals(const ExprT& expression)
{
    using domain_type = typename std::remove_cvref_t<ExprT>::domain_type;

    static_interval_set<domain_type, Capacity> out;

    ::ceph::salad::datum::interval::for_each_interval(expression, [&out](auto&& interval) {
        out.push_back(std::forward<decltype(interval)>(interval));
    });

    return out;
}

// A validation boundary, not an unchecked promise. External mutation must not
// invalidate normalized geometry or its source lifetime while a view is used.
template <std::ranges::view ViewT>
requires std::ranges::forward_range<const ViewT> &&
             stable_interval<std::ranges::range_reference_t<const ViewT>> &&
             std::is_lvalue_reference_v<std::ranges::range_reference_t<const ViewT>>
class canonical_view final : public detail::canonical_tag,
                             public std::ranges::view_interface<canonical_view<ViewT>>
{
    ViewT source;

public:
    using domain_type = expression_domain_t<std::ranges::range_value_t<ViewT>>;

public:
    template <typename RangeT>
    requires (!std::same_as<std::remove_cvref_t<RangeT>, canonical_view>) &&
            std::ranges::viewable_range<RangeT> && std::same_as<std::views::all_t<RangeT>, ViewT>
    constexpr explicit canonical_view(RangeT&& values)
        : source(std::views::all(std::forward<RangeT>(values)))
    {
        auto current = std::ranges::cbegin(source);
        const auto end = std::ranges::cend(source);
        if (current == end) {
            return;
        }
        if (interval::is_empty(*current)) {
            throw std::invalid_argument {"canonical interval range contains an empty interval"};
        }
        auto previous = current++;
        for (; current != end; previous = current++) {
            if (interval::is_empty(*current) || !detail::definitely_before(*previous, *current)) {
                throw std::invalid_argument {
                    "canonical interval range is unordered or coalescible"};
            }
        }
    }

public:
    constexpr auto begin() const { return std::ranges::cbegin(source); }

    constexpr auto end() const { return std::ranges::cend(source); }

public:
    constexpr auto size() const
    requires std::ranges::sized_range<const ViewT>
    {
        return std::ranges::size(source);
    }

};

template <std::ranges::viewable_range RangeT>
canonical_view(RangeT&&) -> canonical_view<std::views::all_t<RangeT>>;

template <ordered_domain DomainT>
class canonical_set final : public detail::canonical_tag
{
    std::vector<query<DomainT>> values;

public:
    using domain_type = DomainT;

public:
    constexpr canonical_set() = default;

    template <expression ExprT>
    requires std::same_as<expression_domain_t<ExprT>, DomainT>
    constexpr explicit canonical_set(const ExprT& source) : values(compile_intervals(source))
    {}

    template <std::ranges::input_range RangeT>
    requires interval_range_for<RangeT, DomainT> && (!expression<RangeT>)
    constexpr explicit canonical_set(RangeT&& source)
    {
        workspace<DomainT> scratch;
        scratch.reset(1);
        for (const auto& value : source) {
            const auto bounds = detail::bounds_of(value);
            if (!detail::explicitly_empty(value)) {
                scratch.append(0, bounds.lower(), bounds.upper());
            }
        }
        detail::normalize_buffer(scratch, 0);
        values = scratch.take(0);
    }

public:
    constexpr auto begin() const noexcept { return std::begin(values); }

    constexpr auto end() const noexcept { return std::end(values); }

public:
    constexpr auto size() const noexcept { return std::size(values); }

};

template <expression ExprT>
canonical_set(const ExprT&) -> canonical_set<expression_domain_t<ExprT>>;

template <std::ranges::input_range RangeT>
canonical_set(RangeT&&) -> canonical_set<expression_domain_t<std::ranges::range_value_t<RangeT>>>;

// Budget failure publishes nothing and clears scratch. A consumer exception
// can occur after publication starts; its effects belong to that consumer.
// The returned span borrows scratch until its next evaluation/reset.
template <expression ExprT>
constexpr std::span<const query<expression_domain_t<ExprT>>>
evaluate(const ExprT& source, workspace<expression_domain_t<ExprT>>& scratch)
{
    scratch.reset(2 + detail::scratch_slots<ExprT>());
    try {
        auto sink = detail::workspace_sink {scratch, 0};
        detail::evaluate_to(source, scratch, 1, sink);

        return scratch.get(0);
    } catch (...) {
        scratch.reset(0);
        throw;
    }
}

template <expression ExprT, typename SinkT>
constexpr void evaluate(const ExprT& source, workspace<expression_domain_t<ExprT>>& scratch,
                        SinkT&& sink)
{
    const auto values = evaluate(source, scratch);
    for (const auto& value : values) {
        interval::for_each_interval(value, sink);
    }
}

namespace detail {

struct interval_counter final
{
    std::size_t count = 0;

public:
    constexpr void emit_interval(const auto&, const auto&) noexcept { ++count; }

};
} // detail

template <expression ExprT>
constexpr std::size_t interval_count(const ExprT& expression)
{
    if constexpr (canonical_sequence<ExprT> && std::ranges::sized_range<const ExprT>) {
        return std::ranges::size(expression);
    }

    if constexpr (!(canonical_sequence<ExprT> && std::ranges::sized_range<const ExprT>)) {
        if constexpr (canonical_sequence<ExprT>) {
            return std::ranges::fold_left(
                expression, std::size_t {0},
                [](const std::size_t count, const auto&) { return count + 1; });
        }
    }
    detail::interval_counter sink;
    ::ceph::salad::datum::interval::for_each_interval(expression, sink);

    return sink.count;
}

template <expression ExprT>
constexpr bool is_empty_expression(const ExprT& expression);

template <expression LhsT, expression RhsT>
constexpr bool is_empty_expression(const detail::set_union_expr<LhsT, RhsT>& expression)
{
    return interval::is_empty_expression(expression.lhs) &&
           interval::is_empty_expression(expression.rhs);
}

template <expression ExprT>
constexpr bool is_empty_expression(const ExprT& expression)
{
    if constexpr (interval_view<ExprT>) {
        return interval::is_empty(expression);
    }

    if constexpr (!interval_view<ExprT>) {
        if constexpr (canonical_sequence<ExprT>) {
            return std::ranges::empty(expression);
        }

        if constexpr (!canonical_sequence<ExprT>) {
            if constexpr (detail::streamable_expression<ExprT>() &&
                          !detail::union_expression<std::remove_cvref_t<ExprT>>) {
                if constexpr (detail::operation<std::remove_cvref_t<ExprT>> ==
                              detail::set_operation::subtract) {
                    if (detail::same_canonical_operand(expression.lhs, expression.rhs)) {
                        return true;
                    }
                }
                const auto lhs = detail::canonical_stream(expression.lhs);
                const auto rhs = detail::canonical_stream(expression.rhs);
                detail::interval_counter sink;
                detail::sweep<detail::operation<std::remove_cvref_t<ExprT>>, true>(lhs, rhs, sink,
                                                                                   [] {});

                return sink.count == 0;
            }
        }
    }

    return 0 == interval::interval_count(expression);
}

template <expression OuterT, expression InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool encloses(const OuterT& outer, const InnerT& inner);

template <interval_view OuterT, interval_view InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool encloses(const OuterT& outer, const InnerT& inner);

namespace detail {

template <boundary_view UpperT, boundary_view LowerT>
requires std::same_as<boundary_domain_t<UpperT>, boundary_domain_t<LowerT>>
constexpr bool disjoint_before(const UpperT& upper, const LowerT& lower)
{
    const auto order = compare_position(upper, lower);

    return std::strong_ordering::less == order ||
           (std::strong_ordering::equal == order && (!upper.inclusive() || !lower.inclusive()));
}
} // detail

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool intersects(const LhsT& lhs, const RhsT& rhs);

template <interval_view LhsT, interval_view RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool intersects(const LhsT& lhs, const RhsT& rhs)
{
    const auto lhs_bounds = detail::bounds_of(lhs);
    const auto rhs_bounds = detail::bounds_of(rhs);

    if (detail::empty_interval(lhs, lhs_bounds) || detail::empty_interval(rhs, rhs_bounds)) {
        return false;
    }

    return !detail::disjoint_before(lhs_bounds.upper(), rhs_bounds.lower()) &&
           !detail::disjoint_before(rhs_bounds.upper(), lhs_bounds.lower());
}

namespace detail {

template <expression ExprT>
constexpr decltype(auto) canonical_input(const ExprT& source)
{
    if constexpr (canonical_sequence<ExprT>) {
        return (source);
    }

    if constexpr (!canonical_sequence<ExprT>) {
        return interval::compile_intervals(source);
    }
}

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool canonical_intersects(const LhsT& lhs, const RhsT& rhs)
{
    decltype(auto) lhs_intervals = canonical_input(lhs);
    decltype(auto) rhs_intervals = canonical_input(rhs);
    auto lhs_it = std::begin(lhs_intervals);
    auto rhs_it = std::begin(rhs_intervals);

    while (lhs_it != std::end(lhs_intervals) && rhs_it != std::end(rhs_intervals)) {
        const auto lhs_bounds = bounds_of(*lhs_it);
        const auto rhs_bounds = bounds_of(*rhs_it);
        if (disjoint_before(lhs_bounds.upper(), rhs_bounds.lower())) {
            ++lhs_it;
            continue;
        }

        if (disjoint_before(rhs_bounds.upper(), lhs_bounds.lower())) {
            ++rhs_it;
            continue;
        }

        return true;
    }

    return false;
}

template <expression OuterT, expression InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool canonical_encloses(const OuterT& outer, const InnerT& inner)
{
    decltype(auto) outer_intervals = canonical_input(outer);
    decltype(auto) inner_intervals = canonical_input(inner);
    auto outer_it = std::begin(outer_intervals);

    const auto outer_end = std::end(outer_intervals);

    return std::ranges::all_of(inner_intervals, [&outer_it, outer_end](const auto& inner_interval) {
        while (outer_it != outer_end && definitely_before(*outer_it, inner_interval)) {
            ++outer_it;
        }

        return outer_it != outer_end && interval::encloses(*outer_it, inner_interval);
    });
}

} // detail

template <expression LhsT, expression RhsT, interval_view OtherT>
requires same_expression_domain<LhsT, OtherT>
constexpr bool intersects(const detail::set_union_expr<LhsT, RhsT>& lhs, const OtherT& rhs)
{
    return interval::intersects(lhs.lhs, rhs) || interval::intersects(lhs.rhs, rhs);
}

template <interval_view OtherT, expression LhsT, expression RhsT>
requires same_expression_domain<OtherT, LhsT>
constexpr bool intersects(const OtherT& lhs, const detail::set_union_expr<LhsT, RhsT>& rhs)
{
    return interval::intersects(lhs, rhs.lhs) || interval::intersects(lhs, rhs.rhs);
}

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool intersects(const LhsT& lhs, const RhsT& rhs)
{
    return detail::canonical_intersects(lhs, rhs);
}

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool is_disjoint(const LhsT& lhs, const RhsT& rhs)
{
    return !interval::intersects(lhs, rhs);
}

template <interval_view OuterT, interval_view InnerT>
requires same_expression_domain<OuterT, InnerT>
constexpr bool encloses(const OuterT& outer, const InnerT& inner)
{
    const auto outer_bounds = detail::bounds_of(outer);
    const auto inner_bounds = detail::bounds_of(inner);
    if (detail::empty_interval(inner, inner_bounds)) {
        return true;
    }
    if (detail::empty_interval(outer, outer_bounds)) {
        return false;
    }
    const auto lower_order = detail::compare_position(outer_bounds.lower(), inner_bounds.lower());
    const auto upper_order = detail::compare_position(outer_bounds.upper(), inner_bounds.upper());

    return (std::strong_ordering::less == lower_order ||
            (std::strong_ordering::equal == lower_order &&
             (outer_bounds.lower().inclusive() || !inner_bounds.lower().inclusive()))) &&
           (std::strong_ordering::greater == upper_order ||
            (std::strong_ordering::equal == upper_order &&
             (outer_bounds.upper().inclusive() || !inner_bounds.upper().inclusive())));
}

template <interval_view OuterT, expression LhsT, expression RhsT>
requires same_expression_domain<OuterT, LhsT>
constexpr bool encloses(const OuterT& outer, const detail::set_union_expr<LhsT, RhsT>& inner)
{
    return interval::encloses(outer, inner.lhs) && interval::encloses(outer, inner.rhs);
}

template <expression LhsT, expression RhsT>
requires same_expression_domain<LhsT, RhsT>
constexpr bool encloses(const LhsT& outer, const RhsT& inner)
{
    return detail::canonical_encloses(outer, inner);
}

// Datum: Named endpoint, prefix and cursor operations over the interval kernel.
// A convenience vocabulary above the mathematical interval representation.
//
// Please keep this layer independent of application keyspaces, backend selectors and
// execution policy.
// Put domain-specific behavior in consumer profiles or adapters.

enum struct endpoint_inclusion : std::uint8_t { excluded, included };

namespace detail {

constexpr bool includes_endpoint(const endpoint_inclusion inclusion) noexcept
{
    return endpoint_inclusion::included == inclusion;
}

template <typename DomainT, typename ValueT>
concept domain_value_source =
    ordered_domain<DomainT> && std::constructible_from<typename DomainT::value_type, ValueT>;

template <ordered_domain DomainT, typename ValueT>
requires domain_value_source<DomainT, ValueT>
constexpr auto value_for(ValueT&& value)
{
    return typename DomainT::value_type(std::forward<ValueT>(value));
}

template <ordered_domain DomainT>
constexpr auto boundary_for(const typename DomainT::value_type& value,
                            const endpoint_inclusion inclusion)
{
    using boundary_type = interval::boundary<DomainT>;

    if (includes_endpoint(inclusion)) {
        return boundary_type::closed(value);
    }

    return boundary_type::open(value);
}

template <typename T>
concept detects_empty_prefix = requires(const typename T::value_type& value) {
    { T::empty_prefix(value) } -> std::same_as<bool>;
};

template <typename EndpointT, ordered_domain DomainT, typename ValueT>
requires domain_value_source<DomainT, ValueT>
constexpr EndpointT endpoint_for(ValueT&& value, const endpoint_inclusion inclusion)
{
    return EndpointT {.value = value_for<DomainT>(std::forward<ValueT>(value)),
                      .inclusion = inclusion};
}

} // detail

template <ordered_domain DomainT>
struct lower_endpoint final
{
    using domain_type = DomainT;
    using value_type = typename DomainT::value_type;
    using boundary_type = interval::boundary<DomainT>;

    value_type value;
    endpoint_inclusion inclusion = endpoint_inclusion::included;

public:
    constexpr boundary_type boundary() const&
    {
        return detail::boundary_for<DomainT>(value, inclusion);
    }

    constexpr boundary_type boundary() &&
    {
        return detail::includes_endpoint(inclusion) ? boundary_type::closed(std::move(value))
                                                    : boundary_type::open(std::move(value));
    }

};

template <ordered_domain DomainT>
struct upper_endpoint final
{
    using domain_type = DomainT;
    using value_type = typename DomainT::value_type;
    using boundary_type = interval::boundary<DomainT>;

    value_type value;
    endpoint_inclusion inclusion = endpoint_inclusion::excluded;

public:
    constexpr boundary_type boundary() const&
    {
        return detail::boundary_for<DomainT>(value, inclusion);
    }

    constexpr boundary_type boundary() &&
    {
        return detail::includes_endpoint(inclusion) ? boundary_type::closed(std::move(value))
                                                    : boundary_type::open(std::move(value));
    }

};

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto lower_at_or_after(ValueT&& value)
{
    return detail::endpoint_for<lower_endpoint<DomainT>, DomainT>(std::forward<ValueT>(value),
                                                                  endpoint_inclusion::included);
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto lower_after(ValueT&& value)
{
    return detail::endpoint_for<lower_endpoint<DomainT>, DomainT>(std::forward<ValueT>(value),
                                                                  endpoint_inclusion::excluded);
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto upper_at_or_before(ValueT&& value)
{
    return detail::endpoint_for<upper_endpoint<DomainT>, DomainT>(std::forward<ValueT>(value),
                                                                  endpoint_inclusion::included);
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto upper_before(ValueT&& value)
{
    return detail::endpoint_for<upper_endpoint<DomainT>, DomainT>(std::forward<ValueT>(value),
                                                                  endpoint_inclusion::excluded);
}

template <ordered_domain DomainT>
constexpr auto between(lower_endpoint<DomainT> lower, upper_endpoint<DomainT> upper)
{
    return query<DomainT>::between(std::move(lower).boundary(), std::move(upper).boundary());
}

template <ordered_domain DomainT>
constexpr auto from(lower_endpoint<DomainT> lower)
{
    return query<DomainT>::between(std::move(lower).boundary(),
                                   boundary<DomainT>::positive_infinity());
}

template <ordered_domain DomainT>
constexpr auto until(upper_endpoint<DomainT> upper)
{
    return query<DomainT>::between(boundary<DomainT>::negative_infinity(),
                                   std::move(upper).boundary());
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto at(ValueT&& value)
{
    return query<DomainT>::singleton(detail::value_for<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto at_or_after(ValueT&& value)
{
    return from(lower_at_or_after<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto after(ValueT&& value)
{
    return from(lower_after<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto at_or_before(ValueT&& value)
{
    return until(upper_at_or_before<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto before(ValueT&& value)
{
    return until(upper_before<DomainT>(std::forward<ValueT>(value)));
}

template <ordered_domain DomainT, typename LowerT, typename UpperT>
requires detail::domain_value_source<DomainT, LowerT> &&
         detail::domain_value_source<DomainT, UpperT>
constexpr auto between(LowerT&& lower, UpperT&& upper)
{
    return between(lower_at_or_after<DomainT>(std::forward<LowerT>(lower)),
                   upper_before<DomainT>(std::forward<UpperT>(upper)));
}

template <ordered_domain DomainT, typename LowerT, typename UpperT>
requires detail::domain_value_source<DomainT, LowerT> &&
         detail::domain_value_source<DomainT, UpperT>
constexpr auto closed_between(LowerT&& lower, UpperT&& upper)
{
    return between(lower_at_or_after<DomainT>(std::forward<LowerT>(lower)),
                   upper_at_or_before<DomainT>(std::forward<UpperT>(upper)));
}

template <ordered_domain DomainT, typename LowerT, typename UpperT>
requires detail::domain_value_source<DomainT, LowerT> &&
         detail::domain_value_source<DomainT, UpperT>
constexpr auto open_between(LowerT&& lower, UpperT&& upper)
{
    return between(lower_after<DomainT>(std::forward<LowerT>(lower)),
                   upper_before<DomainT>(std::forward<UpperT>(upper)));
}

template <successor_domain DomainT, typename ValueT>
requires detail::domain_value_source<DomainT, ValueT>
constexpr auto prefix(ValueT&& value)
{
    auto prefix_value = detail::value_for<DomainT>(std::forward<ValueT>(value));

    if constexpr (detail::detects_empty_prefix<DomainT>) {
        if (DomainT::empty_prefix(prefix_value)) {
            return query<DomainT>::universal();
        }
    }

    return query<DomainT>::prefix(std::move(prefix_value));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto starting_at(ExprT&& expression, ValueT&& value)
{
    using domain_type = expression_domain_t<ExprT>;

    return intersection(std::forward<ExprT>(expression),
                        at_or_after<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto starting_after(ExprT&& expression, ValueT&& value)
{
    using domain_type = expression_domain_t<ExprT>;

    return intersection(std::forward<ExprT>(expression),
                        after<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto ending_at(ExprT&& expression, ValueT&& value)
{
    using domain_type = expression_domain_t<ExprT>;

    return intersection(std::forward<ExprT>(expression),
                        at_or_before<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto ending_before(ExprT&& expression, ValueT&& value)
{
    using domain_type = expression_domain_t<ExprT>;

    return intersection(std::forward<ExprT>(expression),
                        before<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto without(ExprT&& expression, ValueT&& value)
{
    using domain_type = expression_domain_t<ExprT>;

    return difference(std::forward<ExprT>(expression),
                      at<domain_type>(std::forward<ValueT>(value)));
}

template <expression ExprT, typename ValueT>
requires detail::domain_value_source<expression_domain_t<ExprT>, ValueT>
constexpr auto without_prefix(ExprT&& expression, ValueT&& value)
{
    using domain_type = expression_domain_t<ExprT>;

    return difference(std::forward<ExprT>(expression),
                      prefix<domain_type>(std::forward<ValueT>(value)));
}

template <successor_domain DomainT, typename PrefixT, typename CursorT>
requires detail::domain_value_source<DomainT, PrefixT> &&
         detail::domain_value_source<DomainT, CursorT>
constexpr auto prefix_starting_at(PrefixT&& prefix_value, CursorT&& cursor)
{
    return starting_at(prefix<DomainT>(std::forward<PrefixT>(prefix_value)),
                       std::forward<CursorT>(cursor));
}

template <successor_domain DomainT, typename PrefixT, typename CursorT>
requires detail::domain_value_source<DomainT, PrefixT> &&
         detail::domain_value_source<DomainT, CursorT>
constexpr auto prefix_starting_after(PrefixT&& prefix_value, CursorT&& cursor)
{
    return starting_after(prefix<DomainT>(std::forward<PrefixT>(prefix_value)),
                          std::forward<CursorT>(cursor));
}

} // ceph::salad::datum::interval

#endif // CEPH_RGW_DATUM_INTERVAL_QUERY_H
